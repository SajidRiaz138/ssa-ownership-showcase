package io.ssaownership.reconciler.app;

import io.ssaownership.reconciler.ConfigMapStore;
import io.ssaownership.reconciler.Reconciler;
import io.ssaownership.reconciler.WriteStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic unit tests for {@link ReconcileCoordinator}. A real {@link Reconciler} is driven
 * over a controllable in-test {@link ConfigMapStore}; a real single-thread scheduled executor
 * serialises drains. Latches make interleavings explicit — no sleeps as correctness evidence.
 */
class ReconcileCoordinatorTest {

    /** Controllable store: counts passes, can block one pass, can force FAILED via a throwing get. */
    static final class ControllableStore implements ConfigMapStore {
        final AtomicInteger passes = new AtomicInteger();
        volatile CountDownLatch blockOnPass;      // if set, the next get() blocks on it
        volatile boolean failNext = false;
        final AtomicReference<String> value = new AtomicReference<>(null);

        @Override public Optional<String> get(String cm, String key) {
            passes.incrementAndGet();
            CountDownLatch gate = blockOnPass;
            if (gate != null) {
                blockOnPass = null;
                try { if (!gate.await(5, TimeUnit.SECONDS)) throw new AssertionError("gate"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            }
            if (failNext) { failNext = false; throw new StoreException("injected"); }
            return Optional.ofNullable(value.get());
        }
        @Override public void upsert(String cm, String key, String v) { value.set(v); }
        @Override public boolean putIfAbsent(String cm, String key, String v) {
            if (value.get() == null) { value.set(v); return true; } return false;
        }
    }

    private ScheduledExecutorService exec;
    private ControllableStore store;
    private ReconcileCoordinator coord;

    private ReconcileCoordinator build(Duration retry, Duration resync) {
        exec = Executors.newSingleThreadScheduledExecutor();
        store = new ControllableStore();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "desired");
        return new ReconcileCoordinator(r, exec, retry, resync);
    }

    @AfterEach
    void tearDown() {
        if (coord != null) coord.close();
        if (exec != null) { exec.shutdownNow(); }
    }

    @Test @Timeout(10)
    void triggers_during_a_pass_coalesce_into_exactly_one_follow_up() throws Exception {
        coord = build(Duration.ofSeconds(1), Duration.ofMinutes(10));
        CountDownLatch release = new CountDownLatch(1);
        store.blockOnPass = release;                       // the first pass blocks inside get()

        coord.trigger();                                   // starts drain; pass 1 blocks on release
        // wait until pass 1 is actually running (passes incremented to 1)
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (store.passes.get() < 1 && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(1, store.passes.get(), "pass 1 running");
        assertTrue(coord.debugRunning());

        // fire MANY triggers while pass 1 is blocked -> they must collapse into ONE follow-up
        for (int i = 0; i < 50; i++) coord.trigger();
        assertTrue(coord.debugDirty(), "triggers during a pass set the dirty flag");

        release.countDown();                               // let pass 1 finish; one follow-up runs
        // after the dust settles there must be exactly 2 passes total (1 + a single coalesced one)
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (coord.debugRunning() && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(2, store.passes.get(), "50 triggers during a pass collapse into exactly one follow-up");
        assertFalse(coord.debugDirty());
    }

    @Test @Timeout(10)
    void failed_outcome_schedules_a_retry_without_a_new_trigger() throws Exception {
        coord = build(Duration.ofMillis(200), Duration.ofMinutes(10));
        store.value.set("desired");       // in sync, so a pass would normally SKIP...
        store.failNext = true;            // ...but the first get() throws -> FAILED
        coord.trigger();
        // the retry (no new trigger) must produce a second pass:
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (store.passes.get() < 2 && System.nanoTime() < deadline) Thread.onSpinWait();
        assertTrue(store.passes.get() >= 2, "FAILED must schedule a retry pass without a new trigger");
    }

    @Test @Timeout(10)
    void closed_coordinator_declines_new_work() {
        coord = build(Duration.ofSeconds(1), Duration.ofMinutes(10));
        coord.close();
        assertTrue(coord.debugClosed());
        coord.trigger();                                   // must be a no-op
        assertFalse(coord.debugRunning(), "closed coordinator starts no drain");
        assertEquals(0, store.passes.get(), "no pass after close");
    }

    @Test @Timeout(10)
    void trigger_after_executor_shutdown_does_not_throw() {
        coord = build(Duration.ofSeconds(1), Duration.ofMinutes(10));
        exec.shutdownNow();                                // executor gone, coordinator not closed
        assertDoesNotThrow(coord::trigger,
                "a trigger must swallow executor rejection (never throw back into a watch callback)");
    }
}
