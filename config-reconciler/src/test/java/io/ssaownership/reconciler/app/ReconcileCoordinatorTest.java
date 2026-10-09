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
 * serialises drains. Latches signal pass ENTRY and the key interleavings; a couple of helpers
 * bounded-poll a state flag with a short sleep purely to await settlement/termination — never as
 * the correctness assertion itself, which is always a latch, a count, or a state check.
 */
class ReconcileCoordinatorTest
{

    /** Controllable store: signals each pass entry, can block a pass, can force FAILED. */
    static final class ControllableStore implements ConfigMapStore
    {
        final AtomicInteger passes = new AtomicInteger();
        volatile CountDownLatch entered; // counted down at each get() entry (if set)
        volatile CountDownLatch blockOnPass; // if set, the next get() blocks on it
        volatile boolean failNext = false;
        final AtomicReference<String> value = new AtomicReference<>(null);

        @Override
        public Optional<String> get(String cm, String key)
        {
            passes.incrementAndGet();
            CountDownLatch e = entered;
            if (e != null) e.countDown();
            CountDownLatch gate = blockOnPass;
            if (gate != null)
            {
                blockOnPass = null;
                try
                {
                    if (!gate.await(5, TimeUnit.SECONDS)) throw new AssertionError("gate");
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ie);
                }
            }
            if (failNext)
            {
                failNext = false;
                throw new StoreException("injected");
            }
            return Optional.ofNullable(value.get());
        }

        @Override
        public void upsert(String cm, String key, String v)
        {
            value.set(v);
        }

        @Override
        public boolean putIfAbsent(String cm, String key, String v)
        {
            if (value.get() == null)
            {
                value.set(v);
                return true;
            }
            return false;
        }
    }

    private ScheduledExecutorService exec;
    private ControllableStore store;
    private ReconcileCoordinator coord;

    private ReconcileCoordinator build(Duration retry, Duration resync)
    {
        exec = Executors.newSingleThreadScheduledExecutor();
        store = new ControllableStore();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "desired");
        coord = new ReconcileCoordinator(r, exec, retry, resync);
        return coord;
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if (coord != null) coord.close();
        if (exec != null)
        {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS), "executor must terminate");
        }
    }

    @Test
    @Timeout (10)
    void triggers_during_a_pass_coalesce_into_exactly_one_follow_up() throws Exception
    {
        build(Duration.ofSeconds(1), Duration.ofMinutes(10));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        store.entered = entered;
        store.blockOnPass = release; // pass 1 blocks inside get()

        coord.trigger(); // starts drain; pass 1 blocks
        assertTrue(entered.await(5, TimeUnit.SECONDS), "pass 1 must enter");
        assertTrue(coord.debugRunning());

        for (int i = 0; i < 50; i++)
            coord.trigger(); // many triggers while blocked
        assertTrue(coord.debugDirty(), "triggers during a pass set the dirty flag");

        release.countDown(); // let pass 1 finish; one follow-up runs
        awaitSettled();
        assertEquals(2, store.passes.get(), "50 triggers during a pass collapse into exactly one follow-up");
        assertFalse(coord.debugDirty());
    }

    @Test
    @Timeout (10)
    void failed_outcome_schedules_a_retry_without_a_new_trigger() throws Exception
    {
        build(Duration.ofMillis(150), Duration.ofMinutes(10));
        store.value.set("desired");
        store.failNext = true; // first pass FAILED
        coord.trigger();
        awaitPasses(2); // the retry (no new trigger) runs
        assertTrue(store.passes.get() >= 2, "FAILED must schedule a retry pass without a new trigger");
    }

    @Test
    @Timeout (10)
    void close_cancels_a_pending_retry_and_prevents_post_close_reconcile() throws Exception
    {
        // The BLOCKING correctness test: fail -> retry reserved -> close -> run the (canceled)
        // retry body DELIBERATELY -> assert NO additional reconcile pass. Correctness must come
        // from the token/closed check, not from cancellation.
        build(Duration.ofHours(1), Duration.ofMinutes(10)); // long retry so it never fires on its own
        store.failNext = true;
        CountDownLatch firstEntered = new CountDownLatch(1);
        store.entered = firstEntered;
        coord.trigger();
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS), "first pass ran");
        // wait until RETRY_PENDING is established
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!coord.debugRetryPending() && System.nanoTime() < deadline)
        {
            /* short */ Thread.yield();
        }
        assertTrue(coord.debugRetryPending(), "a retry must be pending after FAILED");
        long tokenBefore = coord.debugRetryToken();
        int passesBefore = store.passes.get();

        coord.close();
        assertTrue(coord.debugClosed());
        assertNotEquals(tokenBefore, coord.debugRetryToken(), "close must invalidate the retry token");

        // Deterministically run the retry body with its ORIGINAL (now stale) token, exactly as a
        // dequeued timer would. The token/closed check must make it a no-op — NOT reconcile.
        coord.debugRunRetry(tokenBefore);
        // And a direct trigger after close must also be a no-op:
        coord.trigger();
        assertEquals(passesBefore, store.passes.get(),
                "no reconcile pass may run after close (stale retry and trigger both declined)");
    }

    @Test
    @Timeout (10)
    void a_trigger_expedites_a_pending_retry() throws Exception
    {
        // After FAILED, a real trigger must EXPEDITE: cancel the long delayed retry and run an
        // immediate pass, not wait out the retry delay.
        build(Duration.ofHours(1), Duration.ofMinutes(10)); // retry delay effectively "never"
        store.failNext = true;
        CountDownLatch firstEntered = new CountDownLatch(1);
        store.entered = firstEntered;
        coord.trigger();
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS), "first (failing) pass ran");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!coord.debugRetryPending() && System.nanoTime() < deadline)
            Thread.yield();
        assertTrue(coord.debugRetryPending(), "retry pending after FAILED");

        CountDownLatch secondEntered = new CountDownLatch(1);
        store.entered = secondEntered;
        coord.trigger(); // must expedite, not wait the 1h delay
        assertTrue(secondEntered.await(5, TimeUnit.SECONDS),
                "a trigger must expedite the pending retry into an immediate pass");
        awaitSettled();
        assertEquals(2, store.passes.get(), "exactly the failing pass + the expedited pass (no extra)");
    }

    @Test
    @Timeout (10)
    void closed_coordinator_declines_new_work()
    {
        build(Duration.ofSeconds(1), Duration.ofMinutes(10));
        coord.close();
        assertTrue(coord.debugClosed());
        coord.trigger();
        assertFalse(coord.debugRunning(), "closed coordinator starts no drain");
        assertEquals(0, store.passes.get(), "no pass after close");
    }

    @Test
    @Timeout (10)
    void trigger_after_executor_shutdown_does_not_throw()
    {
        build(Duration.ofSeconds(1), Duration.ofMinutes(10));
        exec.shutdownNow();
        assertDoesNotThrow(coord::trigger,
                "a trigger must swallow executor rejection (never throw back into a watch callback)");
    }

    // ---- duration validation / normalization -------------------------------------------------

    @Test
    @Timeout (10)
    void durations_are_validated_and_normalized()
    {
        exec = Executors.newSingleThreadScheduledExecutor();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, new ControllableStore(), () -> "d");
        assertThrows(IllegalArgumentException.class,
                () -> new ReconcileCoordinator(r, exec, Duration.ZERO, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new ReconcileCoordinator(r, exec, Duration.ofSeconds(1), Duration.ofMillis(-1)));
        IllegalArgumentException ov = assertThrows(IllegalArgumentException.class,
                () -> new ReconcileCoordinator(r, exec, Duration.ofSeconds(Long.MAX_VALUE), Duration.ofSeconds(1)));
        assertTrue(ov.getMessage().contains("retryDelay"), "overflow names retryDelay: " + ov.getMessage());
        assertDoesNotThrow(() -> new ReconcileCoordinator(r, exec, Duration.ofNanos(1), Duration.ofNanos(1)));
    }

    @Test
    @Timeout (10)
    void sub_millisecond_resync_is_normalized_and_actually_schedules() throws Exception
    {
        exec = Executors.newSingleThreadScheduledExecutor();
        store = new ControllableStore();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "d");
        coord = new ReconcileCoordinator(r, exec, Duration.ofSeconds(10), Duration.ofNanos(1));
        CountDownLatch entered = new CountDownLatch(1);
        store.entered = entered;
        coord.startPeriodicResync(); // 0ms would be rejected; 1ms must run
        assertTrue(entered.await(5, TimeUnit.SECONDS), "normalized sub-ms resync must actually run a pass");
    }

    // ---- unexpected-rejection observer -------------------------------------------------------

    @Test
    @Timeout (10)
    void unexpected_rejection_while_open_invokes_observer_exactly_once()
    {
        exec = Executors.newSingleThreadScheduledExecutor();
        store = new ControllableStore();
        AtomicInteger observed = new AtomicInteger();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "d");
        coord = new ReconcileCoordinator(r, exec, Duration.ofSeconds(1), Duration.ofMinutes(10),
                rej -> observed.incrementAndGet());
        exec.shutdownNow(); // executor dies unexpectedly, coordinator OPEN
        coord.trigger();
        assertEquals(1, observed.get(), "unexpected rejection while open invokes the observer exactly once");
    }

    @Test
    @Timeout (10)
    void rejection_after_close_does_not_invoke_the_observer()
    {
        exec = Executors.newSingleThreadScheduledExecutor();
        store = new ControllableStore();
        AtomicInteger observed = new AtomicInteger();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "d");
        coord = new ReconcileCoordinator(r, exec, Duration.ofSeconds(1), Duration.ofMinutes(10),
                rej -> observed.incrementAndGet());
        coord.close();
        exec.shutdownNow();
        coord.trigger();
        assertEquals(0, observed.get(), "a rejection after close must not invoke the observer");
    }

    @Test
    @Timeout (10)
    void a_throwing_observer_does_not_escape_through_trigger()
    {
        exec = Executors.newSingleThreadScheduledExecutor();
        store = new ControllableStore();
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "d");
        coord = new ReconcileCoordinator(r, exec, Duration.ofSeconds(1), Duration.ofMinutes(10),
                rej ->
                {
                    throw new RuntimeException("observer blew up");
                });
        exec.shutdownNow();
        assertDoesNotThrow(coord::trigger, "a throwing observer must not escape through trigger()");
        assertDoesNotThrow(coord::trigger, "state remains consistent after an observer failure");
    }

    // ---- pending-retry handle cancellation (recording scheduled executor) --------------------

    @Test
    @Timeout (10)
    void close_cancels_the_actual_retry_future_and_a_stale_run_does_nothing() throws Exception
    {
        RecordingScheduledExecutor rec = new RecordingScheduledExecutor();
        exec = rec;
        store = new ControllableStore();
        store.failNext = true;
        Reconciler r = new Reconciler("t", "cm", "k", WriteStrategy.UPSERT, store, () -> "d");
        coord = new ReconcileCoordinator(r, rec, Duration.ofSeconds(5), Duration.ofMinutes(10));

        coord.trigger(); // execute() runs inline -> first pass FAILED
        assertTrue(coord.debugRetryPending(), "retry pending after FAILED");
        RecordingScheduledExecutor.RecordedFuture retry = rec.lastScheduled();
        assertNotNull(retry, "a retry future was scheduled");
        long token = coord.debugRetryToken();

        coord.close();
        assertTrue(retry.cancelled.get(), "close must cancel the ACTUAL retry future");
        int before = store.passes.get();
        coord.debugRunRetry(token); // run the stale retry body deliberately
        assertEquals(before, store.passes.get(), "a stale/canceled retry performs no reconcile");
    }

    /** Scheduled executor that runs execute() inline and records schedule() calls with cancelable,
     *  non-auto-running futures — so a test can assert cancellation and run stale bodies by hand. */
    static final class RecordingScheduledExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor
    {
        RecordingScheduledExecutor()
        {
            super(1);
        }

        private volatile RecordedFuture last;

        RecordedFuture lastScheduled()
        {
            return last;
        }

        @Override
        public void execute(Runnable command)
        {
            command.run();
        } // inline, deterministic

        @Override
        public java.util.concurrent.ScheduledFuture<?> schedule(
                                                                Runnable command,
                                                                long delay,
                                                                TimeUnit unit)
        {
            RecordedFuture f = new RecordedFuture();
            last = f;
            return f; // do NOT run; the test drives it
        }

        static final class RecordedFuture implements java.util.concurrent.ScheduledFuture<Object>
        {
            final java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();

            @Override
            public long getDelay(TimeUnit u)
            {
                return 0;
            }

            @Override
            public int compareTo(java.util.concurrent.Delayed o)
            {
                return 0;
            }

            @Override
            public boolean cancel(boolean i)
            {
                cancelled.set(true);
                return true;
            }

            @Override
            public boolean isCancelled()
            {
                return cancelled.get();
            }

            @Override
            public boolean isDone()
            {
                return cancelled.get();
            }

            @Override
            public Object get()
            {
                return null;
            }

            @Override
            public Object get(long t, TimeUnit u)
            {
                return null;
            }
        }
    }

    // ---- helpers ----
    private void awaitSettled() throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (coord.debugRunning() && System.nanoTime() < deadline)
            Thread.sleep(5);
        assertFalse(coord.debugRunning(), "coordinator must settle");
    }

    private void awaitPasses(int n) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (store.passes.get() < n && System.nanoTime() < deadline)
            Thread.sleep(5);
    }
}
