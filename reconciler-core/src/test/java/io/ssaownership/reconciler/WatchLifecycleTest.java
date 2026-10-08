package io.ssaownership.reconciler;

import io.fabric8.kubernetes.client.Watch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic tests for {@link WatchLifecycle}. A manually-controlled {@link ManualScheduler}
 * makes recovery timing explicit (no clock, no sleeps). Threaded tests use latches with CHECKED
 * waits, release gates in finally, and propagate worker exceptions through retained Futures, so a
 * green test cannot silently have skipped its intended interleaving. Timeouts are deadlock guards.
 */
class WatchLifecycleTest {

    /** A watch with identity that fires its bound onClosed exactly once on close. */
    static final class TestWatch implements Watch {
        final int id;
        final Runnable onClosed;
        final AtomicInteger closes = new AtomicInteger();
        TestWatch(int id, Runnable onClosed) { this.id = id; this.onClosed = onClosed; }
        @Override public void close() { if (closes.incrementAndGet() == 1) onClosed.run(); }
        boolean closed() { return closes.get() > 0; }
        @Override public String toString() { return "w#" + id; }
    }

    /** Factory recording every watch it makes. Subclassable for interleaving seams. */
    static class Factory implements WatchFactory {
        final Set<TestWatch> created = ConcurrentHashMap.newKeySet();
        final AtomicInteger seq = new AtomicInteger();
        volatile boolean closeImmediately = false;
        @Override public Watch create(Runnable onClosed) {
            TestWatch w = new TestWatch(seq.incrementAndGet(), onClosed);
            created.add(w);
            if (closeImmediately) w.close();
            return w;
        }
    }

    private TestWatch installedWatch(Factory f, WatchLifecycle lc) {
        return f.created.stream().filter(lc::debugIsInstalled).findFirst().orElseThrow();
    }
    private void assertNoLeak(Factory f, WatchLifecycle lc) {
        for (TestWatch w : f.created) {
            boolean inst = lc.debugIsInstalled(w);
            assertTrue(inst || w.closed(), w + " leaked (neither installed nor closed)");
            if (inst) assertFalse(w.closed(), w + " installed yet closed");
        }
    }

    // ---- basic lifecycle --------------------------------------------------------------------

    @Test @Timeout(10)
    void install_then_deactivate_closes_installed() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        TestWatch w = installedWatch(f, lc);
        lc.deactivate();
        assertEquals(WatchLifecycle.Health.DEACTIVATED, lc.health());
        assertTrue(w.closed(), "installed watch closed on deactivate");
        assertNoLeak(f, lc);
    }

    @Test @Timeout(10)
    void deactivate_cancels_a_pending_recovery() {
        // Create pending recovery (close the installed watch), THEN deactivate, and assert the
        // scheduled recovery task was cancelled (not merely ineligible).
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        installedWatch(f, lc).close();                 // schedules recovery
        ManualScheduler.Task recovery = s.last();
        assertFalse(recovery.cancelled, "precondition: recovery not yet cancelled");
        lc.deactivate();
        assertTrue(recovery.cancelled, "deactivate must cancel the pending recovery handle");
    }

    @Test @Timeout(10)
    void close_before_install_is_handled_without_a_probe_and_schedules_recovery() {
        Factory f = new Factory(); f.closeImmediately = true;
        ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        assertEquals(0, f.created.stream().filter(lc::debugIsInstalled).count(),
                "a watch that closed before install must not be installed");
        assertTrue(lc.debugHasPendingRecovery(), "close-before-install must schedule recovery");
        assertNoLeak(f, lc);
    }

    @Test @Timeout(10)
    void installed_watch_close_schedules_recovery_queued_not_inline() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        int before = f.created.size();
        installedWatch(f, lc).close();
        assertEquals(before, f.created.size(), "recovery must NOT create a watch inline");
        assertEquals(1, s.pendingCount(), "exactly one recovery queued");
        s.runLast();
        assertEquals(before + 1, f.created.size(), "recovery creates the replacement");
        assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count());
        assertNoLeak(f, lc);
    }

    @Test @Timeout(10)
    void stale_recovery_task_run_after_deactivate_creates_no_watch_and_was_cancelled() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        installedWatch(f, lc).close();
        ManualScheduler.Task recovery = s.last();
        lc.deactivate();
        assertTrue(recovery.cancelled, "deactivate cancels the task (cancellation DID occur)");
        int before = f.created.size();
        s.runEvenIfCancelled(recovery);                // eligibility must still protect us
        assertEquals(before, f.created.size(), "stale recovery after deactivate creates no watch");
    }

    @Test @Timeout(10)
    void duplicate_close_callbacks_do_not_create_duplicate_recovery() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        TestWatch w = installedWatch(f, lc);
        w.close();                                     // schedules recovery
        w.onClosed.run();                              // stale duplicate for the same (old) gen
        assertEquals(1, s.pendingCount(), "duplicate close must not double-schedule recovery");
    }

    @Test @Timeout(10)
    void repeated_immediate_close_does_not_recurse_or_spin() {
        Factory f = new Factory(); f.closeImmediately = true;
        ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        assertEquals(1, s.pendingCount());
        s.runLast();
        s.runLast();
        assertEquals(3, f.created.size(), "exactly one watch per recovery; no recursion");
        assertEquals(1, s.pendingCount(), "exactly one recovery pending at a time");
        assertTrue(lc.debugHasPendingRecovery());
    }

    // ---- backoff ----------------------------------------------------------------------------

    @Test @Timeout(10)
    void slow_recovery_latency_does_not_reset_the_streak() {
        long[] clock = {0L};
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s,
                Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(10), () -> clock[0]);
        lc.registerWatch();                                   // install A at t=0
        installedWatch(f, lc).close();                        // A lived 0
        assertEquals(1, lc.debugConsecutiveFailures());
        assertEquals(Duration.ofSeconds(1), s.last().delay, "first delay is base 1s");
        clock[0] = Duration.ofSeconds(40).toNanos();          // recovery latency; nothing alive
        s.runLast();                                          // install B at t=40s
        assertEquals(1, lc.debugConsecutiveFailures(), "recovery latency must NOT reset the streak");
        installedWatch(f, lc).close();                        // B lived 0
        assertEquals(2, lc.debugConsecutiveFailures());
        assertEquals(Duration.ofSeconds(2), s.last().delay, "second delay is 2s (exponential)");
    }

    @Test @Timeout(10)
    void genuinely_stable_watch_resets_the_failure_streak() {
        long[] clock = {0L};
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s,
                Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(10), () -> clock[0]);
        lc.registerWatch();
        installedWatch(f, lc).close();                        // streak 1
        s.runLast();                                          // reinstall B
        assertEquals(1, lc.debugConsecutiveFailures());
        clock[0] += Duration.ofSeconds(30).toNanos();         // B lives 30s (> 10s threshold)
        installedWatch(f, lc).close();
        assertEquals(1, lc.debugConsecutiveFailures(), "stable B resets streak; the close re-increments to 1");
    }

    // ---- supersession / recovery races ------------------------------------------------------

    @Test @Timeout(10)
    void pending_recovery_is_invalidated_by_an_explicit_install() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();                                   // A
        installedWatch(f, lc).close();                        // queue recovery R
        ManualScheduler.Task r = s.last();
        lc.registerWatch();                                   // explicit B invalidates R
        assertTrue(r.cancelled, "explicit install must cancel the detached recovery handle");
        int before = f.created.size();
        TestWatch b = installedWatch(f, lc);
        s.runEvenIfCancelled(r);                              // run the stale recovery deliberately
        assertEquals(before, f.created.size(), "stale pending recovery must create no watch");
        assertTrue(lc.debugIsInstalled(b), "explicit winner B remains installed");
        assertFalse(lc.debugHasPendingRecovery());
    }

    @Test @Timeout(10)
    void during_creation_supersession_rejects_and_cleans_up_the_stale_candidate() {
        // Supersession that happens WHILE recovery is INSIDE the factory (not the pre-creation
        // gap — see authorization_gap_* for that). Single-threaded via a factory seam.
        ManualScheduler s = new ManualScheduler();
        final WatchLifecycle[] ref = new WatchLifecycle[1];
        final boolean[] arm = {false};
        Factory raceFactory = new Factory() {
            @Override public Watch create(Runnable onClosed) {
                if (arm[0]) { arm[0] = false; ref[0].registerWatch(); }  // install newer during create
                return super.create(onClosed);
            }
        };
        WatchLifecycle lc = new WatchLifecycle("t", raceFactory, s);
        ref[0] = lc;
        lc.registerWatch();
        installedWatch(raceFactory, lc).close();
        ManualScheduler.Task recovery = s.last();
        TestWatch[] winnerBox = new TestWatch[1];
        arm[0] = true;
        s.runEvenIfCancelled(recovery);
        assertEquals(1, raceFactory.created.stream().filter(lc::debugIsInstalled).count(),
                "exactly one installed (the newer winner)");
        assertNoLeak(raceFactory, lc);
    }

    @Test @Timeout(10)
    void authorization_gap_recovery_superseded_before_creation_dispatch() throws Exception {
        // THE pre-creation authorization gap: recovery consumes its reservation and authorizes R,
        // then PAUSES at the test seam (OUTSIDE the factory, before creation dispatch). An explicit
        // registration installs B. Recovery resumes with its ORIGINAL identity. B must remain; the
        // stale recovery must not install or displace it. This is the interleaving the old
        // re-minting implementation got wrong.
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        CountDownLatch atSeam = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // seam runs on the recovery thread, outside the lock, after authorize, before create:
        Runnable seam = () -> {
            atSeam.countDown();
            try {
                if (!release.await(2, TimeUnit.SECONDS)) throw new AssertionError("seam not released");
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
        };
        WatchLifecycle lc = new WatchLifecycle("t", f, s,
                Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(60),
                System::nanoTime, seam);
        lc.registerWatch();                                   // A
        installedWatch(f, lc).close();                        // queue recovery R
        ManualScheduler.Task recovery = s.last();

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> recFut = pool.submit(() -> { s.runEvenIfCancelled(recovery); return null; });
        try {
            assertTrue(atSeam.await(2, TimeUnit.SECONDS), "recovery must reach the seam (authorized, pre-create)");
            lc.registerWatch();                               // explicit B while R is paused pre-create
            TestWatch b = installedWatch(f, lc);
            release.countDown();                              // let R proceed to create+install-attempt
            recFut.get(5, TimeUnit.SECONDS);                  // propagate worker exceptions; await completion
            assertTrue(lc.debugIsInstalled(b), "explicit B must remain installed; stale R cannot displace it");
            assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count());
            assertNoLeak(f, lc);
        } finally {
            release.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "worker must terminate");
        }
    }

    @Test @Timeout(10)
    void old_watch_closes_while_replacement_blocked_in_factory() throws Exception {
        // A installed. Explicit re-registration B blocks INSIDE the factory. While B is in flight,
        // A closes. Assert the INTERMEDIATE transition: A detached, no pending recovery, nothing
        // queued — the in-flight authorized B must not be superseded. Then release B and assert B
        // installs.
        CountDownLatch bInFactory = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        AtomicInteger createCount = new AtomicInteger();
        Factory f = new Factory() {
            @Override public Watch create(Runnable onClosed) {
                if (createCount.incrementAndGet() == 2) {     // B is the 2nd create
                    bInFactory.countDown();
                    try {
                        if (!releaseB.await(2, TimeUnit.SECONDS)) throw new AssertionError("B not released");
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
                }
                return super.create(onClosed);
            }
        };
        ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();                                   // A (create #1)
        TestWatch a = installedWatch(f, lc);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> bFut = pool.submit(() -> { lc.registerWatch(); return null; });  // create #2 blocks
        try {
            assertTrue(bInFactory.await(2, TimeUnit.SECONDS), "B must be blocked in the factory");
            a.close();                                        // old installed A closes while B in flight
            // INTERMEDIATE assertions (B still blocked):
            assertFalse(lc.debugIsInstalled(a), "A detached");
            assertFalse(lc.debugHasPendingRecovery(), "no recovery while authorized B in flight");
            assertEquals(0, s.pendingCount(), "A's close must not queue a competing recovery");
            releaseB.countDown();
            bFut.get(5, TimeUnit.SECONDS);                    // propagate exceptions; await
            assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count(), "B installs");
        } finally {
            releaseB.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "worker must terminate");
        }
    }

    @Test @Timeout(10)
    void rejected_scheduling_enters_DEGRADED_not_silent_pending_forever() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();
        s.rejectScheduling = true;
        installedWatch(f, lc).close();
        assertEquals(WatchLifecycle.Health.DEGRADED, lc.health(),
                "rejected recovery scheduling must surface DEGRADED");
        assertFalse(lc.debugHasPendingRecovery(), "no phantom pending recovery after rejection");
    }

    // ---- concurrency ------------------------------------------------------------------------

    @Test @Timeout(15)
    void concurrent_registration_installs_exactly_one_no_leak() throws Exception {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        int threads = 8;
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) futures.add(pool.submit(() -> { start.await(); lc.registerWatch(); return null; }));
            for (Future<?> fut : futures) fut.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
        assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count());
        assertNoLeak(f, lc);
    }
}
