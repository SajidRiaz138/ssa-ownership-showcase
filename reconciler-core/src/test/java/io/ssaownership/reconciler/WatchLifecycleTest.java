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
        // gap — see authorization_gap_* for that). Single-threaded via a factory seam. The
        // explicit replacement B is registered re-entrantly from inside the recovery's create()
        // call; the stale recovery candidate must be rejected on its install attempt. We capture
        // B's identity at the moment of its registration so the assertions prove the EXPLICIT
        // winner stayed installed — not merely that "some one watch" is installed (a stale
        // recovery winning and closing B would satisfy a bare count).
        ManualScheduler s = new ManualScheduler();
        final WatchLifecycle[] ref = new WatchLifecycle[1];
        final boolean[] arm = {false};
        final TestWatch[] winnerBox = new TestWatch[1];
        Factory raceFactory = new Factory() {
            @Override public Watch create(Runnable onClosed) {
                if (arm[0]) {
                    arm[0] = false;
                    ref[0].registerWatch();                 // install newer B during recovery's create
                    winnerBox[0] = installedWatch(this, ref[0]);   // capture the explicit winner B
                }
                return super.create(onClosed);
            }
        };
        WatchLifecycle lc = new WatchLifecycle("t", raceFactory, s);
        ref[0] = lc;
        lc.registerWatch();
        installedWatch(raceFactory, lc).close();
        ManualScheduler.Task recovery = s.last();
        arm[0] = true;
        s.runEvenIfCancelled(recovery);
        assertNotNull(winnerBox[0], "the explicit replacement must have been captured");
        assertTrue(lc.debugIsInstalled(winnerBox[0]),
                "the EXPLICIT replacement B must remain installed; the stale recovery candidate must not displace it");
        assertEquals(1, raceFactory.created.stream().filter(lc::debugIsInstalled).count(),
                "exactly one installed (the explicit winner)");
        assertFalse(lc.debugHasPendingRecovery(), "no competing recovery left pending");
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
            // At the seam the reservation has been CONSUMED (authorized into an Attempt): recovery
            // no longer holds a pending reservation that an explicit registration would need to
            // cancel. This is precisely why the identity must be carried, not re-minted.
            assertFalse(lc.debugHasPendingRecovery(),
                    "authorization must have consumed the reservation before the seam");
            lc.registerWatch();                               // explicit B while R is paused pre-create
            TestWatch b = installedWatch(f, lc);
            release.countDown();                              // let R proceed to create+install-attempt
            recFut.get(5, TimeUnit.SECONDS);                  // propagate worker exceptions; await completion
            assertTrue(lc.debugIsInstalled(b), "explicit B must remain installed; stale R cannot displace it");
            assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count());
            assertFalse(lc.debugHasPendingRecovery(),
                    "the stale recovery must not leave a competing recovery scheduled");
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

    // ---- scheduler handle-publication & shutdown/supersession orderings ---------------------
    //
    // These exercise the races the lifecycle documentation explicitly claims: the handle returned
    // by schedule() can arrive AFTER the task it schedules has already run (or after the lifecycle
    // was superseded/shut down), and a scheduling REJECTION can arrive after supersession/shutdown.
    // A controllable scheduler lets the test fix the ordering. Crucially, tasks run on a REAL
    // separate thread (never inline inside schedule()), so the harness does not itself violate the
    // "never inline" scheduler contract while verifying it.

    /**
     * Controllable {@link RecoveryScheduler}: captures each scheduled task; the test runs it (on a
     * dedicated thread) and independently decides when schedule() returns its handle, so handle
     * publication can be ordered before/after task execution, supersession, or shutdown. Bookkeeping
     * is synchronized; task bodies are NEVER invoked while holding the lock.
     */
    static final class ControllableScheduler implements RecoveryScheduler {
        static final class Handle implements Cancellable {
            final AtomicInteger cancels = new AtomicInteger();
            @Override public void cancel() { cancels.incrementAndGet(); }
            boolean cancelled() { return cancels.get() > 0; }
        }
        final Object mon = new Object();
        Runnable lastTask;                       // guarded by mon
        Handle lastHandle;                       // guarded by mon
        private CountDownLatch captured = new CountDownLatch(1);  // counted down when a task is captured
        private CountDownLatch releaseSchedule;  // if set, schedule() blocks until counted down
        private boolean rejectOnRelease = false; // if set, the released schedule() throws instead of returning

        /** Arrange that the NEXT schedule() call blocks (after capturing the task) until released. */
        CountDownLatch blockNextScheduleReturn() {
            CountDownLatch l = new CountDownLatch(1);
            synchronized (mon) { releaseSchedule = l; rejectOnRelease = false; }
            return l;
        }
        /** Like {@link #blockNextScheduleReturn} but the released schedule() THROWS a rejection. */
        CountDownLatch blockThenRejectNextSchedule() {
            CountDownLatch l = new CountDownLatch(1);
            synchronized (mon) { releaseSchedule = l; rejectOnRelease = true; }
            return l;
        }

        /** Block until a task has been handed to schedule() (replaces test spin loops). */
        void awaitCaptured(long seconds) {
            CountDownLatch c;
            synchronized (mon) { c = captured; }
            try { if (!c.await(seconds, TimeUnit.SECONDS)) throw new AssertionError("no task captured in time"); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
        }

        @Override public Cancellable schedule(Duration delay, Runnable task) {
            CountDownLatch toAwait;
            boolean reject;
            Handle h = new Handle();
            synchronized (mon) {
                lastTask = task; lastHandle = h;
                toAwait = releaseSchedule; releaseSchedule = null;
                reject = rejectOnRelease; rejectOnRelease = false;
                captured.countDown();            // signal capture AFTER recording task+handle
            }
            if (toAwait != null) {
                try { if (!toAwait.await(2, TimeUnit.SECONDS)) throw new AssertionError("schedule() not released"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            }
            if (reject) throw new ManualScheduler.RejectedException("rejected on release");
            return h;
        }
        Runnable task()  { synchronized (mon) { return lastTask; } }
        Handle handle()  { synchronized (mon) { return lastHandle; } }
    }

    private Future<?> runOnThread(ExecutorService pool, Runnable r) {
        return pool.submit(() -> { r.run(); return null; });
    }

    @Test @Timeout(10)
    void recovery_runs_before_schedule_returns_its_handle_then_handle_is_cancelled() throws Exception {
        // Ordering: schedule() captures the task but BLOCKS before returning its handle. The task
        // runs to completion on another thread (consuming the reservation, installing B). Only then
        // does schedule() return. The lifecycle must observe the reservation is no longer pending
        // and CANCEL the late handle rather than record it.
        ControllableScheduler s = new ControllableScheduler();
        Factory f = new Factory();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch releaseSchedule = s.blockNextScheduleReturn();
            lc.registerWatch();                              // A
            // closing A calls scheduleRecovery, which will block inside schedule():
            Future<?> closeFut = runOnThread(pool, () -> installedWatch(f, lc).close());
            // wait until the task was captured (schedule() entered):
            s.awaitCaptured(2);
            // run the recovery task NOW, before schedule() has returned its handle:
            Future<?> runFut = runOnThread(pool, s.task());
            runFut.get(5, TimeUnit.SECONDS);                 // recovery installs B, consumes reservation
            releaseSchedule.countDown();                     // now let schedule() return the late handle
            closeFut.get(5, TimeUnit.SECONDS);
            assertTrue(s.handle().cancelled(),
                    "a handle returned after its task already ran must be cancelled, not recorded");
            assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count(), "B installed");
            assertFalse(lc.debugHasPendingRecovery());
            assertNoLeak(f, lc);
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @Timeout(10)
    void handle_returned_after_supersession_is_cancelled_and_newer_work_unaffected() throws Exception {
        // schedule() blocks before returning. While blocked, an explicit registration supersedes
        // the recovery reservation and installs B. When schedule() finally returns, the stale
        // handle must be cancelled and B must be untouched.
        ControllableScheduler s = new ControllableScheduler();
        Factory f = new Factory();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch releaseSchedule = s.blockNextScheduleReturn();
            lc.registerWatch();                              // A
            Future<?> closeFut = runOnThread(pool, () -> installedWatch(f, lc).close());
            s.awaitCaptured(2);
            lc.registerWatch();                              // explicit B supersedes the reservation
            TestWatch b = installedWatch(f, lc);
            releaseSchedule.countDown();                     // schedule() returns the now-stale handle
            closeFut.get(5, TimeUnit.SECONDS);
            assertTrue(s.handle().cancelled(), "handle for a superseded reservation must be cancelled");
            assertTrue(lc.debugIsInstalled(b), "the newer explicit B is unaffected");
            assertFalse(lc.debugHasPendingRecovery());
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @Timeout(10)
    void handle_returned_after_shutdown_is_cancelled_and_no_reactivation() throws Exception {
        ControllableScheduler s = new ControllableScheduler();
        Factory f = new Factory();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch releaseSchedule = s.blockNextScheduleReturn();
            lc.registerWatch();
            Future<?> closeFut = runOnThread(pool, () -> installedWatch(f, lc).close());
            s.awaitCaptured(2);
            lc.deactivate();                                 // shutdown while schedule() is blocked
            releaseSchedule.countDown();
            closeFut.get(5, TimeUnit.SECONDS);
            assertTrue(s.handle().cancelled(), "late handle after shutdown must be cancelled");
            assertEquals(WatchLifecycle.Health.DEACTIVATED, lc.health(), "shutdown stays terminal");
            assertFalse(lc.debugHasPendingRecovery(), "no reactivation");
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @Timeout(10)
    void scheduling_rejected_after_supersession_does_not_mark_newer_work_degraded() throws Exception {
        // The recovery's scheduleRecovery() REJECTS, but by the time the rejection surfaces an
        // explicit B has already superseded the reservation. The rejection must NOT drag the
        // (healthy) newer state into DEGRADED, because the rejected reservation is no longer the
        // pending one.
        ControllableScheduler s = new ControllableScheduler();
        Factory f = new Factory();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch releaseSchedule = s.blockThenRejectNextSchedule();  // schedule() will throw on release
            lc.registerWatch();                              // A
            Future<?> closeFut = runOnThread(pool, () -> installedWatch(f, lc).close());
            s.awaitCaptured(2);
            lc.registerWatch();                              // explicit B supersedes reservation R
            TestWatch b = installedWatch(f, lc);
            releaseSchedule.countDown();                     // schedule() now throws the rejection
            closeFut.get(5, TimeUnit.SECONDS);               // onClosed's scheduleRecovery sees the throw
            assertTrue(lc.debugIsInstalled(b), "newer explicit B stays installed");
            assertNotEquals(WatchLifecycle.Health.DEGRADED, lc.health(),
                    "a rejection for a superseded reservation must not degrade newer work");
            assertFalse(lc.debugHasPendingRecovery());
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @Timeout(10)
    void scheduling_rejected_after_shutdown_keeps_health_deactivated() throws Exception {
        // scheduleRecovery() rejects AFTER the lifecycle was deactivated: the health==DEACTIVATED
        // guard must keep it terminal, never flipping to DEGRADED.
        ControllableScheduler s = new ControllableScheduler();
        Factory f = new Factory();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch releaseSchedule = s.blockThenRejectNextSchedule();
            lc.registerWatch();
            Future<?> closeFut = runOnThread(pool, () -> installedWatch(f, lc).close());
            s.awaitCaptured(2);
            lc.deactivate();                                 // terminal, while schedule() is blocked
            releaseSchedule.countDown();                     // schedule() throws the rejection now
            closeFut.get(5, TimeUnit.SECONDS);
            assertEquals(WatchLifecycle.Health.DEACTIVATED, lc.health(),
                    "a rejection after shutdown must leave health DEACTIVATED, not DEGRADED");
            assertFalse(lc.debugHasPendingRecovery());
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @Timeout(10)
    void shutdown_while_factory_creation_blocked_closes_the_late_candidate_and_never_installs() throws Exception {
        // registerWatch()'s create() blocks inside the factory. While blocked, the lifecycle is
        // deactivated. When create() returns, the candidate must be CLOSED (cleaned up) and never
        // installed — shutdown prevents installation but not the already-dispatched factory call.
        CountDownLatch inFactory = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Factory f = new Factory() {
            @Override public Watch create(Runnable onClosed) {
                inFactory.countDown();
                try { if (!release.await(2, TimeUnit.SECONDS)) throw new AssertionError("not released"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
                return super.create(onClosed);
            }
        };
        ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> regFut = runOnThread(pool, lc::registerWatch);
            assertTrue(inFactory.await(2, TimeUnit.SECONDS), "create must be blocked in the factory");
            lc.deactivate();                                 // shutdown while candidate is being created
            release.countDown();
            regFut.get(5, TimeUnit.SECONDS);
            assertEquals(0, f.created.stream().filter(lc::debugIsInstalled).count(), "late candidate never installed");
            TestWatch candidate = f.created.iterator().next();
            assertTrue(candidate.closed(), "the late candidate must be closed (cleaned up)");
            assertEquals(WatchLifecycle.Health.DEACTIVATED, lc.health());
        } finally {
            release.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test @Timeout(10)
    void replacement_creation_fails_while_old_healthy_keeps_old_installed_and_follows_recovery_policy() throws Exception {
        // A installed and healthy. An explicit re-registration B's create() THROWS (before any
        // install). A create failure must NOT tear down the healthy A — supersession happens only
        // on a SUCCESSFUL create — and onCreateFailed must schedule recovery for the current
        // (failed) attempt. The subsequent recovery create succeeds and supersedes A.
        AtomicInteger entries = new AtomicInteger();
        Factory f = new Factory() {
            @Override public Watch create(Runnable onClosed) {
                if (entries.incrementAndGet() == 2) {        // entry 1 = A, entry 2 = B (fails), entry 3 = recovery
                    throw new IllegalStateException("boom on B's create");
                }
                return super.create(onClosed);
            }
        };
        ManualScheduler s = new ManualScheduler();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        lc.registerWatch();                                  // A (entry 1 ok)
        TestWatch a = installedWatch(f, lc);
        lc.registerWatch();                                  // B: entry 2 throws
        assertFalse(a.closed(), "a failed replacement create must not close the healthy old watch");
        assertTrue(lc.debugIsInstalled(a), "A remains installed after B's create failure");
        assertTrue(lc.debugHasPendingRecovery(), "a failed create schedules recovery (policy)");
        s.runLast();                                         // entry 3: recovery create succeeds
        assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count(), "exactly one installed");
        assertFalse(lc.debugIsInstalled(a), "recovery's new watch supersedes A");
        assertTrue(a.closed(), "A is closed when the successful replacement installs");
    }

    @Test @Timeout(10)
    void constructor_normalizes_duration_overflow_into_a_named_config_error() {
        Factory f = new Factory(); ManualScheduler s = new ManualScheduler();
        Duration overflowMillis = Duration.ofSeconds(Long.MAX_VALUE);   // toMillis() overflows
        // baseDelay overflow:
        IllegalArgumentException b = assertThrows(IllegalArgumentException.class,
                () -> new WatchLifecycle("t", f, s, overflowMillis, overflowMillis, Duration.ofSeconds(10), System::nanoTime));
        assertTrue(b.getMessage().contains("baseDelay"), "must name baseDelay: " + b.getMessage());
        // stableResetAfter overflow (nanos): valid base/max, overflowing stability window.
        Duration overflowNanos = Duration.ofSeconds(Long.MAX_VALUE / 1_000_000_000L + 1);
        IllegalArgumentException st = assertThrows(IllegalArgumentException.class,
                () -> new WatchLifecycle("t", f, s, Duration.ofSeconds(1), Duration.ofSeconds(30), overflowNanos, System::nanoTime));
        assertTrue(st.getMessage().contains("stableResetAfter"), "must name stableResetAfter: " + st.getMessage());
    }

    @Test @Timeout(10)
    void stale_handle_returning_while_a_newer_recovery_reservation_is_pending_leaves_it_intact() throws Exception {
        // The newer work is a PENDING RECOVERY RESERVATION (not an installed watch). A's recovery
        // R1 blocks in schedule() before returning its handle. R1's task runs and installs B; B
        // then closes, reserving a NEWER recovery R2 (pending, scheduled via a fresh schedule()).
        // Only then does R1's original schedule() return its now-stale handle. That stale handle
        // must be cancelled and must NOT disturb R2; running R2 must still install a watch.
        ControllableScheduler s = new ControllableScheduler();
        Factory f = new Factory();
        WatchLifecycle lc = new WatchLifecycle("t", f, s);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch release1 = s.blockNextScheduleReturn();   // R1's schedule() will block
        try {
            lc.registerWatch();                                  // A
            Future<?> close1 = runOnThread(pool, () -> installedWatch(f, lc).close());  // -> R1 (schedule blocks)
            s.awaitCaptured(2);
            Runnable r1Task = s.task();
            ControllableScheduler.Handle r1Handle = s.handle();

            // Run R1's task now (before its handle returns): installs B.
            runOnThread(pool, r1Task).get(5, TimeUnit.SECONDS);
            TestWatch b = installedWatch(f, lc);

            // B closes -> R2 reserved and scheduled on a FRESH schedule() (not blocked).
            b.close();
            assertTrue(lc.debugHasPendingRecovery(), "a newer recovery reservation R2 is pending");
            ControllableScheduler.Handle r2Handle = s.handle();
            Runnable r2Task = s.task();
            assertNotSame(r1Handle, r2Handle, "R2 has its own handle");

            // Now let R1's ORIGINAL schedule() return its stale handle.
            release1.countDown();
            close1.get(5, TimeUnit.SECONDS);
            assertTrue(r1Handle.cancelled(), "the stale R1 handle must be cancelled");
            assertFalse(r2Handle.cancelled(), "R2's handle must NOT be cancelled");
            assertTrue(lc.debugHasPendingRecovery(), "R2 remains pending and intact");

            // Running R2 must still install a watch (the newer task executes successfully).
            runOnThread(pool, r2Task).get(5, TimeUnit.SECONDS);
            assertEquals(1, f.created.stream().filter(lc::debugIsInstalled).count(), "R2 installs a watch");
            assertNoLeak(f, lc);
        } finally {
            release1.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
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
