package io.ssaownership.reconciler;

import io.fabric8.kubernetes.client.Watch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the REAL {@link ExecutorRecoveryScheduler}. These use bounded waits
 * (latches with timeouts) to observe that a task actually ran — never sleeps as correctness
 * evidence. They verify scheduler mechanics and its wiring to {@link WatchLifecycle}.
 */
class ExecutorRecoverySchedulerIT {

    @Test @Timeout(10)
    void task_runs_on_executor_thread_not_inline() throws Exception {
        try (ExecutorRecoveryScheduler s = ExecutorRecoveryScheduler.createOwning("rec-test")) {
            Thread caller = Thread.currentThread();
            AtomicBoolean ranInline = new AtomicBoolean(false);
            CountDownLatch ran = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(1), () -> {
                if (Thread.currentThread() == caller) ranInline.set(true);
                ran.countDown();
            });
            assertTrue(ran.await(5, TimeUnit.SECONDS), "task should run");
            assertFalse(ranInline.get(), "task must NOT run on the caller thread");
        }
    }

    @Test @Timeout(10)
    void cancel_prevents_a_not_yet_started_task() throws Exception {
        // Occupy the ONLY worker with a blocking task so nothing else can start; schedule the
        // target (0 delay) and CANCEL it while the worker is still blocked; queue a marker (0
        // delay) AFTER the target; release the worker; await the marker and assert the target
        // never ran. With cancel() a no-op the target would run before the marker -> test fails.
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        try {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(exec);
            CountDownLatch workerBusy = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            // block the single worker
            exec.submit(() -> {
                workerBusy.countDown();
                releaseWorker.await();
                return null;
            });
            assertTrue(workerBusy.await(5, TimeUnit.SECONDS), "worker should be occupied");

            AtomicInteger targetRuns = new AtomicInteger();
            var handle = s.schedule(Duration.ofMillis(0), targetRuns::incrementAndGet);
            handle.cancel();                               // cancel while worker is blocked
            CountDownLatch markerRan = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(0), markerRan::countDown);  // queued after target

            releaseWorker.countDown();                     // now the queue drains in order
            assertTrue(markerRan.await(5, TimeUnit.SECONDS), "marker must run");
            assertEquals(0, targetRuns.get(), "cancelled target must NOT have run");
        } finally {
            exec.shutdownNow();
        }
    }

    @Test @Timeout(10)
    void negative_delay_is_rejected() {
        try (ExecutorRecoveryScheduler s = ExecutorRecoveryScheduler.createOwning("rec-test")) {
            assertThrows(IllegalArgumentException.class,
                    () -> s.schedule(Duration.ofMillis(-1), () -> {}));
        }
    }

    @Test @Timeout(10)
    void shared_executor_is_not_shut_down_by_a_lifecycle_scheduler() throws Exception {
        ScheduledExecutorService shared = Executors.newSingleThreadScheduledExecutor();
        try {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(shared);  // not owned
            s.close();                                   // must be a no-op for a shared executor
            assertFalse(shared.isShutdown(), "a shared executor must survive scheduler.close()");
            // and it still works:
            CountDownLatch ran = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(1), ran::countDown);
            assertTrue(ran.await(5, TimeUnit.SECONDS));
        } finally {
            shared.shutdownNow();
        }
    }

    @Test @Timeout(10)
    void executor_rejection_surfaces_as_DEGRADED_in_the_lifecycle() throws Exception {
        // Wire a lifecycle to a scheduler whose executor is already shut down -> schedule() throws
        // RejectedExecutionException -> lifecycle must enter DEGRADED (agreed failure path).
        ScheduledExecutorService dead = Executors.newSingleThreadScheduledExecutor();
        dead.shutdownNow();
        ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(dead);

        final Runnable[] onClosedHolder = new Runnable[1];
        WatchFactory factory = onClosed -> {
            onClosedHolder[0] = onClosed;
            return (Watch) () -> onClosed.run();         // close() triggers onClosed
        };
        WatchLifecycle lc = new WatchLifecycle("deg", factory, s);
        lc.registerWatch();                              // installs a watch
        // trigger a close -> onClosed -> schedule recovery -> rejected -> DEGRADED
        onClosedHolder[0].run();
        assertEquals(WatchLifecycle.Health.DEGRADED, lc.health(),
                "rejected recovery scheduling must surface DEGRADED");
        assertFalse(lc.debugHasPendingRecovery(), "no phantom pending recovery after rejection");
    }
}
