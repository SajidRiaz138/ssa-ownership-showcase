package io.ssaownership.reconciler;

import io.fabric8.kubernetes.client.Watch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
class ExecutorRecoverySchedulerIT
{

    @Test
    @Timeout (10)
    void task_runs_on_executor_thread_not_inline() throws Exception
    {
        try (ExecutorRecoveryScheduler s = ExecutorRecoveryScheduler.createOwning("rec-test"))
        {
            Thread caller = Thread.currentThread();
            AtomicBoolean ranInline = new AtomicBoolean(false);
            CountDownLatch ran = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(1), () ->
            {
                if (Thread.currentThread() == caller) ranInline.set(true);
                ran.countDown();
            });
            assertTrue(ran.await(5, TimeUnit.SECONDS), "task should run");
            assertFalse(ranInline.get(), "task must NOT run on the caller thread");
        }
    }

    @Test
    @Timeout (10)
    void cancel_prevents_a_not_yet_started_task() throws Exception
    {
        // Occupy the ONLY worker with a blocking task so nothing else can start; schedule the
        // target (0 delay) and CANCEL it while the worker is still blocked; queue a marker (0
        // delay) AFTER the target; release the worker; await the marker and assert the target
        // never ran. With cancel() a no-op the target would run before the marker -> test fails.
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch workerBusy = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch blockerDone = new CountDownLatch(1);
        java.util.concurrent.Future<?> blocker = null;
        try
        {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(exec);
            // block the single worker; retain its Future so we can observe completion and surface
            // any failure, and ALWAYS release it in the finally block.
            blocker = exec.submit(() ->
            {
                try
                {
                    workerBusy.countDown();
                    if (!releaseWorker.await(5, TimeUnit.SECONDS)) throw new AssertionError("worker not released");
                }
                finally
                {
                    blockerDone.countDown();
                }
                return null;
            });
            assertTrue(workerBusy.await(5, TimeUnit.SECONDS), "worker should be occupied");

            AtomicInteger targetRuns = new AtomicInteger();
            var handle = s.schedule(Duration.ofMillis(0), targetRuns::incrementAndGet);
            handle.cancel(); // cancel while worker is blocked
            CountDownLatch markerRan = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(0), markerRan::countDown); // queued after target

            releaseWorker.countDown(); // now the queue drains in order
            assertTrue(markerRan.await(5, TimeUnit.SECONDS), "marker must run");
            assertEquals(0, targetRuns.get(), "cancelled target must NOT have run");
            assertTrue(blockerDone.await(5, TimeUnit.SECONDS), "the blocking task must complete");
            blocker.get(5, TimeUnit.SECONDS); // surface any failure from the blocking task
        }
        finally
        {
            releaseWorker.countDown(); // ensure the worker is never left parked
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS), "executor must terminate");
        }
    }

    @Test
    @Timeout (10)
    void owning_scheduler_shuts_down_its_own_executor_on_close() throws Exception
    {
        // createOwning() OWNS its executor; close() must shut that executor down (and then reject
        // further scheduling). Contrast with the shared-executor test above, where close() is a
        // no-op. try-with-resources guarantees cleanup even if an assertion fails before the
        // explicit close; a second close() is a harmless no-op (executor already shut down).
        try (ExecutorRecoveryScheduler s = ExecutorRecoveryScheduler.createOwning("owned-test"))
        {
            CountDownLatch ran = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(1), ran::countDown);
            assertTrue(ran.await(5, TimeUnit.SECONDS), "owned scheduler runs tasks before close");

            s.close(); // must shut down the OWNED executor

            // After close, scheduling must be rejected because the owned executor is shut down.
            assertThrows(RejectedExecutionException.class,
                    () -> s.schedule(Duration.ofMillis(1), () ->
                    {
                    }),
                    "an owned scheduler must reject scheduling after close() shuts its executor down");
        }
    }

    @Test
    @Timeout (10)
    void negative_delay_is_rejected()
    {
        try (ExecutorRecoveryScheduler s = ExecutorRecoveryScheduler.createOwning("rec-test"))
        {
            assertThrows(IllegalArgumentException.class,
                    () -> s.schedule(Duration.ofMillis(-1), () ->
                    {
                    }));
        }
    }

    @Test
    @Timeout (10)
    void shared_executor_is_not_shut_down_by_a_lifecycle_scheduler() throws Exception
    {
        ScheduledExecutorService shared = Executors.newSingleThreadScheduledExecutor();
        try
        {
            ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(shared); // not owned
            s.close(); // must be a no-op for a shared executor
            assertFalse(shared.isShutdown(), "a shared executor must survive scheduler.close()");
            // and it still works:
            CountDownLatch ran = new CountDownLatch(1);
            s.schedule(Duration.ofMillis(1), ran::countDown);
            assertTrue(ran.await(5, TimeUnit.SECONDS));
        }
        finally
        {
            shared.shutdownNow();
        }
    }

    @Test
    @Timeout (10)
    void executor_rejection_surfaces_as_DEGRADED_in_the_lifecycle() throws Exception
    {
        // Wire a lifecycle to a scheduler whose executor is already shut down -> schedule() throws
        // RejectedExecutionException -> lifecycle must enter DEGRADED (agreed failure path).
        ScheduledExecutorService dead = Executors.newSingleThreadScheduledExecutor();
        dead.shutdownNow();
        ExecutorRecoveryScheduler s = new ExecutorRecoveryScheduler(dead);

        final Runnable[] onClosedHolder = new Runnable[1];
        WatchFactory factory = onClosed ->
        {
            onClosedHolder[0] = onClosed;
            return (Watch) () -> onClosed.run(); // close() triggers onClosed
        };
        WatchLifecycle lc = new WatchLifecycle("deg", factory, s);
        lc.registerWatch(); // installs a watch
        // trigger a close -> onClosed -> schedule recovery -> rejected -> DEGRADED
        onClosedHolder[0].run();
        assertEquals(WatchLifecycle.Health.DEGRADED, lc.health(),
                "rejected recovery scheduling must surface DEGRADED");
        assertFalse(lc.debugHasPendingRecovery(), "no phantom pending recovery after rejection");
    }
}
