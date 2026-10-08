package io.ssaownership.reconciler;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RecoveryScheduler} backed by a {@link ScheduledExecutorService}.
 *
 * <h2>Executor ownership (deliberate)</h2>
 * This adapter does NOT own the executor it is given and never shuts it down — several
 * {@link WatchLifecycle} instances may share one executor, so one lifecycle deactivating must not
 * stop the others' recovery. If you want a dedicated, closeable scheduler, use
 * {@link #createOwning(String)} which creates (and, via {@link #close()}, shuts down) its own
 * single-thread executor.
 *
 * <h2>Contract honoured</h2>
 * <ul>
 *   <li>Tasks run on an executor thread, never inline on the scheduling caller's stack.</li>
 *   <li>{@link #schedule} returns a per-task {@link Cancellable} that cancels only that task.</li>
 *   <li>A rejected submission (executor shut down, queue bounded) throws
 *       {@link java.util.concurrent.RejectedExecutionException}, which {@link WatchLifecycle}
 *       translates into its DEGRADED failure path.</li>
 *   <li>Invalid (negative) delays are rejected with {@link IllegalArgumentException}.</li>
 * </ul>
 */
public final class ExecutorRecoveryScheduler implements RecoveryScheduler, AutoCloseable {

    private final ScheduledExecutorService executor;
    private final boolean owned;

    /** Use a shared, externally-owned executor; {@link #close()} will NOT shut it down. */
    public ExecutorRecoveryScheduler(ScheduledExecutorService executor) {
        this(executor, false);
    }

    private ExecutorRecoveryScheduler(ScheduledExecutorService executor, boolean owned) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.owned = owned;
    }

    /** Create a scheduler that OWNS a dedicated single-thread executor, shut down by {@link #close()}. */
    public static ExecutorRecoveryScheduler createOwning(String threadName) {
        ScheduledExecutorService ex = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        });
        return new ExecutorRecoveryScheduler(ex, true);
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        Objects.requireNonNull(delay, "delay");
        Objects.requireNonNull(task, "task");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative: " + delay);
        }
        // Accepts nonnegative durations whose toMillis() conversion fits in a long. A positive
        // sub-millisecond duration is submitted as 1ms (never silently 0). For durations above 1ms
        // the conversion truncates fractional milliseconds — it does NOT round every positive
        // duration upward. Actual scheduling precision and extreme-delay handling depend on the
        // backing executor; conversion overflow is rejected as IllegalArgumentException.
        long millis;
        try {
            millis = delay.toMillis();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("delay too large to represent in millis: " + delay, overflow);
        }
        if (millis == 0 && !delay.isZero()) {
            millis = 1;                       // positive sub-ms -> minimum representable, not dropped
        }
        // May throw RejectedExecutionException — intentionally propagated to the caller.
        ScheduledFuture<?> future = executor.schedule(task, millis, TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);   // best-effort; a running task is not interrupted
    }

    /** Shuts down the executor ONLY if this scheduler created it. No-op for a shared executor. */
    @Override
    public void close() {
        if (owned) {
            executor.shutdownNow();
        }
    }
}
