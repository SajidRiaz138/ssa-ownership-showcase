package io.ssaownership.reconciler;

import java.time.Duration;

/**
 * Delayed, cancellable execution — the ONLY thing {@link WatchLifecycle} needs from
 * infrastructure for recovery. Deliberately dumb: it knows nothing about generations, backoff,
 * eligibility, or coalescing. All of that policy lives in the lifecycle; the scheduler only runs
 * a task after a delay and lets the caller cancel that particular task.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #schedule} runs {@code task} after approximately {@code delay}, on an EXECUTOR
 *       thread — never inline on the scheduling caller's stack. (Executor ownership is
 *       implementation-specific; a shared executor remains externally owned.)</li>
 *   <li>The returned {@link Cancellable#cancel()} is <b>best-effort</b>: a task already running
 *       cannot be un-run. The lifecycle's own eligibility check (is this recovery still the
 *       current reservation of an active lifecycle?) is the correctness mechanism; cancellation
 *       is an optimisation.</li>
 *   <li><b>Required backing-executor behaviour:</b> submissions execute asynchronously, and a
 *       submission that cannot be accepted MUST throw
 *       {@link java.util.concurrent.RejectedExecutionException} (or another runtime exception),
 *       so {@link WatchLifecycle} can surface it as DEGRADED. An executor configured with a
 *       SILENTLY-DISCARDING rejection policy violates this contract: the lifecycle would receive
 *       a handle for a task that never runs instead of an observable rejection. Use the default
 *       (throwing) rejection policy.</li>
 *   <li><b>Delay range/precision:</b> delays are honoured at MILLISECOND precision. A positive
 *       sub-millisecond delay is treated as the minimum representable delay (not zero/not
 *       dropped) by well-behaved implementations; a negative delay is rejected; excessively large
 *       delays beyond the implementation's representable range are rejected. Implementations
 *       document their exact supported range.</li>
 * </ul>
 */
public interface RecoveryScheduler
{

    Cancellable schedule(Duration delay, Runnable task);

    interface Cancellable
    {
        /** Best-effort cancel. A task that has already started may still complete. */
        void cancel();
    }
}
