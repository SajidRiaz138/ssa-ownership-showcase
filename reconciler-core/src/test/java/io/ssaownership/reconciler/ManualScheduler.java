package io.ssaownership.reconciler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Manually-controlled {@link RecoveryScheduler} for deterministic tests. It does NOT run tasks on
 * a timer — the test decides exactly when (and whether) a scheduled task runs, and can even run a
 * CANCELLED task on purpose to prove the lifecycle's own eligibility check (not cancellation) is
 * what protects against stale work. No clock, no sleeps.
 */
final class ManualScheduler implements RecoveryScheduler {

    static final class Task implements Cancellable {
        final Duration delay;
        final Runnable body;
        boolean cancelled = false;
        boolean run = false;
        Task(Duration delay, Runnable body) { this.delay = delay; this.body = body; }
        @Override public void cancel() { cancelled = true; }
    }

    final List<Task> scheduled = new ArrayList<>();
    boolean rejectScheduling = false;        // when true, schedule() throws (reject path test)

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        if (rejectScheduling) {
            throw new RejectedException("scheduler is rejecting");
        }
        Task t = new Task(delay, task);
        scheduled.add(t);
        return t;
    }

    static final class RejectedException extends RuntimeException {
        RejectedException(String m) { super(m); }
    }

    /** Run the most recently scheduled, not-yet-run task regardless of cancellation. */
    void runLast() {
        Task t = scheduled.get(scheduled.size() - 1);
        t.run = true;
        t.body.run();
    }

    /** Run a specific scheduled task even if cancelled — to prove stale-task protection. */
    void runEvenIfCancelled(Task t) { t.run = true; t.body.run(); }

    /** Tasks that are neither cancelled nor already run — i.e. genuinely still pending. */
    int pendingCount() {
        return (int) scheduled.stream().filter(t -> !t.cancelled && !t.run).count();
    }
    Task last() { return scheduled.get(scheduled.size() - 1); }
}
