package io.ssaownership.reconciler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Manually-controlled {@link RecoveryScheduler} for deterministic tests. It does NOT run tasks on
 * a timer — the test decides exactly when (and whether) a scheduled task runs, and can even run a
 * CANCELLED task on purpose to prove the lifecycle's own eligibility check (not cancellation) is
 * what protects against stale work. No clock, no sleeps.
 *
 * <h2>Threading restriction</h2>
 * This fake is intended for SINGLE-THREADED driving: the test thread calls {@code schedule} and
 * the {@code run*} methods. Its bookkeeping is NOT synchronised and must not be driven
 * concurrently. Tests that need a scheduler across threads (handle-publication / rejection
 * orderings) use {@code ControllableScheduler}, which is synchronised. In all cases a task BODY is
 * executed WITHOUT holding any bookkeeping lock, so a task that re-enters the lifecycle cannot
 * deadlock against the scheduler.
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

    /** Run the most recently scheduled task that is NOT yet run, regardless of cancellation. */
    void runLast() {
        Task t = null;
        for (int i = scheduled.size() - 1; i >= 0; i--) {
            if (!scheduled.get(i).run) { t = scheduled.get(i); break; }
        }
        if (t == null) throw new AssertionError("no not-yet-run task to run");
        t.run = true;
        t.body.run();                         // body runs with no lock held
    }

    /** Run a specific scheduled task even if cancelled — to prove stale-task protection. */
    void runEvenIfCancelled(Task t) { t.run = true; t.body.run(); }

    /** Tasks that are neither cancelled nor already run — i.e. genuinely still pending. */
    int pendingCount() {
        return (int) scheduled.stream().filter(t -> !t.cancelled && !t.run).count();
    }
    Task last() { return scheduled.get(scheduled.size() - 1); }
}
