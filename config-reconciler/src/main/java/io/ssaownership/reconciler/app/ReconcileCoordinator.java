package io.ssaownership.reconciler.app;

import io.ssaownership.reconciler.Reconciler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Runtime coordination for reconcile passes. It sits between event sources (a watch, a periodic
 * timer, a startup trigger) and the policy engine ({@link Reconciler}), providing the properties
 * the engine deliberately does NOT: trigger coalescing, retry of failed passes, periodic resync,
 * and a terminal closed state. {@link Reconciler} itself is unchanged.
 *
 * <h2>Explicit state model</h2>
 * One lock guards a small state machine — chosen over a pile of booleans so retry/close/trigger
 * interactions are easy to reason about:
 * <pre>
 *   IDLE          nothing in flight
 *   RUNNING       a drain is executing (possibly with a queued immediate follow-up: {@code dirty})
 *   RETRY_PENDING a delayed retry is scheduled after a FAILED pass; identified by {@code retryToken}
 *   CLOSED        terminal
 * </pre>
 *
 * <h2>Guarantees</h2>
 * <ul>
 *   <li><b>At most one drain in flight.</b> Triggers during a RUNNING drain set {@code dirty};
 *       exactly one follow-up pass runs afterwards. N triggers collapse into one — real coalescing.</li>
 *   <li><b>Bounded queue.</b> At most one drain task is queued on the executor at a time.</li>
 *   <li><b>Retry after FAILED.</b> A FAILED pass moves to RETRY_PENDING and schedules a delayed
 *       retry carrying a {@code retryToken}; the retry runs ONLY if its token is still current and
 *       the coordinator is open.</li>
 *   <li><b>A trigger expedites a pending retry.</b> A trigger during RETRY_PENDING invalidates the
 *       token (so the old timer is a no-op even if it already fired), cancels the handle, and
 *       starts an immediate drain — the event is not made to wait out the retry delay.</li>
 *   <li><b>Periodic resync.</b> A timer triggers a pass every interval — a fallback for events
 *       missed during a disconnection and for destination-only drift.</li>
 *   <li><b>Closed is terminal and SOUND.</b> {@link #close()} invalidates the retry token and
 *       cancels both the retry and resync handles (outside the lock). A retry that already fired
 *       or was dequeued finds the token stale / state CLOSED and does NOT call the engine.
 *       Correctness comes from the token/closed check, not from cancellation.</li>
 * </ul>
 */
public final class ReconcileCoordinator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReconcileCoordinator.class);

    private enum State { IDLE, RUNNING, RETRY_PENDING, CLOSED }

    private final Reconciler reconciler;
    private final ScheduledExecutorService executor;
    private final long retryDelayMs;
    private final long resyncIntervalMs;
    /** Observes an UNEXPECTED executor rejection while open (a liveness failure), distinct from an
     *  expected rejection during shutdown. Never invoked for close-time rejection. */
    private final Consumer<RejectedExecutionException> onUnexpectedRejection;

    private final ReentrantLock lock = new ReentrantLock();
    private State state = State.IDLE;
    private boolean dirty = false;            // a trigger arrived while RUNNING
    private long retryToken = 0;              // identifies the current RETRY_PENDING reservation
    private ScheduledFuture<?> retryHandle;   // the pending retry's handle (guarded)
    private ScheduledFuture<?> resyncHandle;  // periodic resync handle (guarded)

    public ReconcileCoordinator(Reconciler reconciler, ScheduledExecutorService executor,
                                Duration retryDelay, Duration resyncInterval) {
        this(reconciler, executor, retryDelay, resyncInterval, rej ->
                LOG.error("reconcile coordinator: unexpected executor rejection while open "
                        + "(controller is alive but may be inert)", rej));
    }

    public ReconcileCoordinator(Reconciler reconciler, ScheduledExecutorService executor,
                                Duration retryDelay, Duration resyncInterval,
                                Consumer<RejectedExecutionException> onUnexpectedRejection) {
        this.reconciler = Objects.requireNonNull(reconciler, "reconciler");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.onUnexpectedRejection = Objects.requireNonNull(onUnexpectedRejection, "onUnexpectedRejection");
        this.retryDelayMs = normalizeMillis(retryDelay, "retryDelay");
        this.resyncIntervalMs = normalizeMillis(resyncInterval, "resyncInterval");
    }

    /**
     * Normalise a configured duration to a positive millisecond value: reject null/non-positive,
     * map a positive sub-millisecond value to 1ms (never 0 — a 0 period is rejected by
     * {@code scheduleWithFixedDelay}), and turn conversion overflow into a parameter-named
     * {@link IllegalArgumentException}. Accepted precision is milliseconds.
     */
    private static long normalizeMillis(Duration d, String param) {
        Objects.requireNonNull(d, param);
        if (d.isNegative() || d.isZero()) throw new IllegalArgumentException(param + " must be positive");
        final long ms;
        try { ms = d.toMillis(); }
        catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(param + " too large to represent in milliseconds: " + d, overflow);
        }
        return Math.max(1, ms);     // positive sub-ms -> 1ms
    }

    /** Start periodic resync. Call once after construction. */
    public void startPeriodicResync() {
        lock.lock();
        try {
            if (state == State.CLOSED || resyncHandle != null) return;
            resyncHandle = executor.scheduleWithFixedDelay(this::trigger,
                    resyncIntervalMs, resyncIntervalMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rej) {
            handleRejection(rej);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Request a reconcile. Shutdown-aware: a no-op once closed; never throws
     * {@link RejectedExecutionException} back to the caller (e.g. a fabric8 watch callback).
     * <ul>
     *   <li>IDLE → start a drain.</li>
     *   <li>RUNNING → mark {@code dirty}; exactly one follow-up pass will run.</li>
     *   <li>RETRY_PENDING → EXPEDITE: invalidate the retry token, cancel its handle (outside the
     *       lock), and start an immediate drain instead of waiting out the delay.</li>
     * </ul>
     */
    public void trigger() {
        ScheduledFuture<?> toCancel = null;
        boolean submit = false;
        lock.lock();
        try {
            switch (state) {
                case CLOSED -> { return; }
                case RUNNING -> { dirty = true; return; }
                case RETRY_PENDING -> {
                    retryToken++;                 // invalidate the pending retry
                    toCancel = retryHandle;
                    retryHandle = null;
                    state = State.RUNNING;
                    submit = true;
                }
                case IDLE -> { state = State.RUNNING; submit = true; }
            }
        } finally {
            lock.unlock();
        }
        cancelOutsideLock(toCancel);              // expedite: drop the delayed retry
        if (submit) submitDrain();
    }

    private void submitDrain() {
        try {
            executor.execute(this::drain);
        } catch (RejectedExecutionException rej) {
            lock.lock();
            try {
                if (state != State.CLOSED) { state = State.IDLE; handleRejection(rej); }
            } finally {
                lock.unlock();
            }
        }
    }

    private void drain() {
        // Guard BEFORE touching the engine: a dequeued drain/retry must not reconcile after close.
        lock.lock();
        try {
            if (state == State.CLOSED) return;
        } finally {
            lock.unlock();
        }

        Reconciler.Outcome outcome;
        try {
            outcome = reconciler.reconcile();
        } catch (RuntimeException e) {
            outcome = Reconciler.Outcome.FAILED;
            LOG.error("reconcile pass threw unexpectedly", e);
        }

        boolean rerunNow = false;
        boolean scheduleRetry = false;
        long tokenForRetry = 0;
        lock.lock();
        try {
            if (state == State.CLOSED) return;
            if (dirty) {
                dirty = false;
                rerunNow = true;                  // coalesced follow-up; stay RUNNING
            } else if (outcome == Reconciler.Outcome.FAILED) {
                state = State.RETRY_PENDING;
                tokenForRetry = ++retryToken;
                scheduleRetry = true;
            } else {
                state = State.IDLE;               // settled
            }
        } finally {
            lock.unlock();
        }

        if (rerunNow) {
            submitDrain();
        } else if (scheduleRetry) {
            scheduleRetry(tokenForRetry);
        }
    }

    private void scheduleRetry(long token) {
        final ScheduledFuture<?> handle;
        try {
            handle = executor.schedule(() -> runRetry(token), retryDelayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rej) {
            lock.lock();
            try {
                if (state == State.RETRY_PENDING && retryToken == token) {
                    state = State.IDLE;
                    handleRejection(rej);
                }
            } finally {
                lock.unlock();
            }
            return;
        }
        boolean stale;
        lock.lock();
        try {
            // A trigger/close may have superseded this reservation while we were scheduling.
            stale = (state != State.RETRY_PENDING) || (retryToken != token);
            if (!stale) retryHandle = handle;
        } finally {
            lock.unlock();
        }
        if (stale) cancelOutsideLock(handle);
    }

    /** The delayed retry body. Runs only if its token is still current AND not closed. */
    private void runRetry(long token) {
        lock.lock();
        try {
            if (state != State.RETRY_PENDING || retryToken != token) return;  // superseded/closed
            state = State.RUNNING;
            retryHandle = null;
        } finally {
            lock.unlock();
        }
        drain();
    }

    /** Must be called while holding the lock (open state only). */
    private void handleRejection(RejectedExecutionException rej) {
        onUnexpectedRejection.accept(rej);
    }

    @Override
    public void close() {
        ScheduledFuture<?> retry;
        ScheduledFuture<?> resync;
        lock.lock();
        try {
            state = State.CLOSED;
            retryToken++;                         // invalidate any in-flight retry reservation
            retry = retryHandle; retryHandle = null;
            resync = resyncHandle; resyncHandle = null;
        } finally {
            lock.unlock();
        }
        cancelOutsideLock(retry);                 // Invariant: cancel OUTSIDE the lock
        cancelOutsideLock(resync);
    }

    private void cancelOutsideLock(ScheduledFuture<?> f) {
        if (f != null) {
            try { f.cancel(false); } catch (RuntimeException e) { LOG.warn("cancel error (ignored)", e); }
        }
    }

    // ---- test hooks (package-private) ----
    boolean debugRunning()      { lock.lock(); try { return state == State.RUNNING; } finally { lock.unlock(); } }
    boolean debugRetryPending() { lock.lock(); try { return state == State.RETRY_PENDING; } finally { lock.unlock(); } }
    boolean debugDirty()        { lock.lock(); try { return dirty; } finally { lock.unlock(); } }
    boolean debugClosed()       { lock.lock(); try { return state == State.CLOSED; } finally { lock.unlock(); } }
    long debugRetryToken()      { lock.lock(); try { return retryToken; } finally { lock.unlock(); } }
    /** Invoke the retry body with a given token exactly as the scheduled timer would, so a test
     *  can deterministically run a STALE retry (e.g. after close) and prove it does not reconcile. */
    void debugRunRetry(long token) { runRetry(token); }
}
