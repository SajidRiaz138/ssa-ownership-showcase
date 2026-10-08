package io.ssaownership.reconciler.app;

import io.ssaownership.reconciler.Reconciler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runtime coordination for reconcile passes. It sits between event sources (a watch, a periodic
 * timer, a startup trigger) and the policy engine ({@link Reconciler}), providing the properties
 * the engine deliberately does NOT: trigger coalescing, retry of failed passes, periodic resync,
 * and a terminal closed state. {@link Reconciler} itself is unchanged.
 *
 * <h2>Guarantees</h2>
 * <ul>
 *   <li><b>At most one drain in flight.</b> A trigger either starts a drain or, if one is running,
 *       sets a DIRTY flag; the running drain re-runs once more when it finishes. Any number of
 *       triggers arriving during a pass collapse into a single follow-up pass — real coalescing,
 *       not merely skipping redundant writes.</li>
 *   <li><b>Bounded queue.</b> At most one drain task is ever queued on the executor, so a burst of
 *       events cannot accumulate an unbounded backlog of reconcile tasks.</li>
 *   <li><b>Retry after FAILED.</b> If a pass returns {@link Reconciler.Outcome#FAILED}, a retry is
 *       scheduled after {@code retryDelay} (unless a trigger already dirtied it sooner). This is
 *       what repairs e.g. "target ConfigMap was briefly missing" without a new source event.</li>
 *   <li><b>Periodic resync.</b> A timer triggers a pass every {@code resyncInterval}, a fallback
 *       for events missed during a disconnection and for destination-only drift (the source watch
 *       alone would never observe target drift).</li>
 *   <li><b>Closed is terminal.</b> After {@link #close()} no new drain is started or scheduled;
 *       {@link #trigger} becomes a no-op.</li>
 * </ul>
 *
 * <p>This class does not interpret outcomes beyond FAILED→retry; it coordinates WHEN the engine
 * runs, never WHAT it decides.
 */
public final class ReconcileCoordinator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReconcileCoordinator.class);

    private final Reconciler reconciler;
    private final ScheduledExecutorService executor;   // single-thread; serialises drains
    private final Duration retryDelay;
    private final Duration resyncInterval;

    private final ReentrantLock lock = new ReentrantLock();
    private boolean running = false;    // a drain task is executing
    private boolean dirty = false;      // a trigger arrived while running; re-run once more
    private boolean closed = false;
    private java.util.concurrent.ScheduledFuture<?> resyncHandle;

    public ReconcileCoordinator(Reconciler reconciler, ScheduledExecutorService executor,
                                Duration retryDelay, Duration resyncInterval) {
        this.reconciler = Objects.requireNonNull(reconciler, "reconciler");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.retryDelay = requirePositive(retryDelay, "retryDelay");
        this.resyncInterval = requirePositive(resyncInterval, "resyncInterval");
    }

    private static Duration requirePositive(Duration d, String n) {
        Objects.requireNonNull(d, n);
        if (d.isNegative() || d.isZero()) throw new IllegalArgumentException(n + " must be positive");
        return d;
    }

    /** Start periodic resync. Call once after construction. */
    public void startPeriodicResync() {
        lock.lock();
        try {
            if (closed || resyncHandle != null) return;
            long ms = resyncInterval.toMillis();
            resyncHandle = executor.scheduleWithFixedDelay(this::trigger, ms, ms, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Request a reconcile. Shutdown-aware: a no-op once closed. If a drain is running, marks DIRTY
     * so exactly one more pass follows; otherwise starts a drain. Never throws
     * {@link RejectedExecutionException} back to the caller (e.g. a fabric8 watch callback): a
     * rejected submission during shutdown is swallowed after marking state.
     */
    public void trigger() {
        lock.lock();
        try {
            if (closed) return;
            if (running) { dirty = true; return; }
            running = true;
        } finally {
            lock.unlock();
        }
        submitDrain();
    }

    private void submitDrain() {
        try {
            executor.execute(this::drain);
        } catch (RejectedExecutionException shuttingDown) {
            // executor is gone (shutdown). Clear running so state is consistent; nothing queued.
            lock.lock();
            try { running = false; } finally { lock.unlock(); }
            LOG.debug("reconcile submission rejected (shutting down)");
        }
    }

    private void drain() {
        Reconciler.Outcome outcome;
        try {
            outcome = reconciler.reconcile();
        } catch (RuntimeException e) {
            outcome = Reconciler.Outcome.FAILED;
            LOG.error("reconcile pass threw unexpectedly", e);
        }

        boolean rerunNow;
        boolean scheduleRetry;
        lock.lock();
        try {
            if (closed) { running = false; return; }
            if (dirty) {
                dirty = false;
                rerunNow = true;            // a trigger arrived during the pass: coalesced follow-up
                scheduleRetry = false;
            } else if (outcome == Reconciler.Outcome.FAILED) {
                rerunNow = false;
                scheduleRetry = true;       // delayed retry
            } else {
                running = false;            // settled; nothing pending
                rerunNow = false;
                scheduleRetry = false;
            }
        } finally {
            lock.unlock();
        }

        if (rerunNow) {
            submitDrain();                  // stays "running"; one follow-up pass
        } else if (scheduleRetry) {
            try {
                executor.schedule(() -> { submitDrain(); }, retryDelay.toMillis(), TimeUnit.MILLISECONDS);
                // keep running=true until the retry actually drains, so no concurrent drain starts
            } catch (RejectedExecutionException shuttingDown) {
                lock.lock();
                try { running = false; } finally { lock.unlock(); }
            }
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            if (resyncHandle != null) { resyncHandle.cancel(false); resyncHandle = null; }
        } finally {
            lock.unlock();
        }
    }

    // ---- test hooks (package-private) ----
    boolean debugRunning() { lock.lock(); try { return running; } finally { lock.unlock(); } }
    boolean debugDirty()   { lock.lock(); try { return dirty; }   finally { lock.unlock(); } }
    boolean debugClosed()  { lock.lock(); try { return closed; }  finally { lock.unlock(); } }
}
