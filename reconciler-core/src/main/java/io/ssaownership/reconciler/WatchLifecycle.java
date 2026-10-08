package io.ssaownership.reconciler;

import io.fabric8.kubernetes.client.Watch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lifecycle for a single Kubernetes {@link Watch}, correct under concurrent registration, late
 * and duplicate close callbacks, close-before-install, shutdown races, and recovery scheduling
 * races. Recovery POLICY lives here; delayed execution is delegated to a {@link RecoveryScheduler}.
 *
 * <h2>The reservation protocol (the heart of the design)</h2>
 * The unit of work is an {@link Attempt}, whose identity is a monotonic generation claimed UNDER
 * the lock. The crucial property: once an attempt is authorized under the lock, that SAME attempt
 * is carried through creation and install — no later step re-mints a new identity after
 * validating an old one. {@code registerWatch()} and {@code runRecovery(reservation)} both reduce
 * to {@link #createAndInstall(Attempt)}:
 * <pre>
 *   createAndInstall(attempt):
 *     factory.create(...)                    // OUTSIDE the lock
 *     under lock: install candidate IFF attempt is still the current authorized attempt
 *                 AND not shut down AND candidate didn't self-close; else reject
 *     closeQuietly(rejected/superseded)      // OUTSIDE the lock
 * </pre>
 *
 * <h2>Invariants (see lifecycle ADR)</h2>
 * <ol>
 *   <li>Identity survives execution: an authorized attempt is never silently swapped for another.</li>
 *   <li>Only the current eligible attempt installs; superseded candidates are cleaned up.</li>
 *   <li>Explicit registration invalidates+detaches pending recovery under the lock; cancellation
 *       happens outside it.</li>
 *   <li>Closing the old installed watch detaches it; it does NOT invalidate an authorized replacement.</li>
 *   <li>Recovery completion/rejection/cancellation cannot modify a newer reservation's state.</li>
 *   <li>Shutdown is terminal; late factory results / scheduler handles cannot reactivate.</li>
 *   <li>Factory, scheduling, cancellation, and close all happen outside the lock.</li>
 * </ol>
 */
public final class WatchLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(WatchLifecycle.class);

    public enum Health {
        /** Lifecycle enabled and not degraded. Does NOT mean a functioning watch is currently
         *  installed — the lifecycle is ACTIVE before the first watch exists and during delayed
         *  recovery. Do not use this alone as a readiness signal; a watch may be absent/pending. */
        ACTIVE,
        /** Terminal shutdown. */
        DEACTIVATED,
        /** Recovery scheduling was rejected; needs an external trigger (see ADR-0002). */
        DEGRADED
    }

    /** An authorized unit of creation/installation work, identified by generation. */
    private static final class Attempt {
        final long generation;
        Attempt(long generation) { this.generation = generation; }
    }

    /** A reserved recovery, identified by generation, awaiting scheduled execution. */
    private static final class RecoveryReservation {
        final long generation;
        RecoveryScheduler.Cancellable handle;   // set after scheduling; guarded by lock
        RecoveryReservation(long generation) { this.generation = generation; }
    }

    private final String name;
    private final WatchFactory factory;
    private final RecoveryScheduler scheduler;
    private final long baseDelayMs;
    private final long maxDelayMs;
    private final long stableResetAfterNanos;
    private final LongSupplier nanoTime;
    /**
     * TEST SYNCHRONISATION SEAM (not a production extension point). Runs OUTSIDE the lifecycle
     * lock on the RECOVERY path only, after a reservation is consumed and its attempt authorized
     * but before creation is dispatched. Production uses a no-op; a test injects a pause here to
     * deterministically interleave an explicit registration. It must not modify internal state.
     */
    private final Runnable afterRecoveryAuthorized;

    private final ReentrantLock lock = new ReentrantLock();
    // ---- all guarded by lock ----
    private Health health = Health.ACTIVE;
    private long generation = 0;                 // last authorized attempt/reservation id
    private Watch installed;                     // the installed watch (may be null)
    private long installedGeneration = -1;
    private long installedAtNanos = 0;
    private int consecutiveFailures = 0;         // saturating; see nextDelayMsLocked
    private RecoveryReservation pendingRecovery; // at most one

    public WatchLifecycle(String name, WatchFactory factory, RecoveryScheduler scheduler) {
        this(name, factory, scheduler, Duration.ofSeconds(1), Duration.ofSeconds(30),
                Duration.ofSeconds(60), System::nanoTime);
    }

    public WatchLifecycle(String name, WatchFactory factory, RecoveryScheduler scheduler,
                          Duration baseDelay, Duration maxDelay, Duration stableResetAfter,
                          LongSupplier nanoTime) {
        this(name, factory, scheduler, baseDelay, maxDelay, stableResetAfter, nanoTime, () -> {});
    }

    /** Package-private: adds the recovery-path test seam. Public callers get the no-op overload. */
    WatchLifecycle(String name, WatchFactory factory, RecoveryScheduler scheduler,
                   Duration baseDelay, Duration maxDelay, Duration stableResetAfter,
                   LongSupplier nanoTime, Runnable afterRecoveryAuthorized) {
        this.name = Objects.requireNonNull(name, "name");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(baseDelay, "baseDelay");
        Objects.requireNonNull(maxDelay, "maxDelay");
        Objects.requireNonNull(stableResetAfter, "stableResetAfter");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.afterRecoveryAuthorized = Objects.requireNonNull(afterRecoveryAuthorized, "afterRecoveryAuthorized");
        // Validate configuration (fail fast): positive base/stability, maxDelay >= baseDelay.
        if (baseDelay.isNegative() || baseDelay.isZero()) throw new IllegalArgumentException("baseDelay must be positive");
        if (stableResetAfter.isNegative() || stableResetAfter.isZero()) throw new IllegalArgumentException("stableResetAfter must be positive");
        if (maxDelay.compareTo(baseDelay) < 0) throw new IllegalArgumentException("maxDelay must be >= baseDelay");
        // Representable ranges: baseDelay/maxDelay must fit in milliseconds and stableResetAfter in
        // nanoseconds (Duration conversion throws ArithmeticException on overflow). Normalise that
        // overflow into a configuration error that names the offending parameter, rather than
        // letting a raw ArithmeticException escape the constructor.
        this.baseDelayMs = Math.max(1, toMillisOrReject(baseDelay, "baseDelay"));
        this.maxDelayMs = Math.max(this.baseDelayMs, toMillisOrReject(maxDelay, "maxDelay"));
        this.stableResetAfterNanos = toNanosOrReject(stableResetAfter, "stableResetAfter");
    }

    private static long toMillisOrReject(Duration d, String param) {
        try { return d.toMillis(); }
        catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(param + " too large to represent in milliseconds: " + d, overflow);
        }
    }
    private static long toNanosOrReject(Duration d, String param) {
        try { return d.toNanos(); }
        catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(param + " too large to represent in nanoseconds: " + d, overflow);
        }
    }

    /** Explicit (re)registration. Invalidates any pending recovery and authorizes a replacement. */
    public void registerWatch() {
        Attempt attempt;
        RecoveryReservation detachedRecovery;
        lock.lock();
        try {
            if (health == Health.DEACTIVATED) {
                LOG.info("[{}] deactivated; registration refused", name);
                return;
            }
            // Invariant 3: invalidate+detach pending recovery under the lock (cancel outside).
            detachedRecovery = pendingRecovery;
            pendingRecovery = null;
            attempt = new Attempt(++generation);     // authorize THIS attempt
            if (health == Health.DEGRADED) health = Health.ACTIVE;   // actively trying again
        } finally {
            lock.unlock();
        }
        cancelOutsideLock(detachedRecovery);
        createAndInstall(attempt);
    }

    /** The single creation+install path. The authorized {@code attempt} is carried through. */
    private void createAndInstall(Attempt attempt) {
        // Create OUTSIDE the lock; bind the close callback to THIS attempt's generation.
        final Watch candidate;
        try {
            candidate = Objects.requireNonNull(
                    factory.create(() -> onClosed(attempt.generation)),
                    "watch factory returned null");
        } catch (RuntimeException e) {
            LOG.error("[{}] watch creation failed at gen {}", name, attempt.generation, e);
            onCreateFailed(attempt);
            return;
        }

        Watch toClose = null;
        lock.lock();
        try {
            // Close-before-install is enforced here WITHOUT a flag on Attempt, but the guarantee
            // depends on WHEN the close is OBSERVED (the bound onClosed acquiring this lock), not
            // on when the watch physically terminated:
            //   - If this attempt's close notification was PROCESSED BEFORE this installation check,
            //     its onClosed(attempt.generation) already advanced `generation` (reserving
            //     recovery), so attempt.generation != generation and the candidate is rejected here.
            //   - If closure is processed AFTER installation, the candidate is briefly installed and
            //     onClosed then detaches it and arranges recovery (the normal installed-close path).
            // A real client may physically terminate a watch before its callback runs; that latter
            // ordering is handled, not prevented. This is a probe-free distinction between physical
            // termination and lifecycle-observed termination.
            if (health == Health.DEACTIVATED || attempt.generation != generation) {
                // Invariant 2 & 6: superseded, shut down, or self-closed. Reject this candidate.
                LOG.info("[{}] candidate gen {} rejected (health={}, current={})",
                        name, attempt.generation, health, generation);
                toClose = candidate;
            } else {
                toClose = installed;                 // supersede any previous installed watch
                installed = candidate;
                installedGeneration = attempt.generation;
                installedAtNanos = nanoTime.getAsLong();
                health = Health.ACTIVE;
                LOG.info("[{}] watch installed at gen {}", name, attempt.generation);
            }
        } finally {
            lock.unlock();
        }
        closeQuietly(toClose);                       // Invariant 7
    }

    /** Close callback bound to the attempt created at {@code gen}. */
    private void onClosed(long gen) {
        RecoveryReservation reservation = null;
        long delayMs = baseDelayMs;
        lock.lock();
        try {
            if (health == Health.DEACTIVATED) return;        // shutting down

            boolean wasInstalled = (gen == installedGeneration);
            boolean isCurrent = (gen == generation);
            if (!wasInstalled && !isCurrent) {
                // Invariant 4/5: a stale/old watch closed. Detach nothing current, schedule
                // nothing. (If it was the installed one, wasInstalled handles detach below.)
                LOG.info("[{}] ignoring close of non-current watch gen {} (current {})",
                        name, gen, generation);
                return;
            }
            if (wasInstalled) {
                // Backoff decision from ACTUAL live duration (install -> now), not recovery latency.
                long live = nanoTime.getAsLong() - installedAtNanos;
                if (live >= stableResetAfterNanos) consecutiveFailures = 0;
                installed = null;
                installedGeneration = -1;
            }
            // Only arrange recovery if THIS close concerns the current generation. If a newer
            // attempt has already superseded (isCurrent false but wasInstalled true for an old
            // installed watch whose replacement is in flight), we detached the old watch above
            // but must NOT supersede the in-flight replacement (Invariant 4).
            if (!isCurrent) {
                LOG.info("[{}] old installed watch gen {} closed; replacement gen {} in flight, not recovering",
                        name, gen, generation);
                return;
            }
            consecutiveFailures = saturatingInc(consecutiveFailures);
            delayMs = nextDelayMsLocked();
            reservation = new RecoveryReservation(++generation);  // reserve recovery identity
            pendingRecovery = reservation;
        } finally {
            lock.unlock();
        }
        scheduleRecovery(reservation, delayMs);
    }

    private void onCreateFailed(Attempt attempt) {
        RecoveryReservation reservation;
        long delayMs;
        lock.lock();
        try {
            // Only this attempt may arrange recovery, and only if it is still current.
            if (health == Health.DEACTIVATED || attempt.generation != generation) {
                return;      // superseded/shut down: retaining a healthy installed watch if any
            }
            consecutiveFailures = saturatingInc(consecutiveFailures);
            delayMs = nextDelayMsLocked();
            reservation = new RecoveryReservation(++generation);
            pendingRecovery = reservation;
        } finally {
            lock.unlock();
        }
        scheduleRecovery(reservation, delayMs);
    }

    private void scheduleRecovery(RecoveryReservation reservation, long delayMs) {
        final RecoveryScheduler.Cancellable handle;
        try {
            handle = scheduler.schedule(Duration.ofMillis(delayMs), () -> runRecovery(reservation));
        } catch (RuntimeException e) {
            // Rejected scheduling: enter DEGRADED only if this reservation is still current.
            lock.lock();
            try {
                if (pendingRecovery == reservation && health != Health.DEACTIVATED) {
                    pendingRecovery = null;
                    health = Health.DEGRADED;
                    LOG.error("[{}] recovery scheduling rejected at gen {}; DEGRADED (external trigger required)",
                            name, reservation.generation, e);
                }
            } finally {
                lock.unlock();
            }
            return;
        }
        boolean stale;
        lock.lock();
        try {
            // The task may already have run (another thread) or been superseded while scheduling.
            stale = (health == Health.DEACTIVATED) || (pendingRecovery != reservation);
            if (!stale) reservation.handle = handle;   // record only if still current
        } finally {
            lock.unlock();
        }
        if (stale) cancelOutsideLock(handle);          // Invariant 5/7
    }

    /** Scheduler-invoked. Consume the matching reservation and authorize its exact attempt. */
    private void runRecovery(RecoveryReservation reservation) {
        Attempt attempt = null;
        lock.lock();
        try {
            if (health != Health.DEACTIVATED && pendingRecovery == reservation) {
                pendingRecovery = null;
                attempt = new Attempt(reservation.generation);  // SAME identity, now authorized
            } else {
                LOG.info("[{}] recovery gen {} no longer eligible; skipping", name, reservation.generation);
            }
        } finally {
            lock.unlock();
        }
        if (attempt != null) {
            afterRecoveryAuthorized.run();   // TEST SEAM: outside lock, after authorize, before create
            createAndInstall(attempt);       // carries the authorized identity; no re-mint
        }
    }

    /** Terminal shutdown. Invalidates all work; cancels pending recovery; closes installed. */
    public void deactivate() {
        Watch toClose;
        RecoveryReservation toCancel;
        lock.lock();
        try {
            if (health == Health.DEACTIVATED) return;
            health = Health.DEACTIVATED;
            generation++;                              // invalidate in-flight attempts/reservations
            toClose = installed;
            installed = null;
            installedGeneration = -1;
            toCancel = pendingRecovery;
            pendingRecovery = null;
        } finally {
            lock.unlock();
        }
        cancelOutsideLock(toCancel);
        closeQuietly(toClose);
        LOG.info("[{}] deactivated", name);
    }

    public Health health() { lock.lock(); try { return health; } finally { lock.unlock(); } }
    public boolean isActive() { return health() == Health.ACTIVE; }

    // ---- backoff: saturating, validated ------------------------------------------------------
    private static int saturatingInc(int v) { return v == Integer.MAX_VALUE ? v : v + 1; }

    private long nextDelayMsLocked() {
        // Saturating exponential: baseDelayMs * 2^(failures-1), capped at maxDelayMs. No overflow.
        long d = baseDelayMs;
        for (int i = 1; i < consecutiveFailures && d < maxDelayMs; i++) {
            d = (d > maxDelayMs / 2) ? maxDelayMs : d * 2;   // saturate before overflow
        }
        return Math.min(d, maxDelayMs);
    }

    private void closeQuietly(Watch w) {
        if (w == null) return;
        try { w.close(); } catch (RuntimeException e) { LOG.warn("[{}] close error (ignored)", name, e); }
    }
    private void cancelOutsideLock(RecoveryReservation r) {
        if (r != null) cancelOutsideLock(r.handle);
    }
    private void cancelOutsideLock(RecoveryScheduler.Cancellable c) {
        if (c == null) return;
        try { c.cancel(); } catch (RuntimeException e) { LOG.warn("[{}] cancel error (ignored)", name, e); }
    }

    // ---- test hooks (package-private) --------------------------------------------------------
    boolean debugIsInstalled(Watch w) {
        lock.lock(); try { return w != null && w == installed; } finally { lock.unlock(); }
    }
    boolean debugHasPendingRecovery() {
        lock.lock(); try { return pendingRecovery != null; } finally { lock.unlock(); }
    }
    int debugConsecutiveFailures() {
        lock.lock(); try { return consecutiveFailures; } finally { lock.unlock(); }
    }
}
