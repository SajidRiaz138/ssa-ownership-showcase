package io.ssaownership.reconciler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The reconcile engine: derive the desired value for a single owned field, then bring the target
 * ConfigMap to that value using the configured {@link WriteStrategy}. The application — not the
 * Helm chart — writes this field at runtime; this engine is the application's writer.
 *
 * <h2>Contract (observed-destination enforcement)</h2>
 * Each pass compares the desired value against the <em>observed destination</em>, not a cache of
 * what was last derived. Therefore a pass repairs destination drift:
 * <ul>
 *   <li>{@link WriteStrategy#UPSERT}: read the key; if it already equals desired, skip; otherwise
 *       create-or-replace it. This repairs drift <em>when a pass runs</em>.</li>
 *   <li>{@link WriteStrategy#PUT_IF_ABSENT}: call the store's concurrency-safe conditional write;
 *       an existing value is preserved (never reverted).</li>
 * </ul>
 *
 * <h2>What this does NOT claim</h2>
 * <ul>
 *   <li>It is NOT continuous enforcement. It "repairs drift when reconciliation runs." WHEN a
 *       pass runs is decided by event wiring (destination watch and/or periodic resync), which
 *       lives outside this class. Between passes, destination drift can persist.</li>
 *   <li>It does NOT establish Kubernetes SSA field ownership. Write policy is not field-manager
 *       ownership; that is a property of the apply mechanism, demonstrated elsewhere in the repo.</li>
 *   <li>Comparing desired to observed avoids unnecessary writes; it is NOT a transaction. Another
 *       actor may change the destination immediately after a write. Eventual correction comes
 *       from the next pass, not from this method.</li>
 * </ul>
 */
public final class Reconciler {

    private static final Logger LOG = LoggerFactory.getLogger(Reconciler.class);

    private final String name;
    private final String targetConfigMap;
    private final String key;
    private final WriteStrategy strategy;
    private final ConfigMapStore store;
    /** Derives the desired value from current inputs. */
    private final Supplier<String> deriveDesired;

    public Reconciler(String name,
                      String targetConfigMap,
                      String key,
                      WriteStrategy strategy,
                      ConfigMapStore store,
                      Supplier<String> deriveDesired) {
        this.name = Objects.requireNonNull(name, "name");
        this.targetConfigMap = Objects.requireNonNull(targetConfigMap, "targetConfigMap");
        this.key = Objects.requireNonNull(key, "key");
        this.strategy = Objects.requireNonNull(strategy, "strategy");
        this.store = Objects.requireNonNull(store, "store");
        this.deriveDesired = Objects.requireNonNull(deriveDesired, "deriveDesired");
    }

    /** Outcome of one reconcile pass — explicit so callers and tests can assert behaviour. */
    public enum Outcome {
        /** A write was performed. */                         WROTE,
        /** UPSERT: destination already equalled desired. */  SKIPPED_IN_SYNC,
        /** PUT_IF_ABSENT: key already present, preserved. */ SKIPPED_PRESENT,
        /** Derivation or a store call failed; retryable. */  FAILED
    }

    /**
     * Run one reconcile pass. Converts derivation and store {@link RuntimeException}s into
     * {@link Outcome#FAILED} (it does NOT catch {@link Error}).
     *
     * <p><b>Failure semantics (precise).</b> On {@link Outcome#FAILED} no local success state is
     * advanced, so a subsequent pass is permitted. This does NOT prove the destination is
     * unchanged: a store write may have committed remotely before the client observed a timeout,
     * so a failed call has an <em>unknown remote outcome</em>. The next pass re-observes the
     * destination (UPSERT) or conditionally writes it (PUT_IF_ABSENT) rather than assuming either
     * result. "Retryable" means another pass is permitted — not that the failure is transient;
     * invalid input, authorization errors, or a missing ConfigMap will keep failing until an
     * operator intervenes.
     *
     * <p>{@code synchronized} serialises passes of THIS instance only. It provides no
     * coordination across instances or pods. Callers should run passes on worker execution
     * (not directly on a watch callback thread) and coalesce repeated triggers.
     */
    public synchronized Outcome reconcile() {
        final String desired;
        try {
            desired = Objects.requireNonNull(deriveDesired.get(), "derived value must not be null");
        } catch (RuntimeException e) {
            LOG.error("[{}] could not derive desired value; no store write attempted for {}.{}",
                    name, targetConfigMap, key, e);
            return Outcome.FAILED;
        }

        try {
            return switch (strategy) {
                case UPSERT -> upsertEnforce(desired);
                case PUT_IF_ABSENT -> seedIfAbsent(desired);
            };
        } catch (RuntimeException e) {   // e.g. ConfigMapStore.StoreException
            LOG.error("[{}] reconciliation of {}.{} failed (remote outcome unknown); a subsequent pass re-evaluates the configured policy",
                    name, targetConfigMap, key, e);
            return Outcome.FAILED;
        }
    }

    /** UPSERT: compare desired to OBSERVED destination; write only if they differ. */
    private Outcome upsertEnforce(String desired) {
        Optional<String> observed = store.get(targetConfigMap, key);   // may throw -> FAILED
        if (observed.filter(desired::equals).isPresent()) {
            LOG.debug("[{}] {}.{} already in sync; no write", name, targetConfigMap, key);
            return Outcome.SKIPPED_IN_SYNC;
        }
        store.upsert(targetConfigMap, key, desired);
        LOG.info("[{}] enforced {}.{} (upsert)", name, targetConfigMap, key);
        return Outcome.WROTE;
    }

    /** PUT_IF_ABSENT: concurrency-safe seed; preserve any existing value. No diagnostic read. */
    private Outcome seedIfAbsent(String desired) {
        boolean wrote = store.putIfAbsent(targetConfigMap, key, desired);  // may throw -> FAILED
        if (wrote) {
            LOG.info("[{}] seeded {}.{} (put-if-absent)", name, targetConfigMap, key);
            return Outcome.WROTE;
        }
        // false is a successful policy decision: a value already exists and we preserve it.
        LOG.info("[{}] {}.{} already present; left untouched", name, targetConfigMap, key);
        return Outcome.SKIPPED_PRESENT;
    }
}
