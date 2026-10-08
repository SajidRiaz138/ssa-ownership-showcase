package io.ssaownership.reconciler;

import java.util.Optional;

/**
 * Narrow abstraction over the one capability the reconcile engine needs: read, and conditionally
 * write, a single key in a target ConfigMap. Keeping it narrow makes {@link Reconciler} unit
 * testable with an in-memory fake, and confines the fabric8 client to one adapter.
 *
 * <h2>Contract (an implementation MUST honour all of these)</h2>
 * <ul>
 *   <li><b>Namespace.</b> An instance is bound to exactly one namespace, fixed at construction.
 *       {@code configMap} arguments are names within that namespace.</li>
 *   <li><b>Missing ConfigMap or key -> empty read.</b> {@link #get} returns {@link Optional#empty()}
 *       when EITHER the ConfigMap does not exist OR it exists but the key is absent (these two
 *       "absent" cases are not distinguished by {@code get}). A present key whose value is the
 *       empty string returns {@code Optional.of("")}. Writes DO distinguish them:
 *       {@link #upsert}/{@link #putIfAbsent} MUST fail with {@link StoreException} if the ConfigMap
 *       object does not exist (they do NOT create it — that is the chart's responsibility).</li>
 *   <li><b>Read errors are not absence.</b> Any API failure during {@link #get} (auth, network,
 *       server error) MUST throw {@link StoreException}, never be mapped to {@link Optional#empty()}.
 *       Empty means "ConfigMap or key absent" — never "the read failed".</li>
 *   <li><b>putIfAbsent is concurrency-safe at the API boundary.</b> The existence check and the
 *       write MUST be atomic with respect to other writers (optimistic concurrency on the
 *       observed resourceVersion, re-reading and re-evaluating on conflict). A naive
 *       read-then-write that can clobber a concurrent insert does NOT satisfy this contract.</li>
 *   <li><b>Preserve unrelated data and user-managed metadata.</b> Writes touch only {@code key};
 *       they MUST preserve other data keys and user-managed labels/annotations. They do NOT
 *       prevent the API server maintaining system metadata (e.g. {@code resourceVersion},
 *       managed-fields) — that normal server maintenance is expected.</li>
 *   <li><b>Retry exhaustion.</b> If conflict retries are exhausted, the operation MUST throw
 *       {@link StoreException} rather than return a misleading success/failure.</li>
 *   <li><b>Reject null.</b> Null values are rejected ({@link NullPointerException}); null is not
 *       a representable ConfigMap value.</li>
 * </ul>
 *
 * <p>Note on write policy: {@link #upsert} is a <em>write policy</em> ("create or replace the
 * key"), not a licence to force Kubernetes server-side-apply field-ownership conflicts. SSA
 * ownership is a separate concern handled by the apply mechanism, not by this interface.
 */
public interface ConfigMapStore {

    /** Thrown for any store failure (API error, missing ConfigMap on write, retry exhaustion). */
    class StoreException extends RuntimeException {
        public StoreException(String message, Throwable cause) { super(message, cause); }
        public StoreException(String message) { super(message); }
    }

    /**
     * @return the current value of {@code key} in {@code configMap}; empty ONLY if the key is
     *         absent. Throws {@link StoreException} on API failure (never maps error to empty).
     */
    Optional<String> get(String configMap, String key);

    /**
     * Create or replace {@code key = value}, preserving unrelated content.
     * @throws StoreException if the ConfigMap does not exist or the write ultimately fails.
     * @throws NullPointerException if value is null.
     */
    void upsert(String configMap, String key, String value);

    /**
     * Set {@code key = value} only if the key is currently absent, atomically with respect to
     * concurrent writers (re-read and re-evaluate on conflict). Preserves an existing value.
     * @return true if it wrote (key was absent), false if an existing value was left untouched.
     * @throws StoreException if the ConfigMap does not exist or the write ultimately fails.
     * @throws NullPointerException if value is null.
     */
    boolean putIfAbsent(String configMap, String key, String value);
}
