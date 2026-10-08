package io.ssaownership.reconciler.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.ssaownership.reconciler.ConfigMapStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * fabric8-backed {@link ConfigMapStore}, bound to one namespace. Implements the contract exactly,
 * in particular the concurrency-safe {@link #putIfAbsent}: it reads, checks the key, and writes
 * at the OBSERVED resourceVersion; on a 409 conflict it re-reads and re-evaluates (the key may
 * now be present, in which case it preserves it). A naive read-then-write is deliberately avoided.
 *
 * <p><b>UPSERT does not retry conflicts internally.</b> {@link #upsert} performs a single
 * optimistic update at the observed resourceVersion and propagates a 409 conflict as a
 * {@link StoreException}. Retrying a conflicted enforcement is the application's job: the
 * reconcile coordinator retries the pass, which RE-OBSERVES current state before deciding to
 * write again. Keeping the retry at the reconcile level (not inside the adapter) ensures the
 * desired-vs-observed comparison is re-evaluated against fresh state on every attempt.
 *
 * <p><b>Whole-resource update, not key-level SSA.</b> Writes update a copied ConfigMap resource
 * with resourceVersion protection against lost updates; they do NOT establish an independent
 * server-side-apply field manager for the key. Named SSA field ownership is demonstrated by the
 * SchemaBinding operator scenario, not by this adapter.
 */
public final class KubeConfigMapStore implements ConfigMapStore {

    private static final Logger LOG = LoggerFactory.getLogger(KubeConfigMapStore.class);

    private final KubernetesClient client;
    private final String namespace;
    private final int maxConflictRetries;
    /** TEST SEAM: runs inside putIfAbsent AFTER the read/absent-check, BEFORE the write. No-op in
     *  production; a test injects a concurrent insert here to exercise the conflict path. */
    private final Runnable beforeConditionalWrite;

    public KubeConfigMapStore(KubernetesClient client, String namespace) {
        this(client, namespace, 5);
    }

    public KubeConfigMapStore(KubernetesClient client, String namespace, int maxConflictRetries) {
        this(client, namespace, maxConflictRetries, () -> {});
    }

    /** Package-private: adds the pre-write test seam. */
    KubeConfigMapStore(KubernetesClient client, String namespace, int maxConflictRetries,
                       Runnable beforeConditionalWrite) {
        this.client = Objects.requireNonNull(client, "client");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        if (maxConflictRetries < 0) throw new IllegalArgumentException("maxConflictRetries < 0");
        this.maxConflictRetries = maxConflictRetries;
        this.beforeConditionalWrite = Objects.requireNonNull(beforeConditionalWrite, "beforeConditionalWrite");
    }

    @Override
    public Optional<String> get(String configMap, String key) {
        requireName(configMap, "configMap");
        requireName(key, "key");
        final ConfigMap cm;
        try {
            cm = client.configMaps().inNamespace(namespace).withName(configMap).get();
        } catch (KubernetesClientException e) {
            throw new StoreException("get " + configMap + " failed", e);   // NOT mapped to empty
        }
        if (cm == null || cm.getData() == null) {
            return Optional.empty();            // ConfigMap or key absent
        }
        return Optional.ofNullable(cm.getData().get(key));   // present "" -> Optional.of("")
    }

    @Override
    public void upsert(String configMap, String key, String value) {
        requireName(configMap, "configMap");
        requireName(key, "key");
        Objects.requireNonNull(value, "value");
        try {
            ConfigMap cm = require(configMap);  // must exist (do not create)
            Map<String, String> data = cm.getData() == null ? new HashMap<>() : new HashMap<>(cm.getData());
            data.put(key, value);
            cm.setData(data);
            // replace at the observed resourceVersion (cm carries it) -> optimistic concurrency
            client.configMaps().inNamespace(namespace).resource(cm).update();
        } catch (KubernetesClientException e) {
            throw new StoreException("upsert " + configMap + "." + key + " failed", e);
        }
    }

    @Override
    public boolean putIfAbsent(String configMap, String key, String value) {
        requireName(configMap, "configMap");
        requireName(key, "key");
        Objects.requireNonNull(value, "value");
        KubernetesClientException last = null;
        for (int attempt = 0; attempt <= maxConflictRetries; attempt++) {
            ConfigMap cm;
            try {
                cm = require(configMap);
            } catch (KubernetesClientException e) {
                throw new StoreException("putIfAbsent read " + configMap + " failed", e);
            }
            Map<String, String> data = cm.getData();
            if (data != null && data.containsKey(key)) {
                return false;                   // present -> preserve, done
            }
            Map<String, String> next = data == null ? new HashMap<>() : new HashMap<>(data);
            next.put(key, value);
            cm.setData(next);
            if (attempt == 0) {
                beforeConditionalWrite.run();   // TEST SEAM: inject a concurrent insert here
            }
            try {
                client.configMaps().inNamespace(namespace).resource(cm).update();  // at observed rv
                return true;
            } catch (KubernetesClientException e) {
                if (e.getCode() == 409) {       // conflict: someone else wrote; re-read & re-evaluate
                    last = e;
                    LOG.info("putIfAbsent {}.{} conflict (attempt {}); re-reading", configMap, key, attempt);
                    continue;
                }
                throw new StoreException("putIfAbsent " + configMap + "." + key + " failed", e);
            }
        }
        throw new StoreException("putIfAbsent " + configMap + "." + key
                + " exhausted conflict retries", last);
    }

    private static void requireName(String v, String what) {
        Objects.requireNonNull(v, what);
        if (v.isBlank()) throw new IllegalArgumentException(what + " must not be blank");
    }

    private ConfigMap require(String configMap) {
        ConfigMap cm = client.configMaps().inNamespace(namespace).withName(configMap).get();
        if (cm == null) {
            throw new StoreException("ConfigMap " + configMap + " does not exist in " + namespace);
        }
        return cm;
    }
}
