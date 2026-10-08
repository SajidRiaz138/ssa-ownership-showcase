package io.ssaownership.reconciler.k8s;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.ssaownership.reconciler.ConfigMapStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live-API integration tests for {@link KubeConfigMapStore}. These require a reachable cluster
 * (minikube/kind) and are SKIPPED otherwise via {@link #clusterAvailable()}. They verify the
 * guarantees a fake cannot: real resourceVersion enforcement on the conditional write, and
 * server-side preservation of unrelated content.
 */
@EnabledIf("clusterAvailable")
class KubeConfigMapStoreIT {

    static boolean clusterAvailable() {
        try (KubernetesClient c = new KubernetesClientBuilder().build()) {
            c.namespaces().list();      // a cheap call that fails fast if no cluster
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private KubernetesClient client;
    private String ns;
    private final String cm = "it-config";
    private final String key = "message";

    @BeforeEach
    void setUp() {
        client = new KubernetesClientBuilder().build();
        ns = "ssa-it-" + UUID.randomUUID().toString().substring(0, 8);
        client.namespaces().resource(new io.fabric8.kubernetes.api.model.NamespaceBuilder()
                .withNewMetadata().withName(ns).endMetadata().build()).create();
    }

    @AfterEach
    void tearDown() {
        try { client.namespaces().withName(ns).delete(); } finally { client.close(); }
    }

    private void createCm(Map<String, String> data) {
        client.configMaps().inNamespace(ns).resource(new ConfigMapBuilder()
                .withNewMetadata().withName(cm).endMetadata().withData(data).build()).create();
    }

    @Test @Timeout(60)
    void conditional_putIfAbsent_preserves_a_concurrent_insert() {
        // The decisive race: adapter reads key absent; BEFORE its conditional write, another
        // client inserts an operator value; the adapter's write hits a 409, re-reads, sees the
        // key present, and PRESERVES it, returning false. Deterministic via the pre-write seam.
        createCm(Map.of("other", "keep-me"));            // CM exists, key absent
        Runnable concurrentInsert = () -> client.configMaps().inNamespace(ns).withName(cm)
                .edit(c -> new ConfigMapBuilder(c).addToData(key, "operator-value").build());
        KubeConfigMapStore store = new KubeConfigMapStore(client, ns, 5, concurrentInsert);

        boolean wrote = store.putIfAbsent(cm, key, "app-default");
        assertFalse(wrote, "must not report a write when a concurrent insert won");
        assertEquals("operator-value", client.configMaps().inNamespace(ns).withName(cm).get()
                .getData().get(key), "the concurrent operator value must survive");
        assertEquals("keep-me", client.configMaps().inNamespace(ns).withName(cm).get()
                .getData().get("other"), "unrelated key preserved");
    }

    @Test @Timeout(60)
    void putIfAbsent_seeds_when_truly_absent() {
        createCm(Map.of());
        ConfigMapStore store = new KubeConfigMapStore(client, ns);
        assertTrue(store.putIfAbsent(cm, key, "seed"));
        assertEquals("seed", store.get(cm, key).orElseThrow());
    }

    @Test @Timeout(60)
    void upsert_preserves_unrelated_keys() {
        createCm(Map.of("other", "x"));
        ConfigMapStore store = new KubeConfigMapStore(client, ns);
        store.upsert(cm, key, "v");
        var data = client.configMaps().inNamespace(ns).withName(cm).get().getData();
        assertEquals("v", data.get(key));
        assertEquals("x", data.get("other"));
    }

    @Test @Timeout(60)
    void empty_string_is_present_not_absent() {
        createCm(Map.of(key, ""));
        ConfigMapStore store = new KubeConfigMapStore(client, ns);
        assertEquals("", store.get(cm, key).orElseThrow(), "empty string is a present value");
    }

    @Test @Timeout(60)
    void write_to_missing_configmap_throws() {
        ConfigMapStore store = new KubeConfigMapStore(client, ns);   // CM not created
        assertThrows(ConfigMapStore.StoreException.class, () -> store.upsert(cm, key, "v"));
        assertThrows(ConfigMapStore.StoreException.class, () -> store.putIfAbsent(cm, key, "v"));
    }

    @Test @Timeout(60)
    void conflict_retry_exhaustion_throws() {
        createCm(Map.of());
        // seam inserts a DIFFERENT value on every attempt via a changing suffix -> perpetual 409.
        int[] n = {0};
        Runnable alwaysConflict = () -> client.configMaps().inNamespace(ns).withName(cm)
                .edit(c -> new ConfigMapBuilder(c).addToData("churn", "v" + (n[0]++)).build());
        // retries=0 so the first conflict exhausts immediately
        KubeConfigMapStore store = new KubeConfigMapStore(client, ns, 0, alwaysConflict);
        assertThrows(ConfigMapStore.StoreException.class, () -> store.putIfAbsent(cm, key, "x"));
    }
}
