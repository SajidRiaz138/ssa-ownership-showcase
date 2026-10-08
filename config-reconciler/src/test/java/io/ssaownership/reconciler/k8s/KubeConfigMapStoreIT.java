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
        if ("true".equalsIgnoreCase(System.getProperty("ssa.it.requireCluster"))) {
            return true;   // live-it profile: do NOT skip — a real connection failure must surface
        }
        try (KubernetesClient c = new KubernetesClientBuilder().build()) {
            c.getKubernetesVersion();   // cheap, works for namespace-scoped creds (not namespaces().list())
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
    void conflict_retry_exhaustion_throws_immediately_with_zero_retries() {
        createCm(Map.of());
        // retries=0: the FIRST conflict exhausts immediately. The pre-write seam fires only on
        // attempt 0 (by design), so a single injected concurrent insert forces the one-and-only
        // write to conflict. The contract says retry exhaustion MUST throw StoreException, so the
        // test requires the throw (it must NOT be satisfied by a false return).
        Runnable concurrentInsert = () -> client.configMaps().inNamespace(ns).withName(cm)
                .edit(c -> new ConfigMapBuilder(c).addToData(key, "other-writer").build());
        KubeConfigMapStore store = new KubeConfigMapStore(client, ns, 0, concurrentInsert);

        assertThrows(ConfigMapStore.StoreException.class,
                () -> store.putIfAbsent(cm, key, "app-default"),
                "retry exhaustion must throw StoreException (contract), not return false");
        // and the concurrent writer's value must be intact:
        assertEquals("other-writer", store.get(cm, key).orElseThrow(),
                "the concurrent writer's value stands after exhaustion");
    }

    @Test @Timeout(60)
    void conflict_from_unrelated_update_is_resolved_by_reread_then_insert() {
        // A conflict caused by an UNRELATED change (not our key) with retries available: the write
        // hits 409, the adapter re-reads (seeing our key still absent) and inserts it, preserving
        // the unrelated change. Seam fires on attempt 0 only, so exactly one 409 then success.
        createCm(Map.of("other", "v0"));
        Runnable unrelatedBump = () -> client.configMaps().inNamespace(ns).withName(cm)
                .edit(c -> new ConfigMapBuilder(c).addToData("other", "v1").build());
        KubeConfigMapStore store = new KubeConfigMapStore(client, ns, 5, unrelatedBump);

        boolean wrote = store.putIfAbsent(cm, key, "app-default");
        assertTrue(wrote, "our key was absent; after re-read it must be inserted");
        var data = client.configMaps().inNamespace(ns).withName(cm).get().getData();
        assertEquals("app-default", data.get(key), "our key inserted");
        assertEquals("v1", data.get("other"), "the unrelated concurrent change is preserved");
    }

    @Test @Timeout(60)
    void get_on_unreachable_api_throws_StoreException_not_absence() {
        // A read failure must surface as StoreException, never be mapped to Optional.empty().
        // Use a client pointed at a dead endpoint so the GET fails at the transport layer.
        try (KubernetesClient dead = new KubernetesClientBuilder()
                .withConfig(new io.fabric8.kubernetes.client.ConfigBuilder()
                        .withMasterUrl("https://127.0.0.1:1")   // nothing listening
                        .withConnectionTimeout(500).withRequestTimeout(500)
                        .withWatchReconnectLimit(0).build())
                .build()) {
            ConfigMapStore store = new KubeConfigMapStore(dead, ns);
            assertThrows(ConfigMapStore.StoreException.class, () -> store.get(cm, key),
                    "a read failure must throw StoreException, not return empty");
        }
    }

    @Test @Timeout(60)
    void putIfAbsent_on_existing_empty_string_returns_false_and_preserves_it() {
        createCm(Map.of(key, ""));                       // present, empty-string value
        ConfigMapStore store = new KubeConfigMapStore(client, ns);
        assertFalse(store.putIfAbsent(cm, key, "would-overwrite"),
                "an existing empty-string value is PRESENT; putIfAbsent must return false");
        assertEquals("", store.get(cm, key).orElseThrow(), "the empty-string value is preserved");
    }

    @Test @Timeout(60)
    void writes_preserve_labels_annotations_and_binaryData() {
        // Create a CM with labels, annotations and binaryData; an upsert of our key must leave all
        // of them untouched (writes touch only data.<key>).
        client.configMaps().inNamespace(ns).resource(new ConfigMapBuilder()
                .withNewMetadata().withName(cm)
                    .addToLabels("team", "infra")
                    .addToAnnotations("note", "keep")
                .endMetadata()
                .addToData("other", "x")
                .addToBinaryData("blob", "aGVsbG8=")     // base64 "hello"
                .build()).create();
        ConfigMapStore store = new KubeConfigMapStore(client, ns);
        store.upsert(cm, key, "v");
        var fresh = client.configMaps().inNamespace(ns).withName(cm).get();
        assertEquals("v", fresh.getData().get(key));
        assertEquals("x", fresh.getData().get("other"), "unrelated data key preserved");
        assertEquals("infra", fresh.getMetadata().getLabels().get("team"), "label preserved");
        assertEquals("keep", fresh.getMetadata().getAnnotations().get("note"), "annotation preserved");
        assertEquals("aGVsbG8=", fresh.getBinaryData().get("blob"), "binaryData preserved");
    }
}
