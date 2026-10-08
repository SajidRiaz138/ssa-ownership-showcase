package io.ssaownership.reconciler.app;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end: run {@link ConfigReconcilerApp} in-process against a real cluster and verify it
 * OWNS and enforces the target field — initial seeding, source-change propagation, and drift
 * repair. Skipped if no cluster is reachable.
 */
@EnabledIf("clusterAvailable")
class ConfigReconcilerAppIT {

    static boolean clusterAvailable() {
        try (KubernetesClient c = new KubernetesClientBuilder().build()) {
            c.namespaces().list(); return true;
        } catch (Exception e) { return false; }
    }

    private KubernetesClient client;
    private String ns;
    private final String source = "demo-source";
    private final String target = "demo-config";
    private final String key = "message";

    @BeforeEach
    void setUp() {
        client = new KubernetesClientBuilder().build();
        ns = "ssa-app-it-" + UUID.randomUUID().toString().substring(0, 8);
        client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(ns).endMetadata().build()).create();
        // the chart renders the target OBJECT (no data) + a source object:
        client.configMaps().inNamespace(ns).resource(new ConfigMapBuilder()
                .withNewMetadata().withName(target).endMetadata().build()).create();
        client.configMaps().inNamespace(ns).resource(new ConfigMapBuilder()
                .withNewMetadata().withName(source).endMetadata().withData(Map.of("desired", "v1")).build()).create();
    }

    @AfterEach
    void tearDown() {
        try { client.namespaces().withName(ns).delete(); } finally { client.close(); }
    }

    private boolean await(BooleanSupplier cond, long seconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return true;
            TimeUnit.MILLISECONDS.sleep(100);   // polling the cluster, not a correctness sleep
        }
        return cond.getAsBoolean();
    }
    private String targetValue() {
        var cm = client.configMaps().inNamespace(ns).withName(target).get();
        return cm.getData() == null ? null : cm.getData().get(key);
    }

    @Test @Timeout(60)
    void app_enforces_derived_value_and_repairs_drift() throws Exception {
        try (ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key)) {
            app.start();
            // initial reconcile seeds the derived value from source.desired=v1
            assertTrue(await(() -> "v1".equals(targetValue()), 20), "app should enforce v1, was " + targetValue());

            // change the source -> watch triggers -> app enforces v2
            client.configMaps().inNamespace(ns).withName(source)
                    .edit(c -> new ConfigMapBuilder(c).addToData("desired", "v2").build());
            assertTrue(await(() -> "v2".equals(targetValue()), 20), "app should enforce v2, was " + targetValue());

            // external drift on the target -> a reconcile pass must repair it
            client.configMaps().inNamespace(ns).withName(target)
                    .edit(c -> new ConfigMapBuilder(c).addToData(key, "tampered").build());
            app.reconcileNow();
            assertTrue(await(() -> "v2".equals(targetValue()), 20), "app should repair drift back to v2, was " + targetValue());
        }
    }
}
