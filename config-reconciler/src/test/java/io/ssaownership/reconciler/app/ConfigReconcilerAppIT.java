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
import java.time.Duration;
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
        if ("true".equalsIgnoreCase(System.getProperty("ssa.it.requireCluster"))) {
            return true;   // live-it profile: do NOT skip — a real connection failure must surface
        }
        try (KubernetesClient c = new KubernetesClientBuilder().build()) {
            c.getKubernetesVersion(); return true;
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
    void app_enforces_derived_value_and_propagates_source_changes() throws Exception {
        try (ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key)) {
            app.start();
            // initial reconcile seeds the derived value from source.desired=v1
            assertTrue(await(() -> "v1".equals(targetValue()), 20), "app should enforce v1, was " + targetValue());

            // change the source -> watch triggers -> app enforces v2
            client.configMaps().inNamespace(ns).withName(source)
                    .edit(c -> new ConfigMapBuilder(c).addToData("desired", "v2").build());
            assertTrue(await(() -> "v2".equals(targetValue()), 20), "app should enforce v2, was " + targetValue());

            // external drift on the target -> an EXPLICITLY triggered pass repairs it. This proves
            // repair WHEN a pass runs; autonomous repair is covered separately below.
            client.configMaps().inNamespace(ns).withName(target)
                    .edit(c -> new ConfigMapBuilder(c).addToData(key, "tampered").build());
            app.triggerNow();
            assertTrue(await(() -> "v2".equals(targetValue()), 20), "a triggered pass must repair drift to v2, was " + targetValue());
        }
    }

    @Test @Timeout(60)
    void missing_source_preserves_the_target_and_does_not_clobber_with_a_default() throws Exception {
        // Policy: a missing source must NOT overwrite the target with a fabricated default. After
        // the app has enforced v1, delete the source entirely; subsequent passes must FAIL and
        // leave the last enforced value intact.
        try (ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key,
                Duration.ofSeconds(1), Duration.ofSeconds(1))) {
            app.start();
            assertTrue(await(() -> "v1".equals(targetValue()), 20), "initial enforce v1, was " + targetValue());

            client.configMaps().inNamespace(ns).withName(source).delete();   // accidental source loss
            // Give periodic resync several cycles; the target must remain v1 (never a default).
            Thread.sleep(3000);
            assertEquals("v1", targetValue(),
                    "missing source must preserve the last enforced target value, not clobber it");
        }
    }

    @Test @Timeout(60)
    void periodic_resync_repairs_target_drift_autonomously() throws Exception {
        // AUTONOMOUS enforcement: with a short resync interval, target-only drift (which the
        // source watch never observes) is repaired by the coordinator's periodic resync — no
        // source event, no manual trigger.
        try (ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key,
                Duration.ofSeconds(1), Duration.ofSeconds(1))) {
            app.start();
            assertTrue(await(() -> "v1".equals(targetValue()), 20), "initial enforce v1, was " + targetValue());

            client.configMaps().inNamespace(ns).withName(target)
                    .edit(c -> new ConfigMapBuilder(c).addToData(key, "tampered").build());
            assertEquals("tampered", targetValue(), "precondition: target drifted");

            // Do NOT trigger and do NOT touch the source; the periodic resync must repair it.
            assertTrue(await(() -> "v1".equals(targetValue()), 20),
                    "periodic resync must repair target drift autonomously, was " + targetValue());
        }
    }

    @Test @Timeout(60)
    void reconcile_on_reinstall_repairs_drift_without_a_source_event() throws Exception {
        // Reconcile-on-(re)install resync: a watch (re)install must itself trigger a reconcile, so
        // that a change missed while disconnected is corrected on reconnect rather than waiting for
        // the next source event. This drives the SAME watchSource() reconcile-on-install path a
        // recovery uses (reconciler-core's race tests prove a scheduled recovery reaches install);
        // here we drive a (re)install explicitly, drift the target with NO source event, and assert
        // the (re)install's own reconcile repairs it.
        try (ConfigReconcilerApp app = new ConfigReconcilerApp(client, ns, source, target, key)) {
            app.start();
            assertTrue(await(() -> "v1".equals(targetValue()), 20), "initial enforce v1, was " + targetValue());

            // External drift of the owned key, deliberately WITHOUT touching the source (no event):
            client.configMaps().inNamespace(ns).withName(target)
                    .edit(c -> new ConfigMapBuilder(c).addToData(key, "drift-no-event").build());
            assertEquals("drift-no-event", targetValue(), "precondition: target drifted");

            // A (re)install (as recovery would perform) must schedule a reconcile on its own:
            app.lifecycle().registerWatch();
            assertTrue(await(() -> "v1".equals(targetValue()), 20),
                    "the (re)install's own reconcile must repair drift without any source event, was " + targetValue());
        }
    }
}
