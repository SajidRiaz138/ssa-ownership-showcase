package io.ssaownership.schemasync;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.ssaownership.schemasync.model.SchemaBinding;
import io.ssaownership.schemasync.model.SchemaBindingSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.InputStream;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live SSA ownership demonstration on a real cluster. Shows:
 *  (1) the broken operator co-owning spec.schemaVersion makes a chart apply CONFLICT;
 *  (2) the fixed operator (status-only) does NOT conflict with the chart;
 *  (3) the ownership handoff drops the stale spec claim WITHOUT deleting the chart's value.
 * Uses real managedFields — named field managers, actual conflicts, preservation checks.
 * Skipped if no cluster is reachable.
 */
@EnabledIf("clusterAvailable")
class SchemaSyncOwnershipIT {

    static final String CHART_MANAGER = "helm";
    static boolean clusterAvailable() {
        try (KubernetesClient c = new KubernetesClientBuilder().build()) { c.namespaces().list(); return true; }
        catch (Exception e) { return false; }
    }

    private KubernetesClient client;
    private String ns;
    private final String name = "demo-binding";

    @BeforeEach
    void setUp() {
        client = new KubernetesClientBuilder().build();
        ns = "ssa-op-it-" + UUID.randomUUID().toString().substring(0, 8);
        client.namespaces().resource(new io.fabric8.kubernetes.api.model.NamespaceBuilder()
                .withNewMetadata().withName(ns).endMetadata().build()).create();
        try (InputStream crd = getClass().getResourceAsStream("/crd/schemabinding-crd.yaml")) {
            CustomResourceDefinition def = client.apiextensions().v1()
                    .customResourceDefinitions().load(crd).item();
            client.apiextensions().v1().customResourceDefinitions().resource(def).serverSideApply();
        } catch (Exception e) { throw new RuntimeException("failed to install CRD", e); }
        // wait for the CRD to be established
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}
    }

    @AfterEach
    void tearDown() {
        try { client.namespaces().withName(ns).delete(); } finally { client.close(); }
    }

    /** The chart applies spec.schemaVersion as manager "helm" (server-side apply). */
    private void chartApply(String version, boolean force) {
        SchemaBinding sb = new SchemaBinding();
        sb.getMetadata().setName(name);
        SchemaBindingSpec spec = new SchemaBindingSpec();
        spec.setSchemaVersion(version);
        sb.setSpec(spec);
        client.resources(SchemaBinding.class).inNamespace(ns).withName(name)
                .patch(new PatchContext.Builder().withPatchType(PatchType.SERVER_SIDE_APPLY)
                        .withFieldManager(CHART_MANAGER).withForce(force).build(), sb);
    }

    private String specVersion() {
        SchemaBinding sb = client.resources(SchemaBinding.class).inNamespace(ns).withName(name).get();
        return sb != null && sb.getSpec() != null ? sb.getSpec().getSchemaVersion() : null;
    }
    private String statusObserved() {
        SchemaBinding sb = client.resources(SchemaBinding.class).inNamespace(ns).withName(name).get();
        return sb != null && sb.getStatus() != null ? sb.getStatus().getObservedSchemaVersion() : null;
    }

    @Test @Timeout(120)
    void broken_operator_causes_a_chart_apply_conflict() {
        chartApply("1.0", false);                       // chart owns spec.schemaVersion
        new BrokenSchemaSyncReconciler(client, ns).reconcile(name);  // operator co-owns it

        // The chart now tries to change its own field without --force: SSA must CONFLICT because
        // the operator co-owns spec.schemaVersion.
        KubernetesClientException ex = assertThrows(KubernetesClientException.class,
                () -> chartApply("2.0", false),
                "chart apply must conflict when the operator co-owns spec.schemaVersion");
        assertTrue(ex.getMessage() == null || ex.getMessage().toLowerCase().contains("conflict")
                        || ex.getCode() == 409,
                "the failure must be a field-ownership conflict, was: " + ex.getMessage());
    }

    @Test @Timeout(120)
    void fixed_operator_does_not_conflict_and_writes_status() {
        chartApply("1.0", false);
        new FixedSchemaSyncReconciler(client, ns).reconcile(name);   // status only

        // operator wrote observed state to status, left spec alone:
        assertEquals("1.0", statusObserved(), "fixed operator records observed state in status");
        assertEquals("1.0", specVersion(), "spec untouched by the operator");

        // the chart changes its own field WITHOUT --force: no conflict, because no co-ownership.
        assertDoesNotThrow(() -> chartApply("2.0", false),
                "chart apply must NOT conflict when the operator owns only status");
        assertEquals("2.0", specVersion());
    }

    @Test @Timeout(120)
    void ownership_handoff_drops_stale_spec_claim_but_preserves_the_value() {
        // Start in the broken state: operator co-owns spec.schemaVersion.
        chartApply("1.0", false);
        new BrokenSchemaSyncReconciler(client, ns).reconcile(name);
        // sanity: chart apply conflicts now
        assertThrows(KubernetesClientException.class, () -> chartApply("2.0", false));

        // UPGRADE to fixed: perform the ownership handoff.
        FixedSchemaSyncReconciler fixed = new FixedSchemaSyncReconciler(client, ns);
        fixed.releaseStaleSpecOwnership(name);          // drop the stale spec claim

        // The value must be preserved (chart still owns it) ...
        assertEquals("1.0", specVersion(), "handoff must NOT delete the chart's spec value");
        // ... and the chart can now change its field WITHOUT --force (no more co-ownership).
        assertDoesNotThrow(() -> chartApply("2.0", false),
                "after handoff the chart apply must be conflict-free");
        assertEquals("2.0", specVersion());

        // the fixed operator continues, writing only status:
        fixed.reconcile(name);
        assertEquals("2.0", statusObserved());
    }
}
