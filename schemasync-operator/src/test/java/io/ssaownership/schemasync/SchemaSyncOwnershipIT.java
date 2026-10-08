package io.ssaownership.schemasync;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live SSA ownership demonstration on a real cluster, with ACTUAL managedFields inspection:
 *  (1) the broken operator co-owning spec.schemaVersion makes a chart apply conflict (HTTP 409);
 *  (2) the fixed operator (status-only) does NOT conflict and introduces no spec claim;
 *  (3) the ownership handoff drops the broken operator's spec claim while the chart's value
 *      survives — and the test asserts the PRECONDITION (the chart co-owns the field) holds.
 * Named field managers, real 409 codes, and managedFields assertions make this diagnostic.
 *
 * Gated by a live-integration profile (see failsafe config). {@link #clusterAvailable()} only
 * decides local enablement; in CI the profile makes an unreachable cluster a FAILURE, not a skip.
 */
@EnabledIf("clusterAvailable")
class SchemaSyncOwnershipIT {

    static final String CHART_MANAGER = "helm";
    static boolean clusterAvailable() {
        if ("true".equalsIgnoreCase(System.getProperty("ssa.it.requireCluster"))) {
            return true;   // profile active: do NOT skip — let a real failure surface
        }
        try (KubernetesClient c = new KubernetesClientBuilder().build()) { c.namespaces().list(); return true; }
        catch (Exception e) { return false; }
    }

    private KubernetesClient client;
    private String ns;
    private final String name = "demo-binding";

    @BeforeEach
    void setUp() throws Exception {
        client = new KubernetesClientBuilder().build();
        ns = "ssa-op-it-" + UUID.randomUUID().toString().substring(0, 8);
        client.namespaces().resource(new NamespaceBuilder()
                .withNewMetadata().withName(ns).endMetadata().build()).create();
        try (InputStream crd = getClass().getResourceAsStream("/crd/schemabinding-crd.yaml")) {
            CustomResourceDefinition def = client.apiextensions().v1()
                    .customResourceDefinitions().load(crd).item();
            client.apiextensions().v1().customResourceDefinitions().resource(def).serverSideApply();
        }
        awaitCrdEstablished("schemabindings.ssaownership.io", Duration_SECONDS(30));
    }

    @AfterEach
    void tearDown() {
        // NOTE: the SchemaBinding CRD is CLUSTER-SCOPED and intentionally left installed (shared
        // fixture). Namespace cleanup does not remove it; run these on a disposable/dedicated
        // cluster. We delete only our namespace here.
        try { client.namespaces().withName(ns).delete(); } finally { client.close(); }
    }

    private static long Duration_SECONDS(long s) { return TimeUnit.SECONDS.toNanos(s); }

    /** Wait, with a bound, for the CRD's Established condition to be True. Restores interruption. */
    private void awaitCrdEstablished(String crdName, long budgetNanos) {
        long deadline = System.nanoTime() + budgetNanos;
        while (System.nanoTime() < deadline) {
            CustomResourceDefinition d = client.apiextensions().v1()
                    .customResourceDefinitions().withName(crdName).get();
            if (d != null && d.getStatus() != null && d.getStatus().getConditions() != null
                    && d.getStatus().getConditions().stream()
                        .anyMatch(c -> "Established".equals(c.getType()) && "True".equals(c.getStatus()))) {
                return;
            }
            try { TimeUnit.MILLISECONDS.sleep(100); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError("interrupted waiting for CRD", e); }
        }
        throw new AssertionError("CRD " + crdName + " did not reach Established=True within budget");
    }

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

    /** True iff the given field manager owns spec.schemaVersion per managedFields. */
    private boolean ownsSpecSchemaVersion(String manager) {
        SchemaBinding sb = client.resources(SchemaBinding.class).inNamespace(ns).withName(name).get();
        List<ManagedFieldsEntry> mf = sb.getMetadata().getManagedFields();
        if (mf == null) return false;
        for (ManagedFieldsEntry e : mf) {
            if (!manager.equals(e.getManager())) continue;
            Object fields = e.getFieldsV1() == null ? null : e.getFieldsV1().getAdditionalProperties();
            if (fields != null && fields.toString().contains("schemaVersion")) return true;
        }
        return false;
    }
    private boolean anyManagerOwnsSpec() {
        SchemaBinding sb = client.resources(SchemaBinding.class).inNamespace(ns).withName(name).get();
        List<ManagedFieldsEntry> mf = sb.getMetadata().getManagedFields();
        if (mf == null) return false;
        return mf.stream().anyMatch(e -> {
            Object f = e.getFieldsV1() == null ? null : e.getFieldsV1().getAdditionalProperties();
            return f != null && f.toString().contains("f:spec");
        });
    }

    @Test @Timeout(120)
    void broken_operator_causes_a_chart_apply_conflict_409_and_co_ownership_is_visible() {
        chartApply("1.0", false);
        assertTrue(ownsSpecSchemaVersion(CHART_MANAGER), "precondition: chart owns spec.schemaVersion");

        new BrokenSchemaSyncReconciler(client, ns).reconcile(name);
        assertTrue(ownsSpecSchemaVersion(BrokenSchemaSyncReconciler.FIELD_MANAGER),
                "broken operator must CO-OWN spec.schemaVersion (visible in managedFields)");

        KubernetesClientException ex = assertThrows(KubernetesClientException.class,
                () -> chartApply("2.0", false),
                "chart apply must conflict when the operator co-owns spec.schemaVersion");
        assertEquals(409, ex.getCode(), "the failure must be an SSA field-ownership conflict (HTTP 409)");
    }

    @Test @Timeout(120)
    void fixed_operator_writes_status_only_and_introduces_no_spec_claim() {
        chartApply("1.0", false);
        new FixedSchemaSyncReconciler(client, ns).reconcile(name);

        assertEquals("1.0", statusObserved(), "fixed operator records observed state in status");
        assertEquals("1.0", specVersion(), "spec untouched by the operator");
        // the operator must NOT appear as an owner of any spec field:
        assertFalse(ownsSpecSchemaVersion(FixedSchemaSyncReconciler.FIELD_MANAGER),
                "status-only operator must not own spec.schemaVersion");

        assertDoesNotThrow(() -> chartApply("2.0", false),
                "chart apply must NOT conflict when the operator owns no spec field");
        assertEquals("2.0", specVersion());
    }

    @Test @Timeout(120)
    void ownership_handoff_drops_broken_spec_claim_but_chart_value_survives() {
        // Broken state: operator co-owns spec.schemaVersion.
        chartApply("1.0", false);
        new BrokenSchemaSyncReconciler(client, ns).reconcile(name);
        assertTrue(ownsSpecSchemaVersion(BrokenSchemaSyncReconciler.FIELD_MANAGER), "operator co-owns spec");
        KubernetesClientException pre = assertThrows(KubernetesClientException.class, () -> chartApply("2.0", false));
        assertEquals(409, pre.getCode(), "precondition conflict must be a 409");

        // HANDOFF PRECONDITION (explicit): the chart STILL co-owns the field, so dropping the
        // operator's claim cannot delete the value.
        assertTrue(ownsSpecSchemaVersion(CHART_MANAGER),
                "handoff precondition: another manager (chart) owns spec.schemaVersion");

        FixedSchemaSyncReconciler fixed = new FixedSchemaSyncReconciler(client, ns);
        fixed.releaseStaleSpecOwnership(name);

        assertFalse(ownsSpecSchemaVersion(BrokenSchemaSyncReconciler.FIELD_MANAGER),
                "handoff must remove the operator's spec.schemaVersion claim");
        assertTrue(ownsSpecSchemaVersion(CHART_MANAGER), "chart still owns the field after handoff");
        assertEquals("1.0", specVersion(), "handoff must NOT delete the chart's spec value");

        assertDoesNotThrow(() -> chartApply("2.0", false), "after handoff the chart apply is conflict-free");
        assertEquals("2.0", specVersion());

        fixed.reconcile(name);
        assertEquals("2.0", statusObserved());
        assertFalse(ownsSpecSchemaVersion(FixedSchemaSyncReconciler.FIELD_MANAGER),
                "the fixed operator still introduces no spec claim");
    }
}
