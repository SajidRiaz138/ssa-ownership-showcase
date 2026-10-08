package io.ssaownership.schemasync;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.ssaownership.schemasync.model.SchemaBinding;
import io.ssaownership.schemasync.model.SchemaBindingSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BROKEN SchemaSync operator.
 *
 * <p>On each reconcile it performs a server-side apply as field manager
 * {@code schemasync-operator} that INCLUDES {@code spec.schemaVersion} — a field the deployment
 * (Helm chart) also sets. Under SSA that makes the operator a co-owner of a desired-state field
 * it has no business owning. A subsequent chart apply (different manager) then CONFLICTS on
 * {@code spec.schemaVersion}.
 *
 * <p>This is the generic reproduction of the real-world defect: a controller contending for a
 * field whose desired state belongs to another actor. The fix is {@link FixedSchemaSyncReconciler}.
 */
public final class BrokenSchemaSyncReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(BrokenSchemaSyncReconciler.class);
    public static final String FIELD_MANAGER = "schemasync-operator";

    private final KubernetesClient client;
    private final String namespace;

    public BrokenSchemaSyncReconciler(KubernetesClient client, String namespace) {
        this.client = java.util.Objects.requireNonNull(client, "client");
        this.namespace = java.util.Objects.requireNonNull(namespace, "namespace");
    }

    /** Reconcile: (wrongly) server-side-apply the spec, co-owning spec.schemaVersion. */
    public void reconcile(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name must not be blank");
        SchemaBinding current = client.resources(SchemaBinding.class)
                .inNamespace(namespace).withName(name).get();
        if (current == null) return;
        String observed = current.getSpec() != null ? current.getSpec().getSchemaVersion() : null;

        // Build an apply patch that INCLUDES spec.schemaVersion (the bug): the operator claims
        // ownership of the chart's desired-state field.
        SchemaBinding patch = new SchemaBinding();
        patch.getMetadata().setName(name);
        SchemaBindingSpec spec = new SchemaBindingSpec();
        spec.setSchemaVersion(observed);     // echoes/claims the chart-owned field under SSA
        patch.setSpec(spec);

        client.resources(SchemaBinding.class).inNamespace(namespace).withName(name)
                .patch(new PatchContext.Builder()
                                .withPatchType(PatchType.SERVER_SIDE_APPLY)
                                .withFieldManager(FIELD_MANAGER)
                                .withForce(false)
                                .build(),
                        patch);
        LOG.info("[BROKEN] applied spec (co-owning spec.schemaVersion) for {}", name);
    }
}
