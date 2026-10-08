package io.ssaownership.schemasync;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.ssaownership.schemasync.model.SchemaBinding;
import io.ssaownership.schemasync.model.SchemaBindingStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIXED SchemaSync operator.
 *
 * <p>It records OBSERVED state in {@code status} (via the /status subresource) and NEVER writes
 * {@code spec}. Under SSA its field manager owns only {@code status.*}; the chart owns
 * {@code spec.schemaVersion}. There is no overlap, so a chart apply never conflicts with the
 * operator — which is the correct ownership model.
 *
 * <h2>Ownership handoff (upgrade from the broken operator)</h2>
 * Simply ceasing to write {@code spec} does NOT relinquish an EXISTING server-side-apply field
 * ownership that the broken operator established on {@code spec.schemaVersion}. SSA ownership
 * persists until explicitly released. {@link #releaseStaleSpecOwnership} performs the handoff:
 * it re-applies as the SAME field manager with a patch that OWNS NOTHING in spec, which drops the
 * manager's claim on {@code spec.schemaVersion} WITHOUT deleting the field's value (another
 * manager — the chart — still owns it, so the value stays). This is the migration step a real
 * ".status migration" needs beyond the code change.
 */
public final class FixedSchemaSyncReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(FixedSchemaSyncReconciler.class);
    public static final String FIELD_MANAGER = "schemasync-operator";

    private final KubernetesClient client;
    private final String namespace;

    public FixedSchemaSyncReconciler(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    /** Reconcile: write ONLY status.observedSchemaVersion (never spec). */
    public void reconcile(String name) {
        SchemaBinding current = client.resources(SchemaBinding.class)
                .inNamespace(namespace).withName(name).get();
        if (current == null) return;
        String desired = current.getSpec() != null ? current.getSpec().getSchemaVersion() : null;

        SchemaBindingStatus status = new SchemaBindingStatus();
        status.setObservedSchemaVersion(desired);
        status.setPhase(desired == null ? "Pending" : "Synced");
        current.setStatus(status);

        // updateStatus targets the /status subresource — spec is untouched, no spec ownership.
        client.resources(SchemaBinding.class).inNamespace(namespace).resource(current).updateStatus();
        LOG.info("[FIXED] wrote status.observedSchemaVersion={} for {} (spec untouched)", desired, name);
    }

    /**
     * Ownership handoff: drop this manager's SSA claim on {@code spec} established by the broken
     * operator, WITHOUT deleting spec.schemaVersion (the chart still owns the value). Re-applying
     * as the same manager with an empty spec removes the fields that manager previously owned in
     * spec; fields owned by other managers (the chart) remain.
     */
    public void releaseStaleSpecOwnership(String name) {
        SchemaBinding emptyApply = new SchemaBinding();
        emptyApply.getMetadata().setName(name);
        // No spec set -> this manager, after this apply, owns no spec fields.
        client.resources(SchemaBinding.class).inNamespace(namespace).withName(name)
                .patch(new PatchContext.Builder()
                                .withPatchType(PatchType.SERVER_SIDE_APPLY)
                                .withFieldManager(FIELD_MANAGER)
                                .withForce(false)
                                .build(),
                        emptyApply);
        LOG.info("[FIXED] released stale spec ownership for {} (value preserved by the chart's manager)", name);
    }
}
