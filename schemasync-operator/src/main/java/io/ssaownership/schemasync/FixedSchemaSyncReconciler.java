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
 * {@code spec}. The decisive architectural property is WHERE it writes: {@code status}, not
 * {@code spec}. Because it never applies {@code spec}, it never contends with the chart's
 * ownership of {@code spec.schemaVersion}, so a chart apply never conflicts with this operator.
 *
 * <p><b>Update semantics (precise).</b> {@link #reconcile} uses {@code updateStatus()} — a plain
 * update of the {@code /status} subresource (optimistic-concurrency on resourceVersion). It is
 * NOT a server-side apply and does NOT register {@link #FIELD_MANAGER} as an SSA owner of status.
 * This class therefore does not claim SSA ownership of {@code status}; it claims only that it
 * leaves {@code spec} untouched. (If strict SSA status ownership under a named manager were
 * required, {@code reconcile} would instead do a server-side apply of {@code status} with
 * {@code FIELD_MANAGER}; that is a deliberate non-goal here.)
 *
 * <h2>Ownership handoff (upgrade from the broken operator)</h2>
 * Simply ceasing to write {@code spec} does NOT relinquish an EXISTING server-side-apply field
 * ownership that the broken operator established on {@code spec.schemaVersion}. SSA ownership
 * persists until explicitly released. {@link #releaseStaleSpecOwnership} performs the handoff:
 * it re-applies as the SAME field manager with a patch that OWNS NOTHING in spec, which drops the
 * manager's claim on {@code spec.schemaVersion} WITHOUT deleting the field's value — <b>but only
 * because another manager (the chart) still owns that field's value</b>. See the precondition on
 * {@link #releaseStaleSpecOwnership}.
 */
public final class FixedSchemaSyncReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(FixedSchemaSyncReconciler.class);
    public static final String FIELD_MANAGER = "schemasync-operator";

    private final KubernetesClient client;
    private final String namespace;

    public FixedSchemaSyncReconciler(KubernetesClient client, String namespace) {
        this.client = java.util.Objects.requireNonNull(client, "client");
        this.namespace = java.util.Objects.requireNonNull(namespace, "namespace");
    }

    /** Reconcile: write ONLY status.observedSchemaVersion (never spec). */
    public void reconcile(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name must not be blank");
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
     * operator, WITHOUT deleting spec.schemaVersion.
     *
     * <p><b>PRECONDITION (critical).</b> This is safe to use for migration ONLY when another
     * manager (here, the chart) also owns {@code spec.schemaVersion}. Re-applying as this manager
     * with an empty spec removes the fields THIS manager owned in spec; a field co-owned by the
     * chart keeps the chart's value. If this operator were the SOLE owner of the field, dropping
     * its claim could remove the value (or fail validation on a required field). Do not use this as
     * a blind production migration without first confirming another owner exists — the SSA
     * demonstration test asserts the value is preserved precisely because the chart co-owns it.
     */
    public void releaseStaleSpecOwnership(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name must not be blank");
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
