package io.ssaownership.reconciler;

/**
 * How the application writes the field it owns. The two strategies are not interchangeable —
 * choosing the right one is a correctness decision, and getting it wrong is a real bug.
 *
 * <ul>
 *   <li>{@link #UPSERT} — create or replace the target key when reconciliation determines a write
 *       is required (i.e. the observed value differs from desired). Correct when the application
 *       is the source of truth and must enforce its value (e.g. a derived configuration that must
 *       always reflect current inputs).</li>
 *   <li>{@link #PUT_IF_ABSENT} — write only when the key is absent; never replace an existing
 *       value. Correct when the value is a seedable default that another actor may legitimately
 *       have changed, and the app must not revert that change.</li>
 * </ul>
 *
 * <h2>Why the choice is a correctness decision (corrected migration example)</h2>
 * In the migration this models: a derived configuration field used UPSERT; seedable defaults used
 * PUT_IF_ABSENT. Using the wrong one is a real defect:
 * <ul>
 *   <li><b>PUT_IF_ABSENT on derived configuration</b> can <em>preserve stale output</em>: once a
 *       value exists it is never corrected, so the field drifts from its derived truth.</li>
 *   <li><b>UPSERT on editable defaults</b> can <em>revert operator changes</em>: every pass
 *       overwrites a deliberate change back to the default.</li>
 * </ul>
 * Hence the policy is modelled explicitly rather than hard-coded. Note: UPSERT is a write policy,
 * not a licence to force Kubernetes server-side-apply field-ownership conflicts — SSA ownership
 * is a separate concern handled by the apply mechanism.
 */
public enum WriteStrategy {
    UPSERT,
    PUT_IF_ABSENT
}
