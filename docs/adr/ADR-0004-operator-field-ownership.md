# ADR-0004: Operator field ownership — competing spec ownership vs. status-only

Status: Accepted (schemasync-operator); verified against live SSA (minikube k8s 1.30).
Date: 2026-10-08. Author: Sajid Riaz.

## Context

Some Helm 3 → Helm 4 SSA conflicts are NOT fixable from the chart. When a controller and the
chart both write the SAME desired-state field on a resource, SSA conflicts — and the chart
cannot stop writing a field that is legitimately its desired state (e.g. a CRD's
`spec.schemaVersion`). The defect is **competing ownership of a field whose desired state
belongs to another actor**, not "controllers touching spec" in general.

This ADR records the generic reproduction (`SchemaBinding` CRD, `SchemaSync` operator) and the
correct fix.

## Decision

- The CRD has a `/status` subresource. Desired state lives in `spec` (chart-owned); observed
  state lives in `status` (operator-owned).
- **Broken operator** (`BrokenSchemaSyncReconciler`): server-side-applies a patch INCLUDING
  `spec.schemaVersion` as field manager `schemasync-operator`. It thereby co-owns a chart-owned
  field; a subsequent chart apply conflicts.
- **Fixed operator** (`FixedSchemaSyncReconciler`): writes observed state via `updateStatus()`
  (the `/status` subresource) and NEVER writes `spec`. Its field manager owns only `status.*`, so
  there is no overlap with the chart's `spec` ownership.
- **Ownership handoff (upgrade from broken to fixed):** merely ceasing to write `spec` does NOT
  relinquish an EXISTING SSA ownership on `spec.schemaVersion`. `releaseStaleSpecOwnership`
  re-applies as the SAME field manager with an empty spec, which drops that manager's claim on
  `spec.schemaVersion` WITHOUT deleting the value (the chart's manager still owns it, so the value
  survives). This is the migration step a real ".status migration" needs beyond the code change.

## Verification (live SSA, `SchemaSyncOwnershipIT`, 3 tests, with managedFields inspection)

The tests inspect ACTUAL `managedFields` (named managers), assert the real HTTP 409 code, and wait
for the CRD's `Established=True` condition (bounded) rather than sleeping.

- `broken_operator_causes_a_chart_apply_conflict_409_and_co_ownership_is_visible`: managedFields
  show the chart owns `spec.schemaVersion`; after the broken operator applies, managedFields show
  it CO-OWNS that field; a chart apply without `--force-conflicts` then fails with
  `KubernetesClientException.getCode() == 409` (asserted exactly, not a permissive message match).
- `fixed_operator_writes_status_only_and_introduces_no_spec_claim`: observed state is recorded in
  `status`; managedFields show the operator owns NO spec field; the chart changes its field with
  no conflict.
- `ownership_handoff_drops_broken_spec_claim_but_chart_value_survives`: from the broken state
  (asserted 409 precondition), the test first asserts the HANDOFF PRECONDITION — the chart still
  co-owns the field — then the handoff removes the operator's spec claim (managedFields), the
  chart's value survives, and a subsequent chart apply is conflict-free.

### Precise update semantics (corrected)
`FixedSchemaSyncReconciler.reconcile` uses `updateStatus()` — a plain update of the `/status`
subresource (optimistic concurrency), NOT a server-side apply. It therefore does NOT register the
`schemasync-operator` manager as an SSA owner of `status`; the claim verified here is only that it
writes `status` and leaves `spec` untouched. Strict SSA status ownership under a named manager is a
deliberate non-goal; if required, `reconcile` would server-side-apply `status` with the manager.

### Handoff precondition (critical, documented + asserted)
`releaseStaleSpecOwnership` preserves the value ONLY because another manager (the chart) still owns
`spec.schemaVersion`. If the operator were the sole owner, dropping the claim could delete the
field or fail validation. The handoff test asserts the chart co-owns the field before performing
the handoff. (The `"helm"` manager name MODELS a chart writer; it does not verify the actual Helm
CLI/version behaviour — that is the Helm-chart demo's concern.)

## Consequence
This is the class of conflict a chart cannot fix alone. The real-world lesson: when an operator
contends for a chart-owned field, the operator must move its observed state to `status` and
perform an ownership handoff on upgrade. `--force-conflicts` can unblock temporarily but is a
blunt override, not a resolution.
