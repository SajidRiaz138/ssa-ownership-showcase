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

## Verification (live SSA, `SchemaSyncOwnershipIT`, 3 tests)

- `broken_operator_causes_a_chart_apply_conflict`: with the operator co-owning
  `spec.schemaVersion`, a chart apply WITHOUT `--force-conflicts` fails with a real SSA conflict.
- `fixed_operator_does_not_conflict_and_writes_status`: the status-only operator records observed
  state; the chart changes its own field with no conflict.
- `ownership_handoff_drops_stale_spec_claim_but_preserves_the_value`: starting from the broken
  state (chart apply conflicts), the handoff drops the stale claim, the chart's `spec.schemaVersion`
  VALUE is preserved, and a subsequent chart apply is conflict-free.

## Consequence
This is the class of conflict a chart cannot fix alone. The real-world lesson: when an operator
contends for a chart-owned field, the operator must move its observed state to `status` and
perform an ownership handoff on upgrade. `--force-conflicts` can unblock temporarily but is a
blunt override, not a resolution.
