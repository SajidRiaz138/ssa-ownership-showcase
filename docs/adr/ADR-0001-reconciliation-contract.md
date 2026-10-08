# ADR-0001: Reconciliation contract — observed-destination enforcement vs seeding

Status: Accepted (reconciler-core). Date: 2026-10-08. Author: Sajid Riaz.

## Context

A Helm chart and an application both wrote the same ConfigMap field. Under Helm 4 server-side
apply this is a field-ownership conflict. The resolution is to make the application the single
runtime writer of that field. This ADR records how the application writes it.

## Decision

The reconcile engine (`Reconciler`) compares the DESIRED value against the OBSERVED destination
on each pass, using one of two explicit write policies (`WriteStrategy`):

- **UPSERT (enforce):** read the key; if it already equals desired, skip; otherwise create or
  replace it. Repairs destination drift when a pass runs.
- **PUT_IF_ABSENT (seed):** write only when the key is absent, via the store's concurrency-safe
  conditional operation; an existing value is preserved, never reverted.

**When** a pass runs (triggering) is a SEPARATE concern from **what** a write may do (policy).
Triggering (destination watch and/or periodic resync) lives outside this class.

### Why the policy choice is a correctness decision
- PUT_IF_ABSENT on derived configuration can **preserve stale output** (never corrected).
- UPSERT on editable defaults can **revert operator changes** (overwritten every pass).

## Boundaries and non-claims

- **Not continuous enforcement.** The engine "repairs drift WHEN reconciliation runs." Between
  passes, destination drift can persist. Eventual correction depends on reliable triggers.
- **Unknown remote outcome on failure.** `Outcome.FAILED` means no local success state advanced
  and another pass is permitted. It does NOT prove the destination is unchanged: a write may
  commit remotely before the client observes a timeout. The next pass re-observes (UPSERT) or
  conditionally writes (PUT_IF_ABSENT).
- **"Retryable" ≠ "transient".** Invalid input, authorization errors, or a missing ConfigMap keep
  failing until an operator intervenes.
- **Write policy ≠ SSA field ownership.** UPSERT is a write policy, not a licence to force SSA
  field-ownership conflicts. SSA ownership is a property of the apply mechanism, demonstrated
  elsewhere in the repo (operator scenario), NOT by this engine.
- **API-boundary guarantees are not established here.** `ConfigMapStore` documents the required
  contract (concurrency-safe put-if-absent, missing-ConfigMap-throws, errors-not-mapped-to-empty,
  preserve-unrelated, reject-null). The in-memory fake proves POLICY only. The fabric8 adapter and
  the real-cluster conditional-update race are a later, separately-verified increment.

## Verification (this revision)
`reconciler-core` unit tests (JDK 21 / Maven 3.9.10), 13 tests, all passing:
drift repair (same instance), deletion repair (same instance), in-sync skip (no write),
operation-specific get/upsert/putIfAbsent failure retryability, no-diagnostic-read (stub whose
get() throws), empty-string-is-present, put-if-absent preservation, null-derivation→FAILED,
missing-ConfigMap→FAILED, constructor null-rejection. These establish behaviour under the listed
unit conditions, not universal correctness or API-boundary concurrency.
