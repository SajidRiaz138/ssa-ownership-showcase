# ADR-0003: Kubernetes ConfigMap adapter and conditional writes

Status: Accepted (config-reconciler); verified against a live API server (minikube k8s 1.30).
Date: 2026-10-08. Author: Sajid Riaz.

## Context

`ConfigMapStore` (ADR-0001) defines the contract the reconcile engine needs. This ADR records
the fabric8 implementation (`KubeConfigMapStore`) and, in particular, how `putIfAbsent` is made
concurrency-safe at the Kubernetes API boundary — the one guarantee an in-memory fake cannot
establish.

## Decision

- One instance is bound to one namespace.
- `get` returns `Optional.empty()` only for an absent ConfigMap or absent key; any API error
  throws `StoreException` (never mapped to empty).
- `upsert` reads the object and writes it back at its OBSERVED resourceVersion (optimistic
  concurrency); it requires the ConfigMap to exist (does not create it).
- `putIfAbsent` reads, checks the key, and writes at the observed resourceVersion. On HTTP 409
  (conflict) it RE-READS and RE-EVALUATES: if the key is now present it preserves it and returns
  false; otherwise it retries up to a bound, then throws on exhaustion. This is NOT SSA and NOT a
  generic patch — it is read/modify/update with conflict re-evaluation, which is what actually
  implements insert-if-absent against concurrent writers.
- Unrelated data keys and user-managed metadata are preserved; server-maintained metadata
  (resourceVersion, managedFields) is expected to change.

## Verification (live API, `KubeConfigMapStoreIT`, 6 tests)

- **The decisive race (deterministic via a pre-write test seam):** adapter reads the key absent;
  before its conditional write, another client inserts an operator value; the adapter's write hits
  409, re-reads, sees the key present, PRESERVES it, returns false. Verified on a real apiserver —
  real resourceVersion enforcement, not a fake.
- seed-when-absent; upsert preserves unrelated keys; empty-string is present not absent;
  write-to-missing-ConfigMap throws; conflict-retry exhaustion throws.

## Boundaries
- SSA field ownership is a separate concern (ADR-0004), not implemented by this adapter's writes.
- The deterministic race uses a package-private pre-write seam (no production effect).
