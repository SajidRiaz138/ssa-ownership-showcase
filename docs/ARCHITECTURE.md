# Architecture

## The ownership model (the thing this repo is about)

Server-side apply tracks, per field, which **field manager** owns it. A conflict occurs when a
manager tries to set a field another manager owns. Two managers writing one field is tolerated
under Helm 3 (client-side apply, last-writer-wins) and **rejected** under Helm 4 (SSA).

Two cases, two fixes:

```
Case A — ConfigMap data field                 Case B — CRD spec field
──────────────────────────                    ──────────────────────
chart writes .data.message                    chart writes spec.schemaVersion   (legitimate)
app   writes .data.message   (overlap)         operator ALSO writes spec.schemaVersion (defect)
      │                                               │
   SSA conflict                                   SSA conflict
      │                                               │
FIX: chart stops writing it;                   FIX: operator writes status.* only;
     the app owns it at runtime                     ownership HANDOFF releases the stale
     (enforcing reconciler)                         spec claim without deleting the value
```

Case A is fixable from the chart. Case B is **not** — the chart must own `spec.schemaVersion`;
the operator must change. That asymmetry is the key architectural lesson.

## Modules

```
reconciler-core  (no Kubernetes client in its API surface except WatchFactory→Watch)
  WatchLifecycle         single-lock reservation protocol managing one Watch + recovery
  Reconciler             derive desired -> compare to observed destination -> write by policy
  WriteStrategy          UPSERT (enforce) | PUT_IF_ABSENT (seed)
  ConfigMapStore         narrow read/conditional-write boundary (contract in ADR-0001)
  RecoveryScheduler      cancellable delayed execution; ExecutorRecoveryScheduler impl

config-reconciler  (the Case A application)
  KubeConfigMapStore     fabric8 adapter; concurrency-safe putIfAbsent (ADR-0003)
  ConfigReconcilerApp    wires: watch source -> worker runs Reconciler -> enforce target field
  Main                   env-configured entrypoint; Dockerfile builds a runnable image

schemasync-operator  (the Case B operator)
  SchemaBinding CRD      spec (chart-owned) + status subresource (operator-owned)
  BrokenSchemaSyncReconciler   co-owns spec.schemaVersion -> conflict (the defect)
  FixedSchemaSyncReconciler    status-only + releaseStaleSpecOwnership handoff (the fix, ADR-0004)
```

## Data flow (Case A, runtime)

```
 source ConfigMap  --watch event-->  WatchLifecycle  --(worker)-->  Reconciler.reconcile()
                                                                        │ derive desired
                                                                        │ get observed (adapter)
                                                                        │ if differ -> upsert
                                                                        ▼
                                                      target ConfigMap .data.message (app-owned)
 watch drops  -->  onClosed  -->  lifecycle schedules recovery (backoff) -->  re-register
```

## Concurrency design (WatchLifecycle)

One `ReentrantLock` guards all state. The unit of work is an `Attempt` whose identity (a
generation) is claimed under the lock and carried through creation and install — never re-minted
after validation. Recovery is a `RecoveryReservation` scheduled via the (dumb) scheduler;
lifecycle owns all policy (eligibility, coalescing, backoff from a watch's actual live duration,
shutdown invalidation). Factory calls, scheduling, cancellation, and `Watch.close()` all happen
OUTSIDE the lock. Invariants and the transition table are in ADR-0002.
