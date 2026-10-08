# Architecture

## The ownership model (the thing this repo is about)

Server-side apply (SSA) tracks, per field, which **field manager** owns it. A conflict occurs when
a manager tries to set a field another manager owns. Two managers writing one field is tolerated
under Helm 3 (client-side apply, last-writer-wins) and **rejected** under Helm 4 (SSA).

Two cases, two fixes:

```
Case A — ConfigMap data field                 Case B — CRD spec field
──────────────────────────                    ──────────────────────
chart writes .data.message                     chart writes spec.schemaVersion   (legitimate)
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

> Precise terms: in Case A the application is the **runtime source of truth** for the key and
> enforces it through reconciliation; it does NOT register a named SSA field manager for the key
> (ordinary resourceVersion-guarded updates). Named SSA field ownership is demonstrated by Case B.

## Modules

```
reconciler-core  (pure policy/concurrency; the only Kubernetes type in its API is WatchFactory→Watch)
  WatchLifecycle         single-lock reservation protocol managing one Watch + recovery
  Reconciler             derive desired → compare to observed destination → write by policy
  WriteStrategy          UPSERT (enforce) | PUT_IF_ABSENT (seed)
  ConfigMapStore         narrow read / conditional-write boundary (contract: ADR-0001)
  RecoveryScheduler      cancellable delayed execution; ExecutorRecoveryScheduler impl

config-reconciler  (the Case A application)
  KubeConfigMapStore     fabric8 adapter; concurrency-safe putIfAbsent, whole-resource optimistic
                         update, NOT named SSA (ADR-0003)
  ReconcileCoordinator   runtime coordination: coalescing, FAILED retry, periodic resync, close
                         (explicit state machine, ADR-0005)
  ConfigReconcilerApp    composition + resource lifecycle (watch → coordinator → Reconciler)
  Main                   env-configured entrypoint; bounded, hook-coordinated shutdown
  Dockerfile             multi-stage build → non-root runnable image

schemasync-operator  (the Case B operator)
  SchemaBinding CRD      spec (chart-owned) + status subresource (operator-written)
  BrokenSchemaSyncReconciler   SSA-applies spec.schemaVersion → co-ownership → conflict (the defect)
  FixedSchemaSyncReconciler    status-only + releaseStaleSpecOwnership handoff (the fix, ADR-0004)
  Names                  shared null/blank input validation
```

## Runtime data flow (Case A)

```
 source ConfigMap --watch event--> ConfigReconcilerApp.watchSource
                                         │ coordinator.trigger()
 periodic resync (timer) ----------------┤
 (re)install (startup/recovery) ---------┘
                                         ▼
                             ReconcileCoordinator  (single worker; coalesce + retry + resync)
                                         │  drain()
                                         ▼
                                   Reconciler.reconcile()
                                         │ derive desired (from source)
                                         │ get observed (adapter)         ── missing source → FAILED (preserve target)
                                         │ if differ → upsert             ── 409 conflict  → FAILED (coordinator retries)
                                         ▼
                        target ConfigMap .data.<key>   (runtime-enforced by the app)

 watch drops → onClosed → WatchLifecycle schedules recovery (backoff) → re-register → reconcile-on-(re)install
 FAILED pass → coordinator schedules a delayed retry (a real trigger expedites it)
```

Key separation: **WatchLifecycle** decides WHEN a watch exists and recovers it; **ReconcileCoordinator**
decides WHEN a reconcile runs (and retries/coalesces); **Reconciler** decides WHAT to write;
**ConfigMapStore** performs the write with the right concurrency semantics. None of them leak the
others' concerns.

## ReconcileCoordinator — runtime coordination (ADR-0005)

One lock guards a small state machine, chosen over a pile of booleans:

```
IDLE ──trigger──▶ RUNNING ──in-sync/wrote──▶ IDLE
                    │  dirty (triggers during a pass)        → one coalesced follow-up
                    │  FAILED                                → RETRY_PENDING (tokenized delayed retry)
RETRY_PENDING ──trigger (expedite)──▶ RUNNING   (invalidate token, cancel handle, immediate drain)
RETRY_PENDING ──timer fires, token current──▶ RUNNING
any ──close()──▶ CLOSED (terminal; invalidate token, cancel retry+resync handles)
```

Guarantees: at most one drain in flight; N triggers during a pass collapse to one follow-up;
FAILED is retried; a real trigger expedites a pending retry; a periodic resync is the fallback for
missed events and destination-only drift; `close()` prevents work that has not passed its
eligibility check (an admitted pass may complete — application shutdown awaits worker termination);
`trigger()` is shutdown-aware and never throws an executor rejection back into a watch callback;
an UNEXPECTED rejection while open is surfaced to an observer (outside the lock, exceptions
contained). Durations are validated/normalized at construction.

## Application lifecycle & resource ownership (ConfigReconcilerApp + Main)

```
NEW ──start() [one-time]──▶ STARTED ──close() [idempotent]──▶ CLOSED
       │ startPeriodicResync + registerWatch
       │ on failure: rollback (close) + rethrow
```

- Executors are created as locals and shut down if construction fails (no leaked daemon threads).
- `start()` is one-time; `close()` is idempotent; a startup failure rolls back to CLOSED.
- `watchSource` closes a freshly created watch if post-creation setup throws (no leak, since the
  lifecycle never received the handle).
- `Main` owns the client from creation so a construction failure still closes it; the shutdown hook
  SIGNALS the main thread and WAITS (bounded) for cleanup, because the JVM waits for hooks — not
  main-thread work. Cleanup is ordered `app.close → client.close → release hook`, each in its own
  `finally`, so a `client.close()` failure cannot strand the hook.
- **Missing-source policy:** if the source ConfigMap/data/`desired` key is absent, the pass is
  FAILED and performs no write — the target's last value is PRESERVED, never clobbered with a
  default (an accidental source deletion must not overwrite the target).

## Concurrency design (WatchLifecycle, ADR-0002)

One `ReentrantLock` guards all state. The unit of work is an `Attempt` whose identity (a monotonic
generation) is claimed under the lock and carried through creation and install — never re-minted
after validation. Recovery is a `RecoveryReservation` scheduled via the (dumb) `RecoveryScheduler`;
the lifecycle owns all policy (eligibility, coalescing, backoff from a watch's actual live
duration, shutdown invalidation). Factory calls, scheduling, cancellation, and `Watch.close()` all
happen OUTSIDE the lock. Invariants, the transition table, and the invariant→test→evidence map are
in ADR-0002.

## Boundaries / out of scope (acknowledged, not hidden)
- An operational **health/readiness surface** (watch installed, recovery pending, DEGRADED,
  consecutive failures, last success) is not yet exposed; `Health.ACTIVE` is intentionally NOT a
  readiness signal.
- **Multi-pod coordination** (several replicas deriving conflicting desired values) needs leader
  election or a shared lock — out of scope.
- fabric8 terminal-notification reliability for `Watch.close()` is assumed; the lifecycle cannot
  compensate for a close that is never reported.

## Decision records
ADR-0001 reconciliation contract · ADR-0002 watch lifecycle & recovery · ADR-0003 fabric8 adapter ·
ADR-0004 operator field ownership · ADR-0005 runtime coordination. Handled scenarios with their
verifying tests: `docs/SCENARIOS.md`.
