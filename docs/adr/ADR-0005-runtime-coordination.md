# ADR-0005: Runtime coordination (coalescing, retry, resync, shutdown)

Status: Accepted (config-reconciler). Unit-verified (coordinator, watch-leak) and
integration-verified (autonomous resync drift repair) on minikube k8s 1.30.
Date: 2026-10-08. Author: Sajid Riaz.

## Context

`Reconciler` (ADR-0001) decides WHAT to write; `WatchLifecycle` (ADR-0002) manages the watch.
Neither coordinates WHEN reconciliation runs. An earlier `ConfigReconcilerApp` submitted a task
per watch event to an unbounded single-thread queue and only LOGGED a `FAILED` outcome. That had
three defects: (a) no real coalescing — a burst accumulated unbounded tasks; (b) a `FAILED` pass
was never retried unless another event happened to arrive; (c) target-only drift was never
observed because only the source is watched. A watch-acquisition failure path could also leak a
freshly created watch, and the JVM shutdown hook could return before cleanup finished.

## Decision

Introduce `ReconcileCoordinator` between event sources and `Reconciler` (the engine is unchanged):

- **At most one drain in flight.** A trigger either starts a drain or, if one is running, sets a
  DIRTY flag; the running drain re-runs exactly once more when it finishes. N triggers during a
  pass collapse into ONE follow-up — real coalescing, distinct from the engine's skip-if-in-sync.
- **Bounded queue.** At most one drain task is queued, so bursts cannot pile up.
- **Retry after FAILED.** A `FAILED` outcome schedules a delayed retry (unless a trigger dirtied
  it sooner). This repairs e.g. "target ConfigMap briefly missing" with no new source event.
- **Periodic resync.** A timer triggers a pass every interval — the fallback for events missed
  during a disconnection AND for destination-only drift the source watch never sees.
- **Closed is terminal.** After `close()` no drain starts/schedules; `trigger()` is a no-op and
  swallows executor rejection, so a late watch callback never throws `RejectedExecutionException`
  back into fabric8.

<b>Lock discipline.</b> The unexpected-rejection observer (an infrastructure callback) is invoked
OUTSIDE the coordinator lock and wrapped so a throwing observer cannot escape or re-enter under the
lock. Under the lock only the state transition happens (and, on a rejected immediate submit, the
stale `dirty` flag is cleared so it cannot leak forward to a later trigger).

<b>Close guarantee (precise).</b> `close()` invalidates the retry token and cancels the retry/resync
handles outside the lock. It prevents any queued retry/drain that has NOT passed its
state/token eligibility check from reaching the engine; a pass already admitted may complete.
`close()` alone does NOT wait for an in-flight pass — `ConfigReconcilerApp.close()` awaits
worker-executor termination for that.

<b>Missing-source policy.</b> `deriveFromSource` THROWS when the source ConfigMap is absent, has no
data, or lacks the `desired` key, making the pass `FAILED` and performing no write — the current
target value is PRESERVED. An accidental source deletion must not overwrite the target with a
fabricated default. (`missing_source_preserves_the_target...` asserts this on a live cluster.)

Watch acquisition is leak-safe: `createThenSetup` closes the created watch (suppressing any close
error) if post-creation setup throws, because the lifecycle never received the handle.

Shutdown is bounded and coordinated: `ConfigReconcilerApp.close()` deactivates the lifecycle,
closes the coordinator, shuts the executors down and AWAITS their termination within a budget
(`shutdownNow()` only interrupts). `Main`'s shutdown hook SIGNALS the main thread and WAITS (with a
bound) for cleanup to finish, because the JVM waits for hooks — not for main-thread work — so a
hook that merely releases a latch can let the JVM exit mid-cleanup. The Kubernetes client stays
open until application work finishes or the budget expires.

## Verification

- `ReconcileCoordinatorTest` (unit): 50 triggers during a blocked pass collapse into exactly one
  follow-up; a FAILED pass schedules a retry with no new trigger; a closed coordinator declines
  work; a trigger after executor shutdown does not throw.
- `ConfigReconcilerAppWatchLeakTest` (unit): setup failure closes the created watch and rethrows
  (suppressing a failing close); successful setup returns the watch unclosed.
- `ConfigReconcilerAppIT` (live): initial enforce + source-change propagation; an explicitly
  triggered pass repairs drift (repair-when-triggered); **periodic resync repairs target drift
  autonomously** with no source event and no manual trigger.

## Boundaries
Multi-pod coordination (several replicas deriving conflicting desired values) is out of scope and
would need leader election or a shared lock. A destination watch (in addition to periodic resync)
would reduce drift-repair latency; resync is the simpler, sufficient fallback chosen here.
