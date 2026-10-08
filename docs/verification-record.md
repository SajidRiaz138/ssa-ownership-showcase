# Verification record

The reviewer asked that three things be recorded SEPARATELY: corrected source, reviewed test
coverage, and executed verification. This document does that for the lifecycle-acceptance scope.
Environment: JDK 21 (Temurin), Maven 3.9.10, minikube Kubernetes 1.30 (integration tests).

---

## Part A — corrected source (what changed, where to read it)

| Finding | File | What to read |
|---|---|---|
| during-creation test asserts the explicit winner | `reconciler-core/.../WatchLifecycleTest.java` | `during_creation_supersession_rejects_and_cleans_up_the_stale_candidate` — `winnerBox[0]` is written at the nested registration and asserted via `debugIsInstalled` |
| handle publication / rejection / shutdown races | same file | `ControllableScheduler` (nested) + `recovery_runs_before_schedule_returns_its_handle...`, `handle_returned_after_supersession...`, `handle_returned_after_shutdown...`, `scheduling_rejected_after_supersession...`, `scheduling_rejected_after_shutdown...`, `shutdown_while_factory_creation_blocked...`, `replacement_creation_fails_while_old_healthy...` |
| duration overflow normalised, named | `reconciler-core/.../WatchLifecycle.java` | `toMillisOrReject` / `toNanosOrReject`; test `constructor_normalizes_duration_overflow_into_a_named_config_error` |
| pseudocode / inner comment wording | `WatchLifecycle.java` | install pseudocode line + inner reject comment now say "its close notification has not invalidated the attempt" |
| scheduler duration doc | `ExecutorRecoveryScheduler.java` | truncation / 1ms / overflow wording |
| scheduler ownership doc | `RecoveryScheduler.java` | "Executor ownership is implementation-specific; a shared executor remains externally owned." |
| commit-then-throw reconcile | `reconciler-core/.../ReconcilerTest.java` | `CommitThenThrowStore` + `upsert_commit_then_throw...`, `putIfAbsent_commit_then_throw...` |
| SKIPPED_PRESENT log level | `Reconciler.java` | `seedIfAbsent` now logs at DEBUG |
| ConfigMapStore.get() contract | `ConfigMapStore.java` | method javadoc: "empty if the ConfigMap does not exist OR the key is absent" |
| InMemoryStore wording | `reconciler-core/.../InMemoryStore.java` | "must be verified against a real API server (KubeConfigMapStoreIT)" |
| ManualScheduler threading rule | `reconciler-core/.../ManualScheduler.java` | permits ordered cross-thread handoff with explicit happens-before; prohibits unsynchronised concurrent bookkeeping; `runLast()` selects an unexecuted task or fails |
| cancellation IT cleanup | `reconciler-core/.../ExecutorRecoverySchedulerIT.java` | retained blocking `Future`, released in `finally`, `awaitTermination`, observes completion |
| owning scheduler shutdown | same file | `owning_scheduler_shuts_down_its_own_executor_on_close` |
| recording-executor tests | `reconciler-core/.../ExecutorRecoverySchedulerTest.java` | asserts submitted delay AND `TimeUnit.MILLISECONDS`; no unused imports |
| reconcile-on-recovery | `config-reconciler/.../ConfigReconcilerApp.java` + `ConfigReconcilerAppIT.java` | `watchSource` reconciles on (re)install; `reconcile_on_reinstall_repairs_drift_without_a_source_event` |

---

## Part B — reviewed test coverage (inventory)

All tests, by class (surefire = unit, failsafe = `*IT` integration):

| Class | Kind | Tests |
|---|---|---|
| `WatchLifecycleTest` | unit | 24 |
| `ReconcilerTest` | unit | 15 |
| `ExecutorRecoverySchedulerTest` | unit | 4 |
| `ExecutorRecoverySchedulerIT` | IT | 6 |
| `ReconcileCoordinatorTest` | unit | 6 |
| `ConfigReconcilerAppWatchLeakTest` | unit | 3 |
| `ConfigReconcilerAppIT` | IT (cluster) | 3 |
| `KubeConfigMapStoreIT` | IT (cluster) | 10 |
| `SchemaSyncOwnershipIT` | IT (cluster) | 3 |
| **Total** | | **74** |

> Count is from the committed revision's failsafe/surefire reports under `-Plive-it`.

Evidence classes (see ADR-0002 for the per-invariant map): **DEMO** = demonstrated by executed
mutation (2 of them, below); **BEHAV** = deterministic current-behaviour; **STRUCT** = source
review. Only the two mutation rows are DEMO.

---

## Part C — executed verification (captured output)

### C.1 Full reactor build (minikube k8s 1.30, mandatory `-Plive-it`)

```
./mvnw clean verify -Plive-it     # ITs must run; an unreachable cluster FAILS (no silent skip)

reconciler-core  : WatchLifecycleTest 24, ReconcilerTest 15, ExecutorRecoverySchedulerTest 4  (unit)
                   ExecutorRecoverySchedulerIT 6                                               (IT)
config-reconciler: ReconcileCoordinatorTest 6, ConfigReconcilerAppWatchLeakTest 3             (unit)
                   ConfigReconcilerAppIT 3, KubeConfigMapStoreIT 10                            (IT, cluster)
schemasync-operator: SchemaSyncOwnershipIT 3                                                  (IT, cluster)
Reactor: all SUCCESS
BUILD SUCCESS   (74 tests, 0 failures, 0 errors, 0 skipped)
```

### C.2 Mutation 1 — no-op `cancel()` (ExecutorRecoveryScheduler)

Mutant: `schedule(...)` returns `() -> { /* MUTANT */ }`.
```
ExecutorRecoverySchedulerIT.cancel_prevents_a_not_yet_started_task:74
  cancelled target must NOT have run ==> expected: <0> but was: <1>
Tests run: 1, Failures: 1 ... BUILD FAILURE
```
Restored (`() -> future.cancel(false)`): `Tests run: 1, Failures: 0 ... BUILD SUCCESS`.

### C.3 Mutation 2 — re-mint on the recovery path (WatchLifecycle, line 325)

Mutant: `runRecovery` dispatches `registerWatch()` instead of `createAndInstall(attempt)`.
```
WatchLifecycleTest.authorization_gap_recovery_superseded_before_creation_dispatch:289
  explicit B must remain installed; stale R cannot displace it ==> expected: <true> but was: <false>
Tests run: 1, Failures: 1 ... BUILD FAILURE
```
Restored: `Tests run: 1, Failures: 0 ... BUILD SUCCESS`.

> Note: the mutant was confirmed APPLIED (the injected marker present and the test actually
> FAILED) before recording — an earlier substitution that silently did not match was discovered
> and discarded rather than reported as a pass.

---

## Still outside this scope (unchanged, by design)
Real fabric8 terminal-notification semantics beyond the live ITs, cross-pod coordination, and
runtime trigger coalescing/retry/shutdown-coordination of outstanding factory calls are
application/adapter concerns, not core-policy ones. `deactivate()` prevents later installation; it
does not wait for an already-authorized factory call to finish — application shutdown must account
for that separately.
