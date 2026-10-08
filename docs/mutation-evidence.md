# Mutation evidence (old-fails / new-passes)

Regression tests are only as good as their ability to FAIL against the bug they target. Each
regression below was demonstrated by mutating the production code, running the single targeted
test, observing the failure, and restoring the correct source (restored from a saved copy, not
`git checkout`). Reproduce with the commands shown; the captured failures are the actual output.

Environment: JDK 21 (Temurin), Maven 3.9.10. Integration tests require a reachable cluster
(minikube/kind); the two mutations below are a unit test and a failsafe IT respectively.

## 1. The re-mint race (WatchLifecycle)

**Bug injected:** `runRecovery` dispatches the public `registerWatch()` (which mints a NEW
identity) instead of `createAndInstall(attempt)` (which carries the authorized identity).

```
# in runRecovery(...), after afterRecoveryAuthorized.run():
-   createAndInstall(attempt);       // carries the authorized identity; no re-mint
+   registerWatch();                 // MUTANT: re-mint instead of carrying authorized identity
```

**Command:**
```
./mvnw -pl reconciler-core -am test \
  -Dtest='WatchLifecycleTest#authorization_gap_recovery_superseded_before_creation_dispatch'
```

**Result — MUTANT FAILS:**
```
WatchLifecycleTest.authorization_gap_recovery_superseded_before_creation_dispatch:289
  explicit B must remain installed; stale R cannot displace it ==> expected: <true> but was: <false>
Tests run: 1, Failures: 1
BUILD FAILURE
```
**Restored — PASSES:** `Tests run: 1, Failures: 0 … BUILD SUCCESS`.

## 2. No-op cancel (ExecutorRecoveryScheduler)

**Bug injected:** `schedule(...)` returns a `Cancellable` whose `cancel()` does nothing.

```
-   return () -> future.cancel(false);   // best-effort; a running task is not interrupted
+   return () -> { /* MUTANT: no-op cancel */ };
```

**Command:**
```
./mvnw -pl reconciler-core -am verify \
  -Dit.test='ExecutorRecoverySchedulerIT#cancel_prevents_a_not_yet_started_task' -DfailIfNoTests=false
```

**Result — MUTANT FAILS:**
```
ExecutorRecoverySchedulerIT.cancel_prevents_a_not_yet_started_task:73
  cancelled target must NOT have run ==> expected: <0> but was: <1>
Tests run: 1, Failures: 1
BUILD FAILURE
```
**Restored — PASSES:** `Tests run: 1, Failures: 0 … BUILD SUCCESS`.

## 3. Coordinator close-guard (ReconcileCoordinator)

**Bug injected:** `runRetry` drops its token/closed guard, so a stale/dequeued retry proceeds to
`drain()` even after `close()`.

```
-   if (state != State.RETRY_PENDING || retryToken != token) return;  // superseded/closed
-   state = State.RUNNING;
+   state = State.RUNNING;
```

**Command:**
```
./mvnw -pl config-reconciler -am test \
  -Dtest='ReconcileCoordinatorTest#close_cancels_a_pending_retry_and_prevents_post_close_reconcile' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

**Result — MUTANT FAILS:**
```
close_cancels_a_pending_retry_and_prevents_post_close_reconcile:134
  no reconcile pass may run after close ... ==> expected: <1> but was: <2>
Tests run: 1, Failures: 1 ... BUILD FAILURE
```
**Restored — PASSES.** This proves the close/retry correctness comes from the token/closed check,
not from cancellation.

## Scope note

These three are the only claims in the ADRs marked **DEMO** (demonstrated regression). All other
lifecycle/scheduler/reconciler tests are **BEHAV** (they pin current behaviour under deterministic
conditions) or **STRUCT** (source review). See ADR-0002 for the full invariant→test→evidence map.
