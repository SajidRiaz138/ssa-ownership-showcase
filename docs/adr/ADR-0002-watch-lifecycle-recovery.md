# ADR-0002: Watch lifecycle and recovery protocol

Status: Accepted for reconciler-core scope. Unit coverage (fakes) plus a real-executor
integration suite; the Kubernetes watch/SSA boundary is verified in config-reconciler and
schemasync-operator, not here. Date: 2026-10-08. Author: Sajid Riaz.

## Context

A watch-driven controller must survive concurrent (re)registration, late and duplicate close
callbacks, a watch that closes before it is installed, shutdown racing creation, and recovery
scheduling races. An earlier generation-token implementation still contained an identity race:
`runRecovery` validated a reservation and then called an unrestricted registration that minted a
NEW identity, allowing a stale recovery to displace a newer watch.

## Decision — one reservation protocol, one lock

The unit of work is an `Attempt`, identified by a monotonic generation claimed UNDER the lock.
Once authorized, that SAME attempt is carried through creation and install; no later step
re-mints identity. Both entry points reduce to one creation path:

```
registerWatch():          under lock -> invalidate+detach pending recovery; authorize Attempt
                          outside lock -> cancel detached handle; createAndInstall(attempt)
runRecovery(reservation): under lock -> if reservation still current, consume it and authorize an
                                        Attempt with the SAME generation
                          outside lock -> createAndInstall(attempt)   // never public registerWatch
createAndInstall(attempt):outside lock -> factory.create(onClosed bound to attempt.generation)
                          under lock   -> install IFF attempt is still current AND active
                          outside lock -> close rejected/superseded candidate
```

## Invariants (review criteria)

1. Identity survives execution — an authorized attempt is never silently swapped.
2. Only the current eligible attempt installs; superseded candidates are cleaned up.
3. Explicit registration invalidates+detaches pending recovery under the lock; cancels outside.
4. Closing the old installed watch detaches it; it does NOT invalidate an authorized replacement.
5. Recovery completion/rejection/cancellation cannot modify a newer reservation's state.
6. Shutdown is terminal; late factory results / scheduler handles cannot reactivate.
7. Factory, scheduling, cancellation, and close all happen outside the lock.

## Transition table

| Event | Required behaviour |
|---|---|
| Explicit registration | Under lock: invalidate+detach pending recovery, authorize new Attempt. Outside: cancel detached handle; create+install. |
| Old installed watch closes while a replacement is in flight | Detach the old watch; record its closure; do NOT supersede or cancel the in-flight replacement; do not schedule competing recovery. |
| Replacement creation fails while old watch healthy | The failing attempt arranges recovery only if it is still current; a healthy installed watch is retained. Retry continues via the scheduled recovery for the current attempt. |
| Recovery task executes | Under lock: consume the matching reservation, authorize an Attempt with the same generation. Outside: create+install that exact attempt. |
| Scheduling returns late or rejects | Change state only if the reservation is still current. Late handle for a superseded reservation is cancelled. Rejection of a current reservation → DEGRADED. |
| Shutdown | Terminal: bump generation (invalidate in-flight), detach installed, detach pending recovery; cancel/close outside the lock. |

## Degraded-state recovery (explicit)

If the scheduler REJECTS a recovery for the current reservation, health becomes `DEGRADED` and no
recovery is pending. DEGRADED is observable via `health()`. Recovery resumes when an EXTERNAL
trigger calls `registerWatch()` (e.g. a new source event, periodic resync, or operator action);
`registerWatch` clears DEGRADED back to ACTIVE on a successful install. DEGRADED is therefore a
"needs an external nudge" state, not a dead end — and it is surfaced, not silent.

## Backoff

Exponential with saturating arithmetic (no overflow), capped at `maxDelay`. The failure streak
resets only when an installed watch's ACTUAL live duration (install→close) exceeds
`stableResetAfter` — measured at the close transition, so recovery/creation latency cannot be
mistaken for stability. Configuration validated at construction (positive base/stability,
maxDelay ≥ baseDelay).

## Invariant → test mapping (evidence type is explicit)

Evidence types are stated per row so deterministic current-behaviour coverage is not confused
with demonstrated regression evidence:
- **DEMO** — a regression DEMONSTRATED by execution: the test FAILS against a specifically
  mutated/old implementation and PASSES against the current one, with the failure captured
  (see "Mutation evidence" below). This is the strongest claim.
- **BEHAV** — a behavioural test that pins the CURRENT behaviour under deterministic conditions
  (controlled clock/scheduler, forced interleaving). It proves the code does what is claimed now;
  it is not, by itself, proof that it would catch every regression.
- **STRUCT** — justified by source review only; no dedicated executing test.

| Invariant / behaviour | Test | Evidence |
|---|---|---|
| 1 identity survives / 5 stale cannot displace (pre-creation authorization gap) | `authorization_gap_recovery_superseded_before_creation_dispatch` (seam pauses OUTSIDE the factory, after authorize, before dispatch; threaded, retained Future) | **DEMO**: mutating `runRecovery` to re-mint via `registerWatch()` makes it FAIL ("B must remain installed ==> expected true but was false"); the correct `createAndInstall(attempt)` PASSES. |
| 1 / 3 explicit install invalidates pending recovery | `pending_recovery_is_invalidated_by_an_explicit_install` | **DEMO** (against the prior generation-token impl) + BEHAV: asserts the detached handle is cancelled and the stale task installs nothing. |
| 2 only current installs; cleanup (DURING creation) | `during_creation_supersession_rejects_and_cleans_up_the_stale_candidate` (single-threaded factory seam; captures the explicit replacement B at registration and asserts `debugIsInstalled(B)`, so a stale recovery winning would fail — not merely "some one watch installed") | BEHAV. Explicitly labelled as during-creation supersession, NOT the pre-creation gap above. |
| 2 close-before-install | `close_before_install_is_handled_without_a_probe_and_schedules_recovery` | BEHAV (production callback, no probe; mechanism documented in source: generation invalidation). |
| 4 old close doesn't supersede in-flight replacement | `old_watch_closes_while_replacement_blocked_in_factory` (threaded; asserts the INTERMEDIATE transition: A detached, no pending recovery, nothing queued, while B blocked; then B installs) | BEHAV. |
| 4 duplicate/stale close of old watch | `duplicate_close_callbacks_do_not_create_duplicate_recovery` | BEHAV. |
| 5 stale task after shutdown | `stale_recovery_task_run_after_deactivate_creates_no_watch_and_was_cancelled` (asserts the task WAS cancelled AND eligibility still protects) | BEHAV. |
| 6 shutdown terminal; cancels pending recovery | `install_then_deactivate_closes_installed`, `deactivate_cancels_a_pending_recovery` (creates pending recovery first, asserts the handle cancelled) | BEHAV. |
| recovery queued not inline | `installed_watch_close_schedules_recovery_queued_not_inline` | BEHAV. |
| no recursion/spin | `repeated_immediate_close_does_not_recurse_or_spin` (exact counts) | BEHAV. |
| backoff from live duration (latency ≠ stability) | `slow_recovery_latency_does_not_reset_the_streak` (one lifecycle; asserts streak 2 AND next delay 2s), `genuinely_stable_watch_resets_the_failure_streak` | BEHAV (controlled clock). |
| rejection → DEGRADED | `rejected_scheduling_enters_DEGRADED_not_silent_pending_forever` | BEHAV (immediate rejection of the current reservation). |
| concurrency: one installs, no leak | `concurrent_registration_installs_exactly_one_no_leak` (retained Futures propagate worker errors; awaits termination) | BEHAV (contention; not a specific forced interleaving — those are the targeted tests above). |
| 5 handle PUBLICATION: schedule() returns after task already ran | `recovery_runs_before_schedule_returns_its_handle_then_handle_is_cancelled` (controllable scheduler blocks schedule()'s return; the task runs on a real thread first) | BEHAV. Asserts the late handle is cancelled and B installed. |
| 5 handle returns after supersession | `handle_returned_after_supersession_is_cancelled_and_newer_work_unaffected` | BEHAV. Asserts stale handle cancelled, newer B untouched. |
| 6 handle returns after shutdown | `handle_returned_after_shutdown_is_cancelled_and_no_reactivation` | BEHAV. Asserts handle cancelled, health DEACTIVATED, no pending recovery. |
| 5 rejection after supersession | `scheduling_rejected_after_supersession_does_not_mark_newer_work_degraded` (schedule() blocks then THROWS on release, after B supersedes) | BEHAV. Asserts health NOT DEGRADED, B installed. |
| 6 rejection after shutdown | `scheduling_rejected_after_shutdown_keeps_health_deactivated` (schedule() throws after deactivate) | BEHAV. Asserts health stays DEACTIVATED. |
| 6 shutdown DURING factory creation | `shutdown_while_factory_creation_blocked_closes_the_late_candidate_and_never_installs` (threaded; deactivate while create() blocked) | BEHAV. Asserts the late candidate is closed and never installed. |
| 4 replacement create fails while old healthy | `replacement_creation_fails_while_old_healthy_keeps_old_installed_and_follows_recovery_policy` | BEHAV. Asserts A stays installed/u-closed, recovery scheduled, then recovery supersedes A. |
| config: duration overflow normalised | `constructor_normalizes_duration_overflow_into_a_named_config_error` | BEHAV. Asserts IllegalArgumentException naming baseDelay / stableResetAfter. |
| scheduler: not inline / per-task cancel / shared-executor ownership / rejection→DEGRADED / negative-delay reject | `ExecutorRecoverySchedulerIT` (5 tests, failsafe) | BEHAV (bounded waits, no sleeps-as-assertion). | 
| scheduler cancel actually prevents a queued task | `ExecutorRecoverySchedulerIT#cancel_prevents_a_not_yet_started_task` (blocked single-thread executor; retained blocking Future, released in finally, awaits termination) | **DEMO**: a no-op `cancel()` makes it FAIL ("cancelled target must NOT have run ==> expected: <0> but was: <1>"); the real `future.cancel(false)` PASSES. |
| scheduler sub-ms→1ms, zero stays zero, >1ms truncates, overflow rejected | `ExecutorRecoverySchedulerTest` (recording executor captures submitted delay/unit; not elapsed-time) | BEHAV. |

## Mutation evidence (executed, not predicted)

Two regressions were demonstrated by mutating the production code, running the targeted test, and
restoring from a saved copy (not `git checkout`):

1. **Re-mint race.** `runRecovery` changed to dispatch `registerWatch()` (minting a new identity)
   instead of `createAndInstall(attempt)` (carrying the authorized one). Result:
   `authorization_gap_recovery_superseded_before_creation_dispatch` FAILS with
   *"explicit B must remain installed; stale R cannot displace it ==> expected: <true> but was:
   <false>"*. Restored → passes.
2. **No-op cancel.** `ExecutorRecoveryScheduler.schedule` changed to return a do-nothing
   `Cancellable`. Result: `cancel_prevents_a_not_yet_started_task` FAILS with
   *"cancelled target must NOT have run ==> expected: <0> but was: <1>"*. Restored → passes.

These are the only two rows marked DEMO; every other row is BEHAV or STRUCT and labelled as such.

## Known gaps (honestly still absent)
- The handle-publication and rejection-ordering races are now BEHAV-tested (rows above) via a
  controllable scheduler, rather than STRUCT-only. What remains STRUCT: these tests fix ONE
  interleaving each (the one the harness forces); they are not an exhaustive schedule search.
- The owning `ExecutorRecoveryScheduler` actually shutting down ONLY its own executor on
  `close()` is asserted for the shared case (`shared_executor_is_not_shut_down...`); the owning
  case's shutdown is STRUCT only.
- Multi-pod coordination (several lifecycles/reconcilers deriving conflicting desired values) is
  explicitly OUT of scope for this component and belongs to the application architecture.
- Resynchronisation of events MISSED during a watch disconnection is a controller-application
  concern (periodic resync + reconcile-on-recovery), not established by restarting the watch
  alone. Owned by config-reconciler, documented there — not claimed here.

## Verification (this revision)
reconciler-core: **42 unit tests** (23 `WatchLifecycleTest` + 15 `ReconcilerTest` +
4 `ExecutorRecoverySchedulerTest`) and **5 integration tests** (`ExecutorRecoverySchedulerIT`),
all passing on JDK 21 / Maven 3.9.10. The two DEMO regressions fail against the mutated code and
pass against this revision (messages above). Fakes (`ManualScheduler`, `InMemoryStore`) are
single-threaded and prove protocol logic only; the real executor is exercised by the IT suite and
the recording-executor unit tests.
