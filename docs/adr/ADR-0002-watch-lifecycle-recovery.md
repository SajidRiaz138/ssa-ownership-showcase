# ADR-0002: Watch lifecycle and recovery protocol

Status: Accepted for reconciler-core unit scope (fakes). Real-executor and Kubernetes boundaries
remain unverified. Date: 2026-10-08. Author: Sajid Riaz.

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

Evidence types: **DEMO** = demonstrated old-fails/new-passes (mutation or old impl);
**BEHAV** = behavioural test under deterministic conditions; **STRUCT** = source review.

| Invariant / behaviour | Test | Evidence |
|---|---|---|
| 1 identity survives / 5 stale cannot displace (pre-creation authorization gap) | `authorization_gap_recovery_superseded_before_creation_dispatch` (seam pauses OUTSIDE the factory, after authorize, before dispatch; threaded, retained Future) | **DEMO**: mutating `runRecovery` to re-mint via `registerWatch()` makes it FAIL ("B must remain installed ==> expected true but was false"); the correct `createAndInstall(attempt)` PASSES. |
| 1 / 3 explicit install invalidates pending recovery | `pending_recovery_is_invalidated_by_an_explicit_install` | **DEMO** (against the prior generation-token impl) + BEHAV: asserts the detached handle is cancelled and the stale task installs nothing. |
| 2 only current installs; cleanup (DURING creation) | `during_creation_supersession_rejects_and_cleans_up_the_stale_candidate` (single-threaded factory seam) | BEHAV. Explicitly labelled as during-creation supersession, NOT the pre-creation gap above. |
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
| scheduler: not inline / per-task cancel / shared-executor ownership / rejection→DEGRADED / negative-delay reject | `ExecutorRecoverySchedulerIT` (5 tests, failsafe) | BEHAV (bounded waits, no sleeps-as-assertion). Cancellation test uses a blocked single-thread executor so a no-op cancel would fail. |

## Known gaps (honestly still absent)
- Scheduling handle PUBLICATION races — "schedule() returns after supersession/shutdown" and
  "rejection after supersession/shutdown" — are handled in source (handle recorded only if the
  reservation is still current; else cancelled) but are **STRUCT only**, not each a dedicated test.
- "Shutdown DURING factory execution" is covered indirectly (deactivate bumps generation so a
  candidate returned afterwards is rejected) but not a dedicated threaded test.
- "Replacement creation fails while old watch healthy" — `onCreateFailed` retains the old watch
  and schedules recovery only if the attempt is current — is **STRUCT only**.
- The owning `ExecutorRecoveryScheduler` actually shutting down its executor is not directly asserted.
- Reconciler "unknown remote outcome" (write commits then store throws) is documented but not a
  dedicated test (the fake throws only BEFORE writes).
- **Real Kubernetes API-boundary behaviour and SSA ownership remain unverified** (next increment).

## Verification (this revision)
26 tests pass (13 lifecycle + 13 reconciler) on JDK 21 / Maven 3.9.10. The decisive regression
fails against the previous implementation and passes against this one. Fakes
(`ManualScheduler`, `InMemoryStore`) are single-threaded and prove protocol logic only.
