# Scenarios handled

Every scenario this system is designed to handle, mapped to the test(s) that verify it. This is a
coverage ledger, not a wish list: each row names a real test from the suite. Where a scenario is
argued from source rather than executed, it says so.

**Evidence legend**
- **DEMO** — demonstrated by an executed mutation: the test FAILS against the injected bug and
  PASSES against the current code (see `docs/mutation-evidence.md`).
- **BEHAV-unit** — deterministic unit test (fakes / controlled executor / controlled clock).
- **BEHAV-live** — integration test against a real cluster (minikube/kind), `-Plive-it`.
- **STRUCT** — justified by source review; no dedicated executing test (explicitly listed).

Totals at time of writing: **85 tests** (`-Plive-it`, minikube k8s 1.30). See
`docs/verification-record.md` for the executed build output.

---

## 1. SSA field ownership (the domain problem)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| Chart + app both write a ConfigMap key | SSA two-writer conflict under Helm 4 | `charts/` demo + `app_enforces_derived_value_and_propagates_source_changes` (app owns it at runtime) | BEHAV-live |
| Operator co-owns a chart-owned CRD field | Chart apply conflicts with HTTP **409** | `broken_operator_causes_a_chart_apply_conflict_409_and_co_ownership_is_visible` | BEHAV-live |
| Fixed operator writes `/status` only | No spec claim; chart apply conflict-free | `fixed_operator_writes_status_only_and_introduces_no_spec_claim` | BEHAV-live |
| Upgrade broken→fixed: ownership handoff | Drop the stale spec claim WITHOUT deleting the value (chart co-owns) | `ownership_handoff_drops_broken_spec_claim_but_chart_value_survives` | BEHAV-live |
| Who owns what | `managedFields` inspected structurally (manager + Apply op + `f:spec/f:schemaVersion` path) | both operator tests | BEHAV-live |

## 2. Reconciliation policy (Reconciler)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| Destination in sync | No write | `upsert_initially_in_sync_does_not_write`, `upsert_writes_then_skips_when_destination_in_sync` | BEHAV-unit |
| Destination drifted (changed) | Repaired to desired | `upsert_repairs_destination_drift_with_the_SAME_instance` | BEHAV-unit |
| Owned key externally deleted | Restored; unrelated keys preserved | `upsert_repairs_destination_deletion_with_the_SAME_instance` | BEHAV-unit |
| Empty-string value | Treated as present, in sync | `empty_string_value_counts_as_present_and_in_sync` | BEHAV-unit |
| Seed vs enforce | PUT_IF_ABSENT preserves an existing value; no diagnostic read | `putIfAbsent_seeds_when_absent_but_preserves_existing_same_instance`, `putIfAbsent_false_does_NOT_read_the_destination` | BEHAV-unit |
| Read / write failure | FAILED, retryable (no local success cache) | `get_failure_is_FAILED_and_retryable`, `upsert_write_failure_is_FAILED_and_retryable`, `putIfAbsent_write_failure_is_FAILED_and_retryable` | BEHAV-unit |
| Unknown remote outcome (write commits then throws) | FAILED; next pass settles to a benign skip | `upsert_commit_then_throw_is_FAILED_then_next_pass_is_SKIPPED_IN_SYNC`, `putIfAbsent_commit_then_throw_is_FAILED_then_next_pass_is_SKIPPED_PRESENT` | BEHAV-unit |
| Null derivation / missing CM / null deps | FAILED / rejected | `null_derivation_is_FAILED_not_thrown`, `missing_configmap_on_write_is_FAILED`, `constructor_rejects_null_dependencies` | BEHAV-unit |

## 3. Kubernetes conditional writes (KubeConfigMapStore, live API)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| Concurrent same-key insert races our seed | 409 → re-read → PRESERVE the other value → return false | `conditional_putIfAbsent_preserves_a_concurrent_insert` | BEHAV-live |
| Conflict from an UNRELATED update | Re-read, preserve the unrelated change, then insert | `conflict_from_unrelated_update_is_resolved_by_reread_then_insert` | BEHAV-live |
| Retry exhaustion | Throws `StoreException` (per contract — not a false return) | `conflict_retry_exhaustion_throws_immediately_with_zero_retries` | BEHAV-live |
| Read failure (unreachable API) | `StoreException`, never mapped to empty | `get_on_unreachable_api_throws_StoreException_not_absence` | BEHAV-live |
| Seed when truly absent | Writes; empty-string is present not absent | `putIfAbsent_seeds_when_truly_absent`, `empty_string_is_present_not_absent`, `putIfAbsent_on_existing_empty_string_returns_false_and_preserves_it` | BEHAV-live |
| Write to a missing ConfigMap | Throws (does not create) | `write_to_missing_configmap_throws` | BEHAV-live |
| Preserve unrelated content | data keys, labels, annotations, binaryData untouched | `upsert_preserves_unrelated_keys`, `writes_preserve_labels_annotations_and_binaryData` | BEHAV-live |

## 4. Runtime coordination (ReconcileCoordinator)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| Burst of triggers during a pass | Collapse into exactly ONE follow-up (true coalescing) | `triggers_during_a_pass_coalesce_into_exactly_one_follow_up` | BEHAV-unit |
| A pass FAILED | A delayed retry runs with no new trigger | `failed_outcome_schedules_a_retry_without_a_new_trigger` | BEHAV-unit |
| Real trigger during the retry delay | Expedite: cancel the delayed retry, drain now | `a_trigger_expedites_a_pending_retry` | BEHAV-unit |
| close() with a retry pending | Token invalidated; a deliberately-run stale retry does NOT reconcile; the real future is cancelled | `close_cancels_a_pending_retry_and_prevents_post_close_reconcile`, `close_cancels_the_actual_retry_future_and_a_stale_run_does_nothing` | **DEMO** + BEHAV-unit |
| Closed coordinator | Declines new work | `closed_coordinator_declines_new_work` | BEHAV-unit |
| Executor gone (trigger after shutdown) | Swallowed; never thrown back to the caller | `trigger_after_executor_shutdown_does_not_throw` | BEHAV-unit |
| Duration config | Sub-ms→1ms; zero/negative rejected; overflow names the parameter | `durations_are_validated_and_normalized`, `sub_millisecond_resync_is_normalized_and_actually_schedules` | BEHAV-unit |
| Unexpected rejection while open | Observer invoked once, outside the lock; not after close; a throwing observer is contained | `unexpected_rejection_while_open_invokes_observer_exactly_once`, `rejection_after_close_does_not_invoke_the_observer`, `a_throwing_observer_does_not_escape_through_trigger` | BEHAV-unit |

## 5. Watch lifecycle & recovery (WatchLifecycle)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| Pre-creation supersession (authorization gap) | A stale recovery cannot displace an explicit replacement | `authorization_gap_recovery_superseded_before_creation_dispatch` | **DEMO** |
| During-creation supersession | The explicit winner stays installed; stale candidate cleaned up | `during_creation_supersession_rejects_and_cleans_up_the_stale_candidate` | BEHAV-unit |
| Close before install | Handled without a probe; schedules recovery | `close_before_install_is_handled_without_a_probe_and_schedules_recovery` | BEHAV-unit |
| Old watch closes while replacement in flight | Old detached; in-flight replacement not superseded | `old_watch_closes_while_replacement_blocked_in_factory` | BEHAV-unit |
| Scheduler handle returned after task ran / after supersession / after shutdown | Late handle cancelled; newer work intact; no reactivation | `recovery_runs_before_schedule_returns_its_handle_then_handle_is_cancelled`, `handle_returned_after_supersession_is_cancelled_and_newer_work_unaffected`, `handle_returned_after_shutdown_is_cancelled_and_no_reactivation` | BEHAV-unit |
| Stale handle returns while a NEWER reservation is pending | Stale cancelled; the newer reservation still runs | `stale_handle_returning_while_a_newer_recovery_reservation_is_pending_leaves_it_intact` | BEHAV-unit |
| Scheduling rejected after supersession / shutdown | Newer work not DEGRADED; shutdown stays terminal | `scheduling_rejected_after_supersession_does_not_mark_newer_work_degraded`, `scheduling_rejected_after_shutdown_keeps_health_deactivated` | BEHAV-unit |
| Shutdown during factory creation | Late candidate closed, never installed | `shutdown_while_factory_creation_blocked_closes_the_late_candidate_and_never_installs` | BEHAV-unit |
| Replacement create fails while old healthy | Old stays installed; recovery scheduled | `replacement_creation_fails_while_old_healthy_keeps_old_installed_and_follows_recovery_policy` | BEHAV-unit |
| Duplicate / repeated-immediate close | No double recovery; no recursion/spin | `duplicate_close_callbacks_do_not_create_duplicate_recovery`, `repeated_immediate_close_does_not_recurse_or_spin` | BEHAV-unit |
| Backoff | Recovery latency ≠ stability; a genuinely stable watch resets the streak | `slow_recovery_latency_does_not_reset_the_streak`, `genuinely_stable_watch_resets_the_failure_streak` | BEHAV-unit |
| Shutdown terminal | Cancels pending recovery; installed watch closed; late results cannot reactivate | `install_then_deactivate_closes_installed`, `deactivate_cancels_a_pending_recovery`, `stale_recovery_task_run_after_deactivate_creates_no_watch_and_was_cancelled` | BEHAV-unit |
| Concurrent registration | Exactly one installs; no leak | `concurrent_registration_installs_exactly_one_no_leak` | BEHAV-unit |
| Duration config overflow | IllegalArgumentException naming the parameter | `constructor_normalizes_duration_overflow_into_a_named_config_error` | BEHAV-unit |

## 6. Recovery scheduler (ExecutorRecoveryScheduler)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| Never inline | Tasks run on an executor thread | `task_runs_on_executor_thread_not_inline` | BEHAV-live(IT) |
| Per-task cancel | A cancelled not-yet-started task does not run | `cancel_prevents_a_not_yet_started_task` | **DEMO** |
| Shared vs owning executor | Shared survives close(); owning is shut down and then rejects | `shared_executor_is_not_shut_down_by_a_lifecycle_scheduler`, `owning_scheduler_shuts_down_its_own_executor_on_close` | BEHAV-live(IT) |
| Negative delay / sub-ms / overflow | Rejected / 1ms / truncates / rejected (recording executor) | `negative_delay_is_rejected`, `positive_sub_millisecond_delay_is_submitted_as_one_millisecond_not_zero`, `above_one_millisecond_truncates_fractional_millis_not_rounds_up`, `conversion_overflow_is_rejected_as_illegal_argument` | BEHAV-unit |

## 7. Application lifecycle & resource ownership (ConfigReconcilerApp + Main)

| Scenario | Behaviour | Test | Evidence |
|---|---|---|---|
| End-to-end enforce + source change | Seeds, propagates, enforces | `app_enforces_derived_value_and_propagates_source_changes` | BEHAV-live |
| Reconcile on (re)install | A (re)install repairs drift with no source event | `reconcile_on_reinstall_repairs_drift_without_a_source_event` | BEHAV-live |
| Autonomous drift repair | Periodic resync repairs target drift (no trigger, no event) | `periodic_resync_repairs_target_drift_autonomously` | BEHAV-live |
| Missing source | FAILED; last target value PRESERVED (no default clobber) | `missing_source_preserves_the_target_and_does_not_clobber_with_a_default` | BEHAV-live |
| Watch-acquisition failure | Freshly created watch closed; close error suppressed; success returns unclosed | `setup_failure_closes_the_created_watch_and_rethrows`, `setup_failure_with_a_failing_close_suppresses_the_close_error`, `successful_setup_returns_the_watch_unclosed` | BEHAV-unit |
| start/close contract | one-time start; start-after-close rejected; idempotent close; blank config rejected | `start_after_close_is_rejected`, `repeated_close_is_harmless`, `close_without_start_is_harmless`, `blank_configuration_is_rejected_at_construction` | BEHAV-unit |
| start() rollback on failure | On a thrown start body, close() + rethrow | — | **STRUCT** (fabric8 `watch()` does not fail synchronously on a dead endpoint, so a synchronous start() throw can't be forced without a production seam; see the lifecycle test's note) |
| Bounded, coordinated shutdown | Awaits worker termination; hook waits for cleanup | — | **STRUCT** (exercised operationally; no dedicated JVM-shutdown unit test — would need injectable hook/client factories) |

---

## Explicitly NOT handled / out of scope
- Operational **health/readiness metrics** (watch installed, recovery pending, DEGRADED,
  consecutive failures, last success). `Health.ACTIVE` is deliberately not readiness.
- **Multi-pod / multi-writer coordination** (leader election, shared lock).
- Verifying fabric8's **terminal-notification reliability** for `Watch.close()` as a dedicated
  integration test; the lifecycle assumes a close is eventually reported.
- **Executed required-CI live-cluster evidence** — the `-Plive-it` profile and CI workflow exist in
  the repo; final acceptance needs them run on the required CI with reports/versions/SHA recorded.
