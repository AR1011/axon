# Fault matrix

For each feature and each fault this file names the tests that inject that fault into that feature and assert what
must still hold. It shows where the suite is thin, not how good it is: a cited test can still miss a case. `scripts/check.sh
faults` fails when a cited test no longer exists, and prints the gaps per feature.

A cell holds one of three things. The first is one or more tests, written `` `path::name` ``, where `path` is a file
under the repository root and `name` is a `fn` in it: a test, a simulation, a loom model or a unit test. Fuzz targets
are cited as `path::fuzz_target`. The second is `n/a: reason`, when the fault cannot reach the feature. The third is
`gap`, when nothing injects the fault or it is unclear whether a test really does. Only cite a test that injects the
fault. A test that shows the feature works is not enough.

A new feature adds its table in the same commit, with all eighteen rows. The whole-cluster simulation in
`spec/simulation.md`, the `World`, injects most of these faults into real nodes on virtual time. The last column says
how, and names the self-test that shows the World injects it. A feature's cell cites a World test once one injects the
fault into that feature.

| Fault | Meaning here | In the World |
|---|---|---|
| crash | the process or node dies without a goodbye (kill -9, a dropped system or runtime) | `crash(n, Crash::Process \| Crash::Machine)`: `crates/axon-testkit/tests/world.rs::a_process_crash_resets_and_refuses_and_a_machine_crash_goes_silent_until_its_restart`, `crates/axon-testkit/tests/world.rs::after_a_crash_no_byte_leaves_the_node` |
| restart | it comes back as a new incarnation or generation | `restart(n)`: `crates/axon-testkit/tests/world.rs::a_process_crash_resets_and_refuses_and_a_machine_crash_goes_silent_until_its_restart` |
| pause | it stops making progress and later resumes (SIGSTOP, a hung call, a handler that never returns, a suspended VM) | `pause(n, d)`: `crates/axon-testkit/tests/world_faults.rs::a_pause_fires_every_overdue_timer_in_one_burst_on_a_clock_that_ran_through_it`; `suspend(n, d)`: `crates/axon-cluster/tests/world_leases.rs::a_holder_suspended_past_its_lease_wakes_and_writes_and_only_the_epoch_check_refuses_it`; `hold_actors(n, d)`: `crates/axon-testkit/tests/world_faults.rs::held_actors_stop_while_the_nodes_tasks_and_timers_go_on` |
| partition | two sides cannot reach each other, both ways; several such cuts make reachability not transitive (A and C each reach B, not each other) | `partition(a, b)`, `heal()`: `crates/axon-testkit/tests/world_faults.rs::a_partition_holds_bytes_and_dials_both_ways_and_they_flow_on_the_heal`; one lane both ways, `cut_lane`: `crates/axon-testkit/tests/world_faults.rs::a_lane_cut_or_reset_holds_or_ends_that_lanes_connections_and_no_other` |
| one-way cut | A reaches B but B does not reach A | `cut(from, to)`: `crates/axon-testkit/tests/world_faults.rs::a_one_way_cut_holds_the_acknowledgements_too_so_the_reverse_writer_stalls_at_its_window`; one lane, `cut_lane(from, to, lane)`: `crates/axon-testkit/tests/world_faults.rs::a_lane_cut_or_reset_holds_or_ends_that_lanes_connections_and_no_other` |
| loss | a message, answer or call is lost or fails | `Wire { stall_every, stall }`: `crates/axon-testkit/tests/world.rs::a_connection_delivers_its_bytes_in_order_and_holds_a_writer_at_its_window`; `Wire::syn_stall`: `crates/axon-testkit/tests/world.rs::a_syn_that_draws_a_stall_waits_syn_stall_and_the_bytes_after_it_do_not`; `reset(a, b)`: `crates/axon-testkit/tests/world_faults.rs::a_reset_ends_every_connection_between_two_nodes_at_both_ends_and_no_other`; one lane, `reset_lane(a, b, lane)`: `crates/axon-testkit/tests/world_faults.rs::a_lane_cut_or_reset_holds_or_ends_that_lanes_connections_and_no_other` |
| duplication | a message arrives or is applied twice | the application's own retries after a reset or a timeout |
| reordering | messages arrive in another order; in-process, threads interleave differently (loom) | across connections, from `Wire::jitter` and the seeded delivery order; never inside one |
| latency | messages arrive late or with jitter | `wire(from, to, Wire { latency, jitter, .. })`: `crates/axon-testkit/tests/world_faults.rs::a_partition_holds_bytes_and_dials_both_ways_and_they_flow_on_the_heal`, `crates/axon-testkit/tests/world.rs::a_dial_completes_one_round_trip_after_it_starts`; `Wire::bandwidth`: `crates/axon-testkit/tests/world.rs::a_link_with_a_bandwidth_sends_one_chunk_at_a_time_in_the_order_they_came` |
| clock skew or step | clocks disagree between nodes, or one jumps | `drift(n, ppm)`: `crates/axon-testkit/tests/world_faults.rs::a_clock_at_plus_5000_ppm_ticks_1005_times_while_the_world_ticks_1000`; `step_wall(n, ms)`: `crates/axon-testkit/tests/world_faults.rs::a_wall_step_back_gives_an_older_incarnation_that_the_seeds_refuse` |
| CPU starvation | the thread or runtime gets too little CPU | `starve(n, k)`: `crates/axon-testkit/tests/world_faults.rs::a_starved_node_runs_its_timers_late_in_bursts_while_its_clock_runs_on`; `cost(n, d)`: `crates/axon-testkit/tests/world_faults.rs::a_turn_cost_trips_the_turn_budget_and_lapses_a_queued_calls_deadline` |
| memory limit | a bound on bytes held is reached, or the process runs out | `WorldConfig::window`, and the real bounded queues |
| disk full | a store the feature writes to is full | `host.disk()`, `Disk::set_capacity(Some(n))`: `crates/axon-testkit/tests/world.rs::a_machine_crash_undoes_a_truncation_not_synced` |
| disk error | a write fails or is only partly done | `crash(n, Crash::Machine)` keeps a seeded part of what a `Disk` did not sync, a torn tail: `crates/axon-testkit/tests/world.rs::a_machine_crash_keeps_a_seeded_part_of_what_was_not_synced_and_a_process_crash_keeps_it_all`, inside one write `crates/axon-testkit/tests/world.rs::a_machine_crash_inside_a_write_keeps_one_whole` and `crates/axon-testkit/tests/world.rs::a_machine_crash_inside_a_write_leaves_one_of_the_ends_crashes_goes_through`; every end of a crash inside one write, in a store's unit tests: `axon_testkit::crashes`, `crates/axon-testkit/tests/files.rs::a_crash_inside_a_write_ends_in_every_way_files_allow`; other write errors in the store's own unit tests |
| fd exhaustion | no file descriptors or ports are left | `refuse_accepts(n, on)`: `crates/axon-testkit/tests/world_faults.rs::refused_accepts_are_counted_as_accept_errors_until_they_are_allowed_again` |
| panic | user code panics or fails inside the feature | in the test's own actors and boots |
| overload | queues are full and the feature must refuse | real bounded queues, a small `window`, slow nodes, a link's `Wire::bandwidth`: `crates/axon-testkit/tests/world.rs::a_link_with_a_bandwidth_sends_one_chunk_at_a_time_in_the_order_they_came` |
| hostile input | bytes or requests are corrupt, forged, or unauthorised | not modelled: the fuzz targets |

## actors

### actors: mailbox admission and backpressure

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/system.rs::dropping_the_system_stops_actors_without_draining`, `crates/axon/tests/hosted.rs::dropping_the_runtime_stops_actors_and_limits_are_enforced` |
| restart | n/a: a new generation gets a new mailbox (supervision) |
| pause | `crates/axon/tests/embedded.rs::waiter_limit_deadline_and_cancellation_leave_nothing_behind`, `crates/axon/tests/system.rs::a_threadless_ask_times_out_at_its_virtual_deadline` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | `crates/axon/tests/embedded.rs::waiters_are_fifo_and_immediate_sends_cannot_bypass_them`, `crates/axon/src/mailbox/model.rs::a_sender_waiting_for_room_is_woken_by_the_release`, `crates/axon/src/mailbox/model.rs::two_waiting_senders_are_both_admitted_in_turn`, `crates/axon/src/mailbox/model.rs::a_waiter_that_gives_up_hands_its_wake_to_the_next`, `crates/axon/src/mailbox/model.rs::closing_wakes_a_waiting_sender_with_a_refusal` |
| latency | n/a: in-process |
| clock skew or step | n/a: deadlines use the monotonic clock |
| CPU starvation | `crates/axon/tests/hosted.rs::a_handler_that_blocks_does_not_hold_up_the_actors_that_share_its_worker`, `crates/axon/tests/hosted.rs::two_actors_that_keep_waking_each_other_do_not_starve_a_third`, `crates/axon/tests/embedded.rs::time_budgets_are_cooperative_and_keep_the_suffix_queued`, `crates/axon-testkit/tests/world_faults.rs::a_turn_cost_trips_the_turn_budget_and_lapses_a_queued_calls_deadline` |
| memory limit | `crates/axon/tests/embedded.rs::oversized_messages_and_custom_weights` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon/tests/faults.rs::a_panic_at_any_callback_never_unwinds_a_turn_or_leaks_accounting` |
| overload | `crates/axon/tests/embedded.rs::admission_is_gated_bounded_and_returns_the_payload`, `crates/axon/tests/hosted.rs::concurrent_producers_keep_their_own_order_under_backpressure` |
| hostile input | n/a: in-process |

### actors: supervision and restart

| Fault | Covered by |
|---|---|
| crash | n/a: a supervisor dies with the process it supervises |
| restart | `crates/axon/tests/embedded.rs::supervision_restarts_with_fresh_state_then_escalates`, `crates/axon/tests/hosted.rs::hosted_supervision_restarts_and_publishes_the_new_address`, `crates/axon/tests/system.rs::supervised_send_and_ask_wait_out_a_restart`, `crates/axon/tests/system.rs::a_supervised_ask_goes_to_the_next_generation_only_if_untaken`, `crates/axon/tests/system.rs::a_stop_asked_for_as_a_generation_fails_ends_the_family_as_stopped`, `crates/axon/tests/system.rs::a_restart_moves_since_restart_but_not_up` |
| pause | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | n/a: in-process |
| latency | n/a: in-process |
| clock skew or step | `crates/axon/tests/faults.rs::the_restart_budget_counts_only_failures_inside_its_window` |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | n/a: a restart starts with fresh state and an empty mailbox |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon/tests/faults.rs::a_supervisor_outlives_a_factory_and_a_policy_that_panic`, `crates/axon/tests/embedded.rs::supervision_restarts_with_fresh_state_then_escalates` |
| overload | `crates/axon/tests/system.rs::a_full_or_restarting_family_refuses_and_hands_the_message_back`, `crates/axon/tests/embedded.rs::a_restart_burst_is_one_event_at_once_then_one_a_minute_with_its_count` |
| hostile input | n/a: in-process |

### actors: timers

| Fault | Covered by |
|---|---|
| crash | n/a: timers are not persisted |
| restart | `crates/axon/tests/system.rs::a_restart_leaves_the_last_generations_timers_jobs_and_sink_behind` |
| pause | `crates/axon/tests/embedded.rs::timers_fire_in_order_repeat_and_cancel`, `crates/axon-testkit/tests/world_faults.rs::held_actors_stop_while_the_nodes_tasks_and_timers_go_on` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | n/a: in-process |
| latency | n/a: in-process |
| clock skew or step | n/a: timers use the monotonic clock; a late wake is the pause row |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | n/a: bounded by the timer limit (overload row) |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon/tests/faults.rs::a_panic_at_any_callback_never_unwinds_a_turn_or_leaks_accounting` |
| overload | `crates/axon/tests/faults.rs::timer_job_and_child_limits_refuse_hand_the_payload_back_and_free_up_again` |
| hostile input | n/a: in-process |

### actors: jobs (submit_io, submit_future, attach)

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/system.rs::dropping_the_system_stops_actors_without_draining`, `crates/axon/tests/system.rs::jobs_on_a_runtime_of_their_own_run_there_and_the_rest_on_the_systems`, `crates/axon/tests/faults.rs::a_job_its_executor_loses_settles_once_on_any_exit_and_nothing_waits_for_it` |
| restart | `crates/axon/tests/system.rs::a_restart_leaves_the_last_generations_timers_jobs_and_sink_behind` |
| pause | `crates/axon/tests/system.rs::cancelling_a_job_drops_its_future`, `crates/axon/tests/embedded.rs::drain_waits_for_jobs_until_its_deadline` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | `crates/axon/tests/embedded.rs::a_paused_actor_handles_its_job_result_before_any_message_queued_meanwhile`, `crates/axon/tests/embedded.rs::a_paused_actor_holds_other_job_results_until_its_own_is_handled`, `crates/axon/src/mailbox/model.rs::a_pausing_job_settling_as_a_turn_starts_with_results_held_is_seen_or_gets_a_turn` |
| latency | n/a: in-process |
| clock skew or step | n/a: in-process |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | `crates/axon/tests/embedded.rs::an_attached_item_over_the_size_limit_fails_the_actor_and_says_why` |
| disk full | n/a: a job's I/O error is its own result, returned as a message |
| disk error | n/a: a job's I/O error is its own result, returned as a message |
| fd exhaustion | n/a: a job's I/O error is its own result, returned as a message |
| panic | `crates/axon/tests/embedded.rs::jobs_complete_and_a_panicking_one_ends_the_actor_as_panicked`, `crates/axon/tests/faults.rs::a_panic_at_any_callback_never_unwinds_a_turn_or_leaks_accounting`, `crates/axon/tests/system.rs::catch_panic_makes_a_jobs_panic_its_result` |
| overload | `crates/axon/tests/faults.rs::timer_job_and_child_limits_refuse_hand_the_payload_back_and_free_up_again`, `crates/axon/tests/embedded.rs::a_paused_actor_holds_other_job_results_until_its_own_is_handled`, `crates/axon/tests/embedded.rs::a_full_mailbox_pauses_the_source_and_loses_nothing`, `crates/axon/tests/embedded.rs::sources_have_their_own_budget_and_do_not_count_as_waiters` |
| hostile input | n/a: in-process |

### actors: sinks

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/embedded.rs::a_stop_drops_the_sink_and_what_it_queued_without_an_event` |
| restart | `crates/axon/tests/system.rs::a_restart_leaves_the_last_generations_timers_jobs_and_sink_behind` |
| pause | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | n/a: in-process |
| latency | n/a: in-process |
| clock skew or step | n/a: in-process |
| CPU starvation | `crates/axon/tests/embedded.rs::a_sink_kept_busy_flushes_and_yields_its_thread_every_64_items` |
| memory limit | n/a: bounded by the sink's capacity (overload row) |
| disk full | `crates/axon/tests/embedded.rs::a_failing_sink_closes_with_its_error_and_refuses_what_follows` |
| disk error | `crates/axon/tests/embedded.rs::a_failing_sink_closes_with_its_error_and_refuses_what_follows` |
| fd exhaustion | n/a: to the sink it is a failed write (disk error row) |
| panic | `crates/axon/tests/faults.rs::a_panic_at_any_callback_never_unwinds_a_turn_or_leaks_accounting` |
| overload | `crates/axon/tests/embedded.rs::a_sink_takes_items_without_waiting_and_writes_them_in_order_flushing_when_dry` |
| hostile input | n/a: in-process |

### actors: stash (cx.stash)

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/faults.rs::a_stash_leaks_nothing_and_loses_nothing_uncounted_on_any_exit_whatever_panics`, `crates/axon/tests/embedded.rs::what_is_still_stashed_when_the_actor_ends_is_counted_and_its_asks_are_answered` |
| restart | `crates/axon/tests/system.rs::a_generation_that_fails_leaves_its_stash_as_dead_letters_and_its_stashed_ask_answered` |
| pause | `crates/axon/tests/embedded.rs::a_paused_actor_replays_its_stash_only_once_the_pause_lifts` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | `crates/axon/tests/embedded.rs::what_is_still_stashed_when_the_actor_ends_is_counted_and_its_asks_are_answered`, `crates/axon/tests/faults.rs::a_stash_leaks_nothing_and_loses_nothing_uncounted_on_any_exit_whatever_panics` |
| duplication | n/a: in-process; a stashed message is handed back once, by the replay |
| reordering | `crates/axon/tests/embedded.rs::stashed_messages_replay_in_stash_order_before_newer_mailbox_messages_and_the_stash_is_bounded` |
| latency | n/a: in-process |
| clock skew or step | n/a: in-process |
| CPU starvation | `crates/axon/tests/embedded.rs::a_replay_shares_the_turn_budget_of_messages_and_time_with_the_batch_after_it` |
| memory limit | `crates/axon/tests/embedded.rs::stashed_messages_replay_in_stash_order_before_newer_mailbox_messages_and_the_stash_is_bounded`, `crates/axon/tests/embedded.rs::the_stash_holds_as_many_bytes_as_the_mailbox_may` |
| disk full | n/a: in-process |
| disk error | n/a: in-process |
| fd exhaustion | n/a: in-process |
| panic | `crates/axon/tests/faults.rs::a_stash_leaks_nothing_and_loses_nothing_uncounted_on_any_exit_whatever_panics` |
| overload | `crates/axon/tests/embedded.rs::stashed_messages_replay_in_stash_order_before_newer_mailbox_messages_and_the_stash_is_bounded` |
| hostile input | n/a: in-process |

### actors: watches (cx.watch)

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/faults.rs::no_watch_outlives_both_actors_or_tells_twice_in_any_stop_order_whatever_panics` |
| restart | `crates/axon/tests/system.rs::a_watch_on_a_supervised_family_is_told_when_the_family_ends_not_on_a_restart` |
| pause | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| partition | n/a: local actors only; `cluster.watch_key` watches across nodes |
| one-way cut | n/a: local actors only |
| loss | `crates/axon/tests/embedded.rs::a_watcher_is_told_once_of_each_exit_past_a_full_mailbox_and_not_after_a_cancel` |
| duplication | `crates/axon/tests/faults.rs::no_watch_outlives_both_actors_or_tells_twice_in_any_stop_order_whatever_panics`, `crates/axon/src/lifecycle.rs::every_watcher_of_an_exit_is_told_once_before_it_is_visible` |
| reordering | `crates/axon/tests/faults.rs::no_watch_outlives_both_actors_or_tells_twice_in_any_stop_order_whatever_panics` |
| latency | n/a: in-process |
| clock skew or step | n/a: in-process |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | `crates/axon/tests/embedded.rs::a_watch_holds_a_source_slot_until_handled_or_cancelled_and_nothing_follows_a_cancel_or_a_stop` |
| disk full | n/a: in-process |
| disk error | n/a: in-process |
| fd exhaustion | n/a: in-process |
| panic | `crates/axon/tests/faults.rs::no_watch_outlives_both_actors_or_tells_twice_in_any_stop_order_whatever_panics` |
| overload | `crates/axon/tests/embedded.rs::a_watcher_is_told_once_of_each_exit_past_a_full_mailbox_and_not_after_a_cancel`, `crates/axon/tests/embedded.rs::a_watch_holds_a_source_slot_until_handled_or_cancelled_and_nothing_follows_a_cancel_or_a_stop` |
| hostile input | n/a: in-process |

### actors: links (cx.link)

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/faults.rs::a_linked_failure_reaches_every_linked_actor_once_and_a_stop_none_whatever_panics`, `crates/axon/tests/system.rs::a_failure_stops_each_actor_linked_to_it_once_through_a_chain_or_a_cycle`, `crates/axon/tests/system.rs::linking_to_an_actor_that_failed_to_start_already_stops_this_one_at_once` |
| restart | `crates/axon/tests/system.rs::a_supervised_actor_stopped_by_a_link_restarts_whether_it_or_its_family_was_linked` |
| pause | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| partition | n/a: local actors only |
| one-way cut | n/a: local actors only |
| loss | n/a: a link carries no message |
| duplication | `crates/axon/src/driver/tests.rs::a_link_is_registered_once_per_pair_and_leaves_nothing_once_unlinked_or_ended`, `crates/axon/src/driver/tests.rs::two_actors_linking_each_other_at_once_make_one_link_and_one_unlink_takes_it`, `crates/axon/tests/system.rs::a_failure_stops_each_actor_linked_to_it_once_through_a_chain_or_a_cycle` |
| reordering | `crates/axon/tests/faults.rs::a_linked_failure_reaches_every_linked_actor_once_and_a_stop_none_whatever_panics` |
| latency | n/a: in-process |
| clock skew or step | n/a: in-process |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | n/a: at most one link a pair, taken off both actors when either ends or it is unlinked |
| disk full | n/a: in-process |
| disk error | n/a: in-process |
| fd exhaustion | n/a: in-process |
| panic | `crates/axon/tests/faults.rs::a_linked_failure_reaches_every_linked_actor_once_and_a_stop_none_whatever_panics` |
| overload | n/a: a link holds no message and no budget |
| hostile input | n/a: in-process |

### actors: routers (Router)

| Fault | Covered by |
|---|---|
| crash | n/a: in-process; a member failing or panicking is the panic row, one stopped or drained the restart row |
| restart | `crates/axon/tests/router.rs::when_a_member_leaves_or_joins_only_the_keys_it_had_or_wins_move`, `crates/axon/tests/router.rs::a_member_stopping_or_draining_is_passed_over_before_it_ends_and_refuses_only_when_all_are` |
| pause | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| partition | n/a: local actors only |
| one-way cut | n/a: local actors only |
| loss | `crates/axon/tests/router.rs::each_strategy_spreads_messages_as_it_says_and_a_full_member_refuses_with_full`, `crates/axon/tests/router.rs::tells_from_several_threads_reach_each_member_in_turn_and_none_is_lost` |
| duplication | `crates/axon/tests/router.rs::tells_from_several_threads_reach_each_member_in_turn_and_none_is_lost` |
| reordering | `crates/axon/tests/router.rs::tells_from_several_threads_reach_each_member_in_turn_and_none_is_lost` |
| latency | n/a: in-process |
| clock skew or step | n/a: in-process |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | `crates/axon/tests/router.rs::each_strategy_spreads_messages_as_it_says_and_a_full_member_refuses_with_full`, `crates/axon/src/router.rs::a_member_is_watched_once_however_often_added_and_the_last_router_dropped_unwatches_it` |
| disk full | n/a: in-process |
| disk error | n/a: in-process |
| fd exhaustion | n/a: in-process |
| panic | `crates/axon/tests/router.rs::a_member_that_ends_is_never_chosen_once_its_exit_is_delivered`, `crates/axon/tests/router.rs::members_ending_and_joining_under_tells_from_several_threads_neither_block_nor_linger` |
| overload | `crates/axon/tests/router.rs::each_strategy_spreads_messages_as_it_says_and_a_full_member_refuses_with_full` |
| hostile input | n/a: in-process |

### actors: topics (Topic)

| Fault | Covered by |
|---|---|
| crash | n/a: in-process; a subscriber failing or panicking is the panic row, one stopped or restarted the restart row |
| restart | `crates/axon/tests/topic.rs::a_supervised_subscriber_stays_subscribed_across_restarts_and_misses_only_between_generations`, `crates/axon/tests/faults.rs::a_topic_drops_subscribers_that_fail_panic_or_stop_keeps_a_restarting_family_and_leaks_nothing_whatever_panics` |
| pause | `crates/axon/tests/topic.rs::a_publish_never_waits_a_full_subscriber_misses_into_dead_letters_and_one_subscribed_twice_gets_it_once` |
| partition | n/a: local actors only |
| one-way cut | n/a: local actors only |
| loss | `crates/axon/tests/topic.rs::a_publish_never_waits_a_full_subscriber_misses_into_dead_letters_and_one_subscribed_twice_gets_it_once`, `crates/axon/tests/topic.rs::each_publishers_order_is_kept_at_every_subscriber_and_what_it_missed_is_counted` |
| duplication | `crates/axon/tests/topic.rs::a_publish_never_waits_a_full_subscriber_misses_into_dead_letters_and_one_subscribed_twice_gets_it_once`, `crates/axon/src/topic.rs::a_subscriber_is_watched_once_however_often_subscribed_and_unsubscribing_unwatches_it` |
| reordering | `crates/axon/tests/topic.rs::each_publishers_order_is_kept_at_every_subscriber_and_what_it_missed_is_counted` |
| latency | n/a: in-process |
| clock skew or step | n/a: a topic reads no clock |
| CPU starvation | `crates/axon/tests/topic.rs::subscribers_starved_of_their_worker_miss_into_dead_letters_while_publishes_go_on_in_order` |
| memory limit | `crates/axon/tests/topic.rs::subscribers_past_the_limit_are_refused_and_one_leaving_makes_room`, `crates/axon/tests/topic.rs::a_name_holds_one_type_and_its_topic_lasts_while_a_handle_or_a_subscriber_does`, `crates/axon/src/topic.rs::a_subscriber_is_watched_once_however_often_subscribed_and_unsubscribing_unwatches_it` |
| disk full | n/a: in-process |
| disk error | n/a: in-process |
| fd exhaustion | n/a: in-process |
| panic | `crates/axon/tests/topic.rs::a_subscriber_that_ends_is_dropped_before_its_join_returns_and_unsubscribe_takes_one_out`, `crates/axon/tests/faults.rs::a_topic_drops_subscribers_that_fail_panic_or_stop_keeps_a_restarting_family_and_leaks_nothing_whatever_panics` |
| overload | `crates/axon/tests/topic.rs::a_publish_never_waits_a_full_subscriber_misses_into_dead_letters_and_one_subscribed_twice_gets_it_once`, `crates/axon/tests/topic.rs::each_publishers_order_is_kept_at_every_subscriber_and_what_it_missed_is_counted` |
| hostile input | n/a: in-process |

### actors: drain and shutdown

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/system.rs::dropping_the_system_stops_actors_without_draining`, `crates/axon/tests/hosted.rs::dropping_the_runtime_stops_actors_and_limits_are_enforced`, `crates/axon/tests/system.rs::a_forsaken_threadless_system_is_freed_and_leaves_no_thread` |
| restart | `crates/axon/tests/system.rs::a_stop_during_a_restart_backoff_ends_the_family_as_stopped`, `crates/axon/tests/system.rs::a_stop_asked_for_as_a_generation_fails_ends_the_family_as_stopped` |
| pause | `crates/axon/tests/hosted.rs::shutdown_is_honest_about_handlers_that_do_not_finish`, `crates/axon/tests/embedded.rs::drain_waits_for_jobs_until_its_deadline`, `crates/axon/tests/faults.rs::a_shutdown_step_that_hangs_leaves_each_extension_after_it_a_share_and_all_end_within_the_timeout` |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | `crates/axon/src/mailbox/model.rs::a_drain_racing_a_tell_or_a_commit_handles_whatever_was_accepted`, `crates/axon/src/mailbox/model.rs::a_drain_flagged_as_a_turn_starts_is_seen_by_it_or_gets_a_turn`, `crates/axon/tests/hosted.rs::a_committed_message_is_delivered_or_rejected_never_lost`, `crates/axon/tests/system.rs::a_drain_right_after_tells_never_drops_them` |
| latency | n/a: in-process |
| clock skew or step | n/a: deadlines use the monotonic clock |
| CPU starvation | `crates/axon-testkit/tests/world_faults.rs::held_paused_or_starved_actors_get_what_they_were_sent_or_owed_once_and_late` |
| memory limit | n/a: in-process |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon/tests/faults.rs::a_custom_on_batch_that_stalls_or_panics_ends_the_actor_with_every_message_accounted` |
| overload | `crates/axon/tests/system.rs::a_drain_refuses_the_senders_waiting_on_a_full_mailbox_at_once_and_handles_what_it_holds` |
| hostile input | n/a: in-process |

### actors: dead letters

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/system.rs::messages_discarded_at_exit_become_dead_letters` |
| restart | `crates/axon/tests/system.rs::a_generation_that_fails_and_restarts_leaves_its_queue_as_dead_letters` |
| pause | n/a: a letter is recorded where its message is dropped, without waiting |
| partition | n/a: in-process |
| one-way cut | n/a: in-process |
| loss | n/a: in-process |
| duplication | n/a: in-process |
| reordering | n/a: in-process |
| latency | n/a: in-process |
| clock skew or step | `crates/axon/src/dead.rs::repeats_within_a_second_coalesce_and_the_ring_is_bounded` |
| CPU starvation | n/a: in-process |
| memory limit | n/a: a bounded ring (overload row) |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon/tests/system.rs::a_label_that_panics_as_its_message_is_discarded_still_leaves_a_dead_letter` |
| overload | `crates/axon/src/dead.rs::repeats_within_a_second_coalesce_and_the_ring_is_bounded` |
| hostile input | n/a: in-process |

### actors: tracing

| Fault | Covered by |
|---|---|
| crash | `crates/axon/tests/system.rs::messages_discarded_at_exit_become_dead_letters` |
| restart | `crates/axon/tests/system.rs::a_generation_that_fails_and_restarts_leaves_its_queue_as_dead_letters` |
| pause | n/a: a trace context lives in the message that carries it |
| partition | n/a: the context rides its message; the transport rows cover the message |
| one-way cut | n/a: the context rides its message; the transport rows cover the message |
| loss | n/a: the context rides its message; the transport rows cover the message |
| duplication | n/a: the context rides its message; the transport rows cover the message |
| reordering | n/a: the context rides its message; the transport rows cover the message |
| latency | n/a: the context rides its message; the transport rows cover the message |
| clock skew or step | `crates/axon/tests/trace.rs::a_wall_step_back_inside_a_trace_keeps_its_order_parents_and_durations` |
| CPU starvation | n/a: in-process |
| memory limit | `crates/axon/tests/trace.rs::one_in_n_untraced_messages_starts_a_trace` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon/tests/faults.rs::a_panic_at_any_callback_never_unwinds_a_turn_or_leaks_accounting`, `crates/axon/tests/trace.rs::a_subscriber_that_panics_loses_its_event_and_nothing_else` |
| overload | `crates/axon-cluster/tests/cluster/trace.rs::a_body_that_fills_the_frame_is_sent_without_its_trace_rather_than_refused` |
| hostile input | `crates/axon-cluster/tests/cluster/trace.rs::a_call_with_a_short_or_zero_trace_context_is_refused_unrun`, `crates/axon-stream/src/wire/tests.rs::frames_round_trip_and_every_truncation_is_refused`, `fuzz/fuzz_targets/stream_wire.rs::fuzz_target` |

## transport

### transport: handshake

| Fault | Covered by |
|---|---|
| crash | `crates/axon-net/tests/faults.rs::an_io_fault_at_any_operation_ends_both_sides_cleanly` |
| restart | `crates/axon-net/tests/session.rs::a_redial_reaches_the_process_that_replaced_a_gone_one_as_a_newer_incarnation` |
| pause | `crates/axon-net/tests/session.rs::garbage_and_silence_fail_the_handshake_without_detail`, `crates/axon-net/tests/session.rs::a_silent_dial_holds_a_handshake_slot_only_for_its_own_budget`, `crates/axon-net/tests/session.rs::a_slow_connect_and_a_stalled_handshake_share_one_handshake_timeout` |
| partition | n/a: to a handshake a partition is silence (pause row) |
| one-way cut | `crates/axon-net/tests/faults.rs::a_stream_that_stalls_or_is_cut_one_way_at_any_operation_ends_both_sides_on_their_timers` |
| loss | n/a: TCP |
| duplication | n/a: TCP; a replayed hello is the hostile input row |
| reordering | n/a: TCP |
| latency | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| clock skew or step | n/a: the hello carries no time |
| CPU starvation | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| memory limit | n/a: the hello's body length is bounded (hostile input row) |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-testkit/tests/world_faults.rs::refused_accepts_are_counted_as_accept_errors_until_they_are_allowed_again` |
| panic | `crates/axon-net/tests/faults.rs::an_authenticator_that_panics_fails_its_handshake_on_either_side_and_the_listener_serves_the_next` |
| overload | `crates/axon-net/tests/session.rs::handshake_slots_are_limited_per_ip`, `crates/axon-net/tests/session.rs::handshakes_over_the_cap_wait_their_turn` |
| hostile input | `crates/axon-net/tests/session.rs::authentication_is_mutual_and_default_deny`, `crates/axon-net/tests/session.rs::garbage_and_silence_fail_the_handshake_without_detail`, `crates/axon-cluster/tests/seed.rs::a_wrong_credential_and_a_cluster_port_never_attach`, `fuzz/fuzz_targets/handshake.rs::fuzz_target` |

### transport: framing and compression

| Fault | Covered by |
|---|---|
| crash | `crates/axon-net/tests/faults.rs::an_io_fault_at_any_operation_ends_both_sides_cleanly`, `crates/axon-stream/tests/world_relay.rs::leaves_see_every_record_once_while_relays_crash_and_return` |
| restart | n/a: a new session starts a new sequence |
| pause | n/a: framing has no timers (sessions row) |
| partition | n/a: sessions row |
| one-way cut | n/a: sessions row |
| loss | `crates/axon-net/tests/frame.rs::limits_codecs_and_sequence_are_enforced_before_work` |
| duplication | n/a: TCP, and the sequence check (loss row) refuses any frame that is not the next |
| reordering | n/a: TCP, and the sequence check (loss row) refuses any frame that is not the next |
| latency | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| clock skew or step | n/a: frames carry no time |
| CPU starvation | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| memory limit | `crates/axon-net/tests/frame.rs::limits_codecs_and_sequence_are_enforced_before_work` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: sessions row |
| panic | n/a: no user code |
| overload | `crates/axon-net/tests/frame.rs::limits_codecs_and_sequence_are_enforced_before_work` |
| hostile input | `crates/axon-net/tests/frame.rs::mutated_input_never_panics`, `crates/axon-net/tests/frame.rs::limits_codecs_and_sequence_are_enforced_before_work`, `crates/axon-net/tests/session.rs::a_packed_frame_crosses_a_session_without_a_codec_and_a_bad_one_is_handed_back`, `fuzz/fuzz_targets/frame_decode.rs::fuzz_target` |

### transport: TLS

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/cluster/tls.rs::every_node_rotates_to_a_new_ca_with_calls_in_flight_and_none_failing` |
| restart | `crates/axon-cluster/tests/cluster/tls.rs::every_node_rotates_to_a_new_ca_with_calls_in_flight_and_none_failing` |
| pause | `crates/axon-net/tests/faults.rs::a_stream_that_stalls_or_is_cut_one_way_at_any_operation_ends_both_sides_on_their_timers` |
| partition | `crates/axon-net/tests/faults.rs::a_stream_that_stalls_or_is_cut_one_way_at_any_operation_ends_both_sides_on_their_timers` |
| one-way cut | `crates/axon-net/tests/faults.rs::a_stream_that_stalls_or_is_cut_one_way_at_any_operation_ends_both_sides_on_their_timers` |
| loss | n/a: TCP |
| duplication | n/a: TCP |
| reordering | n/a: TCP |
| latency | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| clock skew or step | `crates/axon-net/tests/session.rs::a_certificate_outside_its_validity_is_refused_at_the_handshake_and_the_listener_serves_on` |
| CPU starvation | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| memory limit | n/a: framing row |
| disk full | n/a: certificates are loaded by the caller |
| disk error | n/a: certificates are loaded by the caller |
| fd exhaustion | n/a: TLS runs on a stream already accepted and opens no descriptor; accepts are the handshake row |
| panic | `crates/axon-cluster/tests/cluster/tls.rs::a_rotation_checked_by_an_authenticator_that_panics_is_refused_and_the_caller_goes_on` |
| overload | `crates/axon-net/tests/session.rs::handshakes_over_the_cap_wait_their_turn` |
| hostile input | `crates/axon-net/tests/session.rs::mtls_binds_the_node_id_to_the_certificate`, `crates/axon-cluster/tests/seed.rs::tls_checks_the_host_name_or_the_named_identity`, `crates/axon-cluster/tests/cluster/tls.rs::a_node_given_a_certificate_the_others_do_not_trust_keeps_its_sessions_and_takes_no_new_ones` |

### transport: sessions and keepalive

| Fault | Covered by |
|---|---|
| crash | `crates/axon-net/tests/faults.rs::an_io_fault_at_any_operation_ends_both_sides_cleanly`, `crates/axon-net/tests/session.rs::dropping_the_receiver_closes_both_directions`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| restart | `crates/axon-net/tests/session.rs::listener_accepts_tcp_and_redial_backs_off_a_dead_address`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| pause | `crates/axon-net/tests/session.rs::a_peer_that_stops_reading_is_timed_out_as_idle`, `crates/axon-net/tests/session.rs::a_slow_consumer_is_not_a_silent_peer`, `crates/axon-cluster/tests/world_faults.rs::a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake`, `crates/axon-cluster/tests/world_faults.rs::a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| partition | `crates/axon-cluster/tests/cluster/membership.rs::a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake`, `crates/axon-net/tests/session.rs::a_silent_network_times_out_dials_and_ends_listeners`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| one-way cut | `crates/axon-net/tests/faults.rs::a_stream_that_stalls_or_is_cut_one_way_at_any_operation_ends_both_sides_on_their_timers` |
| loss | `crates/axon-net/tests/session.rs::a_dial_waits_connect_timeout_for_its_connect_and_handshake_timeout_for_the_rest`, `crates/axon-cluster/tests/world_faults.rs::a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable`, `crates/axon-cluster/tests/world_faults.rs::a_lost_syn_costs_no_reachability_under_cluster_config_new`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions`, `crates/axon-stream/tests/world_wan.rs::the_shipped_timings_mark_a_peer_unreachable_on_a_lost_syn_and_the_wan_timings_do_not` |
| duplication | n/a: TCP; framing row |
| reordering | n/a: TCP; framing row |
| latency | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing`, `crates/axon-net/tests/session.rs::the_round_trip_is_known_one_round_trip_after_a_dial`, `crates/axon-stream/tests/world_wan.rs::a_cluster_across_three_regions_runs_quiet_on_the_wan_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| clock skew or step | n/a: keepalive and idle timers use the monotonic clock |
| CPU starvation | `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| memory limit | `crates/axon-net/tests/faults.rs::an_io_fault_at_any_operation_ends_both_sides_cleanly`, `crates/axon-net/tests/faults.rs::a_slow_stream_or_a_starved_runtime_loses_nothing` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-net/tests/session.rs::sessions_run_over_a_custom_network` |
| panic | n/a: no user code |
| overload | `crates/axon-net/tests/session.rs::a_full_outbox_rejects_definitely_and_credit_returns_when_frames_drop` |
| hostile input | n/a: handshake and framing rows |

### transport: lanes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit` |
| restart | n/a: a lane is a session; the engine checks each lane's boot (cluster: lanes) |
| pause | `crates/axon-net/tests/session.rs::a_capped_session_still_answers_pings_on_time` |
| partition | n/a: one session sees one path; one lane cut is in cluster: lanes |
| one-way cut | n/a: as partition |
| loss | `crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit` |
| duplication | n/a: a session resends nothing |
| reordering | `crates/axon-net/src/queued/model.rs::a_paced_writer_keeps_each_senders_order_and_never_writes_past_its_budget`, `crates/axon-net/tests/session.rs::a_capped_session_writes_no_faster_than_its_rate_and_keeps_order` |
| latency | `crates/axon-net/tests/session.rs::a_capped_session_writes_no_faster_than_its_rate_and_keeps_order` |
| clock skew or step | n/a: the bucket uses the monotonic clock |
| CPU starvation | gap |
| memory limit | `crates/axon-net/tests/session.rs::compression_is_negotiated_and_limits_only_shrink` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-net/tests/session.rs::handshake_slots_are_limited_per_ip` |
| panic | n/a: no user code runs in a session |
| overload | `crates/axon-net/tests/faults.rs::the_fault_loop_on_a_capped_session_ends_cleanly_and_returns_its_credit` |
| hostile input | `fuzz/fuzz_targets/handshake.rs::fuzz_target`, `crates/axon-net/src/handshake.rs::a_hello_with_an_unknown_lane_is_malformed` |

## cluster

### cluster: membership

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/cluster/membership.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices_on_seeds`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| restart | `crates/axon-cluster/tests/cluster/membership.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices_on_seeds`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| pause | `crates/axon-cluster/tests/cluster/membership.rs::a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake`, `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_provider_that_hangs_lies_or_panics_leaves_a_stale_view_and_a_working_node`, `crates/axon-cluster/tests/world_faults.rs::a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake`, `crates/axon-cluster/tests/world_leases.rs::every_op_answers_within_two_leases_of_a_paused_owners_resume`, `crates/axon-cluster/tests/cluster/discovery.rs::a_stale_view_is_fresh_again_once_its_watched_provider_answers_and_a_hung_one_holds_no_heartbeat_back`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| partition | `crates/axon-cluster/tests/cluster/membership.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`, `crates/axon-cluster/tests/cluster/discovery.rs::a_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it`, `crates/axon-cluster/tests/cluster/discovery.rs::a_lone_node_cut_off_from_discovery_stops_its_keyed_actors_and_hosts_them_again`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`, `crates/axon-cluster/tests/world_faults.rs::calls_resume_within_the_redial_cap_once_a_long_cut_heals`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_outlives_a_cut_shorter_than_its_lease_while_its_node_is_isolated`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_cut_off_past_its_lease_stops_by_its_fence_and_runs_elsewhere`, `crates/axon-cluster/tests/cluster/discovery.rs::a_node_heard_now_and_then_stays_isolated_until_heard_for_a_whole_grace`, `crates/axon-cluster/tests/cluster/discovery.rs::a_leased_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it`, `crates/axon-cluster/tests/world_faults.rs::nodes_started_with_their_seeds_record_no_isolation`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| one-way cut | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed` |
| loss | `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_fault_at_any_call_of_a_start_leaves_nothing_behind`, `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_that_is_away_while_the_node_starts_is_retried_within_the_budget`, `crates/axon-cluster/tests/world_faults.rs::a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable`, `crates/axon-cluster/tests/world_faults.rs::a_lost_syn_costs_no_reachability_under_cluster_config_new`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions`, `crates/axon-stream/tests/world_wan.rs::the_shipped_timings_mark_a_peer_unreachable_on_a_lost_syn_and_the_wan_timings_do_not` |
| duplication | `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_provider_that_hangs_lies_or_panics_leaves_a_stale_view_and_a_working_node`, `crates/axon-cluster/tests/consul.rs::discovery_round_trip` |
| reordering | `crates/axon-cluster/tests/cluster/discovery.rs::a_snapshot_asked_for_before_a_change_never_replaces_the_view_after_it`, `crates/axon-cluster/tests/cluster/discovery.rs::a_refresh_waits_for_the_snapshot_out_and_one_more_shared_with_every_caller` |
| latency | `crates/axon-cluster/tests/world_faults.rs::a_distant_peer_whose_handshake_outlasts_a_keepalive_stays_reachable`, `crates/axon-cluster/tests/world_faults.rs::a_peer_whose_syn_is_lost_once_after_a_silent_session_stays_reachable`, `crates/axon-stream/tests/world_wan.rs::a_cluster_across_three_regions_runs_quiet_on_the_wan_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| clock skew or step | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent`, `crates/axon-cluster/tests/consul.rs::a_boot_whose_clock_stepped_back_is_listed_once_the_newer_boots_check_lapses`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| CPU starvation | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed` |
| memory limit | `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_provider_that_hangs_lies_or_panics_leaves_a_stale_view_and_a_working_node` |
| disk full | n/a: discovery rows |
| disk error | n/a: discovery rows |
| fd exhaustion | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed` |
| panic | `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_provider_that_hangs_lies_or_panics_leaves_a_stale_view_and_a_working_node` |
| overload | `crates/axon-cluster/src/membership.rs::a_subscriber_that_falls_behind_resyncs_and_ends_when_the_membership_closes` |
| hostile input | `crates/axon-cluster/tests/cluster/discovery.rs::a_discovery_provider_that_hangs_lies_or_panics_leaves_a_stale_view_and_a_working_node`, `crates/axon-cluster/tests/cluster/discovery.rs::static_seeds_and_authorizer` |

### cluster: remote calls

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/cluster/membership.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices_on_seeds`, `crates/axon-cluster/tests/world_faults.rs::a_named_ref_follows_its_name_across_nodes_and_restarts`, `crates/axon-cluster/tests/world_faults.rs::resolve_all_returns_the_live_instances_and_names_a_silent_node_within_the_call_timeout` |
| restart | `crates/axon-cluster/tests/cluster/membership.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices_on_seeds`, `crates/axon-cluster/tests/world_faults.rs::a_named_ref_follows_its_name_across_nodes_and_restarts`, `crates/axon-cluster/tests/world_faults.rs::a_peer_back_as_a_new_boot_is_dialled_as_soon_as_the_view_shows_it`, `crates/axon-cluster/tests/world_faults.rs::a_dial_of_the_old_boot_that_fails_as_the_view_shows_the_new_one_fails_no_call`, `crates/axon-cluster/tests/world_faults.rs::a_process_that_is_not_the_boot_the_view_names_is_refused`, `crates/axon-cluster/src/engine.rs::a_call_that_sees_a_new_boot_before_the_link_is_reset_waits_for_its_dial` |
| pause | `crates/axon-cluster/tests/cluster/membership.rs::a_peer_that_stops_answering_is_unreachable_without_waiting_for_a_handshake`, `crates/axon-cluster/tests/cluster/remote.rs::a_request_its_caller_gave_up_on_is_counted_in_an_outcome`, `crates/axon-cluster/tests/world_faults.rs::a_named_ref_follows_its_name_across_nodes_and_restarts`, `crates/axon-cluster/tests/world_faults.rs::a_dial_of_the_old_boot_that_fails_as_the_view_shows_the_new_one_fails_no_call` |
| partition | `crates/axon-cluster/tests/cluster/remote.rs::a_call_that_runs_out_of_time_before_a_session_is_up_was_not_sent`, `crates/axon-cluster/tests/world_faults.rs::access_revoked_during_a_partition_is_refused_once_it_heals` |
| one-way cut | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent` |
| loss | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| duplication | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| reordering | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| latency | `crates/axon-cluster/tests/world_faults.rs::a_request_runs_once_within_its_deadline_and_never_once_it_arrives_past_it` |
| clock skew or step | `crates/axon-cluster/tests/world_faults.rs::a_request_runs_once_within_its_deadline_and_never_once_it_arrives_past_it` |
| CPU starvation | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent` |
| memory limit | n/a: call tables are bounded (overload row) |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-cluster/tests/world_faults.rs::a_peer_cut_one_way_starved_stalled_out_of_descriptors_or_off_clock_is_marked_only_when_silent` |
| panic | `crates/axon-cluster/tests/cluster/keyed.rs::full_tables_and_a_panicking_decoder_refuse_the_call_and_the_next_one_works`, `crates/axon-cluster/tests/cluster/remote.rs::a_reply_that_does_not_encode_fails_its_send_and_the_caller_hears_no_reply` |
| overload | `crates/axon-cluster/tests/cluster/keyed.rs::full_tables_and_a_panicking_decoder_refuse_the_call_and_the_next_one_works`, `crates/axon-cluster/tests/cluster/remote.rs::control_rpc_is_default_deny_and_bounded`, `crates/axon-cluster/tests/world_faults.rs::request_until_waits_out_a_busy_refusal`, `crates/axon-cluster/tests/cluster/remote.rs::a_request_admitted_late_counts_its_timeout_from_admission`, `crates/axon-cluster/tests/cluster/remote.rs::several_started_requests_are_answered_in_their_order` |
| hostile input | `crates/axon-cluster/tests/cluster/remote.rs::control_rpc_is_default_deny_and_bounded`, `crates/axon-cluster/tests/cluster/tls.rs::a_revoked_peer_is_refused_its_next_call_spawn_and_watch` |

### cluster: lanes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/cluster/membership.rs::crashed_node_is_detected_before_discovery_notices` |
| restart | `crates/axon-cluster/tests/world_lanes.rs::a_peer_that_restarts_between_its_lanes_dials_is_called_as_its_new_boot_only`, `crates/axon-cluster/tests/cluster/membership.rs::crashed_node_is_detected_before_discovery_notices` |
| pause | gap |
| partition | `crates/axon-cluster/tests/world_lanes.rs::a_dead_bulk_lane_fails_only_its_calls_and_the_peer_stays_reachable`, `crates/axon-cluster/tests/world_lanes.rs::a_node_with_a_dead_live_lane_keeps_its_keys_and_its_live_calls_retry_later` |
| one-way cut | gap |
| loss | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| duplication | `crates/axon-cluster/tests/cluster/lanes.rs::a_third_session_of_one_peer_on_one_lane_closes_the_first` |
| reordering | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| latency | `crates/axon-cluster/tests/world_lanes.rs::bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast` |
| clock skew or step | n/a: a lane's dials and keepalives run on the monotonic clock, as Control's |
| CPU starvation | gap |
| memory limit | gap |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | gap |
| panic | n/a: no user code picks a lane; a handler's panic is in cluster: remote calls |
| overload | `crates/axon-cluster/tests/world_lanes.rs::bulk_saturating_the_links_costs_no_lease_and_live_calls_stay_fast` |
| hostile input | `crates/axon-cluster/src/cluster.rs::a_route_with_an_unknown_lane_byte_is_refused`, `crates/axon-cluster/tests/cluster/lanes.rs::publish_and_kind_refuse_control_and_the_raw_builder_takes_it_only_under_axon` |

### cluster: replies in parts

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| restart | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| pause | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back`, `crates/axon-cluster/tests/cluster/parts.rs::a_part_late_past_request_timeout_or_a_reply_past_its_deadline_times_out` |
| partition | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| one-way cut | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| loss | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| duplication | n/a: a session never duplicates a frame; a retry is a new call |
| reordering | n/a: one session is in order, and a reply's parts never leave it |
| latency | `crates/axon-cluster/tests/world_faults.rs::a_reply_in_parts_is_cut_by_a_crash_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| clock skew or step | gap |
| CPU starvation | gap |
| memory limit | `crates/axon-cluster/tests/cluster/parts.rs::a_caller_that_does_not_read_holds_its_window_and_the_producer_waits`, `crates/axon-cluster/tests/cluster/parts.rs::a_session_carries_64_replies_in_parts_and_refuses_the_next_full` |
| disk full | n/a: nothing is stored |
| disk error | n/a: nothing is stored |
| fd exhaustion | n/a: a reply in parts opens no connection |
| panic | `crates/axon-cluster/tests/cluster/parts.rs::a_reply_cut_by_its_producer_ends_in_no_reply_never_in_a_clean_end` |
| overload | `crates/axon-cluster/tests/cluster/parts.rs::dropping_the_parts_stops_the_far_stream_and_frees_its_slot`, `crates/axon-cluster/tests/cluster/parts.rs::a_session_carries_64_replies_in_parts_and_refuses_the_next_full` |
| hostile input | `crates/axon-cluster/tests/cluster/parts.rs::a_short_credit_or_a_parts_request_without_its_window_closes_only_that_session` |

### cluster: requests in parts

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| restart | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| pause | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back`, `crates/axon-cluster/tests/cluster/uploads.rs::an_upload_waits_for_credit_and_for_parts_at_most_request_timeout` |
| partition | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| one-way cut | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| loss | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| duplication | n/a: a session never duplicates a frame; nothing is resent |
| reordering | n/a: one session is in order, and a body's parts never leave it |
| latency | `crates/axon-cluster/tests/world_faults.rs::an_upload_is_cut_by_a_crash_of_either_node_a_reset_a_partition_or_a_cut_and_every_slot_comes_back` |
| clock skew or step | gap |
| CPU starvation | gap |
| memory limit | `crates/axon-cluster/tests/cluster/uploads.rs::a_handler_that_does_not_read_holds_the_window_and_the_caller_waits`, `crates/axon-cluster/tests/cluster/uploads.rs::a_session_carries_64_bodies_and_refuses_the_next_full` |
| disk full | n/a: nothing is stored |
| disk error | n/a: nothing is stored |
| fd exhaustion | n/a: a request in parts opens no connection |
| panic | `crates/axon-cluster/tests/cluster/uploads.rs::a_body_cut_by_its_caller_ends_the_handlers_upload_in_no_reply` |
| overload | `crates/axon-cluster/tests/cluster/uploads.rs::a_handler_that_answers_first_or_drops_the_upload_stops_the_body`, `crates/axon-cluster/tests/cluster/uploads.rs::a_session_carries_64_bodies_and_refuses_the_next_full` |
| hostile input | `crates/axon-cluster/tests/cluster/uploads.rs::a_body_part_past_its_credit_or_a_short_body_frame_closes_only_that_session` |

### cluster: remote spawn

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| restart | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| pause | `crates/axon-cluster/tests/cluster/leases.rs::a_start_whose_caller_gave_up_still_finishes` |
| partition | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| one-way cut | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| loss | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| duplication | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| reordering | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| latency | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| clock skew or step | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| CPU starvation | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| memory limit | n/a: overload row |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| panic | `crates/axon-cluster/tests/world_faults.rs::a_remote_start_under_a_fault_ends_in_time_and_runs_what_its_caller_was_told` |
| overload | `crates/axon-cluster/tests/cluster/keyed.rs::a_node_whose_runtime_is_full_refuses_a_keyed_start_for_later` |
| hostile input | `crates/axon-cluster/tests/cluster/kinds.rs::a_kind_name_with_a_slash_is_refused`, `crates/axon-cluster/tests/cluster/tls.rs::a_revoked_peer_is_refused_its_next_call_spawn_and_watch` |

### cluster: keyed actors (get_or_spawn, rebalance, KeyedRef, idle, watch_key)

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/cluster/keyed.rs::a_crashed_owner_key_moves_although_the_survivors_never_called_it`, `crates/axon-cluster/tests/cluster/leases.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners`, `crates/axon-cluster/tests/world_leases.rs::a_wall_step_back_on_an_acceptor_does_not_let_an_older_epoch_win`, `crates/axon-cluster/tests/world_faults.rs::a_crashed_owner_key_moves_although_the_survivors_never_called_it`, `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-cluster/tests/world_faults.rs::a_watcher_sees_a_crashed_or_cut_off_owner_key_move_and_never_one_epoch_on_two_nodes`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-cluster/tests/world_eager.rs::a_leave_cut_short_by_a_crash_loses_no_eager_key`, `crates/axon-cluster/tests/world_faults.rs::a_keyed_request_until_answers_across_its_owners_crash_and_restart`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_eager.rs::an_unseen_holder_that_ends_after_a_node_starts_has_its_keys_run_again`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| restart | `crates/axon-cluster/tests/cluster/keyed.rs::a_join_hands_each_moved_key_on_and_then_releases_its_lease`, `crates/axon-cluster/tests/cluster/keyed.rs::a_join_that_moves_more_keys_than_calls_in_flight_moves_every_one`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_actor_that_is_slow_to_drain_is_never_alive_twice`, `crates/axon-cluster/tests/cluster/keyed.rs::a_handoff_the_new_owner_refuses_is_counted`, `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners`, `crates/axon-cluster/tests/world_leases.rs::a_wall_step_back_on_an_acceptor_does_not_let_an_older_epoch_win`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_actor_is_named_after_its_kind_and_key`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-cluster/tests/world_faults.rs::a_keyed_request_until_answers_across_its_owners_crash_and_restart`, `crates/axon-cluster/tests/world_leases.rs::a_key_started_elsewhere_while_its_owner_restarted_goes_back_to_it`, `crates/axon-cluster/tests/world_eager.rs::every_eager_key_runs_again_after_a_cold_restart`, `crates/axon-cluster/tests/world_eager.rs::the_eager_keys_of_a_node_that_never_returns_from_a_cold_restart_run_on_the_others`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| pause | `crates/axon-cluster/tests/cluster/leases.rs::a_holder_stalled_past_the_grace_handles_nothing_more`, `crates/axon-cluster/tests/cluster/keyed.rs::a_clean_leave_behind_a_key_that_drains_slowly_still_hands_it_off_and_unregisters`, `crates/axon/tests/system.rs::nothing_is_handled_once_the_fence_lapses`, `crates/axon/tests/system.rs::a_threadless_fence_lapses_at_its_virtual_deadline_and_not_before`, `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners`, `crates/axon-cluster/tests/world_leases.rs::holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_leases.rs::every_op_answers_within_two_leases_of_a_paused_owners_resume`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| partition | `crates/axon-cluster/tests/cluster/membership.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`, `crates/axon-cluster/tests/cluster/discovery.rs::a_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it`, `crates/axon-cluster/tests/cluster/discovery.rs::a_lone_node_cut_off_from_discovery_stops_its_keyed_actors_and_hosts_them_again`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`, `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-cluster/tests/world_faults.rs::a_watcher_sees_a_crashed_or_cut_off_owner_key_move_and_never_one_epoch_on_two_nodes`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_faults.rs::access_revoked_during_a_partition_is_refused_once_it_heals`, `crates/axon-cluster/tests/world_reach.rs::callers_that_reach_the_holder_are_served_while_the_placement_owner_is_cut_from_it`, `crates/axon-cluster/tests/world_reach.rs::callers_are_served_while_the_placement_owner_reaches_only_a_minority_of_acceptors`, `crates/axon-cluster/tests/world_reach.rs::a_key_started_past_its_cut_off_owner_stays_until_the_owner_can_take_it_then_moves_back`, `crates/axon-cluster/tests/world_eager.rs::a_question_the_authority_does_not_answer_ends`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_outlives_a_cut_shorter_than_its_lease_while_its_node_is_isolated`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_cut_off_past_its_lease_stops_by_its_fence_and_runs_elsewhere`, `crates/axon-cluster/tests/cluster/discovery.rs::a_leased_node_cut_off_from_discovery_stops_its_keyed_actors_once_a_peer_has_dropped_it`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| one-way cut | `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners`, `crates/axon-cluster/tests/world_leases.rs::holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive` |
| loss | `crates/axon-cluster/tests/cluster/keyed.rs::a_watch_shows_a_key_stopped_once_its_lease_is_lost_before_its_actor_ends`, `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| duplication | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| reordering | `crates/axon-cluster/tests/world_faults.rs::calls_over_a_lossy_wire_run_at_most_once_and_tells_keep_their_order` |
| latency | `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| clock skew or step | `crates/axon-cluster/tests/world_leases.rs::holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive`, `crates/axon-cluster/tests/world_leases.rs::a_wall_step_back_on_an_acceptor_does_not_let_an_older_epoch_win`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| CPU starvation | `crates/axon-cluster/tests/world_leases.rs::a_holder_starved_past_its_lease_stops_its_keys_before_another_nodes_epoch_writes`, `crates/axon-cluster/src/paxos/core/tests.rs::a_failed_round_says_why`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_outlives_a_cut_shorter_than_its_lease_while_its_node_is_isolated` |
| memory limit | `crates/axon-cluster/tests/cluster/kinds.rs::a_kind_runs_its_actors_with_the_execution_and_mailbox_it_was_registered_with` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases` |
| panic | `crates/axon-cluster/tests/cluster/leases.rs::renewals_that_fail_or_panic_stop_strictly_owned_actors_only_once_the_grace_is_spent`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_actor_that_fails_is_counted_and_the_next_ask_starts_it_again`, `crates/axon-cluster/tests/cluster/keyed.rs::calls_to_a_supervised_keyed_actor_wait_out_its_restart`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_tell_after_its_actor_ended_on_a_live_node_asks_for_the_key_again`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_ref_shows_the_node_and_epoch_of_the_actor_it_calls` |
| overload | `crates/axon-cluster/tests/cluster/keyed.rs::a_node_whose_runtime_is_full_refuses_a_keyed_start_for_later`, `crates/axon-cluster/tests/cluster/leases.rs::keys_start_side_by_side_and_one_key_still_starts_once`, `crates/axon-cluster/tests/cluster/keyed.rs::a_key_asked_of_a_node_while_it_shuts_down_is_refused_as_retryable` |
| hostile input | `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_actor_is_named_after_its_kind_and_key`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_start_refused_its_name_or_its_start_leaves_nothing_running`, `crates/axon-cluster/tests/cluster/keyed.rs::a_watch_the_owner_refuses_ends`, `crates/axon-cluster/tests/cluster/tls.rs::a_revoked_peer_is_refused_its_next_call_spawn_and_watch` |

### cluster: rolling upgrades (wire versions, schemas side by side)

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/world_upgrade.rs::a_rolling_upgrade_loses_no_call_holds_no_key_past_a_lease_and_resets_no_follower` |
| restart | `crates/axon-stream/tests/world_upgrade.rs::a_rolling_upgrade_loses_no_call_holds_no_key_past_a_lease_and_resets_no_follower` |
| pause | n/a: the version is chosen inside the handshake and a schema by the lookup a call makes first, so this fault reaches them as it does transport: handshake and cluster: remote calls |
| partition | n/a: the version is chosen inside the handshake and a schema by the lookup a call makes first, so this fault reaches them as it does transport: handshake and cluster: remote calls |
| one-way cut | n/a: the version is chosen inside the handshake and a schema by the lookup a call makes first, so this fault reaches them as it does transport: handshake and cluster: remote calls |
| loss | n/a: the version is chosen inside the handshake and a schema by the lookup a call makes first, so this fault reaches them as it does transport: handshake and cluster: remote calls |
| duplication | n/a: a call retried after a roll's crash is the caller's own, per `err.retry()` (crash row) |
| reordering | n/a: both hellos are read before either end decides, so their order cannot change the version |
| latency | n/a: the version is chosen inside the handshake and a schema by the lookup a call makes first, so this fault reaches them as it does transport: handshake and cluster: remote calls |
| clock skew or step | n/a: choosing a wire version or a schema reads no clock |
| CPU starvation | n/a: the version is chosen inside the handshake and a schema by the lookup a call makes first, so this fault reaches them as it does transport: handshake and cluster: remote calls |
| memory limit | n/a: each schema of a name takes an endpoint, under the endpoint limit; the hello's body stays bounded |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: negotiation opens nothing of its own (transport: handshake) |
| panic | n/a: no user code runs to choose a version or a schema; a kind's factory is cluster: remote spawn |
| overload | n/a: a node of either version refuses as cluster: keyed actors does. Placement narrows the candidates only in a one-step roll, and then a key whose only candidate refuses for capacity is refused (`Spawn(Capacity)`, `Later`) until the next node comes back upgraded; a two-step roll keeps every node a candidate (`crates/axon-cluster/src/placement.rs::only_nodes_serving_every_callers_schema_of_a_kind_are_candidates`) |
| hostile input | `crates/axon-net/tests/session.rs::a_session_runs_the_highest_version_both_speak_and_ranges_that_do_not_meet_are_refused_naming_both`, `crates/axon-cluster/tests/upgrade.rs::nodes_whose_wire_versions_do_not_meet_are_refused_naming_both_ranges_and_counted`, `crates/axon-cluster/tests/upgrade.rs::keys_go_only_to_nodes_that_speak_every_callers_schema_of_their_kind_and_a_schema_none_speaks_places_nothing`, `crates/axon-cluster/tests/upgrade.rs::a_new_quota_replica_does_not_wait_to_pull_from_a_node_of_another_quota_schema` |

### cluster: quotas

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/world_quota.rs::crashes_of_either_kind_and_a_rolling_restart_at_wan_latency_never_pause_admission`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults` |
| restart | `crates/axon-cluster/tests/world_quota.rs::crashes_of_either_kind_and_a_rolling_restart_at_wan_latency_never_pause_admission`, `crates/axon-cluster/tests/world_quota.rs::a_blue_green_switch_under_load_never_pauses_admission`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::a_restarted_replica_admits_nothing_over_the_limit_while_it_pulls`, `crates/axon-cluster/src/quota/core/tests.rs::a_view_showing_a_later_incarnation_retires_every_earlier_one_even_one_it_never_showed`, `crates/axon-cluster/src/quota/core/tests.rs::a_candidate_serves_the_keys_the_puller_replicates_in_the_pullers_view_not_in_its_own`, `crates/axon-obs/tests/health.rs::a_new_quota_replica_pulls_its_keys_from_a_candidate_whose_view_does_not_show_it`, `crates/axon-cluster/src/quota/core/tests.rs::a_view_showing_a_replica_restarted_points_its_link_at_the_new_incarnation` |
| pause | `crates/axon-cluster/tests/world_quota.rs::pauses_starvation_drift_and_a_wall_step_never_pause_admission_elsewhere`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::a_paused_node_admits_only_the_fallback_over_the_limit`, `crates/axon-cluster/src/quota/core/tests.rs::a_grant_that_lapsed_counts_no_more_after_a_message_that_leaves_it_out` |
| partition | `crates/axon-cluster/tests/world_quota.rs::partitions_cuts_resets_and_refused_accepts_never_pause_admission`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/sim.rs::full_links_a_restart_inside_and_views_that_disagree_keep_a_key_within_its_limit`, `crates/axon-cluster/src/quota/mod.rs::members_count_unreachable_nodes_a_pull_skips_and_leave_out_draining_ones` |
| one-way cut | `crates/axon-cluster/tests/world_quota.rs::partitions_cuts_resets_and_refused_accepts_never_pause_admission`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/sim.rs::full_links_a_restart_inside_and_views_that_disagree_keep_a_key_within_its_limit`, `crates/axon-cluster/src/quota/mod.rs::members_count_unreachable_nodes_a_pull_skips_and_leave_out_draining_ones`, `crates/axon-cluster/src/quota/core/tests.rs::a_push_is_taken_in_only_where_it_answers_one_of_the_last_two_reports` |
| loss | `crates/axon-cluster/tests/world_quota.rs::partitions_cuts_resets_and_refused_accepts_never_pause_admission`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::lost_and_duplicated_messages_admit_nothing_over_the_limit`, `crates/axon-cluster/src/quota/core/tests.rs::a_node_reports_again_what_a_replica_may_have_missed` |
| duplication | `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::lost_and_duplicated_messages_admit_nothing_over_the_limit` |
| reordering | `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::late_messages_admit_nothing_over_the_limit`, `crates/axon-cluster/src/quota/core/tests.rs::a_push_is_acknowledged_only_where_the_replica_heard_every_change_of_its_entries`, `crates/axon-cluster/src/quota/core/tests.rs::an_answer_that_comes_after_a_later_one_has_nothing_reported_again` |
| latency | `crates/axon-cluster/tests/world_quota.rs::crashes_of_either_kind_and_a_rolling_restart_at_wan_latency_never_pause_admission`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::late_messages_admit_nothing_over_the_limit`, `crates/axon-cluster/src/quota/sim.rs::full_links_a_restart_inside_and_views_that_disagree_keep_a_key_within_its_limit` |
| clock skew or step | `crates/axon-cluster/tests/world_quota.rs::pauses_starvation_drift_and_a_wall_step_never_pause_admission_elsewhere`, `crates/axon-cluster/src/quota/sim.rs::keys_admit_within_their_limits_and_the_rows_of_the_faults_and_serve_their_limits_outside_faults`, `crates/axon-cluster/src/quota/model.rs::a_drifting_clock_admits_nothing_over_the_limit` |
| CPU starvation | `crates/axon-cluster/tests/world_quota.rs::pauses_starvation_drift_and_a_wall_step_never_pause_admission_elsewhere`, `crates/axon-cluster/src/quota/core/tests.rs::an_answer_that_comes_after_a_later_one_has_nothing_reported_again` |
| memory limit | `crates/axon-cluster/tests/world_quota.rs::eviction_with_more_keys_than_fit_never_pauses_the_hot_key`, `crates/axon-cluster/src/quota/core/tests.rs::eviction_drops_the_least_recently_used_and_refuses_them_until_a_majority_grants` |
| disk full | n/a: nothing is written; a restarted replica pulls the rows of the others (restart row) |
| disk error | n/a: nothing is written; a restarted replica pulls the rows of the others (restart row) |
| fd exhaustion | `crates/axon-cluster/tests/world_quota.rs::partitions_cuts_resets_and_refused_accepts_never_pause_admission` |
| panic | `crates/axon-cluster/tests/cluster/quota.rs::a_permit_comes_back_when_its_holder_panics` |
| overload | `crates/axon-cluster/src/quota/core/tests.rs::a_report_starts_where_the_last_ran_out_so_that_churn_starves_no_entry`, `crates/axon-cluster/src/quota/core/tests.rs::an_answer_carries_at_most_max_items_and_the_rest_follow_once_it_is_acknowledged`, `crates/axon-cluster/src/quota/core/tests.rs::a_node_behind_gets_at_most_resend_grants_it_has_not_acknowledged_a_message_and_every_one_in_the_end`, `crates/axon-cluster/tests/world_quota.rs::eviction_with_more_keys_than_fit_never_pauses_the_hot_key` |
| hostile input | `crates/axon-cluster/src/quota/wire.rs::messages_round_trip_and_refuse_what_is_cut_or_trailing`, `crates/axon-obs/tests/health.rs::readiness_waits_for_the_quota_replica_to_pull_and_not_for_a_candidate_that_refuses` |

## leases

### leases: Memory

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/world_faults.rs::a_crashed_holders_memory_leases_keep_its_keys_until_they_are_expired` |
| restart | `crates/axon-cluster/tests/world_faults.rs::a_crashed_holders_memory_leases_keep_its_keys_until_they_are_expired` |
| pause | `crates/axon-cluster/tests/cluster/leases.rs::a_holder_stalled_past_the_grace_handles_nothing_more`, `crates/axon-cluster/tests/cluster/leases.rs::a_renewal_that_hangs_stops_strictly_owned_actors_when_the_grace_is_spent` |
| partition | n/a: in-process table |
| one-way cut | n/a: in-process table |
| loss | `crates/axon-cluster/tests/cluster/leases.rs::strict_ownership_fences_and_stops_on_lost_lease`, `crates/axon-cluster/tests/cluster/leases.rs::renewals_that_fail_or_panic_stop_strictly_owned_actors_only_once_the_grace_is_spent`, `crates/axon-cluster/tests/cluster/leases.rs::one_key_whose_renewal_fails_stops_alone`, `crates/axon-cluster/tests/cluster/leases.rs::a_lease_lost_while_its_actor_starts_fails_the_start`, `crates/axon-cluster/tests/cluster/keyed.rs::a_keyed_actor_whose_lease_is_lost_stops_fenced`, `crates/axon-cluster/tests/cluster/keyed.rs::hosted_lists_the_kinds_registered_and_the_keys_run_as_they_start_are_lost_and_move` |
| duplication | n/a: in-process table |
| reordering | n/a: in-process table |
| latency | n/a: in-process table |
| clock skew or step | n/a: the table reads no clock and nothing in it expires by itself |
| CPU starvation | n/a: the table runs no task and reads no clock; a starved holder's grace is the cluster's (keyed actors' row) |
| memory limit | n/a: in-process table |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors |
| panic | n/a: no user code |
| overload | n/a: no queue; each call takes a lock and returns |
| hostile input | n/a: in-process table |

### leases: Consul

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/consul.rs::a_holder_that_stops_renewing_keeps_its_key_until_its_session_lapses_and_the_next_holder_has_a_newer_epoch` |
| restart | `crates/axon-cluster/tests/consul.rs::a_holder_that_stops_renewing_keeps_its_key_until_its_session_lapses_and_the_next_holder_has_a_newer_epoch` |
| pause | `crates/axon-cluster/tests/consul.rs::a_lapsed_session_is_replaced_on_the_next_acquire` |
| partition | `crates/axon-cluster/tests/consul.rs::lease_calls_through_a_cut_or_slow_agent_end_in_time_and_an_acquire_asked_again_keeps_its_epoch`, `crates/axon-cluster/tests/consul.rs::a_lost_release_or_acquire_leaves_no_key_locked_under_the_renewals` |
| one-way cut | `crates/axon-cluster/tests/consul.rs::lease_calls_through_a_cut_or_slow_agent_end_in_time_and_an_acquire_asked_again_keeps_its_epoch`, `crates/axon-cluster/tests/consul.rs::a_lost_release_or_acquire_leaves_no_key_locked_under_the_renewals` |
| loss | `crates/axon-cluster/tests/consul.rs::lease_calls_through_a_cut_or_slow_agent_end_in_time_and_an_acquire_asked_again_keeps_its_epoch`, `crates/axon-cluster/tests/consul.rs::a_lost_release_or_acquire_leaves_no_key_locked_under_the_renewals` |
| duplication | `crates/axon-cluster/tests/consul.rs::lease_calls_through_a_cut_or_slow_agent_end_in_time_and_an_acquire_asked_again_keeps_its_epoch` |
| reordering | `crates/axon-cluster/tests/consul.rs::a_release_that_arrives_after_its_key_was_taken_again_leaves_the_key_held` |
| latency | `crates/axon-cluster/tests/consul.rs::lease_calls_through_a_cut_or_slow_agent_end_in_time_and_an_acquire_asked_again_keeps_its_epoch` |
| clock skew or step | n/a: the leases read no clock: the session TTL runs on the agent's, the holder's grace on its own monotonic clock (keyed actors' row) |
| CPU starvation | `crates/axon-cluster/tests/consul.rs::a_holder_that_stops_renewing_keeps_its_key_until_its_session_lapses_and_the_next_holder_has_a_newer_epoch` |
| memory limit | n/a: the agent holds the state |
| disk full | `crates/axon-cluster/tests/consul.rs::error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken` |
| disk error | `crates/axon-cluster/tests/consul.rs::error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken` |
| fd exhaustion | `crates/axon-cluster/tests/consul.rs::a_burst_of_lease_calls_stays_under_the_agents_connection_limit` |
| panic | n/a: no user code |
| overload | `crates/axon-cluster/tests/consul.rs::a_burst_of_first_acquires_shares_one_session` |
| hostile input | `crates/axon-cluster/tests/consul.rs::error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken` |

### leases: Paxos

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults`, `crates/axon-cluster/src/paxos/core/tests.rs::a_crash_inside_a_write_reads_back_the_log_before_it_or_after`, `crates/axon-cluster/tests/world_eager.rs::every_eager_key_runs_again_after_every_acceptor_crashes_inside_its_start_rewrite`, `crates/axon-cluster/tests/ownership.rs::paxos_leases_need_a_majority_of_acceptors`, `crates/axon-cluster/tests/ownership.rs::a_paxos_lease_nobody_renews_runs_out`, `crates/axon-cluster/tests/cluster/leases.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-cluster/tests/ownership.rs::paxos_held_by_lists_what_any_acceptor_of_a_majority_accepted`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_eager.rs::an_unseen_holder_that_ends_after_a_node_starts_has_its_keys_run_again` |
| restart | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults`, `crates/axon-cluster/src/paxos/core/tests.rs::a_restarted_acceptor_refuses_epochs_from_before_it_restarted`, `crates/axon-cluster/tests/ownership.rs::a_rolling_restart_of_the_acceptors_keeps_the_lease`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-cluster/tests/world_eager.rs::an_eager_key_fenced_on_a_node_that_stays_a_member_starts_again`, `crates/axon-cluster/tests/world_eager.rs::an_eager_key_fenced_by_its_own_turn_on_a_node_that_stays_a_member_starts_again`, `crates/axon-cluster/tests/world_eager.rs::an_eager_key_handed_back_to_a_node_whose_start_then_fails_starts_again`, `crates/axon-cluster/tests/world_eager.rs::an_eager_key_lost_just_before_its_node_leaves_runs_on_a_survivor`, `crates/axon-cluster/tests/ownership.rs::paxos_held_by_lists_what_any_acceptor_of_a_majority_accepted`, `crates/axon-cluster/src/paxos/core/tests.rs::a_restarted_acceptor_lists_from_its_log_and_promises_only_the_floor`, `crates/axon-cluster/tests/world_eager.rs::every_eager_key_runs_again_after_every_acceptor_restarts_at_once`, `crates/axon-cluster/tests/world_eager.rs::every_eager_key_runs_again_after_a_cold_restart`, `crates/axon-cluster/tests/world_eager.rs::every_eager_key_runs_again_after_a_cold_restart_in_memory`, `crates/axon-cluster/tests/world_eager.rs::every_eager_key_runs_again_after_every_acceptor_restarts_at_once_under_a_long_lease`, `crates/axon-cluster/tests/world_eager.rs::the_eager_keys_of_a_node_that_never_returns_from_a_cold_restart_run_on_the_others`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings` |
| pause | `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners`, `crates/axon-cluster/tests/world_leases.rs::holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_leases.rs::every_op_answers_within_two_leases_of_a_paused_owners_resume`, `crates/axon-cluster/src/paxos/mod.rs::a_write_that_hangs_fails_answers_open_until_it_lands`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| partition | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults`, `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases`, `crates/axon-cluster/tests/world_eager.rs::a_lost_holders_eager_keys_restart_on_survivors_and_its_lazy_keys_wait_for_a_caller`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_reach.rs::callers_that_reach_the_holder_are_served_while_the_placement_owner_is_cut_from_it`, `crates/axon-cluster/tests/world_reach.rs::callers_are_served_while_the_placement_owner_reaches_only_a_minority_of_acceptors`, `crates/axon-cluster/tests/world_reach.rs::a_key_started_past_its_cut_off_owner_stays_until_the_owner_can_take_it_then_moves_back`, `crates/axon-cluster/tests/world_eager.rs::a_question_the_authority_does_not_answer_ends`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_outlives_a_cut_shorter_than_its_lease_while_its_node_is_isolated`, `crates/axon-cluster/tests/world_faults.rs::a_leased_key_cut_off_past_its_lease_stops_by_its_fence_and_runs_elsewhere` |
| one-way cut | `crates/axon-cluster/tests/world_leases.rs::holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive`, `crates/axon-cluster/tests/world.rs::owners_crash_pause_and_lose_a_link_but_no_key_has_two_owners` |
| loss | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults` |
| duplication | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults` |
| reordering | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults` |
| latency | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults`, `crates/axon-cluster/src/paxos/core/tests.rs::a_renewal_that_comes_back_late_does_not_extend`, `crates/axon-cluster/src/paxos/core/tests.rs::a_renewal_waits_for_no_other_keys_write`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| clock skew or step | `crates/axon-cluster/src/paxos/sim.rs::leases_stay_exclusive_and_epochs_grow_under_faults`, `crates/axon-cluster/src/paxos/core/tests.rs::a_keys_epoch_never_goes_back_after_a_clock_steps_back_across_restarts`, `crates/axon-cluster/tests/world_leases.rs::holders_at_plus_and_minus_5000_ppm_keep_their_leases_exclusive`, `crates/axon-cluster/tests/world_leases.rs::a_wall_step_back_on_an_acceptor_does_not_let_an_older_epoch_win`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings` |
| CPU starvation | `crates/axon-cluster/tests/world_leases.rs::a_holder_starved_past_its_lease_stops_its_keys_before_another_nodes_epoch_writes` |
| memory limit | `crates/axon-cluster/src/paxos/core/tests.rs::a_failed_round_says_why` |
| disk full | `crates/axon-cluster/tests/ownership.rs::an_acceptor_whose_log_fails_answers_and_logs_again_once_it_can` |
| disk error | `crates/axon-cluster/tests/ownership.rs::an_acceptor_whose_log_fails_answers_and_logs_again_once_it_can`, `crates/axon-cluster/src/paxos/core/tests.rs::the_log_is_rewritten_when_it_grows_or_a_write_fails`, `crates/axon-cluster/src/paxos/core/tests.rs::log_bytes_are_pinned_to_the_version`, `crates/axon-cluster/src/paxos/core/tests.rs::a_log_that_cannot_be_read_fails_the_start`, `crates/axon-cluster/src/paxos/core/tests.rs::a_log_of_another_version_or_configuration_is_replaced` |
| fd exhaustion | `crates/axon-cluster/tests/world_faults.rs::keyed_actors_move_and_stop_with_their_paxos_leases` |
| panic | `crates/axon-cluster/tests/ownership.rs::an_acceptor_whose_log_fails_answers_and_logs_again_once_it_can` |
| overload | `crates/axon-cluster/tests/ownership.rs::a_burst_of_paxos_acquires_is_granted`, `crates/axon-cluster/src/paxos/core/tests.rs::a_burst_of_keys_shares_requests`, `crates/axon-cluster/src/paxos/mod.rs::a_renewal_asked_for_while_one_is_out_is_refused` |
| hostile input | `crates/axon-cluster/tests/ownership.rs::a_node_with_other_acceptors_or_lease_gets_no_votes`, `crates/axon-cluster/src/paxos/core/tests.rs::wire_round_trips_and_refuses_every_truncation` |

## discovery

### discovery: Memory

| Fault | Covered by |
|---|---|
| crash | n/a: an in-process test double |
| restart | n/a: an in-process test double |
| pause | n/a: an in-process test double |
| partition | n/a: an in-process test double |
| one-way cut | n/a: an in-process test double |
| loss | n/a: an in-process test double |
| duplication | n/a: an in-process test double |
| reordering | n/a: an in-process test double |
| latency | n/a: an in-process test double |
| clock skew or step | n/a: an in-process test double |
| CPU starvation | n/a: an in-process test double |
| memory limit | n/a: an in-process test double |
| disk full | n/a: an in-process test double |
| disk error | n/a: an in-process test double |
| fd exhaustion | n/a: an in-process test double |
| panic | n/a: an in-process test double |
| overload | n/a: an in-process test double |
| hostile input | n/a: an in-process test double |

### discovery: Static

| Fault | Covered by |
|---|---|
| crash | n/a: a fixed list; peers it names going down are the membership rows |
| restart | n/a: a fixed list; peers it names going down are the membership rows |
| pause | n/a: a fixed list; peers it names going down are the membership rows |
| partition | n/a: a fixed list; peers it names going down are the membership rows |
| one-way cut | n/a: a fixed list; peers it names going down are the membership rows |
| loss | n/a: a fixed list |
| duplication | n/a: a fixed list |
| reordering | n/a: a fixed list |
| latency | n/a: a fixed list |
| clock skew or step | n/a: a fixed list |
| CPU starvation | n/a: a fixed list |
| memory limit | n/a: a fixed list |
| disk full | n/a: a fixed list |
| disk error | n/a: a fixed list |
| fd exhaustion | n/a: a fixed list |
| panic | n/a: a fixed list |
| overload | n/a: a fixed list |
| hostile input | `crates/axon-cluster/tests/cluster/discovery.rs::static_seeds_and_authorizer` |

### discovery: Consul

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| restart | `crates/axon-cluster/tests/consul.rs::discovery_round_trip` |
| pause | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| partition | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| one-way cut | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| loss | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| duplication | `crates/axon-cluster/tests/consul.rs::discovery_round_trip` |
| reordering | `crates/axon-cluster/tests/consul.rs::discovery_round_trip`, `crates/axon-cluster/tests/consul.rs::a_boot_whose_clock_stepped_back_is_listed_once_the_newer_boots_check_lapses` |
| latency | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| clock skew or step | `crates/axon-cluster/tests/consul.rs::a_boot_whose_clock_stepped_back_is_listed_once_the_newer_boots_check_lapses` |
| CPU starvation | `crates/axon-cluster/tests/consul.rs::discovery_calls_through_a_cut_lossy_down_or_slow_agent_end_in_time_and_a_silent_node_drops_out` |
| memory limit | n/a: the agent holds the state |
| disk full | `crates/axon-cluster/tests/consul.rs::error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken` |
| disk error | `crates/axon-cluster/tests/consul.rs::error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken` |
| fd exhaustion | `crates/axon-cluster/tests/consul.rs::a_burst_of_discovery_calls_stays_under_the_agents_connection_limit` |
| panic | n/a: no user code |
| overload | `crates/axon-cluster/tests/consul.rs::a_burst_of_discovery_calls_stays_under_the_agents_connection_limit` |
| hostile input | `crates/axon-cluster/tests/consul.rs::error_garbage_and_oversized_answers_fail_the_call_and_the_next_proper_answer_is_taken` |

### discovery: Seed

| Fault | Covered by |
|---|---|
| crash | `crates/axon-cluster/src/seed/sim.rs::a_leave_is_seen_at_once_a_crash_in_three_seconds_a_dead_machine_in_eleven`, `crates/axon-cluster/src/seed/sim.rs::one_seed_down_is_invisible_and_two_freeze_everyone`, `crates/axon-cluster/tests/seed.rs::nodes_join_change_leave_crash_and_restart_through_one_seed`, `crates/axon-cluster/tests/seed.rs::one_seed_of_three_is_not_missed_and_two_freeze_the_nodes_until_they_return`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices_on_seeds`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/seed.rs::a_node_does_not_count_a_seed_with_other_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| restart | `crates/axon-cluster/src/seed/sim.rs::a_restart_replaces_the_old_boot_and_an_older_boot_is_refused_until_it_is_gone`, `crates/axon-cluster/tests/seed.rs::nodes_join_change_leave_crash_and_restart_through_one_seed`, `crates/axon-cluster/tests/world_faults.rs::crashed_node_is_detected_before_discovery_notices_on_seeds`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| pause | `crates/axon-cluster/src/seed/mod.rs::only_a_session_that_went_quiet_counts_as_a_dead_machine`, `crates/axon-cluster/tests/world_leases.rs::every_op_answers_within_two_leases_of_a_paused_owners_resume`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| partition | `crates/axon-cluster/src/seed/sim.rs::a_node_that_reaches_a_minority_of_seeds_is_in_no_view`, `crates/axon-cluster/src/seed/sim.rs::a_node_attached_again_is_live_only_with_the_others_cut_off_with_it`, `crates/axon-cluster/src/seed/sim.rs::random_faults_never_break_a_view_and_everything_heals`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-cluster/tests/world_faults.rs::a_node_cut_off_from_its_seeds_and_peers_isolates_before_it_is_removed_at_the_wan_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| one-way cut | `crates/axon-cluster/tests/world_faults.rs::a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed` |
| loss | `crates/axon-stream/tests/world_wan.rs::a_seed_link_redials_across_a_lost_syn_within_the_connect_timeout_and_keeps_the_node_listed` |
| duplication | `crates/axon-cluster/tests/seed.rs::registering_the_same_record_again_is_taken_like_the_first`, `crates/axon-cluster/src/seed/sim.rs::a_batch_keeps_the_last_put_of_a_record_and_every_remove`, `crates/axon-cluster/src/seed/sim.rs::two_entries_that_reach_one_seed_count_once_and_a_named_seed_must_be_that_seed` |
| reordering | `crates/axon-cluster/src/seed/sim.rs::a_node_drops_a_boot_from_the_future_and_an_older_put` |
| latency | `crates/axon-cluster/src/seed/sim.rs::random_faults_never_break_a_view_and_everything_heals`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| clock skew or step | `crates/axon-cluster/src/seed/sim.rs::a_node_drops_a_boot_from_the_future_and_an_older_put`, `crates/axon-cluster/src/seed/mod.rs::a_seed_refuses_a_record_that_is_not_the_sessions_or_is_from_the_future`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| CPU starvation | `crates/axon-cluster/tests/world_faults.rs::a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed` |
| memory limit | `crates/axon-cluster/src/seed/sim.rs::a_seed_refuses_what_it_must_and_stays_within_its_bounds` |
| disk full | n/a: seeds keep their state in memory |
| disk error | n/a: seeds keep their state in memory |
| fd exhaustion | `crates/axon-cluster/tests/world_faults.rs::a_node_cut_one_way_from_its_seeds_rejoins_and_a_starved_node_or_one_failing_seed_is_not_missed` |
| panic | `crates/axon-net/tests/faults.rs::an_authenticator_that_panics_fails_its_handshake_on_either_side_and_the_listener_serves_the_next` |
| overload | `crates/axon-cluster/src/seed/sim.rs::a_seed_refuses_what_it_must_and_stays_within_its_bounds`, `crates/axon-cluster/src/seed/sim.rs::sessions_that_never_register_are_capped`, `crates/axon-cluster/src/seed/mod.rs::a_session_that_cannot_take_a_frame_is_closed` |
| hostile input | `crates/axon-cluster/src/seed/sim.rs::only_the_session_that_registered_a_record_can_end_it`, `crates/axon-cluster/src/seed/sim.rs::a_node_that_a_seed_drops_registers_again_and_an_invalid_record_is_not_taken`, `crates/axon-cluster/src/seed/mod.rs::a_seed_refuses_a_record_that_is_not_the_sessions_or_is_from_the_future`, `crates/axon-cluster/tests/seed.rs::a_wrong_credential_and_a_cluster_port_never_attach`, `crates/axon-cluster/src/seed/sim.rs::a_node_does_not_count_a_seed_with_other_timings` |

## streams

### streams: hub

| Fault | Covered by |
|---|---|
| crash | n/a: the hub is in-process; crashes of the sessions it serves are the relay rows |
| restart | `crates/axon-stream/tests/hub.rs::a_follower_sees_each_restart_as_a_ready_and_waits_through_a_close`, `crates/axon-stream/tests/hub.rs::restart_and_close_end_every_subscription_that_does_not_follow`, `crates/axon-stream/tests/hub.rs::a_replaced_session_loses_its_subscriptions_and_cannot_act`, `crates/axon-stream/tests/hub.rs::a_compacted_stream_hands_a_late_subscriber_the_latest_record_per_key_then_every_record_after_it` |
| pause | `crates/axon-stream/tests/hub.rs::a_slow_branch_ends_alone`, `crates/axon-stream/tests/relay.rs::a_subscriber_that_falls_behind_still_sees_every_record_in_order` |
| partition | n/a: in-process; relay rows |
| one-way cut | n/a: in-process; relay rows |
| loss | `crates/axon-stream/tests/hub.rs::a_relay_stream_adopts_positions_drops_overlap_and_refuses_gaps` |
| duplication | `crates/axon-stream/tests/hub.rs::a_relay_stream_adopts_positions_drops_overlap_and_refuses_gaps` |
| reordering | `crates/axon-stream/tests/hub.rs::a_cursor_ahead_of_a_relay_stream_waits_for_it`, `crates/axon-stream/tests/hub.rs::a_relay_stream_starts_a_state_subscriber_from_the_snapshot_its_parent_sends` |
| latency | n/a: in-process; relay rows |
| clock skew or step | n/a: the hub reads no clock; its callers pass positions, never times |
| CPU starvation | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| memory limit | `crates/axon-stream/tests/hub.rs::every_table_is_bounded_and_a_freed_place_can_be_taken_again`, `crates/axon-stream/tests/hub.rs::a_namespace_ring_bounds_its_streams_and_a_batch_larger_than_the_ring_keeps_its_tail`, `crates/axon-stream/tests/hub.rs::a_record_larger_than_the_branch_resets_the_replay_instead_of_hanging`, `crates/axon-stream/tests/hub.rs::a_compacted_stream_refuses_a_publish_past_its_limits_and_changes_nothing` |
| disk full | n/a: no disk; recovery row |
| disk error | n/a: no disk; recovery row |
| fd exhaustion | n/a: no descriptors |
| panic | `crates/axon-stream/tests/hub.rs::a_policy_that_panics_leaves_the_hub_as_it_was_and_serving`, `crates/axon-stream/tests/hub.rs::a_recovery_source_that_panics_charges_nothing_and_a_sound_one_replays_after_it` |
| overload | `crates/axon-stream/tests/hub.rs::a_slow_branch_ends_alone`, `crates/axon-stream/tests/hub.rs::a_rejected_publish_returns_the_callers_records`, `crates/axon-stream/tests/hub.rs::a_compacted_stream_refuses_a_publish_past_its_limits_and_changes_nothing` |
| hostile input | `crates/axon-stream/tests/hub.rs::revocation_ends_subscriptions_and_drops_interest`, `crates/axon-stream/tests/hub.rs::a_compacted_stream_refuses_a_publish_past_its_limits_and_changes_nothing` |

### streams: relay core and node

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/tests/relay.rs::leaves_see_every_record_once_while_relays_crash_and_return`, `crates/axon-stream/tests/relay.rs::a_node_relaying_a_key_originates_it_once_the_origin_crashed`, `crates/axon-stream/tests/world.rs::a_live_follower_of_a_crashed_origin_sees_every_record_once_and_in_order`, `crates/axon-stream/tests/world.rs::a_relay_whose_parent_returns_is_asked_again_within_a_keepalive_of_the_membership_change`, `crates/axon-stream/tests/world_relay.rs::leaves_see_every_record_once_while_relays_crash_and_return`, `crates/axon-stream/tests/world_relay.rs::a_node_relaying_a_key_originates_it_once_the_origin_crashed`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_relay.rs::a_late_leaf_gets_the_compacted_state_then_every_record_while_its_relay_crashes` |
| restart | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/tests/sim.rs::an_origin_that_relays_opens_its_key_again_while_children_still_ask_for_it`, `crates/axon-stream/tests/sim.rs::a_live_subscription_follows_the_key_through_a_restart_and_a_close`, `crates/axon-stream/tests/sim.rs::a_live_follower_on_the_origin_follows_its_key_to_the_node_that_opens_it_next`, `crates/axon-stream/tests/world.rs::a_live_follower_of_a_crashed_origin_sees_every_record_once_and_in_order`, `crates/axon-stream/tests/world.rs::a_relay_whose_parent_returns_is_asked_again_within_a_keepalive_of_the_membership_change`, `crates/axon-stream/src/node/tests.rs::a_parent_that_loses_its_upstream_just_after_a_membership_change_is_asked_again_at_once`, `crates/axon-stream/tests/world_relay.rs::leaves_see_every_record_once_while_relays_crash_and_return`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_relay.rs::a_late_leaf_gets_the_compacted_state_then_every_record_while_its_relay_crashes`, `crates/axon-stream/src/node/tests.rs::a_push_for_a_child_that_restarted_is_not_sent_to_its_next_boot` |
| pause | `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| partition | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_relay.rs::a_follower_revoked_during_a_partition_gets_no_more_records_once_it_heals`, `crates/axon-stream/tests/world_relay.rs::leaves_miss_nothing_when_their_relay_leaves_on_the_origins_side_of_a_partition`, `crates/axon-stream/tests/relay.rs::a_namespace_with_a_longer_upstream_timeout_waits_longer_before_origin_lost`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| one-way cut | `crates/axon-stream/tests/world.rs::a_live_follower_of_a_crashed_origin_sees_every_record_once_and_in_order` |
| loss | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| duplication | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return` |
| reordering | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return` |
| latency | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/tests/relay.rs::a_namespace_with_a_longer_upstream_timeout_waits_longer_before_origin_lost`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| clock skew or step | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/tests/world.rs::every_call_and_a_live_follower_recover_from_each_fault_under_the_shipped_timings`, `crates/axon-stream/tests/world_wan.rs::every_call_and_a_live_follower_recover_from_each_fault_across_regions` |
| CPU starvation | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| memory limit | `crates/axon-stream/tests/sim.rs::a_backlog_goes_out_in_pushes_that_fit_a_frame_and_a_child_that_leaves_holds_nothing`, `crates/axon-stream/src/wire/tests.rs::a_decoded_record_holds_only_its_own_batch`, `crates/axon-stream/tests/sim.rs::a_state_too_large_for_a_relays_pushes_ends_the_wait_below_it_and_nothing_else`, `crates/axon-stream/tests/sim.rs::a_branch_replays_before_it_takes_the_state`, `crates/axon-stream/tests/sim.rs::a_state_the_origin_has_no_room_for_yet_is_sent_once_room_frees_on_an_idle_stream` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| panic | n/a: no user code |
| overload | `crates/axon-stream/tests/sim.rs::a_parent_that_keeps_refusing_a_stream_is_retried_but_noted_once` |
| hostile input | `crates/axon-stream/tests/sim.rs::a_peer_can_neither_make_a_relay_pull_what_it_may_not_read_nor_take_the_local_session`, `crates/axon-stream/tests/sim.rs::a_child_that_may_not_replay_cannot_make_a_relay_pull_history`, `crates/axon-stream/tests/sim.rs::a_parent_whose_path_names_the_child_is_refused_as_a_loop`, `fuzz/fuzz_targets/stream_core.rs::fuzz_target`, `fuzz/fuzz_targets/stream_wire.rs::fuzz_target`, `crates/axon-stream/tests/relay.rs::a_revoked_edge_gets_no_more_records_once_its_parent_drops_it`, `crates/axon-stream/tests/world_relay.rs::a_follower_revoked_during_a_partition_gets_no_more_records_once_it_heals`, `crates/axon-stream/tests/sim.rs::a_node_denied_the_state_ends_its_waits_and_its_followers_keep_every_record` |

### streams: interests

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/interests.rs::parents_hold_what_their_children_want_through_lost_answers_restarts_and_forgetful_parents` |
| restart | `crates/axon-stream/tests/interests.rs::parents_hold_what_their_children_want_through_lost_answers_restarts_and_forgetful_parents`, `crates/axon-stream/tests/interests.rs::a_listener_that_returns_restarts_the_linger` |
| pause | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| partition | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| one-way cut | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| loss | `crates/axon-stream/tests/interests.rs::parents_hold_what_their_children_want_through_lost_answers_restarts_and_forgetful_parents`, `crates/axon-stream/tests/interests.rs::a_parent_that_forgets_the_child_in_the_middle_of_a_whole_set_is_sent_it_again`, `crates/axon-stream/tests/interests.rs::a_large_wanted_set_goes_out_in_slices_and_a_forgotten_child_sends_it_whole_again` |
| duplication | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| reordering | `crates/axon-stream/tests/interests.rs::parents_hold_what_their_children_want_through_lost_answers_restarts_and_forgetful_parents`, `crates/axon-stream/tests/interests.rs::a_stream_dropped_while_its_slice_is_in_flight_is_not_left_on_the_parent` |
| latency | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| clock skew or step | `crates/axon-stream/tests/interests.rs::parents_hold_what_their_children_want_through_lost_answers_restarts_and_forgetful_parents` |
| CPU starvation | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| memory limit | `crates/axon-stream/tests/interests.rs::a_large_wanted_set_goes_out_in_slices_and_a_forgotten_child_sends_it_whole_again` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: relay rows |
| panic | n/a: no user code |
| overload | `crates/axon-stream/tests/interests.rs::a_branch_with_a_record_is_pushed_however_many_drained_branches_are_ahead_of_it` |
| hostile input | `fuzz/fuzz_targets/stream_wire.rs::fuzz_target` |

### streams: recovery and cursors

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/relay.rs::a_restarted_edge_resumes_after_its_cursor_from_the_origin`, `crates/axon-stream/tests/sim.rs::history_that_nobody_holds_is_an_explicit_reset_never_a_silent_gap`, `crates/axon-stream/tests/world_relay.rs::a_restarted_edge_resumes_after_its_cursor_from_the_origin` |
| restart | `crates/axon-stream/tests/sim.rs::a_new_generation_gets_past_a_relay_that_was_asked_to_continue_the_old_one`, `crates/axon-stream/tests/sim.rs::a_key_closed_for_long_and_opened_again_older_is_followed_without_churn`, `crates/axon-stream/tests/hub.rs::a_relay_refuses_a_parent_that_is_behind_the_newest_generation_it_was_shown`, `crates/axon-stream/tests/world_relay.rs::a_restarted_edge_resumes_after_its_cursor_from_the_origin` |
| pause | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| partition | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| one-way cut | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| loss | `crates/axon-stream/tests/hub.rs::a_source_that_skips_records_is_a_reset_not_a_gap`, `crates/axon-stream/tests/sim.rs::a_node_that_gave_up_a_position_resets_its_follower_rather_than_skip_records`, `crates/axon-stream/tests/hub.rs::a_relay_that_lost_its_position_resets_a_follower_rather_than_move_it_within_the_generation` |
| duplication | `crates/axon-stream/tests/hub.rs::the_memory_store_checks_overlap_and_evicts_from_the_front` |
| reordering | `crates/axon-stream/tests/sim.rs::a_request_that_arrives_behind_a_newer_one_is_refused_and_its_late_answer_changes_nothing`, `crates/axon-stream/tests/sim.rs::a_late_leaf_that_starts_from_the_first_record_gets_the_origin_history_through_relays`, `crates/axon-stream/tests/sim.rs::history_that_nobody_holds_is_an_explicit_reset_never_a_silent_gap` |
| latency | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| clock skew or step | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| CPU starvation | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| memory limit | `crates/axon-stream/tests/hub.rs::recovery_resets_rather_than_deadlock_when_the_global_pool_is_held_by_live_tails` |
| disk full | n/a: the hub only reads a `RecoverySource` and its rings are memory; a failing source is the disk error row |
| disk error | `crates/axon-stream/tests/hub.rs::a_subscribe_that_fails_after_its_snapshot_charges_nothing` |
| fd exhaustion | `crates/axon-stream/tests/world_relay.rs::a_leaf_reads_every_record_once_and_its_parents_hold_what_it_wants_through_a_fault_between_them` |
| panic | `crates/axon-stream/tests/hub.rs::a_recovery_source_that_panics_charges_nothing_and_a_sound_one_replays_after_it` |
| overload | `crates/axon-stream/tests/hub.rs::recovery_blocks_on_a_full_branch_and_resumes_after_polling`, `crates/axon-stream/tests/world_relay.rs::a_live_follower_behind_a_link_slower_than_the_origin_is_reset_and_never_skips` |
| hostile input | `crates/axon-stream/tests/hub.rs::history_that_is_gone_is_refused_at_subscribe`, `crates/axon-stream/tests/hub.rs::a_source_that_skips_records_is_a_reset_not_a_gap`, `fuzz/fuzz_targets/stream_core.rs::fuzz_target` |

### streams: disk store

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/world_disk.rs::a_process_crash_resumes_the_generation_after_the_last_durable_record`, `crates/axon-stream/tests/world_disk.rs::a_machine_crash_loses_at_most_the_last_interval_and_no_subscriber_sees_a_gap`, `crates/axon-stream/tests/world_disk.rs::a_machine_crash_after_a_process_crash_loses_nothing_a_subscriber_saw`, `crates/axon-stream/tests/world_disk.rs::a_compacted_stream_resumes_its_state_after_a_crash` |
| restart | `crates/axon-stream/tests/world_disk.rs::a_process_crash_resumes_the_generation_after_the_last_durable_record`, `crates/axon-stream/tests/world_disk.rs::an_origin_that_relays_its_key_resumes_it_with_no_repeat_and_no_gap`, `crates/axon-stream/tests/world_disk.rs::a_restart_and_a_close_remove_the_old_files`, `crates/axon-stream/src/disk/tests.rs::batches_round_trip_across_segments_and_resume_after_the_last`, `crates/axon-stream/src/disk/tests.rs::a_compacted_state_outlives_restarts_and_retention`, `crates/axon-stream/src/disk/tests.rs::an_older_generation_left_by_a_crash_is_removed_at_the_first_write` |
| pause | `crates/axon-stream/tests/world_disk.rs::a_paused_origin_loses_nothing_and_resumes_after_the_last_durable_record` |
| partition | n/a: the origin writes its own files; its children see a partition as in the relay tables |
| one-way cut | n/a: the origin writes its own files |
| loss | `crates/axon-stream/tests/world_disk.rs::a_machine_crash_loses_at_most_the_last_interval_and_no_subscriber_sees_a_gap`, `crates/axon-stream/tests/world_disk.rs::a_machine_crash_after_a_process_crash_loses_nothing_a_subscriber_saw` |
| duplication | `crates/axon-stream/src/disk/tests.rs::a_write_that_stayed_although_it_failed_is_read_once` |
| reordering | n/a: one write at a time per node, its ops in order |
| latency | `crates/axon-stream/tests/world_disk.rs::a_machine_crash_loses_at_most_the_last_interval_and_no_subscriber_sees_a_gap` |
| clock skew or step | `crates/axon-stream/tests/world_disk.rs::a_stepped_and_drifting_clock_at_the_origin_loses_nothing` |
| CPU starvation | `crates/axon-stream/tests/world_disk.rs::a_starved_origin_writes_late_and_loses_nothing_a_subscriber_saw` |
| memory limit | `crates/axon-stream/src/disk/tests.rs::too_much_unwritten_is_refused_behind`, `crates/axon-stream/src/disk/tests.rs::batches_round_trip_across_segments_and_resume_after_the_last`, `crates/axon-stream/src/node/tests.rs::a_stream_resumed_from_disk_is_charged_to_its_namespace` |
| disk full | `crates/axon-stream/tests/world_disk.rs::a_full_disk_refuses_publishes_and_the_stream_keeps_serving`, `crates/axon-stream/src/disk/tests.rs::a_failed_write_refuses_publishes_until_it_is_written_again` |
| disk error | `crates/axon-stream/src/disk/tests.rs::a_failed_write_refuses_publishes_until_it_is_written_again`, `crates/axon/src/files.rs::os_files_cut_a_failed_write_back_and_take_it_again`, `crates/axon-stream/src/disk/tests.rs::a_torn_tail_is_cut_at_the_last_whole_record`, `crates/axon-stream/src/disk/tests.rs::a_crash_anywhere_inside_a_write_keeps_every_record_written_before`, `crates/axon-stream/src/disk/tests.rs::a_crc_mismatch_cuts_there_and_drops_every_later_segment`, `crates/axon-stream/src/disk/tests.rs::a_history_cut_whole_goes_on_after_what_the_names_prove`, `crates/axon-stream/src/node/tests.rs::an_open_that_fails_after_letting_a_pulled_key_go_pulls_it_again` |
| fd exhaustion | n/a: to the store it is a failed write (disk error row) |
| panic | `crates/axon-stream/tests/world_disk.rs::a_store_that_panics_mid_write_fails_the_write_and_the_writer_goes_on` |
| overload | `crates/axon-stream/src/disk/tests.rs::too_much_unwritten_is_refused_behind` |
| hostile input | `crates/axon-stream/src/disk/tests.rs::a_torn_tail_is_cut_at_the_last_whole_record`, `crates/axon-stream/src/disk/tests.rs::a_crc_mismatch_cuts_there_and_drops_every_later_segment`, `crates/axon-stream/src/disk/tests.rs::a_key_whose_file_names_would_be_too_long_is_refused` |

### streams: namespace limits

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/world_limits.rs::a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins` |
| restart | `crates/axon-stream/tests/world_limits.rs::a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins` |
| pause | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| partition | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| one-way cut | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| loss | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| duplication | n/a: a take and an acquire happen once on the origin; the replicas' messages are Quotas' (cluster: quotas) |
| reordering | n/a: as duplication |
| latency | `crates/axon-stream/tests/world_limits.rs::a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins`, `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| clock skew or step | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| CPU starvation | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| memory limit | `crates/axon-stream/tests/hub.rs::a_namespace_holds_its_rings_to_its_budget_and_a_close_or_its_own_trim_makes_room` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds` |
| panic | n/a: no user code runs inside the limits |
| overload | `crates/axon-stream/tests/world_limits.rs::a_crashed_origin_gives_its_open_stream_back_and_the_rates_hold_across_origins`, `crates/axon-stream/tests/world_limits.rs::faults_between_origins_never_pause_a_namespace_and_hold_it_within_their_bounds`, `crates/axon-stream/tests/hub.rs::a_namespace_holds_its_subscriptions_to_its_limit_and_a_freed_one_can_be_taken_again`, `crates/axon-stream/src/node/tests.rs::namespace_limits_across_the_cluster_need_quotas_and_hold_on_one_node` |
| hostile input | `crates/axon-stream/tests/hub.rs::a_namespace_holds_its_rings_to_its_budget_and_a_close_or_its_own_trim_makes_room` |

### streams: push window

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` |
| restart | `crates/axon-stream/src/core/tests.rs::a_parent_boot_retired_stays_retired`, `crates/axon-stream/src/core/tests.rs::a_downstream_made_again_gets_a_new_channel_and_its_old_pushes_are_retired`, `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides`, `crates/axon-stream/src/node/tests.rs::a_push_for_a_child_that_restarted_is_not_sent_to_its_next_boot` |
| pause | `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` |
| partition | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return` |
| one-way cut | `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides`, `crates/axon-stream/src/node/tests.rs::a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped` |
| loss | `crates/axon-stream/tests/sim.rs::pushes_resent_after_a_lost_answer_are_applied_once`, `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides`, `crates/axon-stream/src/node/tests.rs::a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped` |
| duplication | `crates/axon-stream/tests/sim.rs::pushes_resent_after_a_lost_answer_are_applied_once`, `crates/axon-stream/src/core/tests.rs::a_child_applies_only_the_next_push_of_its_channel` |
| reordering | `crates/axon-stream/tests/sim.rs::every_leaf_sees_every_record_once_while_relays_fail_and_return`, `crates/axon-stream/src/core/tests.rs::a_child_applies_only_the_next_push_of_its_channel` |
| latency | `crates/axon-stream/tests/world_relay.rs::a_leaf_200_ms_away_reads_several_pushes_per_round_trip`, `crates/axon-stream/tests/world_relay.rs::one_stream_200_ms_away_reads_several_pushes_per_round_trip`, `crates/axon-stream/src/core/tests.rs::the_window_grows_with_the_rate_and_stops_at_its_ceiling` |
| clock skew or step | `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` |
| CPU starvation | `crates/axon-stream/tests/world_relay.rs::a_window_of_pushes_survives_a_reset_and_a_restart_on_both_sides` |
| memory limit | `crates/axon-stream/src/core/tests.rs::the_node_cap_holds_every_channel_past_its_first_push`, `crates/axon-stream/src/core/tests.rs::a_slow_reader_is_reset_with_pushes_in_flight`, `crates/axon-stream/src/core/tests.rs::a_streams_charge_is_released_on_applied_on_its_end_and_on_a_retire_but_not_on_a_go_back`, `crates/axon-stream/src/core/tests.rs::a_default_subscriber_holds_the_pushes_of_a_round_trip_applied_back_to_back` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors of its own; a redial that cannot open one is a failed call (loss) |
| panic | n/a: no user code |
| overload | `crates/axon-cluster/tests/cluster/remote.rs::a_request_admitted_late_counts_its_timeout_from_admission`, `crates/axon-stream/src/core/tests.rs::the_node_cap_holds_every_channel_past_its_first_push`, `crates/axon-stream/src/core/tests.rs::a_slow_reader_is_reset_with_pushes_in_flight` |
| hostile input | `fuzz/fuzz_targets/stream_wire.rs::fuzz_target`, `fuzz/fuzz_targets/stream_core.rs::fuzz_target`, `crates/axon-stream/src/wire/tests.rs::frames_round_trip_and_every_truncation_is_refused`, `crates/axon-stream/src/core/tests.rs::an_answer_past_the_last_push_sent_drops_the_child` |

### streams: lanes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-stream/tests/world_lanes.rs::a_child_restart_ends_both_lanes_pumps_and_each_namespace_resumes` |
| restart | `crates/axon-stream/tests/world_lanes.rs::a_child_restart_ends_both_lanes_pumps_and_each_namespace_resumes` |
| pause | gap |
| partition | `crates/axon-stream/tests/world_lanes.rs::a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring` |
| one-way cut | gap |
| loss | `crates/axon-stream/tests/world_lanes.rs::a_dead_bulk_lane_stalls_only_its_namespaces_and_they_catch_up_from_the_ring` |
| duplication | n/a: the relay core drops records it already has (streams rows) |
| reordering | `fuzz/fuzz_targets/stream_core.rs::fuzz_target` |
| latency | `crates/axon-stream/tests/world_lanes.rs::a_bulk_namespace_saturating_the_links_keeps_a_live_namespaces_records_fast` |
| clock skew or step | n/a: lanes read no clock |
| CPU starvation | gap |
| memory limit | `crates/axon-stream/tests/world_lanes.rs::a_bulk_namespace_saturating_the_links_keeps_a_live_namespaces_records_fast`, `crates/axon-stream/src/core/tests.rs::each_lane_has_a_channel_and_a_window_of_its_own` |
| disk full | n/a: the disk rows of streams stand; a lane changes no write |
| disk error | n/a: as disk full |
| fd exhaustion | gap |
| panic | n/a: no user code in the pump |
| overload | `crates/axon-stream/tests/relay.rs::start_refuses_a_namespace_on_control_and_a_push_larger_than_its_lanes_frame`, `crates/axon-stream/src/core/tests.rs::interests_keep_within_max_interest_bytes` |
| hostile input | `fuzz/fuzz_targets/stream_core.rs::fuzz_target` |

## durable log

### durable log: files::Log

| Fault | Covered by |
|---|---|
| crash | `crates/axon/src/files/log/tests.rs::a_crash_at_any_byte_of_a_write_reopens_to_a_prefix`, `crates/axon/src/files/log/tests.rs::random_changes_match_a_model_across_failures_crashes_and_opens`, `crates/axon/src/files/log/tests.rs::a_segment_stays_until_the_frame_that_ended_its_records_is_written`, `crates/axon-testkit/tests/world.rs::a_write_over_synced_bytes_is_lost_to_a_machine_crash_until_it_is_synced`, `crates/axon-cluster/tests/world_log.rs::a_deposit_answered_survives_a_machine_crash_at_any_moment`, `crates/axon-cluster/tests/world_log.rs::a_deposit_answered_survives_a_process_crash_at_any_moment` |
| restart | `crates/axon/src/files/log/tests.rs::records_of_many_keys_go_in_one_write_and_read_back_after_open`, `crates/axon/src/files/log/tests.rs::a_key_written_again_after_a_remove_goes_on_from_its_numbers`, `crates/axon/src/files/log/tests.rs::a_remove_keeps_older_frames_of_its_key_dead_after_the_key_is_trimmed_again`, `crates/axon/src/files/log/tests.rs::random_changes_match_a_model_across_failures_crashes_and_opens`, `crates/axon-cluster/tests/world_log.rs::a_deposit_answered_survives_a_process_crash_at_any_moment` |
| pause | n/a: the log runs no task and waits for nothing; while its one write is out, changes queue up to `max_unwritten_bytes` (overload row) |
| partition | n/a: no network |
| one-way cut | n/a: no network |
| loss | `crates/axon/src/files/log/tests.rs::a_crash_at_any_byte_of_a_write_reopens_to_a_prefix`, `crates/axon/src/files/log/tests.rs::random_changes_match_a_model_across_failures_crashes_and_opens`, `crates/axon-cluster/tests/world_log.rs::a_deposit_answered_survives_a_machine_crash_at_any_moment` |
| duplication | `crates/axon/src/files/log/tests.rs::a_mostly_dead_old_segment_is_copied_forward_and_removed`, `crates/axon/src/files/log/tests.rs::random_changes_match_a_model_across_failures_crashes_and_opens` |
| reordering | `crates/axon/src/files/log/tests.rs::a_crash_at_any_byte_of_a_write_reopens_to_a_prefix` |
| latency | n/a: no network; a slow write is the pause row |
| clock skew or step | n/a: the log reads no clock; `now_ms` only dates the write that is out (`writing_since`) |
| CPU starvation | n/a: as pause |
| memory limit | `crates/axon/src/files/log/tests.rs::refusals_change_nothing` |
| disk full | `crates/axon/src/files/log/tests.rs::refusals_change_nothing`, `crates/axon/src/files/log/tests.rs::a_failed_write_goes_again_byte_for_byte_and_durable_waits`, `crates/axon-cluster/tests/world_log.rs::a_full_disk_holds_deposits_until_it_has_room_and_loses_none` |
| disk error | `crates/axon/src/files/log/tests.rs::a_failed_write_goes_again_byte_for_byte_and_durable_waits`, `crates/axon/src/files/log/tests.rs::a_segment_copies_read_from_stays_until_they_land`, `crates/axon/src/files/log/tests.rs::random_changes_match_a_model_across_failures_crashes_and_opens`, `crates/axon/src/files/log/tests.rs::a_torn_tail_is_cut_and_bytes_beyond_reach_are_refused`, `crates/axon-cluster/tests/world_log.rs::a_deposit_answered_survives_a_machine_crash_at_any_moment`, `crates/axon-cluster/tests/world_log.rs::a_deposit_answered_survives_a_process_crash_at_any_moment` |
| fd exhaustion | n/a: to the log it is a failed write (disk error row) |
| panic | n/a: the log runs no user code; a write that panics, caught as in the guide, is a failed write (disk error row) |
| overload | `crates/axon/src/files/log/tests.rs::refusals_change_nothing`, `crates/axon-cluster/tests/world_log.rs::a_full_disk_holds_deposits_until_it_has_room_and_loses_none`, `crates/axon-cluster/tests/world_log.rs::a_crowd_of_keys_starting_at_once_each_load` |
| hostile input | `crates/axon/src/files/log/tests.rs::open_refuses_a_bad_config_a_log_of_another_id_and_data_after_a_bad_frame`, `crates/axon/src/files/log/tests.rs::a_torn_tail_is_cut_and_bytes_beyond_reach_are_refused`, `fuzz/fuzz_targets/log_frame.rs::fuzz_target` |

## observability

### observability: telemetry

| Fault | Covered by |
|---|---|
| crash | `crates/axon-obs/tests/faults.rs::a_node_that_crashes_and_boots_again_is_shown_and_collected_from_its_new_boot` |
| restart | `crates/axon-obs/tests/telemetry.rs::a_node_is_inspectable_from_an_allowed_peer` |
| pause | `crates/axon-obs/tests/faults.rs::a_node_stopped_by_sigstop_answers_its_probes_and_is_sampled_and_collected_once_it_continues`, `crates/axon-obs/tests/faults.rs::workers_held_by_handlers_that_do_not_return_stop_no_telemetry_probe_console_or_collector` |
| partition | `crates/axon-obs/tests/faults.rs::a_partition_between_the_console_and_a_node_holds_nothing_up_and_loses_nothing` |
| one-way cut | `crates/axon-obs/tests/faults.rs::a_one_way_cut_to_a_node_holds_nothing_up_and_loses_nothing`, `crates/axon-obs/tests/faults.rs::a_one_way_cut_from_a_node_holds_nothing_up_and_loses_nothing` |
| loss | `crates/axon-obs/tests/telemetry.rs::a_reader_that_missed_batches_gets_every_series_again` |
| duplication | `crates/axon-obs/tests/faults.rs::connections_that_keep_breaking_repeat_pages_but_store_each_batch_once` |
| reordering | `crates/axon-obs/tests/faults.rs::answers_that_come_after_the_next_request_store_each_batch_once` |
| latency | `crates/axon-obs/tests/faults.rs::a_slow_wire_to_a_node_holds_nothing_up_and_loses_nothing` |
| clock skew or step | `crates/axon-obs/tests/faults.rs::an_unchanged_series_is_stored_again_when_due_whichever_way_the_wall_clock_steps`, `crates/axon-obs/tests/faults.rs::an_unchanged_series_is_stored_again_once_the_stamps_say_it_is_due_on_a_wall_clock_that_runs_slow` |
| CPU starvation | `crates/axon-obs/tests/faults.rs::workers_spinning_in_their_handlers_stop_no_telemetry_probe_console_or_collector` |
| memory limit | `crates/axon-obs/tests/telemetry.rs::a_links_stats_keep_their_histograms_sparse`, `crates/axon-obs/tests/telemetry.rs::only_the_busiest_connections_get_stored_series`, `crates/axon-obs/tests/telemetry.rs::the_snapshot_counts_the_keys_per_kind_and_lists_them_only_when_asked`, `crates/axon-obs/tests/telemetry.rs::a_node_pages_a_kinds_keys_by_prefix_from_a_key_on`, `crates/axon-obs/tests/telemetry.rs::long_keys_are_listed_up_to_a_budget_so_a_peer_still_reads_the_snapshot` |
| disk full | n/a: no disk |
| disk error | n/a: no disk |
| fd exhaustion | n/a: no descriptors of its own |
| panic | n/a: no user code |
| overload | `crates/axon-obs/tests/telemetry.rs::a_dropped_batch_is_not_counted_as_stored` |
| hostile input | `crates/axon-obs/tests/telemetry.rs::a_node_is_inspectable_from_an_allowed_peer` |

### observability: metrics collection

| Fault | Covered by |
|---|---|
| crash | `crates/axon-obs/tests/metrics.rs::a_collector_stores_the_cluster_and_answers_history` |
| restart | `crates/axon-obs/tests/metrics.rs::a_lost_reply_or_a_new_collector_never_gives_a_sink_the_same_batch_twice`, `crates/axon-obs/tests/metrics.rs::a_collector_stores_each_nodes_timeline_once_and_answers_it_by_node_and_kind` |
| pause | `crates/axon-obs/tests/metrics.rs::a_collector_stores_the_cluster_and_answers_history` |
| partition | `crates/axon-obs/tests/faults.rs::a_partition_between_the_console_and_a_node_holds_nothing_up_and_loses_nothing` |
| one-way cut | `crates/axon-obs/tests/faults.rs::a_one_way_cut_to_a_node_holds_nothing_up_and_loses_nothing`, `crates/axon-obs/tests/faults.rs::a_one_way_cut_from_a_node_holds_nothing_up_and_loses_nothing` |
| loss | `crates/axon-obs/tests/metrics.rs::a_lost_reply_or_a_new_collector_never_gives_a_sink_the_same_batch_twice` |
| duplication | `crates/axon-obs/tests/metrics.rs::a_lost_reply_or_a_new_collector_never_gives_a_sink_the_same_batch_twice`, `crates/axon-obs/tests/metrics.rs::memory_provider_contract` |
| reordering | `crates/axon-obs/tests/faults.rs::answers_that_come_after_the_next_request_store_each_batch_once` |
| latency | `crates/axon-obs/tests/faults.rs::a_slow_wire_to_a_node_holds_nothing_up_and_loses_nothing` |
| clock skew or step | `crates/axon-obs/tests/faults.rs::a_collector_stores_each_batch_once_while_the_wall_clock_steps_back_and_forward` |
| CPU starvation | `crates/axon-obs/tests/faults.rs::workers_spinning_in_their_handlers_stop_no_telemetry_probe_console_or_collector` |
| memory limit | `crates/axon-obs/tests/metrics.rs::memory_metrics_holds_at_most_its_bytes_and_lets_the_oldest_go_first`, `crates/axon-obs/tests/metrics.rs::events_a_sink_cannot_take_are_held_up_to_the_buffer_and_the_oldest_are_dropped_and_counted`, `crates/axon-obs/tests/metrics.rs::a_collector_stores_each_nodes_timeline_once_and_answers_it_by_node_and_kind` |
| disk full | `crates/axon-obs/tests/metrics.rs::a_full_disk_is_the_sinks_error_until_it_frees_and_then_every_batch_is_stored_once` |
| disk error | `crates/axon-obs/tests/metrics.rs::a_collector_stores_the_cluster_and_answers_history` |
| fd exhaustion | n/a: no descriptors of its own |
| panic | `crates/axon-obs/tests/metrics.rs::a_sink_that_panics_after_storing_is_asked_where_it_stands_and_gets_each_batch_once` |
| overload | `crates/axon-obs/tests/metrics.rs::a_cluster_past_max_nodes_is_an_error_not_a_cycle_that_asks_them_all` |
| hostile input | `crates/axon-obs/tests/metrics.rs::a_collector_stores_the_cluster_and_answers_history` |

### observability: ClickHouse sink

| Fault | Covered by |
|---|---|
| crash | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| restart | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| pause | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| partition | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| one-way cut | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| loss | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| duplication | `crates/axon-obs/tests/metrics.rs::clickhouse_provider_contract` |
| reordering | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| latency | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| clock skew or step | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| CPU starvation | n/a: the collector row covers a slow sink |
| memory limit | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| disk full | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| disk error | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| fd exhaustion | `crates/axon-obs/tests/descriptors.rs::probes_and_the_console_outlive_a_process_out_of_descriptors` |
| panic | n/a: no user code |
| overload | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |
| hostile input | `crates/axon-obs/tests/faults.rs::the_clickhouse_sink_stores_each_batch_once_through_every_fault_on_its_path` |

### observability: dashboard host

| Fault | Covered by |
|---|---|
| crash | `crates/axon-obs/tests/faults.rs::a_node_that_crashes_and_boots_again_is_shown_and_collected_from_its_new_boot` |
| restart | `crates/axon-obs/tests/health.rs::a_restarted_node_serves_its_probes_and_console_on_the_same_ports`, `crates/axon-obs/tests/faults.rs::a_node_that_crashes_and_boots_again_is_shown_and_collected_from_its_new_boot` |
| pause | `crates/axon-obs/tests/dashboard.rs::requests_and_live_streams_past_the_limit_are_refused_until_a_place_frees_up`, `crates/axon-obs/tests/dashboard.rs::a_client_has_16_handshakes_under_way_and_a_trusted_proxy_only_the_ports_64` |
| partition | `crates/axon-obs/tests/faults.rs::a_partition_between_the_console_and_a_node_holds_nothing_up_and_loses_nothing` |
| one-way cut | `crates/axon-obs/tests/faults.rs::a_one_way_cut_to_a_node_holds_nothing_up_and_loses_nothing`, `crates/axon-obs/tests/faults.rs::a_one_way_cut_from_a_node_holds_nothing_up_and_loses_nothing` |
| loss | `crates/axon-obs/tests/faults.rs::connections_that_keep_breaking_repeat_pages_but_store_each_batch_once` |
| duplication | n/a: HTTP reads |
| reordering | n/a: HTTP reads |
| latency | `crates/axon-obs/tests/dashboard.rs::a_client_has_16_handshakes_under_way_and_a_trusted_proxy_only_the_ports_64`, `crates/axon-obs/tests/faults.rs::a_slow_wire_to_a_node_holds_nothing_up_and_loses_nothing`, `crates/axon-obs/tests/dashboard.rs::a_node_that_never_answers_holds_the_page_only_until_the_snapshot_deadline` |
| clock skew or step | `crates/axon-obs/tests/dashboard.rs::a_session_is_a_signed_cookie_that_slides_expires_and_rotates_with_its_key`, `crates/axon-obs/tests/faults.rs::a_console_nobody_watches_goes_idle_after_the_wall_clock_steps_back`, `crates/axon-obs/tests/faults.rs::history_is_served_by_the_collector_that_collected_last_whatever_their_walls_say` |
| CPU starvation | `crates/axon-obs/tests/faults.rs::workers_spinning_in_their_handlers_stop_no_telemetry_probe_console_or_collector` |
| memory limit | `crates/axon-obs/tests/dashboard.rs::a_snapshot_past_the_response_bound_is_that_nodes_error_and_the_page_still_loads`, `crates/axon-obs/tests/dashboard.rs::a_request_has_10_s_a_head_16_kib_and_a_sign_in_4_kib_on_every_port` |
| disk full | n/a: the UI is embedded in the binary |
| disk error | n/a: the UI is embedded in the binary |
| fd exhaustion | `crates/axon-obs/tests/descriptors.rs::probes_and_the_console_outlive_a_process_out_of_descriptors`, `crates/axon-obs/tests/health.rs::one_address_holds_64_connections_a_port_256_and_a_probe_from_elsewhere_still_answers` |
| panic | n/a: no user code |
| overload | `crates/axon-obs/tests/health.rs::one_address_holds_64_connections_a_port_256_and_a_probe_from_elsewhere_still_answers`, `crates/axon-obs/tests/dashboard.rs::a_request_has_10_s_a_head_16_kib_and_a_sign_in_4_kib_on_every_port`, `crates/axon-obs/tests/dashboard.rs::an_answer_the_client_stops_reading_is_cut_after_10_s_and_gives_its_place_back`, `crates/axon-obs/tests/dashboard.rs::requests_and_live_streams_past_the_limit_are_refused_until_a_place_frees_up`, `crates/axon-obs/tests/dashboard.rs::sign_ins_are_limited_per_client_and_per_user_and_only_a_trusted_proxy_names_the_client`, `crates/axon-obs/tests/dashboard.rs::a_client_has_16_handshakes_under_way_and_a_trusted_proxy_only_the_ports_64` |
| hostile input | `crates/axon-obs/tests/dashboard.rs::the_dashboard_serves_the_cluster_over_http`, `crates/axon-obs/tests/dashboard.rs::a_session_is_a_signed_cookie_that_slides_expires_and_rotates_with_its_key`, `crates/axon-obs/tests/dashboard.rs::sign_ins_are_limited_per_client_and_per_user_and_only_a_trusted_proxy_names_the_client` |

### observability: health probes

| Fault | Covered by |
|---|---|
| crash | `crates/axon-obs/tests/health.rs::a_dropped_system_fails_liveness` |
| restart | `crates/axon-obs/tests/health.rs::a_restarted_node_serves_its_probes_and_console_on_the_same_ports` |
| pause | `crates/axon-obs/tests/faults.rs::a_node_stopped_by_sigstop_answers_its_probes_and_is_sampled_and_collected_once_it_continues`, `crates/axon-obs/tests/faults.rs::workers_held_by_handlers_that_do_not_return_stop_no_telemetry_probe_console_or_collector` |
| partition | `crates/axon-obs/tests/health.rs::readiness_is_refused_while_a_node_is_isolated_and_given_back_once_it_is_heard_again` |
| one-way cut | `crates/axon-obs/tests/health.rs::a_node_discovery_cannot_hear_stays_ready_while_a_peer_still_lists_it` |
| loss | `crates/axon-obs/tests/health.rs::a_node_whose_discovery_calls_are_lost_one_in_three_stays_ready` |
| duplication | n/a: HTTP reads |
| reordering | n/a: HTTP reads |
| latency | `crates/axon-obs/tests/health.rs::a_node_with_slow_discovery_stays_ready_and_its_probes_answer_at_once` |
| clock skew or step | n/a: a probe reads no clock; the isolation it reports is timed on the monotonic clock, the cluster row |
| CPU starvation | `crates/axon-obs/tests/faults.rs::workers_spinning_in_their_handlers_stop_no_telemetry_probe_console_or_collector` |
| memory limit | `crates/axon-obs/tests/dashboard.rs::a_request_has_10_s_a_head_16_kib_and_a_sign_in_4_kib_on_every_port` |
| disk full | n/a: nothing on disk |
| disk error | n/a: nothing on disk |
| fd exhaustion | `crates/axon-obs/tests/descriptors.rs::probes_and_the_console_outlive_a_process_out_of_descriptors`, `crates/axon-obs/tests/health.rs::one_address_holds_64_connections_a_port_256_and_a_probe_from_elsewhere_still_answers` |
| panic | n/a: no user code |
| overload | `crates/axon-obs/tests/health.rs::one_address_holds_64_connections_a_port_256_and_a_probe_from_elsewhere_still_answers`, `crates/axon-obs/tests/dashboard.rs::a_request_has_10_s_a_head_16_kib_and_a_sign_in_4_kib_on_every_port`, `crates/axon-obs/tests/dashboard.rs::an_answer_the_client_stops_reading_is_cut_after_10_s_and_gives_its_place_back` |
| hostile input | `crates/axon-obs/tests/health.rs::the_dashboard_host_answers_probes_without_credentials_and_readiness_ends_at_shutdown` |

### observability: Prometheus endpoint

| Fault | Covered by |
|---|---|
| crash | `crates/axon-obs/tests/prometheus.rs::a_crashed_node_booted_again_serves_its_new_counters_on_the_same_port` |
| restart | `crates/axon-obs/tests/prometheus.rs::a_crashed_node_booted_again_serves_its_new_counters_on_the_same_port` |
| pause | `crates/axon-obs/tests/faults.rs::workers_held_by_handlers_that_do_not_return_stop_no_telemetry_probe_console_or_collector` |
| partition | n/a: a scrape reads only its own node's last sample, over no cluster session |
| one-way cut | n/a: a scrape reads only its own node's last sample, over no cluster session |
| loss | n/a: a lost scrape is Prometheus's to retry; the counters it reads are cumulative, so the next one loses nothing |
| duplication | n/a: HTTP reads |
| reordering | n/a: HTTP reads |
| latency | n/a: a scrape waits on no other node; a scraper that reads slowly is the overload row |
| clock skew or step | n/a: the text carries no timestamps, Prometheus stamps each scrape on its own clock |
| CPU starvation | `crates/axon-obs/tests/faults.rs::workers_spinning_in_their_handlers_stop_no_telemetry_probe_console_or_collector` |
| memory limit | `crates/axon-obs/tests/prometheus.rs::series_are_per_actor_type_and_per_persisted_actor_never_per_actor` |
| disk full | n/a: nothing on disk |
| disk error | n/a: nothing on disk |
| fd exhaustion | `crates/axon-obs/tests/descriptors.rs::probes_and_the_console_outlive_a_process_out_of_descriptors` |
| panic | n/a: no user code runs; an `Actor::label` is only escaped |
| overload | `crates/axon-obs/tests/dashboard.rs::an_answer_the_client_stops_reading_is_cut_after_10_s_and_gives_its_place_back`, `crates/axon-obs/tests/health.rs::one_address_holds_64_connections_a_port_256_and_a_probe_from_elsewhere_still_answers` |
| hostile input | `crates/axon-obs/tests/prometheus.rs::metrics_are_off_until_served_and_show_the_nodes_latest_sample_escaped_in_the_text_format` |
