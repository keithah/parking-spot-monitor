# Task 6 Report: Bounded Matrix Outbox Worker

## Status

Complete. Matrix network delivery is isolated from the capture path behind a singleton, event-driven outbox worker. Frame-produced notices are durable before delivery, runtime polling drains are removed, shutdown is bounded, and the complete test suite passes.

## Implementation

- Added `matrix.outbox_retry_interval_seconds` with a finite, positive default of 60 seconds and included it in the sanitized configuration summary.
- Added a singleton daemon worker to `MatrixOutboxDelivery`. It wakes on durable enqueue, drains at most one record per pass, paces retries from a monotonic deadline, survives unexpected delivery failures, and exposes only safe worker health metadata.
- Made `close()` idempotent: it signals stop and wake, performs a bounded join, and then closes the Matrix client. The worker checks stop between phases so it cannot begin a later network phase after shutdown proceeds.
- Added `enqueue_text_notice(event_name, event)` for quiet-window and owner-notification notices, preserving existing message text and transaction IDs. Snapshot records declare text/upload/image phases in one durable `enqueue_with_phases` write.
- Routed frame-produced open, occupied, quiet-window, and owner notices through durable enqueue APIs. Lifecycle messages remain immediate outside the capture loop.
- Removed startup/capture outbox drains and their compatibility fallback. The default factory now starts the worker with validated configuration.
- Added narrow runtime Matrix delivery and command-service protocols, promoted cross-module overlay/presence helpers to public names, and enforced that sibling runtime modules do not import underscore-prefixed helpers.
- Added worker health fields to runtime health output while retaining redaction of suspicious values.
- Accepted `queued` dispatch evidence when selecting operator-feedback alert candidates, because durable dispatch is intentionally recorded before asynchronous delivery.

## Worker State Machine and Invariants

State flow:

1. `stopped -> waiting`: `start_worker()` creates at most one live `matrix-outbox-delivery` thread and gives it an initial wake so records persisted by an earlier process are discovered.
2. `waiting -> draining`: an enqueue wake, pending-record continuation, or retry deadline initiates one bounded `drain_outbox(max_records=1)` pass.
3. `draining -> waiting`: an empty queue waits indefinitely; remaining pending work schedules an immediate next pass; retrying work waits until the monotonic retry deadline.
4. `draining -> cooldown`: retryable or unexpected failure records safe error metadata and paces the next attempt. An enqueue during cooldown may deliver a new pending record without prematurely retrying the failed record.
5. `waiting/draining -> stopped`: `close()` sets stop and wake, joins for a fixed bound, then closes the client.

Concurrency invariants:

- The outbox JSON is the source of truth; wake events are hints and never carry work.
- Enqueue persists before setting the wake event. If enqueue races with worker summarization, either the worker observes pending work or the event remains set, so a wake cannot be lost.
- A dedicated drain lock serializes worker and manual drains, preventing duplicate network phases. Enqueue does not take that lock, so capture-thread durable writes never wait on a network call.
- Worker metadata locks are not held across filesystem access, network calls, joins, or client close. There is no reverse lock-acquisition path.
- Worker health exposes booleans, timestamps, and exception class names only; exception messages and payload data are not exported.
- A bounded join cannot forcibly interrupt a Matrix client call that ignores close/cancellation. In that case the daemon may finish its current call after `close()` returns, but it observes stop before beginning another phase and the record remains durable.

## TDD and Verification Evidence

Initial required RED:

```text
$ python3 -m pytest tests/test_matrix_outbox_delivery.py tests/test_startup.py -k 'worker or enqueue or startup_drains' -q
12 failed, 4 passed, 115 deselected in 1.47s
```

The failures covered missing worker/start/text APIs, synchronous quiet/owner delivery, absent default worker startup, and three runtime drain calls.

Additional RED checks:

```text
$ python3 -m pytest tests/test_config.py -k 'outbox_retry' -q
5 failed, 112 deselected in 0.20s

$ python3 -m pytest tests/test_module_decomposition.py -k 'runtime_modules_do_not_import_private_sibling_helpers or promoted_runtime_helpers' -q
2 failed, 142 deselected in 0.27s

$ python3 -m pytest tests/test_startup.py::test_runtime_health_includes_safe_matrix_worker_fields -q
1 failed in 1.17s
```

Focused mutation-oriented RED/GREEN checks also demonstrated that closing logging streams could terminate the worker (`1 failed, 1 warning` before the fix) and that concurrent manual/worker drains could duplicate a text phase (`1 failed` before the drain lock). Both tests passed after their focused fixes.

Required focused GREEN:

```text
$ python3 -m pytest tests/test_matrix_outbox_delivery.py tests/test_startup.py -k 'worker or enqueue or startup_drains' -q
17 passed, 115 deselected in 1.38s
```

Required runtime/Matrix regression GREEN:

```text
$ python3 -m pytest tests/test_matrix_outbox_delivery.py tests/test_outbox_persistence.py tests/test_matrix.py tests/test_startup.py tests/test_module_decomposition.py -q
318 passed in 94.75s (0:01:34)
```

Focused concurrency and syntax verification:

```text
$ git diff --check && python3 -m pytest tests/test_matrix_outbox_delivery.py -k 'worker or manual_drain or enqueue_text_notice' -q && python3 -m compileall -q src/parking_monitor parking_spot_monitor
9 passed, 21 deselected in 0.74s
```

Complete suite, run once after the focused regression set:

```text
$ python3 -m pytest -q
1034 passed in 124.29s (0:02:04)
```

## Mutation Rationale

The tests are designed to fail under the important regressions: removing singleton guarding creates multiple live workers; removing initial/enqueue wake prevents persisted or new records from draining; changing `max_records=1` violates bounded-pass assertions; removing retry pacing produces a second call before the deadline; removing the drain lock duplicates an in-flight Matrix phase; moving client close before bounded join permits later phases against a closed client; allowing logger failures to escape kills the worker; restoring runtime drains or immediate frame dispatch is detected by capture-thread/network call assertions; changing text bodies, transaction IDs, or phase declarations breaks exact record/call assertions; and exporting unsafe health details breaks the health/redaction checks.

## Files Changed

Production changes are in `config.yaml.example`, `parking_spot_monitor/{__main__,capture_loop,config,matrix_dispatch,operator_feedback_alerts,runtime_commands,runtime_frame_plan,runtime_health,runtime_lifecycle,runtime_loop_resources,runtime_overlay,runtime_presence,runtime_state_update}.py`, and `src/parking_monitor/matrix_outbox_delivery.py`.

Tests are in `tests/test_config.py`, `tests/test_matrix_outbox_delivery.py`, `tests/test_module_decomposition.py`, and `tests/test_startup.py`.
