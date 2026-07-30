from __future__ import annotations

import math
import time
from threading import Event, get_ident

import pytest

from parking_spot_monitor.runtime_matrix_commands import (
    MatrixCommandPollState,
    MatrixCommandSchedule,
    command_poll_due,
    record_command_poll_result,
    record_command_poll_requested,
)
from parking_spot_monitor.matrix_commands import MatrixCommandService
from parking_spot_monitor.matrix_models import MatrixSyncResult
from parking_spot_monitor.runtime_command_worker import MatrixCommandPollWorker, advance_matrix_command_poll
from parking_spot_monitor.runtime_command_results import collect_matrix_commands_once
from parking_spot_monitor.logging import StructuredLogger


def test_successful_poll_waits_for_configured_interval() -> None:
    config = MatrixCommandSchedule(
        command_poll_interval_seconds=60,
        command_failure_cooldown_seconds=60,
        command_failure_max_cooldown_seconds=900,
    )
    state = MatrixCommandPollState(last_attempt_at=100.0)

    assert command_poll_due(config, state, 159.9) is False
    assert command_poll_due(config, state, 160.0) is True


def test_failed_half_open_probes_double_cooldown_only_to_configured_maximum() -> None:
    config = MatrixCommandSchedule(60, 60, 200)

    first = record_command_poll_result(
        config, MatrixCommandPollState(), 100.0, failed=True
    )
    second = record_command_poll_result(config, first, 160.0, failed=True)
    third = record_command_poll_result(config, second, 280.0, failed=True)
    fourth = record_command_poll_result(config, third, 480.0, failed=True)

    assert first == MatrixCommandPollState(
        last_attempt_at=100.0, failure_count=1, retry_at=160.0
    )
    assert second.retry_at == 280.0
    assert third.retry_at == 480.0
    assert fourth.retry_at == 680.0


def test_successful_half_open_probe_resets_failure_state() -> None:
    config = MatrixCommandSchedule(60, 60, 900)
    failed = MatrixCommandPollState(
        last_attempt_at=160.0, failure_count=2, retry_at=280.0
    )

    recovered = record_command_poll_result(config, failed, 280.0, failed=False)

    assert recovered == MatrixCommandPollState(
        last_attempt_at=280.0, failure_count=0, retry_at=None
    )
    assert command_poll_due(config, recovered, 339.9) is False
    assert command_poll_due(config, recovered, 340.0) is True


def test_prolonged_failure_history_caps_without_exponential_overflow() -> None:
    config = MatrixCommandSchedule(60.0, 60.0, 900.0)
    prolonged_outage = MatrixCommandPollState(
        last_attempt_at=100.0,
        failure_count=10_000,
        retry_at=1000.0,
    )

    failed = record_command_poll_result(
        config, prolonged_outage, 1000.0, failed=True
    )

    assert failed == MatrixCommandPollState(
        last_attempt_at=1000.0,
        failure_count=10_001,
        retry_at=1900.0,
    )


def test_zero_poll_interval_is_due_each_iteration() -> None:
    config = MatrixCommandSchedule(0, 60, 900)

    assert command_poll_due(
        config, MatrixCommandPollState(last_attempt_at=1.0), 1.0
    ) is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("command_poll_interval_seconds", math.nan),
        ("command_poll_interval_seconds", math.inf),
        ("command_failure_cooldown_seconds", math.nan),
        ("command_failure_cooldown_seconds", math.inf),
        ("command_failure_max_cooldown_seconds", math.nan),
        ("command_failure_max_cooldown_seconds", math.inf),
    ],
)
def test_schedule_rejects_nonfinite_timings(field: str, value: float) -> None:
    values = {
        "command_poll_interval_seconds": 60.0,
        "command_failure_cooldown_seconds": 60.0,
        "command_failure_max_cooldown_seconds": 900.0,
    }
    values[field] = value

    with pytest.raises(ValueError):
        MatrixCommandSchedule(**values)


@pytest.mark.parametrize(
    "values",
    [
        (-1.0, 60.0, 900.0),
        (60.0, 0.0, 900.0),
        (60.0, 60.0, 59.0),
    ],
)
def test_schedule_rejects_invalid_timing_bounds(
    values: tuple[float, float, float],
) -> None:
    with pytest.raises(ValueError):
        MatrixCommandSchedule(*values)


def test_command_fetch_does_not_block_capture_iteration() -> None:
    release = Event()

    def blocking_sync() -> MatrixSyncResult:
        assert release.wait(1)
        return MatrixSyncResult(next_batch="s1", events=())

    worker = MatrixCommandPollWorker(blocking_sync)
    started = time.monotonic()
    assert worker.request() is True
    assert time.monotonic() - started < 0.05
    assert worker.request() is False
    assert worker.take_completed() is None
    release.set()
    deadline = time.monotonic() + 1
    completed = None
    while completed is None and time.monotonic() < deadline:
        completed = worker.take_completed()
        time.sleep(0.001)
    assert isinstance(completed, MatrixSyncResult)
    worker.close()


def test_worker_keeps_one_completed_result_until_capture_thread_collects_it() -> None:
    calls = 0

    def sync() -> MatrixSyncResult:
        nonlocal calls
        calls += 1
        return MatrixSyncResult(next_batch=f"s{calls}", events=())

    worker = MatrixCommandPollWorker(sync)
    assert worker.request() is True
    deadline = time.monotonic() + 1
    while calls == 0 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert calls == 1
    assert worker.request() is False
    result = worker.take_completed()
    assert isinstance(result, MatrixSyncResult)
    assert worker.request() is True
    worker.close()


def test_worker_close_remains_bounded_when_cancellation_raises() -> None:
    started = Event()
    release = Event()

    def blocking_sync() -> MatrixSyncResult:
        started.set()
        release.wait(1)
        return MatrixSyncResult(next_batch="s1", events=())

    def fail_cancel() -> None:
        raise RuntimeError("cancel failed")

    worker = MatrixCommandPollWorker(
        blocking_sync,
        cancel_pending=fail_cancel,
        close_timeout_seconds=0.01,
    )
    assert worker.request() is True
    assert started.wait(1)
    began = time.monotonic()
    worker.close()
    elapsed = time.monotonic() - began
    release.set()

    assert elapsed < 0.1


def test_fetch_is_read_only_and_apply_mutates_on_capture_thread() -> None:
    main_thread = get_ident()

    class Archive:
        def __init__(self) -> None:
            self.cursor = {"next_batch": "s0"}
            self.write_threads: list[int] = []

        def read_matrix_cursor(self) -> dict[str, str]:
            return self.cursor

        def write_matrix_cursor(self, state: dict[str, object]) -> None:
            self.write_threads.append(get_ident())
            self.cursor = dict(state)  # type: ignore[assignment]

    class Client:
        def sync(self, **_kwargs: object) -> MatrixSyncResult:
            return MatrixSyncResult(next_batch="s1", events=())

        def close(self) -> None:
            return None

    archive = Archive()
    service = MatrixCommandService(
        client=Client(),  # type: ignore[arg-type]
        archive=archive,  # type: ignore[arg-type]
        room_id="!room:example.org",
        authorized_senders=(),
        who_snapshot_provider=lambda base: base,
    )
    worker = MatrixCommandPollWorker(service.fetch_once)
    assert worker.request() is True
    deadline = time.monotonic() + 1
    result = None
    while result is None and time.monotonic() < deadline:
        result = worker.take_completed()
        time.sleep(0.001)

    assert archive.write_threads == []
    assert isinstance(result, MatrixSyncResult)
    applied = service.apply_sync_result(result)
    assert applied.next_batch == "s1"
    assert archive.write_threads == [main_thread]
    worker.close()


def test_fetch_error_enters_cooldown_only_after_capture_thread_collects_it() -> None:
    fetch_finished = Event()

    def fail_fetch() -> MatrixSyncResult:
        fetch_finished.set()
        raise RuntimeError("unavailable")

    class Service:
        def apply_sync_result(self, _result: MatrixSyncResult) -> object:
            raise AssertionError("failed fetch must not be applied")

    schedule = MatrixCommandSchedule(0, 10, 60)
    state = MatrixCommandPollState()
    worker = MatrixCommandPollWorker(fail_fetch)
    assert worker.request() is True
    assert fetch_finished.wait(1)

    # The completed exception may sit in the capacity-one result slot without
    # mutating capture-thread cooldown state.
    assert state == MatrixCommandPollState()
    completed = worker.take_completed()
    assert isinstance(completed, RuntimeError)
    outcome = collect_matrix_commands_once(
        Service(),  # type: ignore[arg-type]
        completed,
        logger=StructuredLogger(),
        iteration=1,
    )
    state = record_command_poll_result(
        schedule,
        state,
        100,
        failed=outcome.transport_failed,
    )

    assert state == MatrixCommandPollState(
        last_attempt_at=100,
        failure_count=1,
        retry_at=110,
    )
    worker.close()


class _PollResult:
    processed_count = 0
    ignored_count = 0
    error_count = 0
    bootstrapped = False


class _ApplyService:
    def apply_sync_result(self, _result: MatrixSyncResult) -> _PollResult:
        return _PollResult()


class _Health:
    def __init__(self) -> None:
        self.errors: list[dict[str, object] | None] = []

    def record_command_result(self, error: dict[str, object] | None) -> None:
        self.errors.append(error)


class _ScriptedWorker:
    def __init__(self) -> None:
        self.completed: MatrixSyncResult | BaseException | None = None
        self.outstanding = False
        self.accepted_requests = 0

    def take_completed(self) -> MatrixSyncResult | BaseException | None:
        completed = self.completed
        self.completed = None
        if completed is not None:
            self.outstanding = False
        return completed

    def request(self) -> bool:
        if self.outstanding:
            return False
        self.outstanding = True
        self.accepted_requests += 1
        return True


def _advance(
    worker: _ScriptedWorker,
    state: MatrixCommandPollState,
    now: float,
    *,
    interval: float = 60,
    completed_at: float | None = None,
) -> MatrixCommandPollState:
    return advance_matrix_command_poll(
        _ApplyService(),  # type: ignore[arg-type]
        worker,  # type: ignore[arg-type]
        settings=MatrixCommandSchedule(interval, 10, 60),
        state=state,
        now_monotonic=now,
        logger=StructuredLogger(),
        iteration=1,
        health=_Health(),
        decision_memory_path=None,
        completed_at=lambda: now if completed_at is None else completed_at,
    )


def test_async_success_cadence_is_anchored_to_accepted_request_time() -> None:
    worker = _ScriptedWorker()
    state = _advance(worker, MatrixCommandPollState(), 0)
    assert state == MatrixCommandPollState(last_attempt_at=0)
    assert worker.accepted_requests == 1

    worker.completed = MatrixSyncResult(next_batch="s1", events=())
    state = _advance(worker, state, 60, completed_at=60)

    assert worker.accepted_requests == 2
    assert state == MatrixCommandPollState(last_attempt_at=60)


def test_long_running_async_fetch_does_not_move_attempt_time_on_duplicate_request() -> None:
    worker = _ScriptedWorker()
    state = _advance(worker, MatrixCommandPollState(), 0)

    state = _advance(worker, state, 60)
    assert state == MatrixCommandPollState(last_attempt_at=0)
    assert worker.accepted_requests == 1

    worker.completed = MatrixSyncResult(next_batch="s1", events=())
    state = _advance(worker, state, 90, completed_at=90)
    assert state == MatrixCommandPollState(last_attempt_at=90)
    assert worker.accepted_requests == 2


def test_zero_interval_requests_next_fetch_when_success_is_collected() -> None:
    worker = _ScriptedWorker()
    state = _advance(worker, MatrixCommandPollState(), 0, interval=0)
    worker.completed = MatrixSyncResult(next_batch="s1", events=())

    state = _advance(worker, state, 0, interval=0, completed_at=0)

    assert worker.accepted_requests == 2
    assert state == MatrixCommandPollState(last_attempt_at=0)


def test_async_failure_cooldown_remains_anchored_to_collection_time() -> None:
    worker = _ScriptedWorker()
    state = _advance(worker, MatrixCommandPollState(), 0)
    worker.completed = RuntimeError("unavailable")

    state = _advance(worker, state, 60, completed_at=60)
    assert state == MatrixCommandPollState(last_attempt_at=60, failure_count=1, retry_at=70)
    assert worker.accepted_requests == 1

    state = _advance(worker, state, 70)
    assert state == MatrixCommandPollState(last_attempt_at=70, failure_count=1, retry_at=None)
    assert worker.accepted_requests == 2


def test_request_tracking_preserves_failure_history_and_consumes_retry_gate() -> None:
    state = MatrixCommandPollState(last_attempt_at=60, failure_count=2, retry_at=120)

    assert record_command_poll_requested(state, 120) == MatrixCommandPollState(
        last_attempt_at=120,
        failure_count=2,
        retry_at=None,
    )


def test_real_async_worker_requests_again_at_original_success_cadence() -> None:
    release_first = Event()
    release_second = Event()
    second_started = Event()

    class Service:
        def __init__(self) -> None:
            self.fetch_count = 0
            self.apply_count = 0

        def fetch_once(self) -> MatrixSyncResult:
            self.fetch_count += 1
            if self.fetch_count == 1:
                assert release_first.wait(1)
            else:
                second_started.set()
                release_second.wait(1)
            return MatrixSyncResult(next_batch=f"s{self.fetch_count}", events=())

        def apply_sync_result(self, _result: MatrixSyncResult) -> _PollResult:
            self.apply_count += 1
            return _PollResult()

    service = Service()
    health = _Health()
    worker = MatrixCommandPollWorker(service.fetch_once, cancel_pending=release_second.set)
    state = advance_matrix_command_poll(
        service,  # type: ignore[arg-type]
        worker,
        settings=MatrixCommandSchedule(60, 10, 60),
        state=MatrixCommandPollState(),
        now_monotonic=0,
        logger=StructuredLogger(),
        iteration=1,
        health=health,
        decision_memory_path=None,
        completed_at=lambda: 60,
    )
    assert state == MatrixCommandPollState(last_attempt_at=0)
    release_first.set()

    deadline = time.monotonic() + 1
    while service.apply_count == 0 and time.monotonic() < deadline:
        state = advance_matrix_command_poll(
            service,  # type: ignore[arg-type]
            worker,
            settings=MatrixCommandSchedule(60, 10, 60),
            state=state,
            now_monotonic=60,
            logger=StructuredLogger(),
            iteration=2,
            health=health,
            decision_memory_path=None,
            completed_at=lambda: 60,
        )
        time.sleep(0.001)

    assert service.apply_count == 1
    assert second_started.wait(1)
    assert service.fetch_count == 2
    assert state == MatrixCommandPollState(last_attempt_at=60)
    worker.close()
