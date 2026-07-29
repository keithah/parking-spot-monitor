# Runtime Safety and External-I/O Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound detection-lab and capture resources, make Matrix outage behavior adjustable, and move durable runtime delivery off the capture-critical path.

**Architecture:** Keep the existing synchronous capture loop and JSON outbox. Add narrow admission, scheduling, locking, and worker primitives around existing boundaries; the durable outbox remains the only alert queue.

**Tech Stack:** Python 3.12, Pydantic 2, Pillow, httpx, pytest, Docker Compose

## Global Constraints

- Existing YAML keys, CLI commands, Matrix content, and persisted JSON schemas remain backward compatible.
- No database, broker, event bus, or external service is added.
- Detection-lab execution is capped at one active job.
- Captures are capped at 7,680 pixels per dimension, 33,177,600 pixels total, and 32 MiB encoded.
- All new floating-point configuration rejects NaN and infinity.
- Matrix command polling remains synchronous but becomes interval- and cooldown-controlled.
- The outbox worker drains at most one record per pass and uses the existing durable JSON outbox.
- Every task uses red-green-refactor and ends with a focused commit.

---

### Task 1: Bound Detection-Lab Admission and Retention

**Files:**
- Modify: `parking_spot_monitor/detection_lab.py:63-186`
- Modify: `tests/test_detection_lab.py:1-206`

**Interfaces:**
- Consumes: `DetectionLabManager.start_job(kind: str) -> DetectionLabJob`
- Produces: one-active-job admission, persisted `lab_busy` status, and retention that excludes the active directory

- [ ] **Step 0: Capture the current deployed resource baseline before changing code**

Record `docker compose ps`, `docker stats --no-stream`, the container thread count, outbox file size, health-snapshot duration, and recent INFO/WARNING/ERROR counts in a redaction-safe local artifact under `data/`. If the service or an input is unavailable, record that fact explicitly rather than estimating it. Preserve the artifact until final deployment comparison.

- [ ] **Step 1: Write failing admission and retention tests**

```python
def test_second_job_is_blocked_while_first_job_is_active(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path)
    started = threading.Event()
    release = threading.Event()

    def runner(inputs):
        started.set()
        assert release.wait(2)
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text('{"status_counts": {}, "coverage": {}}', encoding="utf-8")
        return report

    manager = DetectionLabManager(tmp_path, replay_runner=runner)
    first = manager.start_replay()
    assert started.wait(1)
    second = manager.start_replay()

    blocked = manager.summarize(second.job_id)
    assert blocked["status"] == "blocked"
    assert blocked["phase"] == "admission"
    assert blocked["error"]["code"] == "lab_busy"
    assert manager.active_job_id == first.job_id

    release.set()
    assert _wait_for_terminal(manager, first.job_id)["status"] == "succeeded"


def test_retention_never_removes_active_job(tmp_path: Path) -> None:
    _write_fixed_inputs(tmp_path)
    started = threading.Event()
    release = threading.Event()
    def runner(inputs):
        started.set()
        assert release.wait(2)
        report = inputs["job_dir"] / "replay-report.json"
        report.write_text('{"status_counts": {}, "coverage": {}}', encoding="utf-8")
        return report
    manager = DetectionLabManager(tmp_path, replay_runner=runner, max_jobs=1)
    active = manager.start_replay()
    assert started.wait(1)
    blocked = manager.start_replay()
    manager.retain_recent_jobs()
    assert active.job_dir.exists()
    assert blocked.job_dir.exists()
    release.set()
    _wait_for_terminal(manager, active.job_id)
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python3 -m pytest tests/test_detection_lab.py -k 'second_job or active_job' -q`

Expected: FAIL because `active_job_id` and admission control do not exist.

- [ ] **Step 3: Implement one-job admission under the existing lock**

```python
@property
def active_job_id(self) -> str | None:
    with self._lock:
        return self._active_job_id

def _admit(self, job: DetectionLabJob) -> bool:
    with self._lock:
        if self._active_job_id is not None:
            return False
        self._active_job_id = job.job_id
        return True

def _release(self, job: DetectionLabJob) -> None:
    with self._lock:
        if self._active_job_id == job.job_id:
            self._active_job_id = None
```

Replace the current `threading.Lock()` with `threading.RLock()` and initialize `self._active_job_id: str | None = None`. Create the job before admission so a rejected request still gets a normal persisted status. In `_run_job(job, runner)`, call `_release(job)` before retention in `finally`. Make `retain_recent_jobs()` acquire the same `RLock` and exclude the active ID before slicing removable directories.

- [ ] **Step 4: Run the complete detection-lab tests**

Run: `python3 -m pytest tests/test_detection_lab.py tests/test_matrix.py -k 'detection_lab or lab_' -q`

Expected: PASS.

- [ ] **Step 5: Commit the bounded admission change**

```bash
git add parking_spot_monitor/detection_lab.py tests/test_detection_lab.py
git commit -m "fix: bound detection lab concurrency"
```

### Task 2: Enforce Capture Geometry and Byte Ceilings

**Files:**
- Modify: `parking_spot_monitor/config.py:75-141`
- Modify: `parking_spot_monitor/capture.py:191-420`
- Modify: `tests/test_config.py`
- Modify: `tests/test_capture.py:104-306`

**Interfaces:**
- Produces: `MAX_CAPTURE_DIMENSION = 7_680`, `MAX_CAPTURE_PIXELS = 33_177_600`, and `MAX_CAPTURE_JPEG_BYTES = 32 * 1024 * 1024`
- Changes: `_validate_jpeg_output(output_path: Path, *, failure_output_path: Path | None, mode: DecodeMode, secrets: Iterable[str], duration_seconds: float, expected_size: tuple[int, int]) -> int`

- [ ] **Step 1: Add failing configuration-bound tests**

```python
@pytest.mark.parametrize(
    ("width", "height"),
    [(7681, 100), (100, 7681), (7680, 4321)],
)
def test_stream_geometry_rejects_more_than_8k_resource_budget(
    tmp_path: Path, width: int, height: int
) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8")
    config = config.replace("frame_width: 1458", f"frame_width: {width}")
    config = config.replace("frame_height: 806", f"frame_height: {height}")
    path = tmp_path / "config.yaml"
    path.write_text(config, encoding="utf-8")
    with pytest.raises(ConfigError, match="stream.*resource ceiling"):
        load_settings(path, environ=fake_environ())
```

- [ ] **Step 2: Add failing capture-validation tests using real Pillow JPEGs**

```python
def jpeg_bytes(size: tuple[int, int] = (1458, 806)) -> bytes:
    buffer = BytesIO()
    Image.new("RGB", size, (20, 30, 40)).save(buffer, "JPEG")
    return buffer.getvalue()

def test_capture_rejects_wrong_dimensions_and_preserves_previous_frame(tmp_path: Path) -> None:
    settings = fake_settings()
    published = tmp_path / "latest.jpg"
    published.write_bytes(jpeg_bytes(size=(1458, 806)))

    def runner(argv, *, timeout):
        Path(argv[-1]).write_bytes(jpeg_bytes(size=(32, 32)))
        return subprocess.CompletedProcess(argv, 0, stderr="ok")

    with pytest.raises(CaptureError) as raised:
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)

    assert raised.value.reason == "output-dimensions-mismatch"
    assert published.read_bytes() == jpeg_bytes(size=(1458, 806))


def test_capture_rejects_encoded_file_over_32_mib(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(capture, "MAX_CAPTURE_JPEG_BYTES", 64)
    def runner(argv, *, timeout):
        Image.new("RGB", (1458, 806), (20, 30, 40)).save(argv[-1], "JPEG")
        return subprocess.CompletedProcess(argv, 0, stderr="ok")
    with pytest.raises(CaptureError) as raised:
        capture_latest(fake_settings(), tmp_path, modes=[DecodeMode.SOFTWARE], runner=runner)
    assert raised.value.reason == "output-too-large"
```

- [ ] **Step 3: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_config.py tests/test_capture.py -k 'resource_budget or wrong_dimensions or over_32' -q`

Expected: FAIL because the ceilings and full JPEG validation are absent.

- [ ] **Step 4: Implement config and pre-publication validation**

```python
MAX_CAPTURE_DIMENSION = 7_680
MAX_CAPTURE_PIXELS = 33_177_600
MAX_CAPTURE_JPEG_BYTES = 32 * 1024 * 1024

def _validate_stream_geometry(width: int, height: int) -> None:
    if (
        width > MAX_CAPTURE_DIMENSION
        or height > MAX_CAPTURE_DIMENSION
        or width * height > MAX_CAPTURE_PIXELS
    ):
        raise ValueError("stream geometry exceeds the 8K resource ceiling")
```

Use a shared Pydantic after-validator on primary and named profiles. In capture validation, check `stat().st_size` before opening. Convert `Image.DecompressionBombWarning` to an error within `warnings.catch_warnings()`, verify `image.format == "JPEG"`, compare `image.size` to the selected profile, and call `image.verify()`. Map failures to safe `CaptureError.reason` values without publishing the temporary file.

Replace the old marker-only `jpeg_bytes()` fixture in `tests/test_capture.py` with the real Pillow helper from Step 2 so every success-path test exercises a valid JPEG at configured dimensions.

- [ ] **Step 5: Run capture and configuration suites**

Run: `python3 -m pytest tests/test_capture.py tests/test_config.py tests/test_startup.py -k 'capture or stream or config' -q`

Expected: PASS.

- [ ] **Step 6: Commit capture ceilings**

```bash
git add parking_spot_monitor/config.py parking_spot_monitor/capture.py tests/test_config.py tests/test_capture.py
git commit -m "fix: bound captured frame resources"
```

### Task 3: Add Matrix Command Scheduling and Circuit State

**Files:**
- Create: `parking_spot_monitor/runtime_matrix_commands.py`
- Modify: `parking_spot_monitor/config.py:175-190`
- Modify: `parking_spot_monitor/capture_loop.py:33-265`
- Modify: `parking_spot_monitor/runtime_commands.py:12-100`
- Modify: `config.yaml.example`
- Modify: `tests/test_config.py`
- Create: `tests/test_runtime_matrix_commands.py`
- Modify: `tests/test_startup.py`

**Interfaces:**
- Produces: immutable `MatrixCommandPollState(last_attempt_at, failure_count, retry_at)`
- Produces: `command_poll_due(settings, state, now_monotonic) -> bool`
- Produces: `record_command_poll_result(settings, state, now_monotonic, failed) -> MatrixCommandPollState`

- [ ] **Step 1: Write failing pure scheduler tests**

```python
def test_successful_poll_waits_for_configured_interval() -> None:
    config = MatrixCommandSchedule(
        command_poll_interval_seconds=60,
        command_failure_cooldown_seconds=60,
        command_failure_max_cooldown_seconds=900,
    )
    state = MatrixCommandPollState(last_attempt_at=100.0)
    assert command_poll_due(config, state, 159.9) is False
    assert command_poll_due(config, state, 160.0) is True

def test_failures_double_cooldown_and_success_resets() -> None:
    config = MatrixCommandSchedule(60, 60, 900)
    first = record_command_poll_result(config, MatrixCommandPollState(), 100.0, failed=True)
    second = record_command_poll_result(config, first, 160.0, failed=True)
    assert first.retry_at == 160.0
    assert second.retry_at == 280.0
    assert record_command_poll_result(config, second, 280.0, failed=False).failure_count == 0

def test_zero_poll_interval_is_due_each_iteration() -> None:
    config = MatrixCommandSchedule(0, 60, 900)
    assert command_poll_due(config, MatrixCommandPollState(last_attempt_at=1), 1) is True
```

- [ ] **Step 2: Run scheduler tests and verify RED**

Run: `python3 -m pytest tests/test_runtime_matrix_commands.py -q`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the pure scheduler and strict config**

```python
@dataclass(frozen=True, slots=True)
class MatrixCommandSchedule:
    command_poll_interval_seconds: float
    command_failure_cooldown_seconds: float
    command_failure_max_cooldown_seconds: float

@dataclass(frozen=True, slots=True)
class MatrixCommandPollState:
    last_attempt_at: float | None = None
    failure_count: int = 0
    retry_at: float | None = None

def command_poll_due(settings, state, now_monotonic: float) -> bool:
    if state.retry_at is not None:
        return now_monotonic >= state.retry_at
    if settings.command_poll_interval_seconds == 0 or state.last_attempt_at is None:
        return True
    return now_monotonic - state.last_attempt_at >= settings.command_poll_interval_seconds
```

For failures, calculate `min(initial * 2 ** failure_count, maximum)`. Validate maximum ≥ initial and all values finite. Add defaults from the design to `MatrixConfig`, sanitized startup summaries, and `config.yaml.example`.

- [ ] **Step 4: Integrate the scheduler into the capture loop**

Initialize poll state once. Call `_poll_matrix_commands_once` only when due. Update state from whether its returned context is non-`None`. Keep skipped intervals out of decision memory and health failure counts. Demote successful no-op poll logs to DEBUG; retain nonzero results at INFO.

- [ ] **Step 5: Run scheduler, config, and runtime-loop tests**

Run: `python3 -m pytest tests/test_runtime_matrix_commands.py tests/test_config.py tests/test_startup.py -k 'matrix_command or matrix_commands or runtime_loop' -q`

Expected: PASS.

- [ ] **Step 6: Commit command outage control**

```bash
git add parking_spot_monitor/runtime_matrix_commands.py parking_spot_monitor/runtime_commands.py parking_spot_monitor/capture_loop.py parking_spot_monitor/config.py config.yaml.example tests/test_runtime_matrix_commands.py tests/test_config.py tests/test_startup.py
git commit -m "perf: pace Matrix command polling"
```

### Task 4: Bound Unauthorized Replies and Jitter Retries

**Files:**
- Modify: `parking_spot_monitor/matrix_client.py:45-215`
- Modify: `parking_spot_monitor/matrix_commands.py:80-150`
- Modify: `parking_spot_monitor/config.py:175-190`
- Modify: `parking_spot_monitor/__main__.py:289-340`
- Modify: `tests/test_matrix.py`
- Modify: `tests/test_config.py`

**Interfaces:**
- Matrix client constructor adds `retry_jitter_ratio: float = 0.2` and injectable `random_unit: Callable[[], float] = random.random`
- Command service constructor adds `unauthorized_reply_cooldown_seconds: float = 300`, `monotonic`, and a 256-entry LRU sender map

- [ ] **Step 1: Write deterministic retry and limiter tests**

```python
def test_retry_jitter_is_bounded_and_retry_after_is_minimum() -> None:
    assert retry_delay(
        attempt=1, backoff_seconds=10, retry_after_seconds=12,
        jitter_ratio=0.2, random_unit=lambda: 0.0,
    ) == 12
    assert retry_delay(
        attempt=1, backoff_seconds=10, retry_after_seconds=None,
        jitter_ratio=0.2, random_unit=lambda: 1.0,
    ) == 12

def test_sender_reply_limiter_admits_once_per_cooldown_and_stays_bounded() -> None:
    now = 100.0
    limiter = SenderReplyLimiter(cooldown_seconds=300, monotonic=lambda: now, max_senders=256)
    assert limiter.admit("@sender:example.org") is True
    assert limiter.admit("@sender:example.org") is False
    now = 400.0
    assert limiter.admit("@sender:example.org") is True
    for index in range(300):
        limiter.admit(f"@sender-{index}:example.org")
    assert limiter.sender_count == 256
```

Extend the existing unauthorized-command service test with two prefixed events from the same unauthorized sender and an injected monotonic clock. Assert that both events are consumed, only the first sends a rejection during the cooldown, and a later event sends again after the clock advances.

- [ ] **Step 2: Run focused Matrix tests and verify RED**

Run: `python3 -m pytest tests/test_matrix.py -k 'jitter or sender_reply_limiter or unauthorized_rejection' -q`

Expected: FAIL because jitter injection and sender cooldown are absent.

- [ ] **Step 3: Implement bounded jitter and a 256-entry LRU limiter**

Use `OrderedDict[str, float]`. Move a sender to the end when admitted, pop oldest entries while length exceeds 256, and treat cooldown zero as always admitted. Calculate jitter as `local_delay * jitter_ratio * random_unit()`; return `max(retry_after or 0, local_delay + jitter)`.

- [ ] **Step 4: Wire config into both Matrix clients and the command service**

Add validated summary fields without access tokens. Pass the same jitter ratio to delivery and command clients. Pass the reply cooldown to `MatrixCommandService`.

- [ ] **Step 5: Run all Matrix/config tests and commit**

Run: `python3 -m pytest tests/test_matrix.py tests/test_config.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/matrix_client.py parking_spot_monitor/matrix_commands.py parking_spot_monitor/config.py parking_spot_monitor/__main__.py tests/test_matrix.py tests/test_config.py
git commit -m "fix: bound Matrix retry amplification"
```

### Task 5: Make LocalOutbox Thread-Safe and Compact

**Files:**
- Modify: `src/parking_monitor/outbox.py:301-649`
- Modify: `tests/test_outbox_persistence.py`

**Interfaces:**
- Preserves all public `LocalOutbox` methods and schema version
- Adds internal `_lock: threading.RLock` and `_index_by_id: dict[str, int]`
- Adds `enqueue_with_phases(intent: AlertIntent, phases: Sequence[MatrixPhase | str]) -> OutboxRecord` for one-write phase declaration

- [ ] **Step 1: Write failing concurrency, index, and compact-format tests**

```python
def test_concurrent_enqueue_and_transition_preserve_all_records(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "outbox.json")
    intents = [
        AlertIntent(event_id=f"event-{index}", phase="text", body=f"body-{index}")
        for index in range(4)
    ]
    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(outbox.enqueue, intents))
        list(pool.map(lambda record: outbox.mark_retrying(record.id, reason="timeout"), records))
    reloaded = LocalOutbox(tmp_path / "outbox.json")
    assert {record.id for record in reloaded.list_records()} == {record.id for record in records}

def test_persisted_outbox_is_compact_and_schema_compatible(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "outbox.json")
    outbox.enqueue(AlertIntent(event_id="event-1", phase="text", body="Parking status"))
    raw = outbox.path.read_text(encoding="utf-8")
    assert '\n  ' not in raw
    assert json.loads(raw)["schema_version"] == 1

def test_snapshot_phase_declaration_and_dead_letter_each_persist_once(tmp_path: Path, monkeypatch) -> None:
    outbox = LocalOutbox(tmp_path / "outbox.json")
    calls = 0
    original = outbox._persist_records
    def counted(records):
        nonlocal calls
        calls += 1
        return original(records)
    monkeypatch.setattr(outbox, "_persist_records", counted)
    record = outbox.enqueue_with_phases(
        AlertIntent(event_id="event-1", phase="text", body="Parking status"),
        ("text", "upload", "image"),
    )
    assert calls == 1
    outbox.mark_phase_failed(record.id, "text", reason="http_400")
    assert calls == 2
```

- [ ] **Step 2: Run outbox tests and verify RED**

Run: `python3 -m pytest tests/test_outbox_persistence.py -k 'concurrent or compact' -q`

Expected: FAIL because mutations are unsynchronized and JSON is indented.

- [ ] **Step 3: Guard every public read/mutation and rebuild the ID index after retention**

```python
def _set_records(self, records: list[OutboxRecord]) -> None:
    self._records = records
    self._index_by_id = {record.id: index for index, record in enumerate(records)}

def _find_record(self, record_id: str) -> OutboxRecord:
    try:
        return self._records[self._index_by_id[record_id]]
    except KeyError as exc:
        raise OutboxTransitionError("unknown_record") from exc
```

Initialize the lock before loading. Hold it across read-modify-persist-set operations. Return copied lists. Replace pretty JSON with `json.dump(payload, handle, sort_keys=True, separators=(",", ":"))`.

Build every requested phase into the immutable record before the first persistence in `enqueue_with_phases`; keep `enqueue(intent)` as a wrapper using only `intent.phase`. Make `mark_phase_failed` set the failed phase, terminal state, and safe dead-letter reason in one replacement instead of calling two separately persisted transitions.

- [ ] **Step 4: Run persistence and delivery tests**

Run: `python3 -m pytest tests/test_outbox_persistence.py tests/test_matrix_outbox_delivery.py -q`

Expected: PASS.

- [ ] **Step 5: Commit outbox synchronization**

```bash
git add src/parking_monitor/outbox.py tests/test_outbox_persistence.py
git commit -m "perf: synchronize and compact Matrix outbox"
```

### Task 6: Add the Bounded Outbox Worker and Remove Runtime Drains

**Files:**
- Modify: `src/parking_monitor/matrix_outbox_delivery.py:45-358`
- Modify: `parking_spot_monitor/matrix_dispatch.py:59-183`
- Modify: `parking_spot_monitor/runtime_loop_resources.py:1-80`
- Modify: `parking_spot_monitor/capture_loop.py:28-130`
- Modify: `parking_spot_monitor/runtime_health.py`
- Modify: `parking_spot_monitor/__main__.py:267-286`
- Modify: `tests/test_matrix_outbox_delivery.py`
- Modify: `tests/test_startup.py`
- Modify: `tests/test_module_decomposition.py`

**Interfaces:**
- `MatrixOutboxDelivery.start_worker(*, retry_interval_seconds: float) -> None`
- `MatrixOutboxDelivery.close() -> None` stops and joins the worker
- `MatrixOutboxDelivery.enqueue_text_notice(event_name, event) -> OutboxRecord`
- `MatrixOutboxDelivery.outbox_health_summary() -> Mapping[str, Any]` adds worker state

- [ ] **Step 1: Write failing worker lifecycle and wake tests**

```python
def test_worker_is_singleton_wakes_on_enqueue_and_drains_one_record(tmp_path: Path) -> None:
    client = FakeMatrixClient()
    delivery = make_delivery(tmp_path, client)
    delivery.start_worker(retry_interval_seconds=60)
    worker = delivery.worker_thread
    delivery.start_worker(retry_interval_seconds=60)
    assert delivery.worker_thread is worker

    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    delivery.enqueue_open_spot_alert(open_event(source))
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not client.calls:
        time.sleep(0.01)
    assert [call["kind"] for call in client.calls] == ["text", "upload", "image"]
    assert delivery.outbox_health_summary()["worker_running"] is True
    delivery.close()
    assert worker.is_alive() is False

def test_retryable_failure_waits_before_worker_retries(tmp_path: Path) -> None:
    source = tmp_path / "latest.jpg"
    write_jpeg(source)
    client = FakeMatrixClient(fail={"text": MatrixError("timeout", error_type="timeout")})
    delivery = make_delivery(tmp_path, client)
    delivery.start_worker(retry_interval_seconds=3_600)
    delivery.enqueue_open_spot_alert(open_event(source))
    time.sleep(0.1)
    assert [call["kind"] for call in client.calls].count("text") == 1
    delivery.close()
```

- [ ] **Step 2: Write failing runtime-dispatch tests**

Extend startup tests so occupied, open, and quiet-window frame events only enqueue on the capture thread. Assert the network fake is called by the worker, not from `_update_runtime_state_for_frame`. Assert startup no longer calls `drain_matrix_outbox_if_available`.

- [ ] **Step 3: Run worker/runtime tests and verify RED**

Run: `python3 -m pytest tests/test_matrix_outbox_delivery.py tests/test_startup.py -k 'worker or enqueue or startup_drains' -q`

Expected: FAIL because delivery has no worker and runtime still drains synchronously.

- [ ] **Step 4: Implement a one-thread event-driven worker**

```python
def start_worker(self, *, retry_interval_seconds: float) -> None:
    with self._worker_lock:
        if self._worker is not None and self._worker.is_alive():
            return
        self._retry_interval_seconds = retry_interval_seconds
        self._stop_event.clear()
        self._worker = threading.Thread(
            target=self._worker_main,
            name="matrix-outbox-delivery",
            daemon=True,
        )
        self._worker.start()

def _worker_main(self) -> None:
    while not self._stop_event.is_set():
        self._wake_event.wait()
        self._wake_event.clear()
        if self._stop_event.is_set():
            break
        self.drain_outbox(max_records=1)
        counts = self.outbox.compact_status_summary()["counts_by_state"]
        if counts.get("pending", 0):
            self._wake_event.set()
        elif counts.get("retrying", 0):
            self._stop_event.wait(self._retry_interval_seconds)
            if not self._stop_event.is_set():
                self._wake_event.set()
```

Wake after enqueue. Synchronize worker metadata. Close with a bounded join and then close the client. Expose only safe worker health fields.

- [ ] **Step 5: Generalize the existing outbox to text-only frame notices**

Create `enqueue_text_notice` with only a pending `text` phase. Use `enqueue_with_phases` for snapshot records so phase declaration performs one durable write. In `dispatch_matrix_event`, prefer enqueue methods for all frame-produced alerts/notices. Preserve immediate lifecycle delivery before/after the loop. Do not change message bodies or transaction IDs.

- [ ] **Step 6: Remove runtime drain compatibility code and add narrow protocols**

Delete `drain_matrix_outbox_if_available` and its aliases/constants from `runtime_loop_resources.py` and `capture_loop.py`. Define `RuntimeMatrixDelivery` and `RuntimeMatrixCommandService` protocols in the owning runtime modules; replace `Any` where those exact contracts cross the loop. Start the worker in the default factory and rely on existing shutdown close paths.

Rename cross-module private helpers `_write_overlay_for_capture` and `_presence_by_spot` to `write_overlay_for_capture` and `presence_by_spot`. Update all production/test imports and add a decomposition assertion that runtime modules do not import underscore-prefixed names from sibling runtime modules.

- [ ] **Step 7: Run the complete runtime/Matrix regression set**

Run: `python3 -m pytest tests/test_matrix_outbox_delivery.py tests/test_outbox_persistence.py tests/test_matrix.py tests/test_startup.py tests/test_module_decomposition.py -q`

Expected: PASS.

- [ ] **Step 8: Commit worker isolation**

```bash
git add src/parking_monitor/matrix_outbox_delivery.py parking_spot_monitor/matrix_dispatch.py parking_spot_monitor/runtime_loop_resources.py parking_spot_monitor/capture_loop.py parking_spot_monitor/runtime_health.py parking_spot_monitor/__main__.py tests/test_matrix_outbox_delivery.py tests/test_startup.py tests/test_module_decomposition.py
git commit -m "perf: isolate Matrix delivery from capture"
```

### Task 7: Verify Slice 1 as an Independent Deliverable

**Files:**
- Modify only if verification exposes a slice regression

**Interfaces:**
- Verifies all interfaces produced by Tasks 1-6

- [ ] **Step 1: Run the complete Python suite**

Run: `python3 -m pytest -q`

Expected: all tests pass.

- [ ] **Step 2: Run static and structural checks**

Run: `python3 -m compileall -q parking_spot_monitor src scripts tests && git diff --check && python3 -m pytest tests/test_module_decomposition.py -q`

Expected: exit 0 from every command.

- [ ] **Step 3: Record focused resource evidence**

Run the new tests with `-vv` and save their exact pass counts in the task report. Record that active lab jobs ≤ 1, outbox workers ≤ 1, capture size ≤ 32 MiB, and retry attempts remain unchanged during cooldown.

If verification fails, return to the task that owns the failing behavior, add a regression test there, and amend that task with a normal focused correction commit. Do not create an empty verification commit.
