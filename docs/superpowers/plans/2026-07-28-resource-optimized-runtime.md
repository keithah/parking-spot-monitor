# Resource-Optimized Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce steady-state CPU and durable filesystem traffic while preserving conservative occupancy behavior during uncertain and changing conditions.

**Architecture:** Keep the existing synchronous capture/detection ownership model, but add compact live outbox health, one locked decision-memory write per frame, atomic profile-specific capture publication, and a pure adaptive resource policy. The capture loop will always use primary frames for routine operator artifacts, use high-resolution frames only for transition-sensitive or periodic verification, and expose every cadence through validated configuration.

**Tech Stack:** Python 3.11+, Pydantic 2, Pillow, FFmpeg subprocesses, JSON file persistence, pytest.

## Global Constraints

- Preserve the existing occupancy confidence, overlap, confirmation, release, and presence-suppression thresholds.
- `runtime.frame_interval_seconds` remains the active/uncertain cadence and defaults to 30 seconds in the example configuration.
- New production defaults are adaptive polling enabled, 60-second stable cadence, 3 stable settle frames, 60-second debug-overlay cadence, and 600-second high-resolution verification.
- Setting adaptive polling off or setting stable cadence equal to active cadence must restore fixed-cadence behavior.
- Primary capture remains `/data/latest.jpg`; named profiles publish separate sanitized paths.
- Capture failures must preserve the last known-good published JPEG.
- Runtime health must not contain record-level outbox items.
- Decision-memory schema, sanitization, record bounds, and nonfatal failure behavior remain unchanged.
- Matrix background workers, detection-lab queueing, inference-engine replacement, and database migrations are outside this plan.
- Use `/usr/bin/python3 -m pytest` in this workspace because the unqualified `python` shim points to a deleted virtual environment.
- The pre-existing wall-clock-dependent analytics test is not caused by this plan; do not weaken assertions elsewhere to hide it.

---

## File Structure

- `src/parking_monitor/outbox.py`: produce detailed and compact outbox summaries from one in-memory record set.
- `src/parking_monitor/matrix_outbox_delivery.py`: expose compact live outbox health to runtime callers.
- `parking_spot_monitor/runtime_health.py`: select a live compact provider or compact file fallback.
- `parking_spot_monitor/operator_decision_memory.py`: own locked single-record and batch persistence.
- `parking_spot_monitor/runtime_decision_memory.py`: build frame records without individual writes.
- `parking_spot_monitor/runtime_state_update.py`: combine detection and state records into one frame append.
- `parking_spot_monitor/capture.py`: atomically publish primary and named-profile JPEGs.
- `parking_spot_monitor/config.py`: validate adaptive runtime settings.
- `parking_spot_monitor/runtime_resource_policy.py`: pure cadence, uncertainty, deadline, and artifact decisions.
- `parking_spot_monitor/runtime_stream_escalation.py`: make high-resolution acquisition transition-aware.
- `parking_spot_monitor/runtime_frame.py`: pass periodic-verification intent into escalation.
- `parking_spot_monitor/runtime_frame_outcome.py`: retain both primary and final capture identities.
- `parking_spot_monitor/capture_loop.py`: integrate policy state, primary artifacts, and deadline-based pacing.
- `config.yaml.example`, `README.md`: document operational controls and rollback values. The ignored operator-owned `config.yaml` is not edited; model defaults apply until the operator chooses explicit overrides.

---

### Task 1: Compact Live Outbox Health

**Files:**
- Modify: `src/parking_monitor/outbox.py:393-437`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py:39-55`
- Modify: `parking_spot_monitor/runtime_health.py:60-78,209-255`
- Modify: `parking_spot_monitor/capture_loop.py:118-143`
- Test: `tests/test_outbox_persistence.py`
- Test: `tests/test_startup.py:3831-3865`

**Interfaces:**
- Produces: `LocalOutbox.compact_status_summary() -> dict[str, JsonValue]`
- Produces: `MatrixOutboxDelivery.outbox_health_summary() -> Mapping[str, Any]`
- Produces: `matrix_outbox_health_payload(matrix_outbox_file: Path | None, *, summary_provider: Callable[[], Mapping[str, Any]] | None = None) -> dict[str, Any] | None`
- Consumes: existing `LocalOutbox.status_summary()` remains detailed for operator surfaces.

- [ ] **Step 1: Write failing compact-summary tests**

Add a test that enqueues and delivers a record, then asserts detailed output retains `items` while compact output does not:

```python
def test_compact_status_summary_omits_record_items(tmp_path: Path) -> None:
    outbox = LocalOutbox(tmp_path / "matrix-outbox.json")
    record = outbox.enqueue(AlertIntent(event_id="event-1", phase="text", body="Parking status"))
    outbox.mark_delivered(record.id)

    detailed = outbox.status_summary()
    compact = outbox.compact_status_summary()

    assert len(detailed["items"]) == 1
    assert "items" not in compact
    assert compact["total"] == 1
    assert compact["counts_by_state"] == {"delivered": 1}
```

Add a startup/runtime-health test with a provider that returns compact data and monkeypatch `LocalOutbox` to raise if constructed. Assert health uses the provider and omits `items`.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_outbox_persistence.py tests/test_startup.py \
  -k 'compact_status_summary or matrix_outbox_health_payload'
```

Expected: failure because `compact_status_summary` and the provider parameter do not exist.

- [ ] **Step 3: Implement one summary collector with optional details**

Refactor `status_summary()` around a private collector so counts and timestamps are computed once per call:

```python
def status_summary(self) -> dict[str, JsonValue]:
    return self._status_summary(include_items=True)

def compact_status_summary(self) -> dict[str, JsonValue]:
    return self._status_summary(include_items=False)

def _status_summary(self, *, include_items: bool) -> dict[str, JsonValue]:
    counts: dict[str, int] = {}
    retry_reason_counts: dict[str, int] = {}
    dead_letter_reason_counts: dict[str, int] = {}
    items: list[dict[str, JsonValue]] = []
    timestamps: list[str] = []
    for record in self._records:
        counts[record.state] = counts.get(record.state, 0) + 1
        timestamps.extend((record.created_at, record.updated_at))
        if record.retry_reason is not None:
            retry_reason_counts[record.retry_reason] = retry_reason_counts.get(record.retry_reason, 0) + 1
        if record.dead_letter_reason is not None:
            dead_letter_reason_counts[record.dead_letter_reason] = dead_letter_reason_counts.get(record.dead_letter_reason, 0) + 1
        if include_items:
            items.append(self._status_item(record))
    summary: dict[str, JsonValue] = {
        "path": str(self.path),
        "schema_version": _SCHEMA_VERSION,
        "total": len(self._records),
        "counts_by_state": counts,
        "oldest_timestamp": min(timestamps) if timestamps else None,
        "newest_timestamp": max(timestamps) if timestamps else None,
        "retry_reason_counts": retry_reason_counts,
        "dead_letter_reason_counts": dead_letter_reason_counts,
        "recovery": self.recovery.to_json(),
    }
    if include_items:
        summary["items"] = items
    return summary
```

Extract the current record-detail dictionary comprehension into `_status_item(record: OutboxRecord) -> dict[str, JsonValue]`. Do not call it when `include_items` is false.

- [ ] **Step 4: Expose and consume the live provider**

Add:

```python
def outbox_health_summary(self) -> Mapping[str, Any]:
    return self.outbox.compact_status_summary()
```

Update runtime health to prefer the injected provider and use `LocalOutbox(matrix_outbox_file).compact_status_summary()` only as the fallback. Catch provider and fallback failures through the existing safe unavailable payload. In `capture_loop`, resolve the provider once:

```python
outbox_health_provider = getattr(matrix_delivery, "outbox_health_summary", None)
if not callable(outbox_health_provider):
    outbox_health_provider = None
```

Pass it to every health write.

- [ ] **Step 5: Run focused and related tests**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_outbox_persistence.py \
  tests/test_matrix_outbox_delivery.py \
  tests/test_runtime_health_cache.py \
  tests/test_startup.py -k 'matrix_outbox_health or runtime_loop_reuses_vehicle_history_health'
```

Expected: all selected tests pass and serialized health has no outbox `items` key.

- [ ] **Step 6: Commit the compact-health slice**

```bash
git add src/parking_monitor/outbox.py src/parking_monitor/matrix_outbox_delivery.py \
  parking_spot_monitor/runtime_health.py parking_spot_monitor/capture_loop.py \
  tests/test_outbox_persistence.py tests/test_startup.py
git commit -m "perf: compact runtime outbox health"
```

---

### Task 2: Lock and Batch Decision-Memory Persistence

**Files:**
- Modify: `parking_spot_monitor/operator_decision_memory.py:180-203,297-316`
- Modify: `parking_spot_monitor/runtime_decision_memory.py:13-113`
- Modify: `parking_spot_monitor/runtime_detection.py:91-107`
- Modify: `parking_spot_monitor/runtime_state_update.py:33-142`
- Modify: `parking_spot_monitor/capture_loop.py:203-230`
- Test: `tests/test_operator_decision_memory.py`
- Test: `tests/test_startup.py:3623-3715`

**Interfaces:**
- Produces: `append_decision_memory_records(path: str | Path, records: Sequence[DecisionMemoryRecord | Mapping[str, Any]], *, max_records: int = MAX_RECORDS, max_file_bytes: int = MAX_MEMORY_FILE_BYTES, logger: StructuredLogger | None = None) -> bool`
- Produces: `build_detection_memory_records(result: DetectionFilterResult, *, observed_at: Any | None, mode: str, iteration: int | None) -> list[DecisionMemoryRecord]`
- Produces: `build_runtime_state_memory_records(*, previous_state: RuntimeState, next_state: Mapping[str, Any], detection_result: DetectionFilterResult, quiet_status: Any, observed_at: Any, configured_spot_ids: Sequence[str], presence_by_spot: Mapping[str, bool]) -> list[DecisionMemoryRecord]`
- Preserves: `append_decision_memory_record(path: str | Path, record: DecisionMemoryRecord | Mapping[str, Any], *, max_records: int = MAX_RECORDS, max_file_bytes: int = MAX_MEMORY_FILE_BYTES, logger: StructuredLogger | None = None) -> bool` as a one-record wrapper.
- Extends `_update_runtime_state_for_frame` with keyword parameter `pending_decision_records: Sequence[DecisionMemoryRecord] = ()`.

- [ ] **Step 1: Write a failing batch-write test**

Patch `_write_memory` with `wraps` and append two records through the wished-for batch API:

```python
def test_batch_append_persists_multiple_records_once(tmp_path: Path) -> None:
    path = decision_memory_path(tmp_path)
    records = [_record("first"), _record("second")]
    with patch("parking_spot_monitor.operator_decision_memory._write_memory", wraps=_write_memory) as write:
        assert append_decision_memory_records(path, records)
    assert write.call_count == 1
    assert [record.summary for record in load_decision_memory(path).records] == ["first", "second"]
```

- [ ] **Step 2: Write a failing concurrent-writer test**

Use a `threading.Barrier` around two batch calls and assert both unique summaries survive. Repeat enough times to exercise interleaving without using sleeps. The production change that makes this pass is a lock spanning load through atomic replace.

- [ ] **Step 3: Run the decision-memory tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_operator_decision_memory.py \
  -k 'batch_append or concurrent_writer'
```

Expected: batch symbol is missing and the concurrent contract is unsupported.

- [ ] **Step 4: Implement the locked batch primitive**

Add a module-level `threading.RLock` and keep the complete read-modify-write inside it:

```python
_MEMORY_WRITE_LOCK = threading.RLock()

def append_decision_memory_records(path, records, *, max_records=MAX_RECORDS,
                                   max_file_bytes=MAX_MEMORY_FILE_BYTES,
                                   logger=None) -> bool:
    memory_path = Path(path)
    try:
        sanitized = [_record_from_any(record) for record in records]
        if not sanitized:
            return True
        with _MEMORY_WRITE_LOCK:
            loaded = load_decision_memory(memory_path, max_file_bytes=max_file_bytes, logger=logger)
            retained = [*loaded.records, *sanitized][- _positive_limit(max_records, MAX_RECORDS):]
            _write_memory(memory_path, retained)
    except Exception as exc:
        _log(
            logger,
            "warning",
            "operator-decision-memory-append-failed",
            path=memory_path,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return False
    return True
```

Have `append_decision_memory_record` delegate to the batch function with a one-item tuple.

- [ ] **Step 5: Convert per-spot writers into pure record builders**

Split the existing loops so `build_detection_memory_records` and `build_runtime_state_memory_records` return sanitized `DecisionMemoryRecord` objects. Retain `record_detection_memory_records` as a compatibility wrapper that calls the batch API once.

In `_update_runtime_state_for_frame`, combine the records after `build_runtime_frame_plan`:

```python
frame_records = [
    *pending_decision_records,
    *build_runtime_state_memory_records(
        previous_state=runtime_state,
        next_state=frame_plan.occupancy_update.state_by_spot,
        detection_result=detection_result,
        quiet_status=frame_plan.quiet_status,
        observed_at=observed_at,
        configured_spot_ids=configured_spot_ids,
        presence_by_spot=frame_plan.presence_by_spot,
    ),
]
append_decision_memory_records(decision_memory_path, frame_records, logger=logger)
```

Build detection records in `capture_loop` and pass them to `_update_runtime_state_for_frame`; remove the earlier immediate detection-memory write.

- [ ] **Step 6: Add an integration assertion for one frame write**

Extend `test_runtime_loop_appends_sanitized_decision_memory_records` to monkeypatch the batch function and assert one call contains four baseline records for the two configured spots. Continue asserting redaction and bounded record content.

- [ ] **Step 7: Run focused and runtime tests**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_operator_decision_memory.py \
  tests/test_runtime_stream_escalation.py \
  tests/test_startup.py -k 'decision_memory'
```

Expected: all selected tests pass; single-record callers retain behavior; one normal runtime frame performs one batch append.

- [ ] **Step 8: Commit the decision-memory slice**

```bash
git add parking_spot_monitor/operator_decision_memory.py \
  parking_spot_monitor/runtime_decision_memory.py parking_spot_monitor/runtime_detection.py \
  parking_spot_monitor/runtime_state_update.py parking_spot_monitor/capture_loop.py \
  tests/test_operator_decision_memory.py tests/test_startup.py tests/test_runtime_stream_escalation.py
git commit -m "perf: batch runtime decision memory"
```

---

### Task 3: Atomically Publish Profile-Specific Captures

**Files:**
- Modify: `parking_spot_monitor/capture.py:126-356`
- Test: `tests/test_capture.py:104-265`

**Interfaces:**
- Produces: `_capture_output_path(output_dir: Path, profile_name: str) -> Path`
- Produces: `_capture_temp_path(output_path: Path) -> Path`
- Preserves the existing `capture_latest` parameters and `FrameCaptureResult` return type while retaining the primary output contract.

- [ ] **Step 1: Write failing profile-path and preservation tests**

Add tests with these assertions:

```python
def test_named_profile_publishes_separate_latest_path(tmp_path: Path) -> None:
    result = capture_latest(settings, tmp_path, stream_profile="high_resolution",
                            modes=[DecodeMode.SOFTWARE], runner=jpeg_runner)
    assert result.latest_path == tmp_path / "latest-high_resolution.jpg"
    assert not (tmp_path / "latest.jpg").exists()

def test_invalid_capture_preserves_previous_published_frame(tmp_path: Path) -> None:
    published = tmp_path / "latest.jpg"
    published.write_bytes(jpeg_bytes())
    with pytest.raises(CaptureError):
        capture_latest(settings, tmp_path, modes=[DecodeMode.SOFTWARE], runner=invalid_runner)
    assert published.read_bytes() == jpeg_bytes()
    assert not list(tmp_path.glob(".latest.*.jpg"))
```

Also assert the runner receives a temporary path rather than the published path.

- [ ] **Step 2: Run capture tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_capture.py \
  -k 'named_profile or preserves_previous or returns_result_shape'
```

Expected: high resolution still reports `latest.jpg`, and invalid output overwrites the old image.

- [ ] **Step 3: Implement sanitized destinations and per-attempt temporary files**

Use `re.sub(r"[^A-Za-z0-9_.-]+", "-", profile_name).strip("-._")` and reject an empty result defensively. Primary maps to `latest.jpg`; other profiles map to `latest-<name>.jpg`.

For each decode attempt, allocate a temporary `.jpg` in the output directory, pass it to FFmpeg, validate it, `chmod(0o644)`, and publish with `os.replace(temp_path, output_path)`. In a `finally` block, unlink any unpublished temporary file. Capture errors must continue to report the public `output_path`, not a random temporary name.

- [ ] **Step 4: Update existing capture expectations**

Update the selected-profile test to assert the high-resolution path and continue verifying URL selection and geometry. Keep all primary tests asserting `latest.jpg`.

- [ ] **Step 5: Run capture and escalation tests**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_capture.py \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py
```

Expected: all selected tests pass and fake captures use separate profile files where their assertions depend on real capture behavior.

- [ ] **Step 6: Commit atomic capture publication**

```bash
git add parking_spot_monitor/capture.py tests/test_capture.py \
  tests/test_runtime_stream_escalation.py tests/test_runtime_loop_stream_escalation.py
git commit -m "fix: publish captured frames atomically"
```

---

### Task 4: Add Validated Adaptive Runtime Configuration

**Files:**
- Modify: `parking_spot_monitor/config.py:80-121,230-245,323-328`
- Modify: `config.yaml.example`
- Modify: `parking-spot-monitor-spec.md`
- Test: `tests/test_config.py:291-310`
- Test: `tests/test_operator_config_docs.py`

**Interfaces:**
- Extends `RuntimeConfig` with `adaptive_polling_enabled`, `stable_frame_interval_seconds`, `stable_settle_frames`, and `debug_overlay_interval_seconds`.
- Extends `StreamConfig` with `escalation_verification_seconds`.

- [ ] **Step 1: Write failing default, override, and validation tests**

Assert the example configuration loads these exact values:

```python
assert settings.runtime.adaptive_polling_enabled is True
assert settings.runtime.stable_frame_interval_seconds == 60
assert settings.runtime.stable_settle_frames == 3
assert settings.runtime.debug_overlay_interval_seconds == 60
assert settings.stream.escalation_verification_seconds == 600
```

Parameterize invalid negative overlay/verification intervals, zero settle frames, and stable cadence below active cadence. Assert each `ConfigError` names the relevant field.

- [ ] **Step 2: Run configuration tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_config.py -k 'adaptive or stable_frame or overlay_interval or escalation_verification'
```

Expected: new fields are rejected by strict configuration or missing from models.

- [ ] **Step 3: Implement model fields and cross-field validation**

Add:

```python
class RuntimeConfig(StrictModel):
    health_file: Path
    log_level: str = "INFO"
    startup_timeout_seconds: int = Field(default=30, gt=0)
    frame_interval_seconds: float = Field(default=30, gt=0)
    adaptive_polling_enabled: bool = True
    stable_frame_interval_seconds: float = Field(default=60, gt=0)
    stable_settle_frames: int = Field(default=3, gt=0)
    debug_overlay_interval_seconds: float = Field(default=60, ge=0)

    @model_validator(mode="after")
    def stable_interval_not_faster_than_active(self) -> Self:
        if self.stable_frame_interval_seconds < self.frame_interval_seconds:
            raise ValueError("stable_frame_interval_seconds must be greater than or equal to frame_interval_seconds")
        return self
```

Add `escalation_verification_seconds: float = Field(default=600, ge=0)` to `StreamConfig` and include every field in `sanitized_summary()`.

- [ ] **Step 4: Update operator configuration documentation**

Add exact values to `config.yaml.example` and document:

- `adaptive_polling_enabled: false` for fixed cadence.
- Equal active/stable intervals as a second rollback path.
- Zero overlay interval disables periodic overlays.
- Zero escalation verification disables periodic verification, not transition-driven escalation.

- [ ] **Step 5: Run configuration and documentation contracts**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_config.py tests/test_operator_config_docs.py tests/test_operator_docs.py
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit adaptive configuration**

```bash
git add parking_spot_monitor/config.py config.yaml.example \
  parking-spot-monitor-spec.md tests/test_config.py tests/test_operator_config_docs.py
git commit -m "feat: configure adaptive runtime cadence"
```

---

### Task 5: Implement the Pure Runtime Resource Policy

**Files:**
- Create: `parking_spot_monitor/runtime_resource_policy.py`
- Create: `tests/test_runtime_resource_policy.py`
- Modify: `tests/test_module_decomposition.py`

**Interfaces:**
- Produces: `RuntimeResourcePolicyState(stable_success_count=0, last_verification_at=None, last_overlay_at=None)`.
- Produces: `RuntimeResourceDecision(interval_seconds: float, reason: str, stable_success_count: int)`.
- Produces: `decide_runtime_interval(settings, runtime_state, *, previous_stable_success_count, frame_had_transition, frame_has_weak_presence, degraded) -> RuntimeResourceDecision`.
- Produces: `verification_due(*, now_monotonic, last_verification_at, interval_seconds) -> bool`.
- Produces: `artifact_due(*, now_monotonic, last_written_at, interval_seconds, transition) -> bool`.
- Produces: `remaining_sleep_seconds(*, interval_seconds, iteration_started_at, now_monotonic) -> float`.

- [ ] **Step 1: Write failing cadence decision tests**

Cover unknown state, partial hit/miss streaks, stable occupied/empty state, settle frames, degraded state, and disabled adaptation. Representative assertions:

```python
decision = decide_runtime_interval(
    settings,
    stable_runtime_state(),
    previous_stable_success_count=2,
    frame_had_transition=False,
    frame_has_weak_presence=False,
    degraded=False,
)
assert decision.interval_seconds == 60
assert decision.reason == "stable"

uncertain = decide_runtime_interval(
    settings,
    unknown_runtime_state(),
    previous_stable_success_count=2,
    frame_had_transition=False,
    frame_has_weak_presence=False,
    degraded=False,
)
assert uncertain.interval_seconds == 15
assert uncertain.stable_success_count == 0
```

- [ ] **Step 2: Write failing deadline and artifact tests**

Assert verification at 600 seconds, zero disables periodic verification, transition forces an overlay even when not periodically due, zero disables a non-transition overlay, and work time is subtracted:

```python
assert remaining_sleep_seconds(interval_seconds=60, iteration_started_at=100, now_monotonic=107.5) == 52.5
assert remaining_sleep_seconds(interval_seconds=15, iteration_started_at=100, now_monotonic=118) == 0
```

- [ ] **Step 3: Run policy tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_runtime_resource_policy.py
```

Expected: collection fails because the policy module does not exist.

- [ ] **Step 4: Implement immutable policy types and pure functions**

Keep the module independent of filesystem, Matrix, Pillow, and capture types. Determine state uncertainty from `OccupancyStatus`, `hit_streak`, `miss_streak`, and configured confirmation/release values. Return bounded reason strings from this set: `adaptive-disabled`, `degraded`, `transition-settle`, `unknown`, `partial-streak`, `weak-presence`, `settling`, `stable`.

- [ ] **Step 5: Add the module size/dependency contract**

Update decomposition tests to cap the new module at 220 lines and reject imports from Matrix, capture, Pillow, or vehicle-history modules.

- [ ] **Step 6: Run policy and decomposition tests**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_runtime_resource_policy.py tests/test_module_decomposition.py
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit the resource policy**

```bash
git add parking_spot_monitor/runtime_resource_policy.py \
  tests/test_runtime_resource_policy.py tests/test_module_decomposition.py
git commit -m "feat: add adaptive runtime resource policy"
```

---

### Task 6: Make Escalation Transition-Aware and Periodic

**Files:**
- Modify: `parking_spot_monitor/runtime_stream_escalation.py:15-110`
- Modify: `parking_spot_monitor/runtime_frame.py:25-71`
- Modify: `parking_spot_monitor/runtime_frame_outcome.py:11-85`
- Test: `tests/test_runtime_stream_escalation.py`
- Test: `tests/test_runtime_loop_stream_escalation.py`

**Interfaces:**
- Extends `detect_with_stream_escalation` with keyword parameter `periodic_verification_due: bool = False`.
- Extends: `StreamDetectionResult` with `primary_capture` and `escalated`.
- Extends: `RuntimeFrameDetected` and `RuntimeFrameLoopResult` with `primary_capture` and `escalated`.
- Extends `capture_and_detect_runtime_frame` with keyword parameter `periodic_verification_due: bool = False`.

- [ ] **Step 1: Write failing stable-versus-transition escalation tests**

Use the same weak accepted detection for two prior states:

```python
def test_weak_candidate_for_stable_occupied_spot_does_not_escalate() -> None:
    state = occupied_state(hit_streak=12)
    result = detect_with_stream_escalation(
        settings,
        tmp_path,
        capture=capture,
        detector=detector,
        runtime_state=state,
        primary_result=primary,
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
        periodic_verification_due=False,
    )
    assert captured_profiles == [None]
    assert result.escalated is False

def test_weak_candidate_for_empty_spot_escalates_before_confirmation() -> None:
    state = empty_state()
    result = detect_with_stream_escalation(
        settings,
        tmp_path,
        capture=capture,
        detector=detector,
        runtime_state=state,
        primary_result=primary,
        logger=StructuredLogger(),
        mode="runtime-loop",
        iteration=1,
        periodic_verification_due=False,
    )
    assert captured_profiles == [None, "high_resolution"]
    assert result.escalated is True
```

Retain a test for an occupied spot whose next miss reaches `release_frames`.

- [ ] **Step 2: Write a failing periodic-verification test**

For stable strong primary evidence, set `periodic_verification_due=True` and assert high-resolution capture occurs with escalation reason `periodic-verification`.

- [ ] **Step 3: Run escalation tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_runtime_stream_escalation.py \
  -k 'stable_occupied or empty_spot or periodic_verification'
```

Expected: stable weak occupied evidence currently escalates and the periodic parameter is absent.

- [ ] **Step 4: Implement transition-aware classification**

Change `_should_escalate_stream_result` to return a reason or `None`:

```python
def _stream_escalation_reason(settings, runtime_state, detection_result,
                              *, periodic_verification_due: bool) -> str | None:
    if periodic_verification_due:
        return "periodic-verification"
    for spot_id, result in detection_result.by_spot.items():
        prior = runtime_state.state_by_spot.get(spot_id, SpotOccupancyState())
        if result.accepted is not None and result.accepted.confidence < settings.stream.escalation_min_confidence:
            if prior.status is not OccupancyStatus.OCCUPIED:
                return "weak-transition-candidate"
    # Preserve the existing near-release occupied-miss check.
    return None
```

Log the returned bounded reason. Populate `primary_capture` and `escalated` on success and carry primary identity through frame outcomes.

- [ ] **Step 5: Preserve failure semantics**

Update failed-escalation tests to assert the primary published path still exists, no processed frame is recorded, and health retains the last successful primary capture metadata.

- [ ] **Step 6: Run escalation and frame-outcome suites**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_runtime_loop_health.py
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit transition-aware escalation**

```bash
git add parking_spot_monitor/runtime_stream_escalation.py \
  parking_spot_monitor/runtime_frame.py parking_spot_monitor/runtime_frame_outcome.py \
  tests/test_runtime_stream_escalation.py tests/test_runtime_loop_stream_escalation.py \
  tests/test_runtime_loop_health.py
git commit -m "perf: make stream escalation transition aware"
```

---

### Task 7: Integrate Adaptive Pacing and Primary-Frame Artifacts

**Files:**
- Modify: `parking_spot_monitor/capture_loop.py:80-274`
- Modify: `parking_spot_monitor/runtime_state_update.py:24-142`
- Modify: `parking_spot_monitor/runtime_frame.py:25-71`
- Test: `tests/test_startup.py:2487-2703,3186-3600`
- Test: `tests/test_runtime_loop_stream_escalation.py`
- Test: `tests/test_runtime_resource_policy.py`

**Interfaces:**
- Extends `run_capture_loop` with keyword parameter `monotonic: Callable[[], float] = time.monotonic`.
- Extends: `FrameUpdateResult` with `transition_occurred: bool`.
- Consumes: `RuntimeResourcePolicyState`, policy decision functions, and `RuntimeFrameLoopResult.primary_capture`.

- [ ] **Step 1: Write a failing stable-cadence integration test**

Run enough successful stable frames to cross the three-frame settling threshold with injected monotonic values. Assert early sleeps use active cadence and the next sleep uses stable cadence. Keep `sleep` as a recorder rather than actually waiting.

- [ ] **Step 2: Write a failing deadline-based pacing test**

Inject monotonic timestamps representing 4 seconds of processing in a 15-second active interval and assert the loop sleeps 11 seconds, not 15. Add a processing-overrun case that records zero sleep without passing a negative value.

- [ ] **Step 3: Write failing primary-artifact tests**

Use a fake capture that returns different primary and high-resolution paths. Force escalation, then assert:

```python
assert timeline_frames[0].read_bytes() == primary_path.read_bytes()
assert overlay_sources == [primary_path]
assert high_path.read_bytes() != timeline_frames[0].read_bytes()
```

Add an overlay cadence test showing stable frames within 60 seconds do not rewrite the overlay, while a state transition forces one write.

- [ ] **Step 4: Run integration tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_startup.py -k 'stable_cadence or deadline_pacing or primary_artifact or overlay_cadence' \
  tests/test_runtime_loop_stream_escalation.py
```

Expected: the loop always sleeps the fixed configured value and uses the final escalated image for routine artifacts.

- [ ] **Step 5: Integrate policy state and monotonic deadlines**

At iteration start, capture `iteration_started_at = monotonic()`. Before capture, compute periodic verification from policy state and pass it through `capture_and_detect_runtime_frame`. After state update, derive transition and uncertainty, update `stable_success_count`, and compute the selected interval. Sleep with:

```python
sleep_seconds = remaining_sleep_seconds(
    interval_seconds=decision.interval_seconds,
    iteration_started_at=iteration_started_at,
    now_monotonic=monotonic(),
)
logger.debug("capture-loop-paced", iteration=iteration,
             sleep_seconds=sleep_seconds, cadence_reason=decision.reason)
sleep(sleep_seconds)
```

On capture failures, preserve the existing reconnect backoff rather than adaptive cadence.

- [ ] **Step 6: Use primary frames for routine artifacts**

Call `record_timeline_frame(frame_result.primary_capture.latest_path, data_dir=data_dir, observed_at=frame_result.primary_capture.timestamp)`. Gate `_write_overlay_for_capture` with `artifact_due`; use the primary path and update `last_overlay_at` only after success. Add `transition_occurred=bool(frame_plan.occupancy_update.events)` to `FrameUpdateResult`.

When escalation succeeds, update `last_verification_at` whether the reason was periodic or transition-driven so a transition verification also resets the periodic deadline.

- [ ] **Step 7: Preserve fixed-cadence compatibility**

Update the existing `test_runtime_loop_success_writes_health_and_uses_configured_frame_interval` fixture to set `adaptive_polling_enabled: false`; keep its assertion that two iterations sleep exactly the configured value. Add a second case where active and stable intervals are equal with adaptation enabled.

- [ ] **Step 8: Run runtime suites**

Run:

```bash
/usr/bin/python3 -m pytest -q \
  tests/test_startup.py \
  tests/test_runtime_loop_health.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_resource_policy.py \
  tests/test_timeline_buffer.py
```

Expected: all selected tests pass except the separately documented wall-clock analytics test if `tests/test_matrix_operator_cockpit.py` is included indirectly.

- [ ] **Step 9: Commit adaptive loop integration**

```bash
git add parking_spot_monitor/capture_loop.py parking_spot_monitor/runtime_state_update.py \
  parking_spot_monitor/runtime_frame.py tests/test_startup.py \
  tests/test_runtime_loop_stream_escalation.py tests/test_runtime_resource_policy.py
git commit -m "perf: adapt capture cadence to occupancy stability"
```

---

### Task 8: Documentation and Final Verification

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `parking-spot-monitor-spec.md`
- Modify: `tests/test_operator_runtime_docs.py`
- Modify: `tests/test_readme_live_proof_contract.py` only if its existing configuration excerpt requires the new keys.

**Interfaces:**
- Documents the exact rollback and tuning controls introduced by Tasks 4-7.
- Does not introduce new runtime behavior.

- [ ] **Step 1: Write failing documentation contract assertions**

Require the operator documentation to include these literal keys and meanings:

```python
for key in (
    "adaptive_polling_enabled",
    "stable_frame_interval_seconds",
    "stable_settle_frames",
    "debug_overlay_interval_seconds",
    "escalation_verification_seconds",
):
    assert key in readme
assert "fixed cadence" in readme.lower()
assert "primary" in readme.lower()
```

- [ ] **Step 2: Run documentation tests and verify RED**

Run:

```bash
/usr/bin/python3 -m pytest -q tests/test_operator_runtime_docs.py tests/test_operator_config_docs.py
```

Expected: required tuning and rollback language is missing.

- [ ] **Step 3: Document operation and rollback**

Document:

- Active versus stable cadence and the three-frame settling period.
- Transition-driven and periodic high-resolution verification.
- Primary-image ownership for `latest`, timeline, and routine overlays.
- Zero-value semantics for periodic overlay and verification.
- Fixed-cadence rollback using `adaptive_polling_enabled: false`.
- Expected health change from hundreds of item details to compact counts.

Add a changelog entry that names the durability and resource changes without claiming measured savings before deployment.

- [ ] **Step 4: Run static and focused verification**

Run:

```bash
/usr/bin/python3 -m compileall -q parking_spot_monitor src main.py
/usr/bin/python3 -m pytest -q \
  tests/test_capture.py \
  tests/test_config.py \
  tests/test_operator_decision_memory.py \
  tests/test_outbox_persistence.py \
  tests/test_matrix_outbox_delivery.py \
  tests/test_runtime_health_cache.py \
  tests/test_runtime_resource_policy.py \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_runtime_loop_health.py \
  tests/test_timeline_buffer.py \
  tests/test_operator_runtime_docs.py \
  tests/test_operator_config_docs.py \
  tests/test_module_decomposition.py
```

Expected: compile exit 0 and all selected tests pass.

- [ ] **Step 5: Run the complete test suite and classify the known baseline**

Run:

```bash
/usr/bin/python3 -m pytest -q
```

Expected: all resource-optimization tests pass. If `test_matrix_operator_context_routes_parsed_analytics_command_to_cockpit` remains the only failure, report it verbatim as the pre-existing wall-clock baseline; any other failure is a regression and must be fixed before completion.

- [ ] **Step 6: Validate packaging and repository state**

Run:

```bash
docker compose config --quiet
git diff --check
git status --short
```

Expected: Compose and diff checks exit 0; status contains only intentional implementation files before the final documentation commit.

- [ ] **Step 7: Commit documentation**

```bash
git add README.md CHANGELOG.md parking-spot-monitor-spec.md \
  tests/test_operator_runtime_docs.py tests/test_readme_live_proof_contract.py
git commit -m "docs: explain adaptive resource controls"
```

- [ ] **Step 8: Capture post-deployment comparison commands**

Do not mutate the live deployment during implementation. Include these commands in the handoff for a later 24-hour comparison:

```bash
docker stats --no-stream parking-spot-monitor-parking-spot-monitor-1
docker logs --since 24h parking-spot-monitor-parking-spot-monitor-1 2>&1 \
  | jq -r '.event? // empty' | sort | uniq -c | sort -nr
du -sh data data/timeline data/snapshots data/vehicle-history
stat -c '%n %s bytes' data/health.json data/operator-decision-memory.json
```

Expected after deployment: health is a few kilobytes, stable-state escalation is periodic rather than near-continuous, routine timeline/overlay frames are primary resolution, and transition latency remains within the configured active cadence.
