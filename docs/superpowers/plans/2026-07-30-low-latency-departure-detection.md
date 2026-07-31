# Low-Latency Occupancy Transition Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the corresponding Matrix alert within 30 seconds of a real arrival or departure when the host, camera, inference, and Matrix service are healthy.

**Architecture:** Use the existing low-resolution primary stream every eight seconds during confirmation, occupancy, and transitions, and every 12 seconds when every spot is stably empty. Preserve conditional 4K entry verification and threshold-reaching 4K release verification, and wire a configurable per-decoder timeout through the single runtime capture callable. Build occupied alerts from confirmed state transitions before optional vehicle-history enrichment, with durable text fallback when occupied snapshot preparation fails, then expose bounded latency fields at transition and outbox-delivery boundaries.

**Tech Stack:** Python 3.11+, Pydantic v2 configuration, pytest, existing FFmpeg capture modes, existing primary/4K stream escalation, structured JSON logging, durable Matrix outbox, Docker Compose.

## Global Constraints

- The healthy-path target is at most 30 seconds from physical arrival or departure to its Matrix alert.
- Production unknown, confirmation, and transition polling is 8 seconds.
- Production primary polling while a spot is occupied is 8 seconds.
- Production stable all-empty polling is 12 seconds.
- Production entry confirmation is 2 frames.
- Production release confirmation is 2 frames.
- Production per-decoder capture timeout is 4 seconds; omission remains backward-compatible at 15 seconds.
- Omitted `runtime.occupied_frame_interval_seconds` resolves to `runtime.frame_interval_seconds`.
- Routine polling stays on the 1458×806 primary stream; only transition verification uses 3840×2160.
- Capture or verification failures never count as occupancy evidence in either direction.
- Confirmed occupied transitions queue exactly one base alert independently of vehicle-history enrichment.
- Occupied snapshot-preparation failure durably falls back to text with the same event ID.
- The service does not modify or restart unrelated containers.
- Every production-code change begins with a failing test and preserves secret-redacted diagnostics.
- All latency values are finite, non-negative seconds rounded to six decimal places.

---

## File Map

- `parking_spot_monitor/config.py`: validate and summarize the new capture timeout and occupied cadence.
- `parking_spot_monitor/runtime_resource_policy.py`: choose the occupied cadence with a stable reason code.
- `parking_spot_monitor/runtime_loop_resources.py`: subtract iteration work from every adaptive cadence.
- `parking_spot_monitor/__main__.py`: bind the configured timeout once into the runtime capture callable.
- `parking_spot_monitor/runtime_vehicle_events.py`: merge optional vehicle-history enrichment onto a mandatory base occupied alert.
- `parking_spot_monitor/runtime_state_update.py`: dispatch exactly one occupied alert per confirmed transition.
- `src/parking_monitor/matrix_outbox_delivery.py`: persist a text-only occupied fallback when snapshot preparation fails before durable enqueue.
- `parking_spot_monitor/runtime_transition_latency.py`: pure, bounded transition-observation timing helpers.
- `parking_spot_monitor/capture_loop.py`: maintain in-memory last occupied evidence and log confirmed release latency.
- `src/parking_monitor/matrix_outbox_delivery.py`: also log observation-to-enqueue and enqueue-to-delivery timing when an outbox record reaches `delivered`.
- `config.yaml.example`, `README.md`, `docs/deployment.md`: document defaults, the 8/8/12 production cadence, two-frame thresholds, healthy-host assumptions, rollback, and resource preflight.
- Focused tests live beside the existing configuration, policy, startup, escalation, outbox, and documentation suites.

---

### Task 1: Configuration Controls

**Files:**
- Modify: `parking_spot_monitor/config.py`
- Modify: `config.yaml.example`
- Test: `tests/test_config_paths_and_streams.py`
- Test: `tests/test_config_runtime_validation.py`
- Test: `tests/test_operator_config_docs.py`

**Interfaces:**
- Produces: `StreamConfig.capture_timeout_seconds: float` with default `15.0`.
- Produces: `RuntimeConfig.occupied_frame_interval_seconds: float`, resolving an omitted value to the effective `frame_interval_seconds`.
- Produces: sanitized summary keys `stream.capture_timeout_seconds` and `runtime.occupied_frame_interval_seconds`.

- [ ] **Step 1: Write failing configuration tests**

Add tests that load `config.yaml.example`, assert the explicit example values, remove each new key to verify compatibility defaults, and reject zero, negative, `nan`, and `inf` values:

```python
def test_low_latency_capture_and_occupied_cadence_are_configurable(tmp_path: Path) -> None:
    config = Path("config.yaml.example").read_text(encoding="utf-8")
    path = tmp_path / "config.yaml"
    path.write_text(config, encoding="utf-8")

    settings = load_settings(
        path,
        environ={
            "RTSP_URL": "rtsp://primary.example/camera",
            "RTSP_URL_4K": "rtsp://high.example/camera",
            "MATRIX_ACCESS_TOKEN": "test-token",
        },
    )

    assert settings.stream.capture_timeout_seconds == 15
    assert settings.runtime.occupied_frame_interval_seconds == 30
    summary = settings.sanitized_summary()
    assert summary["stream"]["capture_timeout_seconds"] == 15
    assert summary["runtime"]["occupied_frame_interval_seconds"] == 30


def test_omitted_occupied_cadence_follows_custom_active_cadence(tmp_path: Path) -> None:
    config = (
        Path("config.yaml.example").read_text(encoding="utf-8")
        .replace("  frame_interval_seconds: 30", "  frame_interval_seconds: 17")
        .replace("  occupied_frame_interval_seconds: 30\n", "")
    )
    path = tmp_path / "config.yaml"
    path.write_text(config, encoding="utf-8")

    settings = load_settings(
        path,
        environ={
            "RTSP_URL": "rtsp://primary.example/camera",
            "RTSP_URL_4K": "rtsp://high.example/camera",
            "MATRIX_ACCESS_TOKEN": "test-token",
        },
    )

    assert settings.runtime.occupied_frame_interval_seconds == 17
```

When the target test module already exposes an environment fixture, use it in place of the explicit mapping while preserving these three variable names.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
pytest -q \
  tests/test_config_paths_and_streams.py \
  tests/test_config_runtime_validation.py \
  tests/test_operator_config_docs.py
```

Expected: failures report missing `capture_timeout_seconds` and `occupied_frame_interval_seconds` attributes or missing example keys.

- [ ] **Step 3: Implement validated fields and omission compatibility**

Add the fields:

```python
class StreamConfig(CaptureGeometryConfig):
    rtsp_url: ResolvedSecret
    capture_timeout_seconds: float = Field(default=15, gt=0, allow_inf_nan=False)
    reconnect_seconds: int = Field(default=5, gt=0)


class RuntimeConfig(StrictModel):
    health_file: Path
    log_level: str = "INFO"
    startup_timeout_seconds: int = Field(default=30, gt=0)
    frame_interval_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
    occupied_frame_interval_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
```

Replace the existing omitted-stable validator with one pre-validation resolver that independently fills both state-specific intervals:

```python
@model_validator(mode="before")
@classmethod
def resolve_omitted_runtime_intervals(cls, value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    resolved = dict(value)
    try:
        active_interval = float(resolved.get("frame_interval_seconds", 30))
    except (TypeError, ValueError):
        return value
    resolved.setdefault("occupied_frame_interval_seconds", active_interval)
    resolved.setdefault("stable_frame_interval_seconds", max(60.0, active_interval))
    return resolved
```

Add both values to `RuntimeSettings.sanitized_summary()`. Add explicit compatible values to `config.yaml.example`:

```yaml
stream:
  capture_timeout_seconds: 15

runtime:
  frame_interval_seconds: 30
  occupied_frame_interval_seconds: 30
```

- [ ] **Step 4: Run focused configuration tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass with no warnings.

- [ ] **Step 5: Commit the configuration slice**

```bash
git add parking_spot_monitor/config.py config.yaml.example \
  tests/test_config_paths_and_streams.py tests/test_config_runtime_validation.py \
  tests/test_operator_config_docs.py
git commit -m "feat: configure low-latency capture cadence"
```

---

### Task 2: Occupied-State Cadence Policy

**Files:**
- Modify: `parking_spot_monitor/runtime_resource_policy.py`
- Modify: `parking_spot_monitor/runtime_loop_resources.py`
- Test: `tests/test_runtime_resource_policy.py`

**Interfaces:**
- Consumes: `RuntimeConfig.occupied_frame_interval_seconds` from Task 1.
- Produces: `RuntimeResourceReason` value `"occupied"`.
- Produces: `decide_runtime_interval(...) -> RuntimeResourceDecision` selecting the occupied interval for confirmed occupied state and partial release streaks.

- [ ] **Step 1: Write failing occupied-policy tests**

Configure the fixture's active, occupied, and stable intervals as 30, 8, and 60 seconds. Replace the previous expectation that confirmed occupied state becomes `stable`, while preserving the stable-empty expectation:

```python
def test_confirmed_occupied_spot_uses_occupied_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.OCCUPIED, hit_streak=3)
    )

    assert decide(settings, state) == RuntimeResourceDecision(
        interval_seconds=8.0,
        reason="occupied",
        stable_success_count=0,
    )


def test_partial_release_streak_stays_on_occupied_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(
            status=OccupancyStatus.OCCUPIED,
            miss_streak=1,
        )
    )

    assert decide(settings, state) == RuntimeResourceDecision(8.0, "occupied", 0)


def test_all_confirmed_empty_spots_reach_stable_cadence(settings) -> None:
    state = runtime_state(
        SpotOccupancyState(status=OccupancyStatus.EMPTY, miss_streak=3)
    )

    assert decide(settings, state) == RuntimeResourceDecision(60.0, "stable", 3)
```

Add a pacing regression showing adaptive occupied cadence subtracts elapsed inference time even when active and stable intervals happen to be equal.

- [ ] **Step 2: Run the policy module and verify RED**

Run:

```bash
pytest -q tests/test_runtime_resource_policy.py
```

Expected: occupied decisions return the existing stable or partial-streak reason and the new pacing regression reports excess sleep.

- [ ] **Step 3: Implement the occupied decision boundary**

Extend the protocol and reason literal:

```python
RuntimeResourceReason = Literal[
    "adaptive-disabled",
    "degraded",
    "transition-settle",
    "unknown",
    "occupied",
    "partial-streak",
    "weak-presence",
    "settling",
    "stable",
]

class _RuntimeCadenceSettings(Protocol):
    frame_interval_seconds: float
    occupied_frame_interval_seconds: float
    adaptive_polling_enabled: bool
    stable_frame_interval_seconds: float
    stable_settle_frames: int
```

After degraded, transition, and unknown handling—but before generic partial-streak handling—return the occupied cadence whenever any current state remains confirmed occupied:

```python
if adaptive_enabled and any(
    state.status is OccupancyStatus.OCCUPIED for state in states
):
    return _active_decision(
        settings.runtime.occupied_frame_interval_seconds,
        "occupied",
    )
```

In `paced_sleep_seconds`, keep fixed cadence only for explicitly disabled adaptation; every adaptive decision uses the monotonic deadline helper:

```python
if not settings.runtime.adaptive_polling_enabled:
    return decision.interval_seconds
return remaining_sleep_seconds(
    interval_seconds=decision.interval_seconds,
    iteration_started_at=iteration_started_at,
    now_monotonic=now_monotonic,
)
```

- [ ] **Step 4: Run the policy tests and verify GREEN**

Run `pytest -q tests/test_runtime_resource_policy.py`. Expected: the module passes.

- [ ] **Step 5: Commit the cadence slice**

```bash
git add parking_spot_monitor/runtime_resource_policy.py \
  parking_spot_monitor/runtime_loop_resources.py \
  tests/test_runtime_resource_policy.py
git commit -m "feat: poll occupied spots on fast cadence"
```

---

### Task 3: Runtime Capture Timeout Wiring

**Files:**
- Modify: `parking_spot_monitor/__main__.py`
- Test: `tests/test_startup_capture_and_pacing.py`
- Test: `tests/test_startup_recovery_and_cli.py`

**Interfaces:**
- Consumes: `StreamConfig.capture_timeout_seconds` from Task 1.
- Produces: the existing `StreamProfileCapture` callable with `logger` and `timeout_seconds` bound once at startup.
- Preserves: injected test captures remain unchanged and do not need a timeout keyword.

- [ ] **Step 1: Write a failing runtime-wiring test**

Patch `parking_spot_monitor.__main__.capture_latest` with a recorder, invoke the existing capture-once startup path using a configuration with `capture_timeout_seconds: 4`, and assert:

```python
assert capture_calls == [
    {
        "stream_profile": None,
        "timeout_seconds": 4.0,
    }
]
```

The recorder should accept `logger`, `timeout_seconds`, and `stream_profile`, write a valid primary JPEG, and return the existing `FrameCaptureResult` fixture shape. Add a second test using the injected `capture=` argument to prove the injection signature does not change.

- [ ] **Step 2: Run the two startup modules and verify RED**

Run:

```bash
pytest -q \
  tests/test_startup_capture_and_pacing.py \
  tests/test_startup_recovery_and_cli.py
```

Expected: the recorder sees no configured `timeout_seconds` before the implementation.

- [ ] **Step 3: Bind the timeout in the default runtime capture**

Change only the default callable construction:

```python
capture_fn = (
    capture
    if capture is not None
    else partial(
        capture_latest,
        logger=logger,
        timeout_seconds=settings.stream.capture_timeout_seconds,
    )
)
```

Because primary capture, release escalation, capture-once, and live-proof all receive `capture_fn`, this single boundary applies the timeout consistently without expanding `StreamProfileCapture`.

- [ ] **Step 4: Run startup and capture tests and verify GREEN**

Run:

```bash
pytest -q \
  tests/test_startup_capture_and_pacing.py \
  tests/test_startup_recovery_and_cli.py \
  tests/test_capture.py \
  tests/test_runtime_loop_stream_escalation.py
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit the timeout slice**

```bash
git add parking_spot_monitor/__main__.py \
  tests/test_startup_capture_and_pacing.py \
  tests/test_startup_recovery_and_cli.py
git commit -m "fix: bound runtime decoder attempts"
```

---

### Task 4: Two-Frame Transition Confirmation and 4K Verification

**Files:**
- Test: `tests/test_runtime_stream_escalation.py`
- Test: `tests/test_runtime_loop_stream_escalation.py`
- Test: `tests/test_calibration_replay.py`
- Test: `tests/test_startup_cadence_and_shutdown.py`

**Interfaces:**
- Consumes: existing `_stream_escalation_reason(...)` and `detect_with_stream_escalation(...)` behavior.
- Verifies: two accepted frames confirm entry with `confirm_frames=2`.
- Verifies: low-confidence entry evidence captures `stream.escalation_profile` immediately.
- Verifies: a primary miss that would reach `release_frames=2` captures `stream.escalation_profile` before occupancy state advances.
- Verifies: high-resolution vehicle evidence cancels the release; high-resolution absence produces one open event.

- [ ] **Step 1: Write failing production-cadence transition tests**

Build settings with active, occupied, and stable intervals of 8, 8, and 12 seconds and both confirmation thresholds set to two. Seed stable empty state, provide two accepted entry frames, and assert the first hit returns to the eight-second active cadence and the second hit confirms occupancy:

```python
assert sleeps[:2] == [pytest.approx(12.0), pytest.approx(8.0)]
assert final_state.state_by_spot["right_spot"].status is OccupancyStatus.OCCUPIED
assert len(occupied_transitions) == 1
```

Add a low-confidence first entry candidate and assert the capture profile sequence includes one immediate `high_resolution` verification. Add a failed high-resolution capture case and assert hit streak and status do not advance from incomplete evidence.

- [ ] **Step 2: Write the two-frame release integration tests**

Build settings with `release_frames=2`, seed an occupied spot with `miss_streak=1`, and provide a capture/detector sequence in which primary and high-resolution frames both contain no accepted vehicle:

```python
assert captured_profiles == [None, "high_resolution"]
assert result.escalated is True
assert result.detection.by_spot["right_spot"].accepted is None
```

Pass the result through the existing frame-plan/state-update path and assert exactly one `occupancy-open-event`. Add the inverse case where the 4K frame contains an accepted vehicle and assert no open event and occupied state is retained.

- [ ] **Step 3: Run cadence and escalation tests and establish the baseline**

Run:

```bash
pytest -q \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_calibration_replay.py \
  tests/test_startup_cadence_and_shutdown.py
```

Expected: the production-cadence tests pass using the interfaces completed in Tasks 1–2, and verification tests pass if the existing escalation boundary already covers both two-frame thresholds. Any failure identifies the smallest correction allowed in Step 4.

- [ ] **Step 4: Make the smallest escalation correction only if a verification test remains RED**

The intended condition remains:

```python
if spot_state.miss_streak + 1 >= settings.occupancy.release_frames:
    return "release-transition-candidate"
```

Do not introduce routine 4K polling. If the baseline already passes, retain production code and commit the regression tests alone.

- [ ] **Step 5: Re-run cadence and escalation tests and verify GREEN**

Run the Step 3 command. Expected: all selected tests pass, failures never advance evidence, and no duplicate event is emitted.

- [ ] **Step 6: Commit the verified transition contract**

```bash
git add tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_calibration_replay.py tests/test_startup_cadence_and_shutdown.py \
  parking_spot_monitor/runtime_stream_escalation.py
git commit -m "test: lock low-latency transition verification"
```

If `runtime_stream_escalation.py` is unchanged, omit it from `git add`.

---

### Task 5: Occupied Alert Independence and Text Fallback

**Files:**
- Modify: `parking_spot_monitor/runtime_vehicle_events.py`
- Modify: `parking_spot_monitor/runtime_state_update.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`
- Test: `tests/test_startup_runtime_alerts.py`
- Test: `tests/test_startup_runtime_commands_and_health.py`
- Test: `tests/test_matrix_outbox_retention_and_failures.py`
- Test: `tests/test_matrix_outbox_retry_and_derivatives.py`

**Interfaces:**
- Produces: `build_occupied_transition_alerts(events, enriched_alerts) -> list[dict[str, Any]]`.
- Produces: one base `occupancy-occupied-event` for every confirmed non-occupied-to-occupied transition, even when history is unavailable or broken.
- Produces: optional history/profile fields merged into the base alert without replacing its event identity, spot, observation time, or authoritative snapshot path.
- Produces: text-only durable occupied fallback for recognized snapshot-preparation failures.
- Preserves: outbox persistence failures remain failures; retries and event-ID idempotency remain intact.

- [ ] **Step 1: Write failing history-independence tests**

Add runtime-loop tests for three failure boundaries: `start_session` raises, `attach_occupied_images` raises the live `VehicleHistoryImageError("vehicle image recovery remains pending")`, and no history archive is supplied. Each test confirms occupancy and asserts:

```python
assert len(delivery.occupied_alerts) == 1
alert = delivery.occupied_alerts[0]
assert alert["event_type"] == "occupancy-occupied-event"
assert alert["spot_id"] == "right_spot"
assert alert["occupied_snapshot_path"] == str(tmp_path / "latest.jpg")
assert alert["event_id"].startswith("occupancy-occupied-event:right_spot:")
```

Retain assertions that the history failure appears in health and redacted logs. Add a success case asserting there is still exactly one alert and useful profile/estimate enrichment remains present.

- [ ] **Step 2: Run runtime alert tests and verify RED**

Run:

```bash
pytest -q \
  tests/test_startup_runtime_alerts.py \
  tests/test_startup_runtime_commands_and_health.py
```

Expected: the live image-recovery and archive-start failure cases produce zero occupied alerts.

- [ ] **Step 3: Build mandatory base alerts and merge optional enrichment**

Add a pure helper in `runtime_vehicle_events.py`:

```python
def build_occupied_transition_alerts(
    events: Sequence[OccupancyEvent],
    enriched_alerts: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enriched_by_transition = {
        (str(alert.get("spot_id", "")), str(alert.get("observed_at", ""))): alert
        for alert in enriched_alerts
    }
    alerts: list[dict[str, Any]] = []
    for event in events:
        if (
            event.event_type is not OccupancyEventType.STATE_CHANGED
            or event.previous_status is OccupancyStatus.OCCUPIED
            or event.new_status is not OccupancyStatus.OCCUPIED
        ):
            continue
        key = (event.spot_id, str(event.observed_at))
        enrichment = dict(enriched_by_transition.get(key, {}))
        base: dict[str, Any] = {
            "event_type": OCCUPIED_SPOT_EVENT_TYPE,
            "spot_id": event.spot_id,
            "observed_at": event.observed_at,
            "source_timestamp": event.source_timestamp,
            "occupied_snapshot_path": event.snapshot_path,
        }
        payload = base | enrichment | base
        payload["event_id"] = occupied_spot_event_id(payload)
        alerts.append(payload)
    return alerts
```

Import `Mapping` and `occupied_spot_event_id`. The final `| base` deliberately protects transition identity and the authoritative frame from optional enrichment.

In `runtime_state_update.py`, replace direct iteration over `history_result.occupied_alerts` with `build_occupied_transition_alerts(frame_plan.occupancy_update.events, history_result.occupied_alerts)`. Dispatch that single merged list through the existing occupied-event branch.

- [ ] **Step 4: Run runtime alert tests and verify GREEN**

Run the Step 2 command. Expected: every confirmed occupied transition queues exactly one alert; success cases retain enrichments.

- [ ] **Step 5: Write failing occupied snapshot-fallback tests**

Patch `MatrixOutboxSnapshots.enqueue` to raise a `MatrixError` with `error_type="snapshot_copy_failed"` before durable enqueue. Call `enqueue_occupied_spot_alert` and assert:

```python
assert record.state == "pending"
assert record.intent.event_id == occupied_spot_event_id(event)
assert record.phase_states == {"text": "pending"}
assert record.intent.metadata["event_type"] == "occupancy-occupied-event"
assert record.intent.metadata["snapshot_degraded_reason"] == "snapshot_copy_failed"
```

Drain the record and assert one text send and no upload/image send. Add cases for `snapshot_invalid_source`, `snapshot_metadata_failed`, and `snapshot_resize_failed`. Add an outbox persistence failure case proving it propagates instead of falling back to an in-memory send.

- [ ] **Step 6: Run outbox tests and verify RED**

Run:

```bash
pytest -q \
  tests/test_matrix_outbox_retention_and_failures.py \
  tests/test_matrix_outbox_retry_and_derivatives.py
```

Expected: recognized snapshot errors currently escape and no text-only record exists.

- [ ] **Step 7: Implement durable text fallback at the outbox boundary**

Catch only `MatrixError` from `_enqueue_snapshot_alert`. Extract its bounded `error_type`; re-raise unless it belongs to:

```python
_OCCUPIED_SNAPSHOT_FALLBACK_REASONS = frozenset(
    {
        "snapshot_invalid_source",
        "snapshot_missing_source",
        "snapshot_copy_failed",
        "snapshot_metadata_failed",
        "snapshot_resize_failed",
    }
)
```

Before creating a fallback, call `self.outbox.find_event_record(event_id)`. If a prior snapshot-path attempt already durably created the event, wake the worker and return that record so the pending text phase delivers. Otherwise persist one `AlertIntent` with phases `("text",)`, the formatted occupied body, and metadata containing only `event_type`, `spot_id`, `observed_at`, and `snapshot_degraded_reason`. Log `matrix-outbox-occupied-snapshot-degraded`, log the normal enqueue event with `phase="text"`, wake the worker, and return the record.

- [ ] **Step 8: Run runtime and outbox tests and verify GREEN**

Run:

```bash
pytest -q \
  tests/test_startup_runtime_alerts.py \
  tests/test_startup_runtime_commands_and_health.py \
  tests/test_matrix_outbox_retention_and_failures.py \
  tests/test_matrix_outbox_retry_and_derivatives.py \
  tests/test_startup_matrix_dispatch.py
```

Expected: all selected tests pass with exactly-once event identity and no secret-bearing error text in metadata or logs.

- [ ] **Step 9: Commit the occupied-alert reliability slice**

```bash
git add parking_spot_monitor/runtime_vehicle_events.py \
  parking_spot_monitor/runtime_state_update.py \
  src/parking_monitor/matrix_outbox_delivery.py \
  tests/test_startup_runtime_alerts.py \
  tests/test_startup_runtime_commands_and_health.py \
  tests/test_matrix_outbox_retention_and_failures.py \
  tests/test_matrix_outbox_retry_and_derivatives.py
git commit -m "fix: deliver occupied alerts independently"
```

---

### Task 6: Transition and Matrix Latency Telemetry

**Files:**
- Create: `parking_spot_monitor/runtime_transition_latency.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`
- Test: `tests/test_runtime_transition_latency.py`
- Test: `tests/test_runtime_loop_stream_escalation.py`
- Test: `tests/test_matrix_outbox_worker_lifecycle.py`

**Interfaces:**
- Produces: `TransitionEvidenceTracker` with `observe(...)` and `confirmed_transition_fields(...)` pure timestamp calculations.
- Produces: `occupancy-transition-latency` structured events for arrival and departure.
- Produces: bounded `capture-loop-cadence-changed` records only when cadence interval or reason changes.
- Produces: `matrix-outbox-record-delivered` fields `observation_to_enqueue_seconds` and `enqueue_to_delivery_seconds` when timestamps are valid.
- Preserves: invalid, absent, backward, or non-finite timestamps omit latency fields instead of failing delivery.

- [ ] **Step 1: Write failing pure telemetry tests**

Define the expected tracker API through tests:

```python
def test_confirmed_departure_partitions_observation_and_capture_latency() -> None:
    tracker = TransitionEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
        evidence_status=OccupancyStatus.OCCUPIED,
    )

    fields = tracker.confirmed_transition_fields(
        spot_id="right_spot",
        previous_status=OccupancyStatus.OCCUPIED,
        new_status=OccupancyStatus.EMPTY,
        confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
        primary_capture_seconds=0.4,
        verification_capture_seconds=1.2,
        cadence_seconds=8.0,
        cadence_reason="occupied",
    )

    assert fields == {
        "spot_id": "right_spot",
        "transition_direction": "occupied-to-empty",
        "opposite_evidence_to_confirmation_seconds": 16.0,
        "primary_capture_seconds": 0.4,
        "verification_capture_seconds": 1.2,
        "cadence_seconds": 8.0,
        "cadence_reason": "occupied",
    }
```

Add the symmetric empty-to-occupied case, plus absent prior evidence, backward datetimes, same-state calls, and non-finite durations. Those cases must return only safe fields or `None`, never throw from the capture loop.

- [ ] **Step 2: Run the new telemetry module and verify RED**

Run `pytest -q tests/test_runtime_transition_latency.py`. Expected: import failure because the module does not exist.

- [ ] **Step 3: Implement the bounded tracker**

Use a small in-memory mapping and one shared numeric sanitizer:

```python
@dataclass
class TransitionEvidenceTracker:
    _last_by_spot_status: dict[tuple[str, OccupancyStatus], datetime] = field(
        default_factory=dict
    )

    def observe(
        self,
        *,
        spot_id: str,
        observed_at: datetime,
        evidence_status: OccupancyStatus,
    ) -> None:
        if observed_at.tzinfo is not None:
            self._last_by_spot_status[(spot_id, evidence_status)] = observed_at

    def confirmed_transition_fields(
        self,
        *,
        spot_id: str,
        previous_status: OccupancyStatus,
        new_status: OccupancyStatus,
        confirmed_at: datetime,
        primary_capture_seconds: float,
        verification_capture_seconds: float | None,
        cadence_seconds: float,
        cadence_reason: str,
    ) -> dict[str, str | float] | None:
        if previous_status is new_status:
            return None
        previous = self._last_by_spot_status.pop(
            (spot_id, previous_status),
            None,
        )
        if previous is None or confirmed_at.tzinfo is None:
            return None
        elapsed = (confirmed_at - previous).total_seconds()
        if not isfinite(elapsed) or elapsed < 0:
            return None
        fields: dict[str, str | float] = {
            "spot_id": spot_id,
            "transition_direction": f"{previous_status.value}-to-{new_status.value}",
            "opposite_evidence_to_confirmation_seconds": round(elapsed, 6),
            "primary_capture_seconds": bounded_seconds(primary_capture_seconds),
            "cadence_seconds": bounded_seconds(cadence_seconds),
            "cadence_reason": cadence_reason,
        }
        if verification_capture_seconds is not None:
            fields["verification_capture_seconds"] = bounded_seconds(
                verification_capture_seconds
            )
        return fields
```

`bounded_seconds` returns `0.0` for negative or non-finite values and otherwise rounds to six decimals.

- [ ] **Step 4: Integrate transition telemetry with a failing loop test**

In runtime-loop tests, run an occupied evidence frame followed by two misses, and an empty evidence frame followed by two accepted detections. Assert one JSON log record for each direction:

```python
latency = next(
    record
    for record in records
    if record["event"] == "occupancy-transition-latency"
    and record["transition_direction"] == "occupied-to-empty"
)
assert latency["spot_id"] == "right_spot"
assert latency["opposite_evidence_to_confirmation_seconds"] == 16.0
assert latency["verification_capture_seconds"] >= 0
assert latency["cadence_reason"] == "occupied"
```

Expected before integration: no matching record.

- [ ] **Step 5: Integrate the tracker in `capture_loop.py`**

Instantiate one tracker before entering the loop. After detection and before state replacement, record `OCCUPIED` evidence for an accepted candidate and `EMPTY` evidence only when a spot has neither an accepted candidate nor weak presence. Preserve `previous_runtime_state`, then compare it with `frame_update.runtime_state`; for each changed confirmed status, log `occupancy-transition-latency` using the primary capture duration, the final capture duration only when `frame_result.escalated`, and the policy decision that scheduled the current iteration.

Store the prior `RuntimeResourceDecision` beside `resource_policy_state`, initializing it with `frame_interval_seconds` and reason `"unknown"`, then replace it after every successful policy advance. This keeps telemetry descriptive and does not affect cadence decisions.

When the new policy decision differs from the prior interval or reason, emit one info-level record and then replace the prior decision:

```python
if policy_update.decision != previous_policy_decision:
    logger.info(
        "capture-loop-cadence-changed",
        iteration=iteration,
        previous_interval_seconds=previous_policy_decision.interval_seconds,
        previous_reason=previous_policy_decision.reason,
        interval_seconds=policy_update.decision.interval_seconds,
        cadence_reason=policy_update.decision.reason,
    )
previous_policy_decision = policy_update.decision
```

- [ ] **Step 6: Write failing outbox-delivery timing tests**

Create an open-alert outbox record whose metadata contains `observed_at=2026-07-31T05:00:16Z`, monkeypatch the outbox clock so `created_at=2026-07-31T05:00:18Z`, drain it with delivery time `2026-07-31T05:00:22Z`, and assert the delivered record log contains:

```python
assert delivered_log["observation_to_enqueue_seconds"] == 2.0
assert delivered_log["enqueue_to_delivery_seconds"] == 4.0
```

Add malformed and backward timestamp cases that assert these optional keys are absent while the record still reaches `delivered`.

- [ ] **Step 7: Add safe delivery timing fields**

At the `current.state == "delivered"` boundary, calculate fields from `current.intent.metadata["observed_at"]`, `current.created_at`, and `current.updated_at` using the existing `parse_utc_timestamp` helper. Clamp neither backward interval into a plausible value; omit that field when parsing fails or the result is negative/non-finite.

Log:

```python
self._log(
    "info",
    "matrix-outbox-record-delivered",
    item_id=current.id,
    event_id=current.intent.event_id,
    **_delivery_latency_fields(current),
)
```

- [ ] **Step 8: Run telemetry and integration tests and verify GREEN**

Run:

```bash
pytest -q \
  tests/test_runtime_transition_latency.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_matrix_outbox_worker_lifecycle.py \
  tests/test_matrix_outbox_retention_and_failures.py
```

Expected: all selected tests pass with delivery behavior unchanged.

- [ ] **Step 9: Commit the telemetry slice**

```bash
git add parking_spot_monitor/runtime_transition_latency.py \
  parking_spot_monitor/capture_loop.py \
  src/parking_monitor/matrix_outbox_delivery.py \
  tests/test_runtime_transition_latency.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_matrix_outbox_worker_lifecycle.py
git commit -m "feat: expose occupancy transition latency"
```

---

### Task 7: Operator Documentation and Production Profile

**Files:**
- Modify: `README.md`
- Modify: `docs/deployment.md`
- Modify: `config.yaml.example`
- Test: `tests/test_operator_runtime_docs.py`
- Test: `tests/test_deployment_docs.py`

**Interfaces:**
- Documents: compatible defaults of 15-second timeout and active-matching occupied cadence.
- Documents: production values `capture_timeout_seconds: 4`, active/occupied/stable cadence `8/8/12`, and confirmation/release thresholds `2/2`.
- Documents: fixed-cadence and conservative transition rollback.
- Documents: confirmed occupied alerts are independent of history enrichment and have durable text fallback.
- Documents: host resource preflight and the healthy-host scope of the 30-second target.

- [ ] **Step 1: Write failing documentation contract tests**

Extend documentation tests to require these exact concepts and keys:

```python
required_fragments = (
    "capture_timeout_seconds: 4",
    "frame_interval_seconds: 8",
    "occupied_frame_interval_seconds: 8",
    "stable_frame_interval_seconds: 12",
    "confirm_frames: 2",
    "release_frames: 2",
    "healthy host",
    "low-resolution",
    "high-resolution verification",
    "30 seconds",
    "docker stats --no-stream",
    "free -h",
)
for fragment in required_fragments:
    assert fragment in deployment_text
```

Also assert rollback text restores timeout 15, restores the prior cadence, omits or raises occupied cadence, and restores confirmation and release frames to 3.

- [ ] **Step 2: Run documentation tests and verify RED**

Run:

```bash
pytest -q tests/test_operator_runtime_docs.py tests/test_deployment_docs.py
```

Expected: missing new settings and latency-profile fragments.

- [ ] **Step 3: Document configuration, measurement, and rollback**

Update the README configuration reference and deployment guide with:

```yaml
stream:
  capture_timeout_seconds: 4

occupancy:
  confirm_frames: 2
  release_frames: 2

runtime:
  frame_interval_seconds: 8
  occupied_frame_interval_seconds: 8
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 12
```

Explain that primary low-resolution captures use 12 seconds only when every spot is stably empty and eight seconds otherwise. Document conditional high-resolution entry verification, threshold-reaching high-resolution release verification, and the authoritative final result for an escalated iteration. Document that confirmed occupied transitions create the base alert independently of history/profile work and that occupied snapshot preparation degrades to durable text. Include `free -h` and bounded `docker stats --no-stream` commands, state that external host starvation invalidates the latency target, and state that unrelated containers require separate operator authorization.

- [ ] **Step 4: Run documentation tests and verify GREEN**

Run the Step 2 command. Expected: all documentation tests pass.

- [ ] **Step 5: Commit the documentation slice**

```bash
git add README.md docs/deployment.md config.yaml.example \
  tests/test_operator_runtime_docs.py tests/test_deployment_docs.py
git commit -m "docs: add low-latency deployment profile"
```

---

### Task 8: Verification, Deployment, and Measurement

**Files:**
- Modify outside Git: `/home/keith/src/parking-spot-monitor/config.yaml` after creating a protected backup.
- Verify: `docker-compose.yml`
- Verify: existing deployment override `/tmp/parking-spot-monitor-task6-compose.yml`.

**Interfaces:**
- Consumes: the tested code and production profile from Tasks 1–7.
- Produces: protected rollback bundle, new immutable image ID, healthy recreated parking-monitor service, and before/after resource and latency evidence.

- [ ] **Step 1: Run bytecode compilation and focused suites**

The project declares pytest but no formatter or static type checker. Run:

```bash
python -m compileall -q parking_spot_monitor src/parking_monitor
pytest -q \
  tests/test_config_paths_and_streams.py \
  tests/test_config_runtime_validation.py \
  tests/test_runtime_resource_policy.py \
  tests/test_capture.py \
  tests/test_startup_capture_and_pacing.py \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_startup_runtime_alerts.py \
  tests/test_startup_runtime_commands_and_health.py \
  tests/test_runtime_transition_latency.py \
  tests/test_matrix_outbox_worker_lifecycle.py \
  tests/test_matrix_outbox_retention_and_failures.py \
  tests/test_matrix_outbox_retry_and_derivatives.py \
  tests/test_operator_runtime_docs.py \
  tests/test_deployment_docs.py
```

Expected: all selected tests pass with no unexpected warnings.

- [ ] **Step 2: Run the complete test suite with resource measurement**

```bash
/usr/bin/time -v pytest -q
```

Expected: all tests pass; record wall time and maximum resident set size. Compare with the previous baseline of 1,727 passing tests, 53.40 seconds wall time, and 206,748 KiB peak RSS, allowing for the added tests.

- [ ] **Step 3: Capture pre-deployment evidence and create rollback assets**

Record:

```bash
git rev-parse HEAD
docker inspect parking-spot-monitor-parking-spot-monitor-1 \
  --format '{{.Image}} {{.State.Status}} {{.State.Health.Status}} {{.RestartCount}}'
free -h
docker stats --no-stream \
  --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}'
```

Create a timestamped backup directory under `/home/keith/backups/parking-spot-monitor/` containing the current operator config, Compose files, container inspect output, Git SHA, and current image ID. Tag the current image with a unique `rollback-pre-low-latency-<timestamp>` tag. Do not overwrite prior bundles or tags.

- [ ] **Step 4: Apply and validate only the parking-monitor configuration**

Use `apply_patch` to change only these operator-owned values in `/home/keith/src/parking-spot-monitor/config.yaml`:

```yaml
stream:
  capture_timeout_seconds: 4
occupancy:
  confirm_frames: 2
  release_frames: 2
runtime:
  frame_interval_seconds: 8
  occupied_frame_interval_seconds: 8
  stable_frame_interval_seconds: 12
```

Run the containerized `--validate-config` command with the same env file, Compose files, mounts, and image that production uses. Expected exit code: 0 with redacted configuration output.

- [ ] **Step 5: Build and recreate only the parking-monitor service**

Use the established Compose project and overrides:

```bash
docker compose --project-name parking-spot-monitor \
  --env-file /home/keith/src/parking-spot-monitor/.env \
  -f /home/keith/src/parking-spot-monitor/.worktrees/resource-hardening/docker-compose.yml \
  -f /tmp/parking-spot-monitor-task6-compose.yml \
  build parking-spot-monitor

docker compose --project-name parking-spot-monitor \
  --env-file /home/keith/src/parking-spot-monitor/.env \
  -f /home/keith/src/parking-spot-monitor/.worktrees/resource-hardening/docker-compose.yml \
  -f /tmp/parking-spot-monitor-task6-compose.yml \
  up -d --no-deps parking-spot-monitor
```

Record the new image ID and apply a unique release tag containing the short Git SHA and UTC timestamp.

- [ ] **Step 6: Verify health, cadence, escalation, and resource use**

Wait only in bounded polls of at most 30 seconds. Verify container health, zero restart/OOM count, startup configuration summary, successful low-resolution captures, and `capture-loop-cadence-changed` records showing eight seconds for active/occupied state and 12 seconds for stable all-empty state.

Run bounded resource samples for at least five minutes and record parking-monitor CPU, memory, PIDs, host available memory, and swap. Report competing workloads separately; do not mutate them.

- [ ] **Step 7: Measure controlled or naturally occurring transitions**

For a departure, record the last accepted occupied frame, first primary miss, second primary miss, `release-transition-candidate` 4K escalation, open event, outbox enqueue, and outbox delivered timestamps. For an arrival, record the last unambiguous empty frame, first entry evidence, any `weak-transition-candidate` 4K escalation, second accepted evidence, occupied event, outbox enqueue, and outbox delivered timestamps. Success requires:

```text
one open event
one occupied event
one immediate 4K verification for release and for uncertain entry
occupancy-transition-latency <= 30 seconds in each direction under healthy conditions
no duplicate alert
no capture or verification failure counted as transition evidence
vehicle-history enrichment failure does not suppress the occupied alert
```

If either physical transition is not available during deployment, report its automated timing coverage as passed and leave that live latency validation explicitly pending rather than synthesizing a production event.

- [ ] **Step 8: Commit any verification-only documentation corrections and push**

Run `git status --short`, inspect every diff, commit only intentional tracked changes, and push `optimize/resource-hardening` to `https://github.com/keithah/parking-spot-monitor.git`. Verify the remote branch SHA matches local HEAD. Do not enable or rewrite the saved push URL.

---

## Completion Evidence

Before declaring completion, provide:

- Focused and full-suite pass counts, wall time, and peak RSS.
- Local and remote Git SHA.
- Immutable deployed image ID and release tag.
- Protected rollback bundle and image tag.
- Container health, restart count, OOM state, CPU, and memory.
- Effective redacted cadence, timeout, confirmation, and release threshold values.
- Measured arrival-to-delivery and departure-to-delivery timelines, or explicit notes for any live physical validation still pending.
- Current host available memory, swap use, and top competing container consumers without modifying them.
