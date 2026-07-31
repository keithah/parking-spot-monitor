# Low-Latency Departure Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a high-resolution-verified Matrix open-spot alert within 30 seconds of a real departure when the host, camera, inference, and Matrix service are healthy.

**Architecture:** Extend the pure runtime resource policy with an occupied-state cadence that uses the existing low-resolution primary stream every eight seconds while a car is present. Preserve the existing release-candidate escalation so the second primary-stream miss is immediately verified with the 4K profile, and wire a configurable per-decoder capture timeout through the single runtime capture callable. Add bounded latency fields at the transition and outbox-delivery boundaries so future delays can be assigned to observation, capture, confirmation, enqueue, or Matrix delivery.

**Tech Stack:** Python 3.11+, Pydantic v2 configuration, pytest, existing FFmpeg capture modes, existing primary/4K stream escalation, structured JSON logging, durable Matrix outbox, Docker Compose.

## Global Constraints

- The healthy-path target is at most 30 seconds from physical departure to Matrix alert.
- Production primary polling while a spot is occupied is 8 seconds.
- Production release confirmation is 2 frames.
- Production per-decoder capture timeout is 4 seconds; omission remains backward-compatible at 15 seconds.
- Omitted `runtime.occupied_frame_interval_seconds` resolves to `runtime.frame_interval_seconds`.
- Routine polling stays on the 1458×806 primary stream; only transition verification uses 3840×2160.
- Capture or verification failures never count as empty evidence.
- The service does not modify or restart unrelated containers.
- Every production-code change begins with a failing test and preserves secret-redacted diagnostics.
- All latency values are finite, non-negative seconds rounded to six decimal places.

---

## File Map

- `parking_spot_monitor/config.py`: validate and summarize the new capture timeout and occupied cadence.
- `parking_spot_monitor/runtime_resource_policy.py`: choose the occupied cadence with a stable reason code.
- `parking_spot_monitor/runtime_loop_resources.py`: subtract iteration work from every adaptive cadence.
- `parking_spot_monitor/__main__.py`: bind the configured timeout once into the runtime capture callable.
- `parking_spot_monitor/runtime_transition_latency.py`: pure, bounded transition-observation timing helpers.
- `parking_spot_monitor/capture_loop.py`: maintain in-memory last occupied evidence and log confirmed release latency.
- `src/parking_monitor/matrix_outbox_delivery.py`: log observation-to-enqueue and enqueue-to-delivery timing when an outbox record reaches `delivered`.
- `config.yaml.example`, `README.md`, `docs/deployment.md`: document defaults, production values, healthy-host assumptions, rollback, and resource preflight.
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

### Task 4: Two-Frame Release with Immediate 4K Verification

**Files:**
- Test: `tests/test_runtime_stream_escalation.py`
- Test: `tests/test_runtime_loop_stream_escalation.py`
- Test: `tests/test_calibration_replay.py`

**Interfaces:**
- Consumes: existing `_stream_escalation_reason(...)` and `detect_with_stream_escalation(...)` behavior.
- Verifies: a primary miss that would reach `release_frames` captures `stream.escalation_profile` before occupancy state advances.
- Verifies: high-resolution vehicle evidence cancels the release; high-resolution absence produces one open event.

- [ ] **Step 1: Write an integration test for the production threshold**

Build settings with `release_frames=2`, seed an occupied spot with `miss_streak=1`, and provide a capture/detector sequence in which primary and high-resolution frames both contain no accepted vehicle:

```python
assert captured_profiles == [None, "high_resolution"]
assert result.escalated is True
assert result.detection.by_spot["right_spot"].accepted is None
```

Pass the result through the existing frame-plan/state-update path and assert exactly one `occupancy-open-event`. Add the inverse case where the 4K frame contains an accepted vehicle and assert no open event and occupied state is retained.

- [ ] **Step 2: Run escalation tests and establish the baseline**

Run:

```bash
pytest -q \
  tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_calibration_replay.py
```

Expected: the new tests should pass if the existing escalation contract fully covers a two-frame threshold. If either fails, the failure must identify a real missing boundary before production code is changed.

- [ ] **Step 3: Make the smallest correction only if RED identifies one**

The intended condition remains:

```python
if spot_state.miss_streak + 1 >= settings.occupancy.release_frames:
    return "release-transition-candidate"
```

Do not introduce routine 4K polling. If the baseline already passes, retain production code and commit the regression tests alone.

- [ ] **Step 4: Re-run escalation tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass and no duplicate event is emitted.

- [ ] **Step 5: Commit the verified escalation contract**

```bash
git add tests/test_runtime_stream_escalation.py \
  tests/test_runtime_loop_stream_escalation.py \
  tests/test_calibration_replay.py parking_spot_monitor/runtime_stream_escalation.py
git commit -m "test: lock two-frame release verification"
```

If `runtime_stream_escalation.py` is unchanged, omit it from `git add`.

---

### Task 5: Transition and Matrix Latency Telemetry

**Files:**
- Create: `parking_spot_monitor/runtime_transition_latency.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`
- Test: `tests/test_runtime_transition_latency.py`
- Test: `tests/test_runtime_loop_stream_escalation.py`
- Test: `tests/test_matrix_outbox_worker_lifecycle.py`

**Interfaces:**
- Produces: `OccupiedEvidenceTracker` with `observe(...)` and `confirmed_release_fields(...)` pure timestamp calculations.
- Produces: `departure-detection-latency` structured event.
- Produces: bounded `capture-loop-cadence-changed` records only when cadence interval or reason changes.
- Produces: `matrix-outbox-record-delivered` fields `observation_to_enqueue_seconds` and `enqueue_to_delivery_seconds` when timestamps are valid.
- Preserves: invalid, absent, backward, or non-finite timestamps omit latency fields instead of failing delivery.

- [ ] **Step 1: Write failing pure telemetry tests**

Define the expected tracker API through tests:

```python
def test_confirmed_release_partitions_observation_and_capture_latency() -> None:
    tracker = OccupiedEvidenceTracker()
    tracker.observe(
        spot_id="right_spot",
        observed_at=datetime(2026, 7, 31, 5, 0, 0, tzinfo=UTC),
        occupied_evidence=True,
    )

    fields = tracker.confirmed_release_fields(
        spot_id="right_spot",
        confirmed_at=datetime(2026, 7, 31, 5, 0, 16, tzinfo=UTC),
        primary_capture_seconds=0.4,
        verification_capture_seconds=1.2,
        cadence_seconds=8.0,
        cadence_reason="occupied",
    )

    assert fields == {
        "spot_id": "right_spot",
        "occupied_evidence_to_confirmation_seconds": 16.0,
        "primary_capture_seconds": 0.4,
        "verification_capture_seconds": 1.2,
        "cadence_seconds": 8.0,
        "cadence_reason": "occupied",
    }
```

Add cases for absent prior evidence, backward datetimes, and non-finite durations. Those cases must return only safe fields or `None`, never throw from the capture loop.

- [ ] **Step 2: Run the new telemetry module and verify RED**

Run `pytest -q tests/test_runtime_transition_latency.py`. Expected: import failure because the module does not exist.

- [ ] **Step 3: Implement the bounded tracker**

Use a small in-memory mapping and one shared numeric sanitizer:

```python
@dataclass
class OccupiedEvidenceTracker:
    _last_by_spot: dict[str, datetime] = field(default_factory=dict)

    def observe(self, *, spot_id: str, observed_at: datetime, occupied_evidence: bool) -> None:
        if occupied_evidence and observed_at.tzinfo is not None:
            self._last_by_spot[spot_id] = observed_at

    def confirmed_release_fields(
        self,
        *,
        spot_id: str,
        confirmed_at: datetime,
        primary_capture_seconds: float,
        verification_capture_seconds: float | None,
        cadence_seconds: float,
        cadence_reason: str,
    ) -> dict[str, str | float] | None:
        previous = self._last_by_spot.pop(spot_id, None)
        if previous is None or confirmed_at.tzinfo is None:
            return None
        elapsed = (confirmed_at - previous).total_seconds()
        if not isfinite(elapsed) or elapsed < 0:
            return None
        fields: dict[str, str | float] = {
            "spot_id": spot_id,
            "occupied_evidence_to_confirmation_seconds": round(elapsed, 6),
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

In a runtime-loop test, run an occupied evidence frame followed by the two misses that produce a high-resolution-confirmed open event. Assert one JSON log record:

```python
latency = next(
    record for record in records if record["event"] == "departure-detection-latency"
)
assert latency["spot_id"] == "right_spot"
assert latency["occupied_evidence_to_confirmation_seconds"] == 16.0
assert latency["verification_capture_seconds"] >= 0
assert latency["cadence_reason"] == "occupied"
```

Expected before integration: no matching record.

- [ ] **Step 5: Integrate the tracker in `capture_loop.py`**

Instantiate one tracker before entering the loop. After detection and before state replacement, record accepted vehicle evidence for spots that were confirmed occupied. Preserve `previous_runtime_state`, then compare it with `frame_update.runtime_state`; for each `OCCUPIED -> EMPTY` transition, log `departure-detection-latency` using the primary capture duration, the final capture duration only when `frame_result.escalated`, and the policy decision that scheduled the current iteration.

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
git commit -m "feat: expose departure alert latency"
```

---

### Task 6: Operator Documentation and Production Profile

**Files:**
- Modify: `README.md`
- Modify: `docs/deployment.md`
- Modify: `config.yaml.example`
- Test: `tests/test_operator_runtime_docs.py`
- Test: `tests/test_deployment_docs.py`

**Interfaces:**
- Documents: compatible defaults of 15-second timeout and active-matching occupied cadence.
- Documents: production values `capture_timeout_seconds: 4`, `occupied_frame_interval_seconds: 8`, and `release_frames: 2`.
- Documents: fixed-cadence and conservative release rollback.
- Documents: host resource preflight and the healthy-host scope of the 30-second target.

- [ ] **Step 1: Write failing documentation contract tests**

Extend documentation tests to require these exact concepts and keys:

```python
required_fragments = (
    "capture_timeout_seconds: 4",
    "occupied_frame_interval_seconds: 8",
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

Also assert rollback text restores timeout 15, omits or raises occupied cadence, and restores release frames 3.

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
  release_frames: 2

runtime:
  frame_interval_seconds: 30
  occupied_frame_interval_seconds: 8
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 60
```

Explain that primary low-resolution captures run at the occupied cadence, the threshold-reaching miss immediately invokes one high-resolution verification, and the high-resolution result is authoritative for that iteration. Include `free -h` and bounded `docker stats --no-stream` commands, state that external host starvation invalidates the latency target, and state that unrelated containers require separate operator authorization.

- [ ] **Step 4: Run documentation tests and verify GREEN**

Run the Step 2 command. Expected: all documentation tests pass.

- [ ] **Step 5: Commit the documentation slice**

```bash
git add README.md docs/deployment.md config.yaml.example \
  tests/test_operator_runtime_docs.py tests/test_deployment_docs.py
git commit -m "docs: add low-latency deployment profile"
```

---

### Task 7: Verification, Deployment, and Measurement

**Files:**
- Modify outside Git: `/home/keith/src/parking-spot-monitor/config.yaml` after creating a protected backup.
- Verify: `docker-compose.yml`
- Verify: existing deployment override `/tmp/parking-spot-monitor-task6-compose.yml`.

**Interfaces:**
- Consumes: the tested code and production profile from Tasks 1–6.
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
  tests/test_runtime_transition_latency.py \
  tests/test_matrix_outbox_worker_lifecycle.py \
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
  release_frames: 2
runtime:
  occupied_frame_interval_seconds: 8
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

Wait only in bounded polls of at most 30 seconds. Verify container health, zero restart/OOM count, startup configuration summary, successful low-resolution captures, and `capture-loop-paced` records with `cadence_reason=occupied` and an interval near eight seconds when a spot is occupied. Confirm stable empty state backs off to the configured stable interval.

Run bounded resource samples for at least five minutes and record parking-monitor CPU, memory, PIDs, host available memory, and swap. Report competing workloads separately; do not mutate them.

- [ ] **Step 7: Measure the next controlled or naturally occurring departure**

For a confirmed occupied spot, record the last accepted occupied frame, first primary miss, second primary miss, `release-transition-candidate` 4K escalation, open event, outbox enqueue, and outbox delivered timestamps. Success requires:

```text
one open event
one immediate 4K verification
departure-detection-latency <= 30 seconds under healthy conditions
no duplicate alert
no capture counted as empty after a capture/verification failure
```

If a physical departure is not available during deployment, report automated timing coverage as passed and leave live latency validation explicitly pending rather than synthesizing a production event.

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
- Effective redacted cadence, timeout, and release threshold values.
- A measured departure-to-delivery timeline, or an explicit note that live physical validation remains pending.
- Current host available memory, swap use, and top competing container consumers without modifying them.
