# Final Audit Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the residual whole-project audit by simplifying runtime boundaries, bounding shutdown and outage work, reducing file and image amplification, and proving the deployed resource improvement without changing operator-visible behavior or unsafe schema migrations.

**Architecture:** Keep the existing single-service Docker Compose topology and file-backed durability. Add small service-scoped owners for the detector, decision memory, and Matrix command fetch; keep all operator mutations on the capture thread; use optional, backward-readable persisted fields only where retry timing or an upload derivative must survive restart. Reuse existing streamed archive readers and the existing health snapshot cache rather than adding an archive index. Decompose large modules behind compatibility facades rather than introducing a database, broker, generic event bus, or service container.

**Tech Stack:** Python 3.12 container runtime, pytest, Pydantic v2, Pillow, httpx, Ultralytics YOLO, JSON/JSONL-compatible local files, Docker BuildKit, and Docker Compose.

## Global Constraints

- Existing YAML keys and their meanings remain backward compatible; every new key has a safe default and existing configuration files continue to load.
- Existing CLI commands, exit codes, Matrix command text, alert content, and Compose topology remain backward compatible.
- Runtime state, decision memory, vehicle history, corrections, and Matrix outbox retain their existing schema versions. New outbox retry fields are optional within schema version 1, default correctly when absent, and are ignored safely by the prior reader.
- No database, broker, external service, general-purpose executor, unbounded queue, service container, or event-bus framework is added.
- Lower resource use is more important than immediate Matrix command responsiveness; background command work may be slower, and all new timing controls remain adjustable.
- Capture geometry remains capped at 7,680 pixels per dimension, 33,177,600 total pixels, and 32 MiB encoded JPEG size.
- Existing detector implementations and test doubles remain supported through one construction-time adapter; runtime hot paths do not inspect signatures or retry side-effecting calls after `TypeError`.
- The durable Matrix outbox remains the only alert queue. Network delivery never moves back onto the capture-critical path.
- Archive health remains streamed and TTL/revision-cached through the existing boundary. No archive index, recent-session API, journal, database, or durable secondary format is introduced; bounded full reconciliation/analytics scans without a current hot-path consumer remain intentionally unchanged.
- Schema-compatible outbox durability requires one complete atomic JSON publication after each durable network phase boundary. This plan may remove only proven duplicate/no-op mutations and summary rescans; it does not eliminate those bounded full-file phase writes without a separately reviewed storage migration.
- `pytest-xdist` is deliberately skipped: parallel test workers increase peak CPU and RSS and conflict with the operator's resource priority. Verification runs serially.
- ONNX and TorchScript remain benchmark-only. Production backend selection does not change unless the fixed replay corpus meets every accuracy-parity gate and a separate reviewed change approves the switch.
- Every behavior change follows RED-GREEN-REFACTOR. Run the named focused test before and after each production edit, then commit only the task's cohesive files.

## File and Responsibility Map

| File | Responsibility after this plan |
|---|---|
| `tests/dependency_lock_helpers.py` | Shared lock-test fixtures and parsers only. |
| `tests/test_dependency_lock_contract.py` | Static manifest/lock/source-digest contracts. |
| `tests/test_dependency_lock_generation.py` | Authenticated generation, staging, publication, rollback, and cleanup. |
| `tests/test_dependency_lock_validation.py` | Requirement grammar, version, index, and compiler-output validation. |
| `parking_spot_monitor/detector_adapter.py` | One construction-time legacy detector adapter and one shared lazy detector owner. |
| `parking_spot_monitor/owner_vehicles.py` | Size-bounded parsing plus typed strict/permissive registry loads. |
| `parking_spot_monitor/runtime_owner_vehicle_cache.py` | Stable snapshot validation; malformed data never replaces the last valid cache entry. |
| `parking_spot_monitor/runtime_lifecycle.py` | Signal state, interruptible waits, and durable lifecycle enqueue. |
| `parking_spot_monitor/runtime_command_worker.py` | One fetch-only Matrix sync worker with a one-result handoff. |
| `parking_spot_monitor/runtime_log_aggregation.py` | Bounded success/failure/diagnostic counters and periodic summaries. |
| `src/parking_monitor/outbox_models.py` | Outbox value types, validation, optional retry timing, and JSON conversion. |
| `src/parking_monitor/outbox_storage.py` | Atomic persistence, recovery, quarantine, retention, and schema-v1 compatibility. |
| `src/parking_monitor/outbox.py` | Compatibility facade plus indexed transactional mutations and revision-cached summaries. |
| `parking_spot_monitor/decision_memory_store.py` | Service-scoped bounded records, dirty tracking, timed checkpoints, and close flush. |
| `parking_spot_monitor/vehicle_history_profiles.py` | Profile estimates, including reuse of already-loaded effective records in profile summaries. |
| `parking_spot_monitor/runtime_health_cache.py` | Existing revision/TTL cache over streamed archive-health snapshots. |
| `parking_spot_monitor/jpeg_artifacts.py` | Atomic canonical JPEG publication plus one bounded decode/draft/RGB/close helper shared by snapshot callers. |
| `scripts/benchmark_detector_backends.py` | Offline `.pt`/ONNX/TorchScript timing, memory, and parity evidence only. |
| `docs/deployment.md` | New timing controls, graceful stop, Ultralytics state, benchmark gate, rollout, measurement, and rollback. |

---

### Task 1: Structural Cleanup and Dependency-Test Decomposition

**Files:**
- Create: `tests/dependency_lock_helpers.py`
- Create: `tests/test_dependency_lock_contract.py`
- Create: `tests/test_dependency_lock_generation.py`
- Create: `tests/test_dependency_lock_validation.py`
- Modify: `tests/test_module_decomposition.py`
- Delete: `tests/test_dependency_locks.py`
- Modify: `parking_spot_monitor/runtime_decision_memory.py`
- Modify: `parking_spot_monitor/runtime_detection.py`
- Modify: `tests/test_runtime_stream_escalation.py`
- Modify: `parking_spot_monitor/runtime_commands.py`
- Modify: `parking_spot_monitor/operator_cockpit_shared.py`
- Modify: `parking_spot_monitor/matrix_snapshots.py`
- Modify: `parking_spot_monitor/runtime_vehicle_events.py`
- Modify: `parking_spot_monitor/vehicle_history_storage.py`
- Modify: `parking_spot_monitor/matrix_delivery.py`
- Modify: `parking_spot_monitor/matrix_command_catalog.py`
- Modify: `parking_spot_monitor/operator_feedback_models.py`
- Modify: `parking_spot_monitor/vehicle_history_sessions.py`
- Modify: `parking_spot_monitor/matrix_alerts.py`
- Modify: `parking_spot_monitor/replay.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`

**Interfaces:**
- Consumes: `scripts.lock_dependencies.main(argv: list[str] | None) -> int`, `scripts.dependency_lock_validation` public validation helpers, and `runtime_detection_support._candidate_summary(candidate: SpotDetectionCandidate) -> dict[str, Any]`.
- Produces: the same dependency-lock assertions under three focused test modules; `runtime_decision_memory.build_detection_memory_records(...)` imports `_candidate_summary` directly from `runtime_detection_support`; `runtime_detection.record_detection_memory_records` is removed.
- Produces: removal of the unreferenced `ReplayValidationError` declaration in `parking_spot_monitor/replay.py` and `_retry_reason` helper in `src/parking_monitor/matrix_outbox_delivery.py`, after a whole-tree reference check.
- Preserves: `parking_spot_monitor.vehicle_history` and `parking_spot_monitor.matrix` compatibility-facade exports. Imports used only for those documented facades are not classified as dead.

- [ ] **Step 1: Add a failing decomposition and cycle contract**

```python
def test_dependency_lock_tests_are_split_by_responsibility() -> None:
    root = Path(__file__).resolve().parents[1]
    assert not (root / "tests" / "test_dependency_locks.py").exists()
    expected = (
        "dependency_lock_helpers.py",
        "test_dependency_lock_contract.py",
        "test_dependency_lock_generation.py",
        "test_dependency_lock_validation.py",
    )
    for name in expected:
        path = root / "tests" / name
        assert path.exists(), name
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 500, name


def test_runtime_decision_memory_does_not_import_runtime_detection() -> None:
    imports = imported_modules(ROOT / "parking_spot_monitor" / "runtime_decision_memory.py")
    assert "parking_spot_monitor.runtime_detection" not in imports
```

- [ ] **Step 2: Run the new structural tests and verify RED**

Run: `python -m pytest tests/test_module_decomposition.py::test_dependency_lock_tests_are_split_by_responsibility tests/test_module_decomposition.py::test_runtime_decision_memory_does_not_import_runtime_detection -q`

Expected: FAIL because the 1,109-line test file still exists and `runtime_decision_memory.py` dynamically imports `runtime_detection`.

- [ ] **Step 3: Extract shared lock-test helpers without changing assertions**

Move `ROOT`, `LOCK_SCRIPT`, `LOCK_PATHS`, `_run_check`, `_copy_lock_inputs`, `_requirement_blocks`, `_lock_pin_versions`, `_remove_lock_requirement`, `_load_lock_module`, `_compiled_requirement`, and `_valid_compiled` into `tests/dependency_lock_helpers.py`. Preserve their signatures exactly and import them explicitly from each new test module:

```python
from tests.dependency_lock_helpers import (
    LOCK_PATHS,
    LOCK_SCRIPT,
    ROOT,
    compiled_requirement,
    copy_lock_inputs,
    load_lock_module,
    lock_pin_versions,
    requirement_blocks,
    run_check,
    valid_compiled,
)
```

Rename the extracted helpers without a leading underscore because pytest modules share them as a supported test utility. Do not duplicate subprocess environment construction across the three modules.

- [ ] **Step 4: Split the tests along real review boundaries**

Place tests through the current source-digest and broad-bound checks in `test_dependency_lock_contract.py`; authenticated staging/generation/publication/rollback/cleanup tests in `test_dependency_lock_generation.py`; and grammar, PEP 440, interpreter, continuation, index, and compiler-output validation tests in `test_dependency_lock_validation.py`. Preserve every current test name so CI history remains searchable, then delete `tests/test_dependency_locks.py`.

Run: `python -m pytest tests/test_dependency_lock_contract.py tests/test_dependency_lock_generation.py tests/test_dependency_lock_validation.py -q`

Expected: PASS with the same total collected test cases as `python -m pytest tests/test_dependency_locks.py --collect-only -q` recorded before the move.

- [ ] **Step 5: Remove the detection pass-through and import cycle**

Replace the dynamic import in `runtime_decision_memory.py` with:

```python
from parking_spot_monitor.runtime_detection_support import _candidate_summary
```

Delete `record_detection_memory_records` and its now-unused `append_decision_memory_records`, `build_detection_memory_records`, `_DETECT_CAPABILITY_CACHE`, and `_candidate_summary` imports from `runtime_detection.py`. Update `tests/test_runtime_stream_escalation.py` to construct records with `build_detection_memory_records(...)` and persist them with `append_decision_memory_records(...)`, which are the two actual owners being tested.

- [ ] **Step 6: Remove confirmed dead imports and declarations, not compatibility exports**

First prove the two declarations have no consumers:

```bash
rg -n 'ReplayValidationError|_retry_reason' parking_spot_monitor src tests
```

Expected before deletion: only the class definition in `parking_spot_monitor/replay.py` and function definition in `src/parking_monitor/matrix_outbox_delivery.py`; no imports, calls, annotations, string lookups, or tests. Add a structural assertion that those two source files no longer define `class ReplayValidationError` or `def _retry_reason`, then delete both declarations.

Remove only these AST-confirmed unused declarations: `Any` from `runtime_commands.py`; `timedelta` from `operator_cockpit_shared.py`; `Mapping`, `datetime`, and `timezone` from `matrix_snapshots.py`; `Path` from `runtime_vehicle_events.py`; `redact_diagnostic_text` and `redact_diagnostic_value` from `vehicle_history_storage.py`; `OWNER_VEHICLE_QUIET_WINDOW_EVENT_TYPE` from `matrix_delivery.py`; `Sequence` from `matrix_command_catalog.py`; `datetime` from `operator_feedback_models.py`; `Any` from `vehicle_history_sessions.py`; and `DISPLAY_TIMEZONE` from `matrix_alerts.py`.

Keep facade-only imports in `matrix.py`, `vehicle_history.py`, `operator_feedback.py`, and `operator_cockpit.py`, and keep `cutoff_older_than_days` re-exported through `vehicle_history_archive.py` because `vehicle_history.py` imports it from that compatibility boundary.

- [ ] **Step 7: Verify structural GREEN and compile**

Run: `python -m pytest tests/test_module_decomposition.py tests/test_dependency_lock_contract.py tests/test_dependency_lock_generation.py tests/test_dependency_lock_validation.py tests/test_runtime_stream_escalation.py tests/test_replay.py tests/test_matrix_outbox_delivery.py -q`

Expected: PASS.

Run: `python -m compileall -q parking_spot_monitor src scripts tests`

Expected: exit 0 with no output.

- [ ] **Step 8: Commit the structural cleanup**

```bash
git add tests/dependency_lock_helpers.py tests/test_dependency_lock_contract.py tests/test_dependency_lock_generation.py tests/test_dependency_lock_validation.py tests/test_module_decomposition.py tests/test_runtime_stream_escalation.py parking_spot_monitor/runtime_decision_memory.py parking_spot_monitor/runtime_detection.py parking_spot_monitor/runtime_commands.py parking_spot_monitor/operator_cockpit_shared.py parking_spot_monitor/matrix_snapshots.py parking_spot_monitor/runtime_vehicle_events.py parking_spot_monitor/vehicle_history_storage.py parking_spot_monitor/matrix_delivery.py parking_spot_monitor/matrix_command_catalog.py parking_spot_monitor/operator_feedback_models.py parking_spot_monitor/vehicle_history_sessions.py parking_spot_monitor/matrix_alerts.py parking_spot_monitor/replay.py src/parking_monitor/matrix_outbox_delivery.py
git add -u tests/test_dependency_locks.py
git commit -m "refactor: clean structural audit findings"
```

### Task 2: Simplify Detector Adaptation and Share One Lazy Model

**Files:**
- Create: `parking_spot_monitor/detector_adapter.py`
- Delete: `parking_spot_monitor/runtime_detector_capabilities.py`
- Modify: `parking_spot_monitor/detection.py`
- Modify: `parking_spot_monitor/runtime_detection.py`
- Modify: `parking_spot_monitor/runtime_frame.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `parking_spot_monitor/__main__.py`
- Create: `tests/test_detector_adapter.py`
- Delete: `tests/test_runtime_detector_capabilities.py`
- Modify: `tests/test_detection.py`
- Modify: `tests/test_startup.py`
- Modify: `tests/test_module_decomposition.py`

**Interfaces:**
- Consumes: legacy detectors exposing `detect(frame_path, *, confidence_threshold=None)` with optional `inference_image_size`, and optional `detect_image(image, *, confidence_threshold=None, inference_image_size=None)`.
- Produces: `adapt_detector(detector: object) -> DetectorAdapter`; `DetectorAdapter.detect_path(...) -> list[VehicleDetection]`; `DetectorAdapter.detect_image_if_supported(...) -> list[VehicleDetection] | None`; `SharedLazyDetector(factory: Callable[[], object])` implementing the same two calls and `loaded: bool`.
- Produces: `_default_matrix_command_service_factory(..., incident_detector: object)` receives the same `SharedLazyDetector` instance returned to the runtime detector factory.
- Removes: global weak-reference capability caches, callable fingerprints, per-frame signature logic, `_LazyIncidentReplayDetector`, and misleading eager behavior in the class documented as lazy.

- [ ] **Step 1: Write failing adapter and shared-owner tests**

```python
def test_adapter_inspects_legacy_signature_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    real_signature = inspect.signature

    def counted(value: object) -> inspect.Signature:
        nonlocal calls
        calls += 1
        return real_signature(value)

    monkeypatch.setattr(inspect, "signature", counted)
    raw = LegacyDetector()
    adapter = adapt_detector(raw)
    adapter.detect_path(Path("one.jpg"), confidence_threshold=0.1, inference_image_size=640)
    adapter.detect_path(Path("two.jpg"), confidence_threshold=0.2, inference_image_size=320)
    assert calls <= 2


def test_shared_lazy_detector_constructs_one_backend_for_runtime_and_incident() -> None:
    constructed: list[FakeDetector] = []

    def factory() -> FakeDetector:
        detector = FakeDetector()
        constructed.append(detector)
        return detector

    shared = SharedLazyDetector(factory)
    assert shared.loaded is False
    shared.detect_path(Path("runtime.jpg"), confidence_threshold=0.1, inference_image_size=640)
    shared.detect_path(Path("incident.jpg"), confidence_threshold=0.1, inference_image_size=640)
    assert shared.loaded is True
    assert len(constructed) == 1
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_detector_adapter.py -q`

Expected: FAIL with `ModuleNotFoundError: parking_spot_monitor.detector_adapter`.

- [ ] **Step 3: Implement one construction-time call plan**

```python
@dataclass(frozen=True, slots=True)
class DetectorCallPlan:
    path_accepts_image_size: bool
    image_method: Callable[..., object] | None


class DetectorAdapter:
    def __init__(self, detector: object) -> None:
        self.raw = detector
        path_method = getattr(detector, "detect")
        self._path_method = path_method
        self._plan = DetectorCallPlan(
            path_accepts_image_size=_accepts_keyword(path_method, "inference_image_size"),
            image_method=_compatible_image_method(detector),
        )

    def detect_path(self, frame_path: Path, *, confidence_threshold: float, inference_image_size: int | None) -> list[VehicleDetection]:
        kwargs: dict[str, object] = {"confidence_threshold": confidence_threshold}
        if self._plan.path_accepts_image_size:
            kwargs["inference_image_size"] = inference_image_size
        return list(self._path_method(frame_path, **kwargs))

    def detect_image_if_supported(self, image: object, *, confidence_threshold: float, inference_image_size: int | None) -> list[VehicleDetection] | None:
        if self._plan.image_method is None:
            return None
        return list(self._plan.image_method(image, confidence_threshold=confidence_threshold, inference_image_size=inference_image_size))
```

`_accepts_keyword` and `_compatible_image_method` call `inspect.signature` only in `DetectorAdapter.__init__`. They return `False`/`None` for uninspectable callables and never catch `TypeError` from an actual detector invocation.

- [ ] **Step 4: Implement the thread-safe shared lazy owner**

```python
class SharedLazyDetector:
    def __init__(self, factory: Callable[[], object]) -> None:
        self._factory = factory
        self._lock = threading.Lock()
        self._adapter: DetectorAdapter | None = None

    @property
    def loaded(self) -> bool:
        return self._adapter is not None

    def _get(self) -> DetectorAdapter:
        if self._adapter is None:
            with self._lock:
                if self._adapter is None:
                    self._adapter = adapt_detector(self._factory())
        return self._adapter
```

Delegate `detect_path` and `detect_image_if_supported` to `_get()`. The lock protects the detection-lab/command boundary without retaining replaced methods, code objects, or detector IDs globally.

- [ ] **Step 5: Route runtime and incident review through the same owner**

In `_main`, create exactly one `SharedLazyDetector(lambda: detector_fn(settings))`. Pass `lambda _settings: shared_detector` to `run_capture_loop`, and pass `shared_detector` as the required `incident_detector` argument when calling the default command-service factory. Custom command-service factories retain their existing four-argument injection contract; only the default factory receives the shared production owner.

In `runtime_detection.py`, call `detector.detect_image_if_supported(...)` for crops and `detector.detect_path(...)` for paths. Retain the bounded temporary-JPEG fallback when the image method returns `None`.

- [ ] **Step 6: Make Ultralytics construction genuinely lazy**

Keep `UltralyticsVehicleDetector` as the backend adapter, but move `YOLO(model_path)` from `__init__` into a locked `_load_model()` called by `_predict`. Store the injected `yolo_class` until first use. Preserve the current `DetectionError` phases and safe diagnostics.

```python
def _load_model(self) -> object:
    if self._model is None:
        yolo = self._yolo_class if self._yolo_class is not None else _load_ultralytics_yolo()
        self._model = yolo(self.model_path)
    return self._model
```

- [ ] **Step 7: Replace mutation-heavy capability tests with stable contracts**

Delete tests that mutate `__code__`, descriptors, defaults, `__signature__`, or custom `__getattribute__` after adapter construction. Add tests for a legacy path-only detector, a modern in-memory detector, a non-weakrefable detector, propagation of internal `TypeError`, one-time adapter construction, one backend shared by runtime and incident replay, and retry after a failed lazy model load.

Run: `python -m pytest tests/test_detector_adapter.py tests/test_detection.py tests/test_startup.py -q`

Expected: PASS.

- [ ] **Step 8: Enforce deletion and commit**

Add a module-decomposition assertion that `runtime_detector_capabilities.py` is absent and no production import names it.

```bash
git add parking_spot_monitor/detector_adapter.py parking_spot_monitor/detection.py parking_spot_monitor/runtime_detection.py parking_spot_monitor/runtime_frame.py parking_spot_monitor/capture_loop.py parking_spot_monitor/__main__.py tests/test_detector_adapter.py tests/test_detection.py tests/test_startup.py tests/test_module_decomposition.py
git add -u parking_spot_monitor/runtime_detector_capabilities.py tests/test_runtime_detector_capabilities.py
git commit -m "refactor: share one lazy detector adapter"
```

### Task 3: Bound Owner Registry Loads and Require Snapshot Provision

**Files:**
- Modify: `parking_spot_monitor/owner_vehicles.py`
- Modify: `parking_spot_monitor/runtime_owner_vehicle_cache.py`
- Modify: `parking_spot_monitor/runtime_frame_plan.py`
- Modify: `parking_spot_monitor/runtime_state_update.py`
- Modify: `parking_spot_monitor/runtime_vehicle_events.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `parking_spot_monitor/matrix_command_runtime.py`
- Modify: `parking_spot_monitor/matrix_commands.py`
- Modify: `parking_spot_monitor/matrix_command_catalog.py`
- Modify: `parking_spot_monitor/__main__.py`
- Modify: `tests/test_owner_vehicles.py`
- Modify: `tests/test_runtime_owner_vehicle_cache.py`
- Modify: `tests/test_matrix.py`
- Modify: `tests/test_startup.py`

**Interfaces:**
- Produces: `MAX_OWNER_REGISTRY_BYTES = 65_536`; `OwnerVehicleRegistryError(code: OwnerVehicleRegistryErrorCode, message: str)`; `load_owner_vehicle_registry(path, *, strict: bool = False, max_bytes: int = MAX_OWNER_REGISTRY_BYTES) -> OwnerVehicleRegistry`.
- Produces: `OwnerVehicleRuntimeCache(registry_path, *, logger)` with last-known-good retained only across an invalid replacement; initial invalid data raises `OwnerVehicleSnapshotUnavailableError` and produces no owner alert.
- Produces: `OwnerVehicleSnapshotProvider` protocol with `snapshot(archive: OwnerVehicleArchive) -> OwnerVehicleSnapshot`. A required `owner_vehicle_snapshot_provider: OwnerVehicleSnapshotProvider` keyword is threaded through `build_runtime_frame_plan`, `_update_runtime_state_for_frame`, and `_owner_vehicle_quiet_window_alerts`; the capture loop always supplies its service-scoped `OwnerVehicleRuntimeCache`.
- Produces: `WhoSnapshotProvider = Callable[[str], str | MatrixCommandResponse]`; `MatrixCommandService(..., who_snapshot_provider: WhoSnapshotProvider)` and `MatrixCommandRuntime.who_snapshot_provider: WhoSnapshotProvider` are required.
- Preserves: permissive direct loader behavior for compatibility (`strict=False` returns empty for malformed data); missing file remains a valid empty registry in both modes. Inactive quiet-window evaluation returns before invoking the required owner snapshot provider.
- Distinguishes: the Matrix `WhoSnapshotProvider` is independently required for command rendering and cannot substitute for the runtime owner-registry/session `OwnerVehicleSnapshotProvider`.

- [ ] **Step 1: Add strict, size, LKG, and required-provider tests**

```python
def test_strict_owner_registry_rejects_oversize_before_json_decode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "owner-vehicles.json"
    path.write_bytes(b"{" + b" " * 65_536 + b"}")
    called = False

    def forbidden_loads(_text: str) -> object:
        nonlocal called
        called = True
        raise AssertionError("oversized registry reached json.loads")

    monkeypatch.setattr(json, "loads", forbidden_loads)
    with pytest.raises(OwnerVehicleRegistryError, match="too_large"):
        load_owner_vehicle_registry(path, strict=True)
    assert called is False


def test_runtime_cache_keeps_valid_registry_across_invalid_replace(tmp_path: Path) -> None:
    path = tmp_path / "owner-vehicles.json"
    write_registry(path, "profile-a")
    cache = OwnerVehicleRuntimeCache(path, logger=StructuredLogger())
    first = cache.snapshot(FakeArchive())
    path.write_text("{broken", encoding="utf-8")
    second = cache.snapshot(FakeArchive())
    assert second.registry is first.registry
    assert second.registry.owner_for_profile("profile-a") is not None
```

Add `test_matrix_command_service_requires_who_snapshot_provider` asserting construction without the keyword raises `TypeError`, and `test_active_assignments_always_calls_required_snapshot_provider` asserting the provider receives the formatted base reply exactly once.

Add runtime tests asserting: omitting `owner_vehicle_snapshot_provider` from each of the three internal runtime boundaries raises `TypeError`; the capture loop passes the same cache instance on every frame; active owner evaluation calls it once; and an inactive quiet window returns `[]` without calling a provider whose `snapshot` raises if touched. Add a cache contract test whose fake archive lacks `active_session_signature`, expecting `AttributeError` rather than silently accepting an incomplete protocol.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_owner_vehicles.py tests/test_runtime_owner_vehicle_cache.py tests/test_matrix.py tests/test_startup.py -q`

Expected: FAIL because strict errors, byte bounding, LKG behavior, both required provider contracts, and unconditional capture-loop owner-provider wiring do not exist.

- [ ] **Step 3: Add typed strict parsing without breaking permissive callers**

```python
OwnerVehicleRegistryErrorCode = Literal["read_failed", "too_large", "invalid_json", "invalid_schema"]


class OwnerVehicleRegistryError(ValueError):
    def __init__(self, code: OwnerVehicleRegistryErrorCode, message: str) -> None:
        super().__init__(code)
        self.code = code
        self.safe_message = message
```

Stat before read, reject `st_size > max_bytes`, read at most `max_bytes + 1`, translate JSON and schema failures to typed codes, and never include raw file content in the error. In permissive mode catch the typed error and return `OwnerVehicleRegistry.empty()`; in strict mode re-raise it.

- [ ] **Step 4: Retain only a previously validated stable registry**

Track `_last_good_registry`, `_last_good_signature`, and `_last_failed_signature` separately from the combined active-session snapshot. On a strict load failure, log one redacted warning per failed signature. If a last-good registry exists, reuse it while still refreshing active sessions for the current archive revision; if none exists, raise `OwnerVehicleSnapshotUnavailableError`. Never assign the invalid signature to `_entry`. A later valid replacement must clear the failed signature and become the new LKG. In `_snapshot_key`, call `archive.active_session_signature()` directly as required by `OwnerVehicleArchive`; delete the `getattr(..., None)` fallback and make incomplete implementations fail at the typed boundary.

- [ ] **Step 5: Make both snapshot providers real, separate invariants**

Define `OwnerVehicleSnapshotProvider` beside `OwnerVehicleRuntimeCache`. Remove the default and `None` branch for `owner_vehicle_snapshot_provider` from `build_runtime_frame_plan`, `_update_runtime_state_for_frame`, and `_owner_vehicle_quiet_window_alerts`. Keep the early `history_archive is None or not quiet_status.active` return before the required provider is invoked, but when owner evaluation is active call only `owner_vehicle_snapshot_provider.snapshot(history_archive)`; delete the direct `load_owner_vehicle_registry`/`load_active_sessions` fallback. Construct one cache during startup and make `capture_loop.py` pass it to every state update.

Define the `WhoSnapshotProvider` alias in `matrix_command_runtime.py`, move `who_snapshot_provider` before fields with defaults in `MatrixCommandRuntime`, remove the `None` branch from `ActiveSpotAssignmentsCommand.apply`, and remove the default from `MatrixCommandService.__init__`. Update every test service constructor to pass either its real provider or `lambda base_reply: base_reply`.

- [ ] **Step 6: Verify repair, fail-closed alerting, and startup wiring**

Run: `python -m pytest tests/test_owner_vehicles.py tests/test_runtime_owner_vehicle_cache.py tests/test_matrix.py tests/test_startup.py -q`

Expected: PASS, including malformed initial registry -> warning/no alert, valid -> malformed -> valid replacement recovery, required owner provider propagation, inactive-window no-call, strict archive signature access, and required command-service provider wiring.

- [ ] **Step 7: Commit owner-boundary hardening**

```bash
git add parking_spot_monitor/owner_vehicles.py parking_spot_monitor/runtime_owner_vehicle_cache.py parking_spot_monitor/runtime_frame_plan.py parking_spot_monitor/runtime_state_update.py parking_spot_monitor/runtime_vehicle_events.py parking_spot_monitor/capture_loop.py parking_spot_monitor/matrix_command_runtime.py parking_spot_monitor/matrix_commands.py parking_spot_monitor/matrix_command_catalog.py parking_spot_monitor/__main__.py tests/test_owner_vehicles.py tests/test_runtime_owner_vehicle_cache.py tests/test_matrix.py tests/test_startup.py
git commit -m "fix: harden owner and snapshot boundaries"
```

### Task 4: Make Shutdown Interruptible, Durable, and Cleanup-Safe

**Files:**
- Modify: `parking_spot_monitor/runtime_lifecycle.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `parking_spot_monitor/matrix_dispatch.py`
- Modify: `parking_spot_monitor/__main__.py`
- Modify: `parking_spot_monitor/matrix_client.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`
- Modify: `docker-compose.yml`
- Modify: `tests/test_startup.py`
- Modify: `tests/test_matrix_outbox_delivery.py`
- Modify: `tests/test_docker_contract.py`

**Interfaces:**
- Produces: `ShutdownState.request(signum: int) -> None`, `ShutdownState.wait(timeout_seconds: float) -> bool`, and `ShutdownState.requested: bool` backed by one `threading.Event`.
- Produces: `RuntimeMatrixDelivery.enqueue_lifecycle_notice(event: Mapping[str, Any]) -> object`; lifecycle events use the durable text outbox and wake the existing worker.
- Produces: `MatrixClient.cancel_pending() -> None`; retry backoff waits on cancellation rather than `time.sleep`.
- Produces: `_close_resources(resources: Sequence[tuple[str, object | None]], logger: StructuredLogger) -> None`, which attempts every close and logs safe errors independently.
- Configures: Compose `init: true`, `stop_signal: SIGTERM`, and `stop_grace_period: 2m`.

- [ ] **Step 1: Write failing interrupt, durability, cleanup, and Compose tests**

```python
def test_shutdown_state_wakes_wait_immediately() -> None:
    state = ShutdownState()
    started = Event()
    finished = Event()

    def wait() -> None:
        started.set()
        assert state.wait(60) is True
        finished.set()

    thread = Thread(target=wait)
    thread.start()
    assert started.wait(1)
    state.request(signal.SIGTERM)
    assert finished.wait(1)
    thread.join(1)


def test_close_resources_continues_after_first_close_failure() -> None:
    closed: list[str] = []
    _close_resources(
        (("commands", FailingClose()), ("delivery", RecordingClose(closed))),
        logger=StructuredLogger(),
    )
    assert closed == ["delivery"]
```

Add a restart test proving a queued shutdown lifecycle record survives process close and is delivered once by the next worker. Add a Docker contract asserting the rendered service has `init: true`, `stop_signal: SIGTERM`, and a 120-second grace.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_startup.py tests/test_matrix_outbox_delivery.py tests/test_docker_contract.py -q`

Expected: FAIL because waits are blocking sleeps, lifecycle sends are immediate, cleanup is not isolated, and Compose uses Docker's default stop grace.

- [ ] **Step 3: Back shutdown state with an Event and replace cadence sleeps**

```python
@dataclass
class ShutdownState:
    signum: int | None = None
    _event: threading.Event = field(default_factory=threading.Event, repr=False)

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def request(self, signum: int) -> None:
        if not self._event.is_set():
            self.signum = signum
            self._event.set()

    def wait(self, timeout_seconds: float) -> bool:
        return self._event.wait(max(0.0, timeout_seconds))
```

The signal handler calls only `state.request(signum)`. Replace successful-loop and reconnect `sleep(...)` calls with `shutdown_state.wait(...)` and branch immediately to `return_if_shutdown_requested` when it returns true. Preserve injected test pacing with a `wait` callable parameter; production passes `ShutdownState.wait`.

- [ ] **Step 4: Persist lifecycle notices before any network work**

Add `enqueue_lifecycle_notice` to `MatrixOutboxDelivery` by delegating to `enqueue_text_notice(event_type, event)`. Change the lifecycle branch in `dispatch_matrix_event` to use the same queued outcome/reason fields as other durable text notices. Retain `send_lifecycle_notice` only as a compatibility method for explicit tooling; runtime startup/shutdown must not call it.

- [ ] **Step 5: Make worker cancellation cooperative and bounded**

Add a cancellation event to `MatrixClient`; `cancel_pending()` sets it and closes the underlying client once. `_request_with_retry` checks cancellation before each attempt and replaces retry `sleep(delay)` with `cancel_event.wait(delay)`. Raise a safe `MatrixError(error_type="cancelled")` when set.

In `MatrixOutboxDelivery.close`, set worker stop, wake it, call `client.cancel_pending()`, join for `_WORKER_JOIN_TIMEOUT_SECONDS`, log `matrix-outbox-worker-cancel-timeout` if still alive, then perform idempotent client close. Never begin another phase after `_worker_stop_requested()` becomes true.

- [ ] **Step 6: Isolate all finalizers and extend container grace**

Replace sequential `_close_if_available` calls with:

```python
def _close_resources(resources: Sequence[tuple[str, object | None]], *, logger: StructuredLogger) -> None:
    for name, resource in resources:
        close = getattr(resource, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except Exception as exc:
            logger.warning("runtime-resource-close-failed", resource=name, error_type=type(exc).__name__)
```

Set Compose `init: true`, `stop_signal: SIGTERM`, and `stop_grace_period: 2m` beside `restart`. The two-minute window exceeds the 60-second stable cadence, 30-second startup timeout, and bounded Matrix cancellation/cleanup.

- [ ] **Step 7: Verify GREEN and commit**

Run: `python -m pytest tests/test_startup.py tests/test_matrix_outbox_delivery.py tests/test_docker_contract.py -q`

Expected: PASS, including signal during a 60-second wait completing within one second, durable lifecycle restart, worker cancellation, and second-resource cleanup after first-resource failure.

```bash
git add parking_spot_monitor/runtime_lifecycle.py parking_spot_monitor/capture_loop.py parking_spot_monitor/matrix_dispatch.py parking_spot_monitor/__main__.py parking_spot_monitor/matrix_client.py src/parking_monitor/matrix_outbox_delivery.py docker-compose.yml tests/test_startup.py tests/test_matrix_outbox_delivery.py tests/test_docker_contract.py
git commit -m "fix: make runtime shutdown durable and interruptible"
```

### Task 5: Bound Reconnects and Move Command Fetch off the Capture Path

**Files:**
- Create: `parking_spot_monitor/runtime_command_worker.py`
- Create: `parking_spot_monitor/runtime_log_aggregation.py`
- Modify: `parking_spot_monitor/config.py`
- Modify: `config.yaml.example`
- Modify: `parking_spot_monitor/matrix_commands.py`
- Modify: `parking_spot_monitor/runtime_commands.py`
- Modify: `parking_spot_monitor/runtime_matrix_commands.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `parking_spot_monitor/runtime_detection.py`
- Modify: `parking_spot_monitor/runtime_presence.py`
- Modify: `parking_spot_monitor/runtime_health.py`
- Modify: `parking_spot_monitor/__main__.py`
- Modify: `tests/test_config.py`
- Modify: `tests/test_runtime_matrix_commands.py`
- Modify: `tests/test_runtime_loop_health.py`
- Modify: `tests/test_logging.py`
- Modify: `tests/test_startup.py`

**Interfaces:**
- Produces: stream keys `reconnect_max_seconds: float = 60` and `reconnect_jitter_ratio: float = 0.2`.
- Produces: Matrix keys `command_request_timeout_seconds: float = 2` and `command_retry_attempts: int = 1`; existing alert `timeout_seconds` and `retry_attempts` keep their meanings.
- Produces: runtime key `log_summary_interval_seconds: float = 900`.
- Produces: `capture_reconnect_delay(failure_count, *, initial_seconds, max_seconds, jitter_ratio, random_unit) -> float`.
- Produces: `MatrixCommandService.fetch_once() -> MatrixSyncResult` (read/network only) and `apply_sync_result(result: MatrixSyncResult) -> MatrixCommandPollResult` (cursor writes, archive mutations, and replies on the capture thread).
- Produces: `MatrixCommandPollWorker.request() -> bool`, `take_completed() -> MatrixSyncResult | BaseException | None`, and `close() -> None`, with one thread and one result slot.
- Produces: immutable `RuntimeLogSummary(processed_frames: int, capture_failures: Mapping[str, int], detection_failures: Mapping[str, int], suppressed_diagnostics: Mapping[str, int])`; `RuntimeLogAggregator.record_success(kind: str) -> None`, `record_failure(kind: str, error_type: str) -> bool`, and `flush_if_due(now_monotonic: float) -> RuntimeLogSummary | None`.

- [ ] **Step 1: Add failing policy and nonblocking tests**

```python
def test_capture_reconnect_delay_exponentially_caps_with_injected_jitter() -> None:
    delays = [capture_reconnect_delay(n, initial_seconds=5, max_seconds=60, jitter_ratio=0.2, random_unit=lambda: 0.5) for n in range(1, 7)]
    assert delays == [5.5, 11.0, 22.0, 44.0, 60.0, 60.0]


def test_command_fetch_does_not_block_capture_iteration() -> None:
    release = Event()
    worker = MatrixCommandPollWorker(lambda: blocking_sync(release))
    started = monotonic()
    assert worker.request() is True
    assert monotonic() - started < 0.05
    assert worker.take_completed() is None
    release.set()
    assert wait_until(lambda: worker.take_completed() is not None, timeout=1)
    worker.close()
```

Add log tests asserting per-frame `detection-frame-processed`, repeated identical capture failures, and missed-occupied diagnostics are DEBUG after the first/transition record, while a periodic `runtime-loop-summary` INFO contains bounded counts and no candidate arrays.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_config.py tests/test_runtime_matrix_commands.py tests/test_runtime_loop_health.py tests/test_logging.py -q`

Expected: FAIL because reconnect is fixed, command polling performs synchronous sync/apply, and hot INFO records are emitted every frame.

- [ ] **Step 3: Add validated reconnect and command-client controls**

Validate finite `reconnect_max_seconds >= reconnect_seconds`, `0 <= reconnect_jitter_ratio <= 1`, positive command timeout/attempts, and positive summary interval. Copy the exact defaults into `config.yaml.example` and sanitized startup summaries.

Implement reconnect delay as `min(initial * 2 ** (failure_count - 1) * (1 + jitter_ratio * random_unit()), maximum)`; reset failure count after the first successful captured frame. Inject randomness for deterministic tests and use `ShutdownState.wait(delay)` from Task 4.

- [ ] **Step 4: Split Matrix fetch from mutation and reply**

```python
def fetch_once(self) -> MatrixSyncResult:
    cursor = self.archive.read_matrix_cursor()
    since = cursor.get("next_batch") if isinstance(cursor, Mapping) else None
    return self.client.sync(room_id=self.room_id, since=since, timeout_ms=self.sync_timeout_ms, limit=self.sync_limit)


def apply_sync_result(self, result: MatrixSyncResult) -> MatrixCommandPollResult:
    return self._apply_events_and_advance_cursor(result)
```

The worker calls only `fetch_once`; it never writes the cursor, mutates vehicle history, sends a reply, or accesses a detector. The capture loop drains at most one completed result per iteration and calls `apply_sync_result` on its own thread before requesting another fetch. A capacity-one slot rejects duplicate `request()` calls while one fetch or result is outstanding.

- [ ] **Step 5: Give the command client its short independent policy**

Construct the command-service `MatrixClient` with `command_request_timeout_seconds` and `command_retry_attempts`; alert/outbox delivery continues using `timeout_seconds` and `retry_attempts`. Close the fetch worker before closing its service/client. A failed fetch enters the existing monotonic circuit cooldown only when the completed exception is collected.

- [ ] **Step 6: Aggregate hot logging without hiding state transitions**

```python
@dataclass(slots=True)
class RuntimeLogAggregator:
    interval_seconds: float
    next_summary_at: float
    processed_frames: int = 0
    capture_failures: Counter[str] = field(default_factory=Counter)
    detection_failures: Counter[str] = field(default_factory=Counter)
    suppressed_diagnostics: Counter[str] = field(default_factory=Counter)
```

Keep startup, occupancy transitions, non-empty Matrix results, first failure in a streak, periodic failure summaries, WARNING, and ERROR at their required levels. Move routine frame completion and repeated identical failures to DEBUG. Build candidate summaries only for DEBUG or a transition. Replace per-frame missed-occupied INFO calls with counters and emit their bounded reason/class counts in `runtime-loop-summary` every 900 seconds by default.

- [ ] **Step 7: Verify nonblocking behavior and commit**

Run: `python -m pytest tests/test_config.py tests/test_runtime_matrix_commands.py tests/test_runtime_loop_health.py tests/test_logging.py tests/test_startup.py -q`

Expected: PASS, including one fetch thread, one result slot, main-thread mutations, two-second/one-attempt command policy, exponential cap/reset, and periodic bounded INFO output.

```bash
git add parking_spot_monitor/runtime_command_worker.py parking_spot_monitor/runtime_log_aggregation.py parking_spot_monitor/config.py config.yaml.example parking_spot_monitor/matrix_commands.py parking_spot_monitor/runtime_commands.py parking_spot_monitor/runtime_matrix_commands.py parking_spot_monitor/capture_loop.py parking_spot_monitor/runtime_detection.py parking_spot_monitor/runtime_presence.py parking_spot_monitor/runtime_health.py parking_spot_monitor/__main__.py tests/test_config.py tests/test_runtime_matrix_commands.py tests/test_runtime_loop_health.py tests/test_logging.py tests/test_startup.py
git commit -m "perf: bound reconnect command and logging work"
```

### Task 6: Decompose the Outbox and Persist Retry Scheduling

**Files:**
- Create: `src/parking_monitor/outbox_models.py`
- Create: `src/parking_monitor/outbox_storage.py`
- Modify: `src/parking_monitor/outbox.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`
- Modify: `parking_spot_monitor/config.py`
- Modify: `config.yaml.example`
- Modify: `tests/test_outbox_persistence.py`
- Modify: `tests/test_matrix_outbox_delivery.py`
- Modify: `tests/test_module_decomposition.py`

**Interfaces:**
- Produces optional schema-v1 `OutboxRecord.retry_attempt_count: int = 0` and `retry_due_at: str | None = None`.
- Produces: `OutboxRetryPolicy(initial_seconds: float, max_seconds: float, jitter_ratio: float)`, immutable `RetrySchedule(due_at: str, attempt_count: int, reason: str)`, and Matrix key `outbox_retry_max_seconds: float = 900`.
- Produces: `LocalOutbox.mark_retrying(record_id, *, reason, retry_due_at, retry_attempt_count)`, `next_due_record(now: datetime) -> OutboxRecord | None`, `revision: int`, and revision-cached `compact_status_summary()`.
- Produces: one atomic `apply_phase_result(record_id: str, phase: MatrixPhase, *, delivered_result: PhaseResult | None = None, retry: RetrySchedule | None = None, terminal_reason: str | None = None) -> OutboxRecord` mutation for delivered/retryable/terminal phase outcomes; validation requires exactly one outcome.
- Preserves: imports from `parking_monitor.outbox`; schema version remains 1; legacy records without new fields load with immediate first-retry eligibility; atomic replace and directory sync remain mandatory.
- Accepts: the schema-compatible JSON store must publish the complete document once after every durable text/upload/image phase boundary. Network I/O cannot be grouped across those writes without weakening restart semantics. Removing that bounded full-file amplification requires a separate storage migration, which this plan explicitly does not add.

- [ ] **Step 1: Add failing legacy, restart, cache, and write-count tests**

```python
def test_legacy_record_without_retry_fields_loads_with_safe_defaults(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    path.write_text(json.dumps(legacy_schema_v1_payload()), encoding="utf-8")
    record = LocalOutbox(path).list_records()[0]
    assert record.retry_attempt_count == 0
    assert record.retry_due_at is None


def test_retry_due_and_exponential_count_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "matrix-outbox.json"
    outbox = LocalOutbox(path)
    record = outbox.enqueue(alert_intent())
    outbox.mark_retrying(record.id, reason="timeout", retry_due_at="2026-07-30T12:05:00Z", retry_attempt_count=3)
    reloaded = LocalOutbox(path).list_records()[0]
    assert reloaded.retry_attempt_count == 3
    assert reloaded.retry_due_at == "2026-07-30T12:05:00Z"
```

Add tests that two compact summary calls at one revision scan records once and mutation invalidates the cache. Instrument `_persist_records` across a text/upload/image delivery and assert exactly one complete atomic JSON publication after each successfully completed durable phase. For one retryable phase failure, assert exactly one publication records the retry outcome for that phase. Add a baseline characterization that fails only if a single logical phase mutation currently publishes more than once; do not invent a coalescing change when the baseline is already one.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_outbox_persistence.py tests/test_matrix_outbox_delivery.py tests/test_module_decomposition.py -q`

Expected: FAIL because retry timing is worker-local, summaries rescan, retry outcomes are not expressed through one transactional mutation, and `outbox.py` is 957 lines. The phase-boundary write-count characterization may already pass; retain it as a durability guard.

- [ ] **Step 3: Move values and storage behind the existing facade**

Move JSON types, `AlertIntent`, `OutboxRecord`, `OutboxRetentionPolicy`, sanitization, and parse helpers to `outbox_models.py`. Move recovery result types, load/persist/quarantine/fsync/retention mechanics to `outbox_storage.py`. Keep `LocalOutbox`, stable ID helpers, exception exports, and explicit re-exports in `outbox.py` so existing imports do not change.

Enforce line caps in `test_module_decomposition.py`: each new module at most 500 lines and `outbox.py` at most 450 lines.

- [ ] **Step 4: Parse optional retry fields safely in schema version 1**

```python
retry_attempt_count=_optional_non_negative_int(payload, "retry_attempt_count", default=0),
retry_due_at=_optional_utc_timestamp(payload, "retry_due_at"),
```

Serialize the fields only when non-default, so untouched records remain byte-compatible in shape. Reject negative counts, non-UTC timestamps, non-finite policy values, and due timestamps on terminal records. Quarantine malformed records with safe reason codes exactly like current malformed phase data.

- [ ] **Step 5: Persist per-record exponential due times**

On retryable failure compute `attempt = current.retry_attempt_count + 1`, delay `min(initial * 2 ** (attempt - 1), maximum) * (1 + jitter_ratio * random_unit())`, and UTC `retry_due_at` using injected clocks. Pending records are immediately eligible; retrying records are eligible only at/after their due time. The worker waits until the earliest persisted due time and no longer maintains one global `retry_deadline`.

- [ ] **Step 6: Remove only proven duplicate mutations and cache summaries**

`apply_phase_result` accepts one phase plus exactly one of delivered result, retry schedule, or terminal reason; it computes the record/phase state and publishes the complete JSON document exactly once for that durable phase result. If the write-count baseline proves duplicate or no-op mutations inside one phase, collapse only those duplicates. Do not combine persistence across text, upload, image, or intervening network calls. Increment `_revision` and rebuild the ID index only after successful persistence. Cache compact summaries as `(revision, immutable_summary)` and return a fresh JSON-safe mapping so callers cannot mutate the cache.

- [ ] **Step 7: Prove durability and migration safety**

Run: `python -m pytest tests/test_outbox_persistence.py tests/test_matrix_outbox_delivery.py tests/test_module_decomposition.py -q`

Expected: PASS for legacy fixtures, retry restart/cap/jitter, per-record scheduling, phase idempotency, upload-before-image durability, corrupt quarantine, atomic failure reconciliation, cached summaries, and exactly one full atomic publication per durable phase outcome. Record that remaining bounded phase-boundary write amplification is intentionally unchanged and why in Task 11.

- [ ] **Step 8: Commit outbox decomposition**

```bash
git add src/parking_monitor/outbox_models.py src/parking_monitor/outbox_storage.py src/parking_monitor/outbox.py src/parking_monitor/matrix_outbox_delivery.py parking_spot_monitor/config.py config.yaml.example tests/test_outbox_persistence.py tests/test_matrix_outbox_delivery.py tests/test_module_decomposition.py
git commit -m "perf: persist bounded outbox retry scheduling"
```

### Task 7: Add a Service-Scoped Decision Store and Correction Event-ID Cache

**Files:**
- Create: `parking_spot_monitor/decision_memory_store.py`
- Modify: `parking_spot_monitor/operator_decision_memory.py`
- Modify: `parking_spot_monitor/operator_feedback.py`
- Modify: `parking_spot_monitor/runtime_decision_memory.py`
- Modify: `parking_spot_monitor/matrix_dispatch.py`
- Modify: `parking_spot_monitor/runtime_state_update.py`
- Modify: `parking_spot_monitor/runtime_lifecycle.py`
- Modify: `parking_spot_monitor/capture_loop.py`
- Modify: `parking_spot_monitor/__main__.py`
- Modify: `parking_spot_monitor/vehicle_history_models.py`
- Modify: `parking_spot_monitor/vehicle_history_correction_cache.py`
- Modify: `parking_spot_monitor/vehicle_history_corrections.py`
- Modify: `parking_spot_monitor/matrix_command_runtime.py`
- Modify: `parking_spot_monitor/config.py`
- Modify: `config.yaml.example`
- Modify: `tests/test_operator_decision_memory.py`
- Modify: `tests/test_vehicle_history.py`
- Modify: `tests/test_matrix.py`
- Modify: `tests/test_startup.py`

**Interfaces:**
- Produces: runtime key `decision_memory_checkpoint_interval_seconds: float = 300`.
- Produces: runtime key `decision_memory_checkpoint_max_pending_records: int = 50`.
- Produces: `DecisionMemoryDurability = Literal["routine", "immediate"]` and `DecisionMemoryStore(path, *, checkpoint_interval_seconds, checkpoint_max_pending_records, max_records=MAX_RECORDS, monotonic, logger)` with `append(record: DecisionMemoryRecord, *, durability: DecisionMemoryDurability) -> bool`, `extend(records: Sequence[DecisionMemoryRecord], *, durability: DecisionMemoryDurability) -> bool`, `checkpoint_if_due() -> bool`, `flush() -> bool`, `close() -> bool`, and `records: tuple[DecisionMemoryRecord, ...]`. Boolean results report whether requested persistence succeeded; failures retain dirty state and are logged safely.
- Preserves: public path-based `append_decision_memory_record(s)`, `load_decision_memory`, formatting functions, schema version 1, quarantine, field bounds, and atomic writes.
- Extends: `CorrectionReplayState.matrix_event_ids: frozenset[str]`; `VehicleHistoryCorrectionMixin.correction_event_seen(event_id: str) -> bool`; `MatrixCommandArchive.correction_event_seen` replaces `load_corrections` in the command dedupe path.
- Guarantees: occupancy transitions, Matrix alert/command outcomes, operator feedback/corrections, and startup/shutdown lifecycle records request immediate atomic flush. Only routine per-frame detection/state diagnostics may wait for checkpoint. After a successful prior flush, crash loss is bounded to fewer than 50 routine records and less than 300 seconds of routine records under the capture loop's per-iteration checkpoint call; immediate records have zero acknowledged crash-loss window.

- [ ] **Step 1: Add failing durability-tier, crash-bound, and correction-cache tests**

```python
def test_routine_decision_records_batch_until_count_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    writes = 0
    monkeypatch.setattr(operator_decision_memory, "_write_memory", counted_writer(lambda: writes))
    store = DecisionMemoryStore(
        tmp_path / "operator-decision-memory.json",
        checkpoint_interval_seconds=300,
        checkpoint_max_pending_records=3,
        monotonic=lambda: 0,
    )
    store.extend((record("first"), record("second")), durability="routine")
    assert writes == 0
    store.append(record("third"), durability="routine")
    assert writes == 1


def test_immediate_transition_flushes_prior_routine_records(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    writes = count_memory_writes(monkeypatch)
    store = decision_store(tmp_path, checkpoint_interval_seconds=300, checkpoint_max_pending_records=50)
    store.append(record("frame-diagnostic"), durability="routine")
    assert writes() == 0
    assert store.append(record("occupancy-transition"), durability="immediate") is True
    assert writes() == 1
    assert [item.kind for item in reloaded_records(tmp_path)] == ["frame-diagnostic", "occupancy-transition"]


def test_correction_event_seen_reuses_replay_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    append_known_correction(archive, matrix_event_id="$event")
    loads = count_correction_file_loads(monkeypatch)
    assert archive.correction_event_seen("$event") is True
    assert archive.correction_event_seen("$event") is True
    assert loads() == 1
```

Add store tests for external replacement reconciliation before flush, failed immediate write retaining dirty state and returning `False`, corrupt startup quarantine, max-record truncation, time-due checkpoint, count-due checkpoint at exactly the configured bound, close flush, and default validation rejecting non-positive time/count limits. Add routing tests proving transition, alert, correction, command outcome, and lifecycle records are immediately reloadable, while a non-transition frame diagnostic remains unwritten until time or count is due. Simulate a crash by constructing a second reader without calling `close()` and assert at most `checkpoint_max_pending_records - 1` routine records are absent and no successfully acknowledged immediate record is absent.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_operator_decision_memory.py tests/test_vehicle_history.py tests/test_matrix.py tests/test_startup.py -q`

Expected: FAIL because each append reloads/rewrites the entire document, runtime call sites have no durability classification or bounded pending count, and command dedupe scans all corrections.

- [ ] **Step 3: Implement one loaded bounded store**

Load and sanitize once in `DecisionMemoryStore.__init__`, keep a `deque(maxlen=max_records)` under the existing memory lock, and mark dirty on append. Track the number of routine records added since the last successful write. An immediate append/extend flushes the complete dirty snapshot before returning; routine append/extend flushes when the pending-record bound is reached. Before any flush, compare `(mtime_ns, size)` with the last successful signature; if externally replaced, load that file and merge its tail before local dirty records using stable record JSON as the dedupe key. On write failure keep `_dirty=True`, retain the pending count, return `False`, and update neither signature nor checkpoint deadline.

```python
def checkpoint_if_due(self) -> bool:
    if not self._dirty or self._monotonic() < self._next_checkpoint_at:
        return False
    return self.flush()
```

- [ ] **Step 4: Route runtime writes by durability through the service owner**

Construct one store in `_main`, pass it through capture, state update, Matrix dispatch, operator feedback, and lifecycle calls, call `checkpoint_if_due()` once per successful or failed iteration, and close it through Task 4 cleanup isolation. Split `build_runtime_state_memory_records(...)` output at the existing `previous_status != new_status` boundary: append transition records with `durability="immediate"` and detection/no-transition diagnostics with `durability="routine"`. Matrix alert and command outcome recording, correction/feedback recording, and lifecycle recording use `durability="immediate"`. Keep path functions for CLI/tests and external compatibility; runtime call sites must not bypass the store.

If an immediate write returns `False`, emit the existing redacted persistence diagnostic and keep the service running with dirty state so the next checkpoint/close can retry. Never log or acknowledge an immediate record as durable when its write failed.

- [ ] **Step 5: Fold event IDs into the existing correction replay cache**

While building `CorrectionReplayState`, collect non-empty `event.matrix_event_id` values into a `frozenset`. Implement `correction_event_seen` as a constant-time lookup on `self.correction_replay_state()`. Do not create a second correction cache; existing revision and file-signature invalidation remains authoritative.

Change `MatrixCommandRuntime.correction_already_seen` to `return self.archive.correction_event_seen(event_id)` and narrow `MatrixCommandArchive` accordingly.

- [ ] **Step 6: Verify reduced writes and unchanged schemas**

Run: `python -m pytest tests/test_operator_decision_memory.py tests/test_vehicle_history.py tests/test_matrix.py tests/test_startup.py -q`

Expected: PASS, with zero writes for routine non-transition frames before the time/count bound, immediate reloadability for transitions/alerts/corrections/lifecycle decisions, the documented crash-loss bound, one close flush when dirty, schema-v1 reload, external replacement reconciliation, and one correction replay load for repeated event checks.

- [ ] **Step 7: Commit service-scoped memory**

```bash
git add parking_spot_monitor/decision_memory_store.py parking_spot_monitor/operator_decision_memory.py parking_spot_monitor/operator_feedback.py parking_spot_monitor/runtime_decision_memory.py parking_spot_monitor/matrix_dispatch.py parking_spot_monitor/runtime_state_update.py parking_spot_monitor/runtime_lifecycle.py parking_spot_monitor/capture_loop.py parking_spot_monitor/__main__.py parking_spot_monitor/vehicle_history_models.py parking_spot_monitor/vehicle_history_correction_cache.py parking_spot_monitor/vehicle_history_corrections.py parking_spot_monitor/matrix_command_runtime.py parking_spot_monitor/config.py config.yaml.example tests/test_operator_decision_memory.py tests/test_vehicle_history.py tests/test_matrix.py tests/test_startup.py
git commit -m "perf: checkpoint service decision memory"
```

### Task 8: Reuse Loaded Profile Records and Preserve the Existing Health Cache

**Files:**
- Modify: `parking_spot_monitor/vehicle_history_corrections.py`
- Modify: `parking_spot_monitor/vehicle_history_profiles.py`
- Modify: `parking_spot_monitor/runtime_health_cache.py` only if its bounded TTL behavior test exposes a defect
- Modify: `tests/test_vehicle_history.py`
- Modify: `tests/test_runtime_health_cache.py`

**Interfaces:**
- Produces: private `_estimate_for_profile_records(profile_id: str | None, records: Sequence[SessionRecord], *, state: CorrectionReplayState, min_samples: int, min_profile_confidence: float) -> VehicleHistoryEstimate` so `profile_summary` reuses its already-loaded effective closed records.
- Preserves: public `estimate_for_profile` and `profile_summary` signatures, archive JSON schemas, streamed `health_snapshot()`, and the existing `VehicleHistoryHealthSnapshotCache` revision/TTL contract.
- Deliberately omits: `vehicle_history_index.py`, mutation summaries, and a `recent_sessions` API because recon found no current archive-recent consumer. Full reconciliation and analytics directory scans remain bounded, command/on-demand work and intentionally unchanged unless a failing characterization identifies a defect at an existing cache boundary.

- [ ] **Step 1: Add a failing single-scan test and characterize existing health caching**

```python
def test_profile_summary_scans_closed_sessions_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive = seeded_profile_archive(tmp_path)
    loads = count_calls(monkeypatch, archive, "list_closed_sessions")
    summary = archive.profile_summary("prof_a")
    assert summary["profile_id"] == "prof_a"
    assert loads() == 1
```

Add output-equivalence coverage for the estimate fields before/after reuse. In `tests/test_runtime_health_cache.py`, characterize the current contract: repeated reads at one revision and within TTL call streamed `health_snapshot()` once; an in-process revision increment invalidates immediately; an external source-file change may remain cached only until TTL; and the first read at/after TTL refreshes it. Do not add a recent-session test or consumer.

- [ ] **Step 2: Run focused tests and verify the exact RED**

Run: `python -m pytest tests/test_vehicle_history.py tests/test_runtime_health_cache.py -q`

Expected: the profile-summary scan-count test FAILS because `profile_summary` loads closed sessions and then `estimate_for_profile` loads them again. Existing health cache tests should pass; if external change remains stale beyond TTL, treat only that failure as authorization to repair the current cache.

- [ ] **Step 3: Reuse the already-loaded effective closed records**

Extract the exact private helper:

```python
def _estimate_for_profile_records(
    self,
    profile_id: str | None,
    records: Sequence[SessionRecord],
    *,
    state: CorrectionReplayState,
    min_samples: int,
    min_profile_confidence: float,
) -> VehicleHistoryEstimate:
    canonical = self.resolve_profile_id(profile_id, merges=state.merges)
    return estimate_vehicle_history(
        canonical,
        records,
        min_samples=min_samples,
        min_profile_confidence=min_profile_confidence,
    )
```

Public `estimate_for_profile` keeps loading/effectively filtering closed sessions and delegates to the helper. `profile_summary` passes its existing `closed` list and already-loaded replay `state`, eliminating only the confirmed duplicate scan without changing wrong-match filtering or estimate semantics.

- [ ] **Step 4: Keep health work inside the existing boundary**

Retain `VehicleHistoryHealthSnapshotCache` and streamed `VehicleHistoryArchive.health_snapshot()` as-is when the characterization passes. If the TTL boundary test fails, make the smallest correction to comparison/invalidation in `runtime_health_cache.py`; do not cache session records or add another owner. Document full health reconciliation and on-demand analytics scans as bounded/intentionally unchanged because there is no hot recent-session consumer and a new index would add reconciliation complexity without measured benefit.

- [ ] **Step 5: Verify focused behavior and commit**

Run: `python -m pytest tests/test_vehicle_history.py tests/test_runtime_health_cache.py -q`

Expected: PASS with one closed-session scan for `profile_summary`, identical estimate fields, revision invalidation, TTL-bounded external staleness, and unchanged streamed health fields.

```bash
git add parking_spot_monitor/vehicle_history_corrections.py parking_spot_monitor/vehicle_history_profiles.py tests/test_vehicle_history.py tests/test_runtime_health_cache.py
git add parking_spot_monitor/runtime_health_cache.py  # only when the failing TTL characterization required a repair
git commit -m "perf: reuse loaded profile history"
```

### Task 9: Establish One Canonical Full JPEG and Persist Upload Derivatives

**Files:**
- Create: `parking_spot_monitor/jpeg_artifacts.py`
- Modify: `parking_spot_monitor/vehicle_history_images.py`
- Modify: `parking_spot_monitor/vehicle_history_sessions.py`
- Modify: `parking_spot_monitor/matrix_snapshots.py`
- Modify: `parking_spot_monitor/operator_cockpit_snapshots.py`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py`
- Modify: `parking_spot_monitor/vehicle_history_maintenance_utils.py`
- Modify: `tests/test_vehicle_history.py`
- Modify: `tests/test_matrix_outbox_delivery.py`
- Modify: `tests/test_image_budget.py`
- Modify: `tests/test_operator_cockpit.py`

**Interfaces:**
- Produces: `JpegPublication(path: Path, strategy: Literal["reflink", "copy"], identity: FileIdentity)` and `publish_canonical_jpeg(source, destination) -> JpegPublication`. The `(dev, ino)` identity is an ownership token: immediate consumers must bind an `O_NOFOLLOW` descriptor to it, and failure cleanup may remove only that exact published inode.
- Preserves a 32 MiB `MAX_CANONICAL_JPEG_BYTES` ceiling: source size is checked before hashing or destination creation, bounded copy writes exactly the preflight size and probes once for growth, and reflink/copy temporaries must retain that size and validated source evidence.
- Produces: `JpegDecodeError(code: Literal["unidentified", "decompression_bomb", "invalid_dimensions", "read_failed"])` and `open_decoded_rgb_jpeg(path: Path, *, initial_max_dimension: int) -> ContextManager[DecodedRgbJpeg]`, where `DecodedRgbJpeg(image: Image.Image, source_width: int, source_height: int)` owns bounded draft, decode, RGB conversion, and deterministic closure.
- Produces: immutable `MatrixUploadDerivative(path: Path, info: Mapping[str, int | str])` and `prepare_upload_derivative(snapshot: MatrixSnapshot, *, destination: Path) -> MatrixUploadDerivative`.
- Adds outbox intent metadata `upload_derivative_path` and `upload_derivative_info`; both are optional, sanitized schema-v1 metadata.
- Preserves: `SessionRecord.occupied_snapshot_path` as the canonical archive full JPEG and `occupied_crop_path` as its crop; no session schema change.

- [ ] **Step 1: Add failing link fallback and restart-reuse tests**

```python
def test_canonical_jpeg_prefers_reflink_without_reencoding(tmp_path: Path) -> None:
    source = valid_jpeg(tmp_path / "latest.jpg")
    publication = publish_canonical_jpeg(source, tmp_path / "archive" / "full.jpg")
    assert publication.strategy == "reflink"
    assert publication.path.read_bytes() == source.read_bytes()
    assert publication.path.stat().st_ino != source.stat().st_ino


def test_upload_retry_reuses_persisted_derivative_after_restart(tmp_path: Path) -> None:
    first = delivery_with_upload_timeout(tmp_path)
    record = first.enqueue_open_spot_alert(open_event(large_jpeg(tmp_path)))
    first.drain_outbox(record_id=record.id)
    persisted = LocalOutbox(tmp_path / "matrix-outbox.json").list_records()[0]
    derivative = Path(str(persisted.intent.metadata["upload_derivative_path"]))
    before = derivative.read_bytes()
    delivered = restarted_delivery(tmp_path).drain_outbox(record_id=record.id)
    assert delivered.delivered_count == 1
    assert derivative.read_bytes() == before
```

Add unsupported-reflink tests proving fallback order reflink -> bounded copy and temp cleanup after failure. Prove the returned destination remains unchanged when the writable source is later modified.

Add a source-mode regression: create a source with mode `0o600`, force the independent reflink path, and assert both source and publication remain `0o600`; monkeypatch `os.chmod`/`os.fchmod` to fail if called for the source inode. Add shared-decoder tests proving both `matrix_snapshots.py` and `operator_cockpit_snapshots.py` use `open_decoded_rgb_jpeg`, draft before load, close the opened and converted images on success/failure, and preserve caller-specific mapping: Matrix raises `MatrixError(error_type="snapshot_resize_failed")`, while operator Who returns `LatestSnapshotValidation(state="error", error_type="resize failed")` and its redacted warning.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_vehicle_history.py tests/test_matrix_outbox_delivery.py tests/test_image_budget.py tests/test_operator_cockpit.py -q`

Expected: FAIL because full frames are decoded/re-encoded and upload derivatives exist only in memory.

- [ ] **Step 3: Publish the validated source JPEG without re-encoding**

Open the source once, validate JPEG format/dimensions from that descriptor, and bind stable dev/inode/size/mtime/ctime plus SHA-256 evidence to publication. Create a temporary destination in the target directory, try Linux `FICLONE` via `fcntl.ioctl`, and otherwise stream a descriptor-bound copy in 1 MiB chunks. Validate the independent temporary artifact against the digest before and after file `fsync`, atomically replace the destination, and fsync its directory. Preserve source mode on the independently owned reflink/copy destination. Generic mutable sources must never use hardlinks because later source writes would mutate a successfully returned publication.

`capture_occupied_images` publishes the full frame with this helper, then opens the canonical path once through its returned identity to create only the crop. A pathname or parent-root replacement must fail without decoding or deleting the replacement.

Full-frame publication, identity-bound decode, and crop staging/commit retain archive, images, full-frame, and crop directory descriptors for the whole transaction. Failure cleanup operates only through those held descriptors. Identity cleanup first rejects stable non-regular or mismatched targets, then uses a random same-directory quarantine to close the check/unlink race. A raced mismatch is restored with atomic no-clobber hardlink/unlink semantics; if a new original-name blocker prevents immediate restore, both unrelated files are preserved. The next cleanup attempt performs an exact-name recovery scan capped at 256 directory entries before its ownership check, and `recover_quarantined_path()` exposes the same bounded sweep explicitly.

- [ ] **Step 4: Share one full JPEG decode lifecycle**

Move the common `Image.open` -> positive dimension validation -> bounded-size calculation -> `draft("RGB", bounded_size)` -> `load()` -> conditional `convert("RGB")` sequence into `open_decoded_rgb_jpeg`. Its context manager closes the converted image when distinct and always closes the opened image. Use it in both `_resize_jpeg_for_matrix_upload_result` and `_resize_who_snapshot_for_matrix`; callers retain their own byte budgets, encoders, logs, return types, and error translation. Do not catch `MatrixError` inside the canonical helper.

- [ ] **Step 5: Persist the exact bytes selected for Matrix upload**

After bounded JPEG selection, atomically write the derivative beside retained outbox snapshots and persist its path plus immutable width/height/size/mimetype in intent metadata before network upload. `_upload_phase` reads and validates that derivative on retry; it regenerates only when legacy metadata lacks it. Protect derivative paths during retention while their records are pending/retrying and delete them with terminal retained artifacts.

- [ ] **Step 6: Verify unchanged image semantics and commit**

Run: `python -m pytest tests/test_vehicle_history.py tests/test_matrix_outbox_delivery.py tests/test_image_budget.py tests/test_operator_cockpit.py -q`

Expected: PASS with no full-frame encode, correct crop pixels, restart derivative reuse, retained-file protection, bounded fallback copies, and prior upload info/result schema.

```bash
git add parking_spot_monitor/jpeg_artifacts.py parking_spot_monitor/vehicle_history_images.py parking_spot_monitor/vehicle_history_sessions.py parking_spot_monitor/matrix_snapshots.py parking_spot_monitor/operator_cockpit_snapshots.py src/parking_monitor/matrix_outbox_delivery.py parking_spot_monitor/vehicle_history_maintenance_utils.py tests/test_vehicle_history.py tests/test_matrix_outbox_delivery.py tests/test_image_budget.py tests/test_operator_cockpit.py
git commit -m "perf: reuse canonical jpeg artifacts"
```

### Task 10: Isolate Ultralytics State and Run a Benchmark-Only Backend Spike

**Files:**
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `parking_spot_monitor/__main__.py`
- Create: `scripts/benchmark_detector_backends.py`
- Create: `tests/test_detector_backend_benchmark.py`
- Modify: `tests/test_docker_contract.py`
- Modify: `tests/test_startup.py`
- Modify: `docs/deployment.md`

**Interfaces:**
- Configures: `YOLO_CONFIG_DIR=/data/ultralytics`; startup creates it with mode `0750` before the first Ultralytics import.
- Produces offline command: `python scripts/benchmark_detector_backends.py --manifest PATH --pt-model PATH --onnx-model PATH --torchscript-model PATH --output PATH --warmup 3 --iterations 20`.
- Produces JSON evidence with backend, load seconds, per-frame p50/p95 seconds, peak RSS bytes, detection count/class parity, maximum confidence delta, and minimum bbox IoU.
- Does not modify: `DetectionConfig.model`, `_default_detector_factory`, Compose command, production detector backend, or runtime fallback behavior.

- [ ] **Step 1: Add failing container-state and benchmark-contract tests**

```python
def test_compose_routes_ultralytics_config_to_writable_data() -> None:
    service = rendered_compose_service()
    assert service["environment"]["YOLO_CONFIG_DIR"] == "/data/ultralytics"


def test_backend_benchmark_marks_accuracy_mismatch_ineligible(tmp_path: Path) -> None:
    report = run_fake_benchmark(tmp_path, onnx_detections=[different_box()])
    assert report["backends"]["onnx"]["parity_passed"] is False
    assert report["production_switch_eligible"] is False
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python -m pytest tests/test_detector_backend_benchmark.py tests/test_docker_contract.py tests/test_startup.py -q`

Expected: FAIL because the config path and benchmark script do not exist.

- [ ] **Step 3: Route Ultralytics settings to the data mount**

Set `ENV YOLO_CONFIG_DIR=/data/ultralytics` in detector runtime stages and the same explicit Compose environment value. Create the directory after runtime paths are validated and before constructing `SharedLazyDetector`. Add a startup test that imports a fake Ultralytics module and proves no `$HOME/.config/Ultralytics/settings.json` is created.

- [ ] **Step 4: Implement a strict offline benchmark harness**

Run each backend in its own spawned subprocess so `resource.getrusage` peak RSS is attributable to that backend. In each subprocess, load every manifest frame serially, warm three times, time twenty iterations, and return only JSON-safe normalized results and metrics. Normalize through `_normalize_ultralytics_results`. Compare every alternative to `.pt`: exact frame count, exact ordered class names/count, minimum bbox IoU `0.99`, maximum confidence absolute delta `0.02`, and no new/omitted detection.

Set `production_switch_eligible` only when all parity gates pass and at least one resource gate improves by 15% (`p95_seconds` or `peak_rss_bytes`). The script exits 0 for a completed honest benchmark even when ineligible, and exits 2 for missing models/manifest or malformed evidence.

- [ ] **Step 5: Document the no-switch gate and serial execution**

Document export/staging commands, benchmark invocation, evidence location, and the rule that a backend change requires a separate design/review after parity. State explicitly that heavy backend runs are serial and `pytest-xdist` is not used because peak host resource use is the priority.

- [ ] **Step 6: Verify and commit the spike**

Run: `python -m pytest tests/test_detector_backend_benchmark.py tests/test_docker_contract.py tests/test_startup.py -q`

Expected: PASS without importing Torch/Ultralytics in the standard test process.

```bash
git add Dockerfile docker-compose.yml parking_spot_monitor/__main__.py scripts/benchmark_detector_backends.py tests/test_detector_backend_benchmark.py tests/test_docker_contract.py tests/test_startup.py docs/deployment.md
git commit -m "chore: isolate and benchmark detector backends"
```

### Task 11: Re-Audit, Redeploy, Measure, and Document Acceptance

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/deployment.md`
- Create: `docs/final-audit-remediation-report.md`
- Modify: focused files only if this task's verification exposes a confirmed defect; use a separate fix commit before the documentation commit.

**Interfaces:**
- Consumes: all Task 1-10 focused tests, healthcheck CLI, Compose service, existing redaction-safe deployment workflow, and Docker resource evidence.
- Produces: a checked audit matrix mapping every residual finding to code/tests; before/after CPU, RSS, threads, block writes, capture duration, command-fetch duration, INFO/WARNING/ERROR counts, outbox writes/retry latency, decision-memory writes, JPEG encodes, and shutdown latency.
- Acceptance: no missed occupancy transitions, no schema-version changes, no secret-bearing evidence, no increase in peak RSS, and measurable improvement in at least steady-state block writes, INFO volume, or Matrix-outage capture duration.

- [ ] **Step 1: Run all focused suites serially**

```bash
python -m pytest \
  tests/test_module_decomposition.py \
  tests/test_dependency_lock_contract.py \
  tests/test_dependency_lock_generation.py \
  tests/test_dependency_lock_validation.py \
  tests/test_detector_adapter.py \
  tests/test_owner_vehicles.py \
  tests/test_runtime_owner_vehicle_cache.py \
  tests/test_matrix_outbox_delivery.py \
  tests/test_outbox_persistence.py \
  tests/test_runtime_matrix_commands.py \
  tests/test_operator_decision_memory.py \
  tests/test_vehicle_history.py \
  tests/test_image_budget.py \
  tests/test_operator_cockpit.py \
  tests/test_detector_backend_benchmark.py \
  tests/test_docker_contract.py -q
```

Expected: PASS. Do not add `-n` or install `pytest-xdist`.

- [ ] **Step 2: Run complete static and test verification**

```bash
python -m compileall -q parking_spot_monitor src scripts tests
python -m pytest -q
python scripts/lock_dependencies.py --check
docker compose config --no-interpolate >/tmp/parking-spot-monitor-compose.yaml
docker compose build parking-spot-monitor
```

Expected: every command exits 0; the serial pytest suite passes; locks are current; Compose renders; the image builds from hashes.

- [ ] **Step 3: Run container smoke and graceful-stop proof**

```bash
docker compose run --rm parking-spot-monitor python -m parking_spot_monitor --config /config/config.yaml --validate-config
docker compose up -d --no-build --force-recreate parking-spot-monitor
docker compose exec -T parking-spot-monitor python -m parking_spot_monitor.healthcheck --health-file /data/health.json --max-age-seconds 120
time docker compose stop parking-spot-monitor
docker compose up -d --no-build parking-spot-monitor
```

Expected: validation and healthcheck exit 0; stop completes without SIGKILL/corruption; restart drains the durable shutdown lifecycle record exactly once; service returns healthy.

- [ ] **Step 4: Capture comparable resource evidence**

Use the same healthy and Matrix-failure observation windows as the existing deployment baseline. Record commands and redaction-safe output in `docs/final-audit-remediation-report.md`:

```bash
docker stats --no-stream "$(docker compose ps -q parking-spot-monitor)"
docker compose top parking-spot-monitor
docker compose logs --since 30m parking-spot-monitor | awk -F'"level":"' 'NF>1 {split($2,a,"\""); count[a[1]]++} END {for (k in count) print k, count[k]}'
docker compose exec -T parking-spot-monitor sh -c 'wc -c /data/matrix-outbox.json /data/operator-decision-memory.json /data/health.json; find /data -xdev -type f | wc -l'
```

Compare against the current baseline for CPU, RSS, threads, block writes, iteration duration, command outage duration, log counts, retry latency, decision writes, JPEG encode attempts, and shutdown seconds. Report workload differences instead of claiming unsupported precision.

- [ ] **Step 5: Re-run the audit checklist and document every disposition**

The report table must contain the eleven task groups, exact commit SHA, focused tests, measured outcome, schema/compatibility result, and either `fixed`, `benchmark-only`, or `intentionally unchanged` with rationale. Explicitly classify complete atomic outbox JSON publication after each durable network phase as bounded/intentionally unchanged because preserving schema-v1 restart semantics precludes eliminating it without a storage migration. Also classify streamed full archive reconciliation and on-demand analytics directory scans as bounded/intentionally unchanged because there is no current recent-session hot-path consumer and the existing health cache already supplies revision/TTL reuse. Confirm detector backend remains `.pt` unless Task 10 parity/resource gates passed and a separate approved production-switch change exists.

- [ ] **Step 6: Update operator documentation**

Document new timing keys/defaults, durable lifecycle behavior, `stop_grace_period`, command-fetch lag expectations, retry due persistence, decision checkpoint time/count bounds, immediate durability for transition/alert/correction/lifecycle decisions, the routine-record crash-loss bound, existing health-cache TTL behavior, canonical JPEG/derivative retention, `YOLO_CONFIG_DIR`, benchmark gate, serial verification, backup, upgrade, observation, and rollback. Update log guidance from per-frame INFO expectations to periodic aggregate summaries.

- [ ] **Step 7: Commit verification documentation**

```bash
git add README.md CHANGELOG.md docs/deployment.md docs/final-audit-remediation-report.md
git commit -m "docs: verify final audit remediation"
```

## Final Self-Review Checklist

- [ ] Every compatibility-contract bullet in the approved design is mapped to a global constraint or a named test.
- [ ] All eleven requested residual groups have one independently reviewable task and focused commit.
- [ ] Outbox optional fields remain schema version 1 and legacy-absence tests exist.
- [ ] Decision memory and vehicle-history JSON shapes remain unchanged.
- [ ] No archive index or recent-session API is added; the confirmed duplicate profile-summary scan is removed and existing health streaming/revision/TTL caching remains authoritative.
- [ ] Complete atomic outbox JSON publication remains exactly once per durable network phase, with no journal/database migration and no claim that bounded full-file phase writes were eliminated.
- [ ] Decision-memory transition, alert, correction, command-outcome, and lifecycle records flush immediately; only routine diagnostics use the documented time/count crash-loss bound.
- [ ] The canonical JPEG decoder is shared by Matrix and operator Who callers, and canonical publication returns an independently owned reflink/copy whose bytes cannot change through the writable source path.
- [ ] Matrix command fetching is background-only; cursor writes, archive mutation, detector work, and replies stay on the capture thread.
- [ ] The detector backend is shared/lazy, and ONNX/TorchScript cannot alter production configuration in this plan.
- [ ] `pytest-xdist` is explicitly skipped and no verification command uses `-n`.
- [ ] Every created interface has one owner, exact parameter/return types, and named consumers.
- [ ] No implementation step contains an unspecified error-handling, validation, testing, or migration instruction.
- [ ] Each task ends with serial verification and a cohesive commit command.
