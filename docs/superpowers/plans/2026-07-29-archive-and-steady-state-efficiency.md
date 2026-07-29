# Archive and Steady-State Efficiency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make archive health, corrections, owner-alert scans, merge resolution, and bounded formatting scale with current work rather than total retained history.

**Architecture:** Retain filesystem JSON as the source of truth. Add streaming accumulators and single-entry revision/signature caches with explicit invalidation; do not add secondary persisted indexes.

**Tech Stack:** Python 3.12, dataclasses, pathlib, JSON/JSONL, itertools, weak references, pytest

## Global Constraints

- Vehicle-history, correction, feedback, and decision-memory JSON schemas remain unchanged.
- Closed history remains retained indefinitely but is not materialized solely for health.
- Every cache stores at most one current value per source and has revision/signature invalidation.
- External file replacement must invalidate caches through `(mtime_ns, size)`.
- Cache read/stat failure falls back to recomputation.
- Vehicle descriptor output is not changed.
- Every task uses red-green-refactor and ends with a focused commit.

---

### Task 1: Stream Vehicle-History Health and Unsorted Latest Lookup

**Files:**
- Modify: `parking_spot_monitor/vehicle_history_storage.py:48-90`
- Modify: `parking_spot_monitor/vehicle_history_maintenance.py:264-300`
- Modify: `parking_spot_monitor/vehicle_history_maintenance_utils.py:158-205`
- Modify: `tests/test_vehicle_history.py:335-418`

**Interfaces:**
- Produces: `iter_closed_sessions() -> Iterator[SessionRecord]`
- Produces: `SessionHealthAccumulator.add(record) -> None` and `to_json() -> dict[str, int | str | None]`
- Preserves: `list_closed_sessions() -> list[SessionRecord]` for callers that explicitly require a list

- [ ] **Step 1: Write a failing large-archive streaming test**

```python
def test_health_snapshot_streams_closed_sessions_without_list_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    for index in range(250):
        spot_id = f"closed-{index}"
        archive.start_session(
            occupied_event(spot_id=spot_id, observed_at=f"2026-05-18T{index // 60:02d}:{index % 60:02d}:00Z")
        )
        archive.close_session(
            open_event(spot_id=spot_id, observed_at=f"2026-05-19T{index // 60:02d}:{index % 60:02d}:00Z")
        )

    def forbidden_list_closed_sessions():
        raise AssertionError("health must stream the closed archive")

    monkeypatch.setattr(archive, "list_closed_sessions", forbidden_list_closed_sessions)
    health = archive.health_snapshot()

    assert health["closed_session_count"] == 250
    assert health["oldest_retained_session_started_at"] is not None
```

- [ ] **Step 2: Write a failing no-sort wrong-match lookup test**

```python
def test_wrong_match_subject_does_not_sort_archive_paths(tmp_path: Path, monkeypatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.start_session(
        occupied_event(spot_id="left_spot", observed_at="2026-05-18T10:00:00Z")
    )
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-18T10:30:00Z"))
    second = archive.start_session(
        occupied_event(spot_id="left_spot", observed_at="2026-05-19T10:00:00Z")
    )
    archive.close_session(open_event(spot_id="left_spot", observed_at="2026-05-19T10:30:00Z"))
    monkeypatch.setattr(
        vehicle_history_storage,
        "sorted",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected sort")),
        raising=False,
    )
    assert archive.resolve_wrong_match_subject("left_spot") == second.session_id
    assert first.session_id != second.session_id
```

- [ ] **Step 3: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_vehicle_history.py -k 'streams_closed or does_not_sort' -q`

Expected: FAIL because health calls `list_closed_sessions` and record iteration sorts paths.

- [ ] **Step 4: Implement explicit ordered and unordered iterators**

```python
def _iter_records(self, directory: Path, *, ordered: bool = True) -> Iterator[SessionRecord]:
    paths: Iterable[Path] = directory.glob("*.json")
    if ordered:
        paths = sorted(paths)
    for path in paths:
        record = self._load_record(path)
        if record is not None:
            yield record

def iter_closed_sessions(self) -> Iterator[SessionRecord]:
    return self._iter_records(self.closed_dir, ordered=False)
```

Keep ordered iteration for public list behavior. Use unordered iteration only for latest-match reduction and health aggregation.

- [ ] **Step 5: Implement one-pass session health accumulation**

```python
@dataclass
class SessionHealthAccumulator:
    count: int = 0
    oldest_started_at: str | None = None
    missing_refs: int = 0
    profile_unknown_sessions: int = 0

    def add(self, record: SessionRecord) -> None:
        self.count += 1
        if self.oldest_started_at is None or record.started_at < self.oldest_started_at:
            self.oldest_started_at = record.started_at
        self.missing_refs += _missing_image_reference_count(record)
        self.profile_unknown_sessions += int(record.profile_id is None)
```

Feed active and closed iterators into the accumulator while maintaining separate counts. Preserve every existing health key and value.

- [ ] **Step 6: Run vehicle-history tests and commit**

Run: `python3 -m pytest tests/test_vehicle_history.py tests/test_runtime_health_cache.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/vehicle_history_storage.py parking_spot_monitor/vehicle_history_maintenance.py parking_spot_monitor/vehicle_history_maintenance_utils.py tests/test_vehicle_history.py
git commit -m "perf: stream vehicle history health"
```

### Task 2: Cache Correction Replay and Precompress Merge Chains

**Files:**
- Modify: `parking_spot_monitor/vehicle_history_storage.py:21-46`
- Modify: `parking_spot_monitor/vehicle_history_corrections.py:136-210,294-311`
- Modify: `parking_spot_monitor/vehicle_history_models.py`
- Modify: `tests/test_vehicle_history.py:972-1167,1331`

**Interfaces:**
- Adds: `correction_revision() -> int` and `_bump_correction_revision() -> None`
- Adds: immutable cache key `CorrectionReplaySignature(revision, corrections_stat, quarantine_stat)`
- Extends: `CorrectionReplayState.canonical_profile_ids: Mapping[str, str]`

- [ ] **Step 1: Write failing cache-hit and external-invalidation tests**

```python
def test_correction_replay_is_cached_until_revision_changes(tmp_path: Path, monkeypatch) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    calls = 0
    original = archive.load_corrections

    def counted():
        nonlocal calls
        calls += 1
        return original()

    monkeypatch.setattr(archive, "load_corrections", counted)
    assert archive.correction_replay_state() == archive.correction_replay_state()
    assert calls == 1
    archive._bump_correction_revision()
    archive.correction_replay_state()
    assert calls == 2


def test_external_correction_replace_invalidates_signature_cache(tmp_path: Path) -> None:
    archive = VehicleHistoryArchive(tmp_path)
    first = archive.correction_replay_state()
    archive.corrections_dir.mkdir(parents=True, exist_ok=True)
    archive.corrections_path.write_text(
        json.dumps({
            "schema_version": 1,
            "correction_id": "corr-external-1",
            "action": "profile_summary_requested",
            "created_at": "2026-05-18T14:45:00Z",
            "matrix_event_id": None,
            "matrix_sender": None,
            "matrix_room_id": None,
            "profile_id": "profile-a",
        }) + "\n",
        encoding="utf-8",
    )
    assert archive.correction_replay_state() != first
```

- [ ] **Step 2: Write a failing path-compression test**

```python
def test_effective_sessions_use_precompressed_canonical_map(tmp_path: Path, monkeypatch) -> None:
    assert _canonical_profile_map({"a": "b", "b": "c", "c": "d"}) == {
        "a": "d", "b": "d", "c": "d", "d": "d"
    }
```

- [ ] **Step 3: Run focused correction tests and verify RED**

Run: `python3 -m pytest tests/test_vehicle_history.py -k 'replay_is_cached or signature_cache or precompressed' -q`

Expected: FAIL because replay is recomputed and canonical maps are absent.

- [ ] **Step 4: Implement a one-entry correction cache**

Store `self._correction_revision`, `self._correction_cache_signature`, and `self._correction_cache`. Build safe stat signatures with `None` for missing files. Bump correction revision after correction append and quarantine writes. Compute quarantine count once per rebuild. Because loading may quarantine malformed lines, recompute and store the signature after the rebuild completes. Wrap cached canonical mappings with `MappingProxyType` so callers cannot mutate shared replay state.

- [ ] **Step 5: Build canonical IDs with cycle detection and path compression**

```python
def _canonical_profile_map(merges: Mapping[str, str]) -> dict[str, str]:
    canonical: dict[str, str] = {}
    for profile_id in set(merges) | set(merges.values()):
        trail: list[str] = []
        current = profile_id
        while current in merges and current not in canonical:
            if current in trail:
                raise ArchiveSchemaError("profile merge cycle detected")
            trail.append(current)
            current = merges[current]
        resolved = canonical.get(current, current)
        canonical[current] = resolved
        for item in trail:
            canonical[item] = resolved
    return canonical
```

Use the canonical map in `resolve_profile_id` and `_effective_sessions`. Preserve the current behavior when callers pass an explicit merge mapping by building a local compressed map once.

- [ ] **Step 6: Run all correction/profile tests and commit**

Run: `python3 -m pytest tests/test_vehicle_history.py -k 'correction or profile or estimate or health' -q`

Expected: PASS.

```bash
git add parking_spot_monitor/vehicle_history_storage.py parking_spot_monitor/vehicle_history_corrections.py parking_spot_monitor/vehicle_history_models.py tests/test_vehicle_history.py
git commit -m "perf: cache correction replay state"
```

### Task 3: Cache Owner Registry and Active Sessions by Explicit Revisions

**Files:**
- Create: `parking_spot_monitor/runtime_owner_vehicle_cache.py`
- Modify: `parking_spot_monitor/runtime_vehicle_events.py:45-104`
- Modify: `parking_spot_monitor/capture_loop.py:50-90`
- Modify: `parking_spot_monitor/runtime_frame_plan.py:29-100`
- Modify: `tests/test_startup.py:2945-3060`
- Create: `tests/test_runtime_owner_vehicle_cache.py`

**Interfaces:**
- Produces: `OwnerVehicleRuntimeCache.snapshot(archive) -> OwnerVehicleSnapshot`
- `OwnerVehicleSnapshot` contains immutable registry and active-session tuple
- Cache key combines registry `(mtime_ns, size)` and `archive.mutation_revision()`

- [ ] **Step 1: Write failing cache and invalidation tests**

```python
def test_owner_snapshot_reuses_registry_and_active_sessions(monkeypatch, tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text('{"schema_version":1,"owner_vehicles":[]}', encoding="utf-8")
    class FakeArchive:
        root = tmp_path
        revision = 1
        active_loads = 0
        def mutation_revision(self):
            return self.revision
        def load_active_sessions(self):
            self.active_loads += 1
            return []
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    first = cache.snapshot(archive)
    second = cache.snapshot(archive)
    assert second is first
    assert archive.active_loads == 1

def test_owner_snapshot_invalidates_on_registry_replace_and_archive_mutation(tmp_path: Path) -> None:
    registry_path = tmp_path / "owner-vehicles.json"
    registry_path.write_text('{"schema_version":1,"owner_vehicles":[]}', encoding="utf-8")
    class FakeArchive:
        revision = 1
        def mutation_revision(self):
            return self.revision
        def load_active_sessions(self):
            return []
    archive = FakeArchive()
    cache = OwnerVehicleRuntimeCache(registry_path)
    first = cache.snapshot(archive)
    registry_path.write_text(
        '{"schema_version":1,"owner_vehicles":[{"profile_id":"profile-a","label":"Car"}]}',
        encoding="utf-8",
    )
    second = cache.snapshot(archive)
    assert second is not first
    archive.revision += 1
    assert cache.snapshot(archive) is not second
```

- [ ] **Step 2: Run cache tests and verify RED**

Run: `python3 -m pytest tests/test_runtime_owner_vehicle_cache.py -q`

Expected: FAIL because the cache module does not exist.

- [ ] **Step 3: Implement the single-entry cache**

```python
@dataclass(frozen=True, slots=True)
class OwnerVehicleSnapshot:
    registry: OwnerVehicleRegistry
    active_sessions: tuple[SessionRecord, ...]

class OwnerVehicleRuntimeCache:
    def snapshot(self, archive: VehicleHistoryArchive) -> OwnerVehicleSnapshot:
        key = (_file_signature(self.registry_path), archive.mutation_revision())
        if key != self._key:
            self._value = OwnerVehicleSnapshot(
                registry=load_owner_vehicle_registry(self.registry_path),
                active_sessions=tuple(archive.load_active_sessions()),
            )
            self._key = key
        return self._value
```

On stat/load failure, do not overwrite the previous key with a false success. Let the existing caller catch/log the exception.

- [ ] **Step 4: Inject the cache once per capture-loop lifetime**

Construct it beside `VehicleHistoryHealthSnapshotCache`. Pass it through `build_runtime_frame_plan` into `_owner_vehicle_quiet_window_alerts`. Remove direct registry/session loads from the frame helper.

- [ ] **Step 5: Run owner/runtime tests and commit**

Run: `python3 -m pytest tests/test_runtime_owner_vehicle_cache.py tests/test_startup.py -k 'owner_vehicle or health_snapshot_cached' -q`

Expected: PASS.

```bash
git add parking_spot_monitor/runtime_owner_vehicle_cache.py parking_spot_monitor/runtime_vehicle_events.py parking_spot_monitor/runtime_frame_plan.py parking_spot_monitor/capture_loop.py tests/test_runtime_owner_vehicle_cache.py tests/test_startup.py
git commit -m "perf: cache runtime owner vehicle inputs"
```

### Task 4: Bound Diagnostic Collection Consumption

**Files:**
- Modify: `parking_spot_monitor/operator_decision_memory.py:387-448`
- Modify: `parking_spot_monitor/operator_feedback_models.py:276-300`
- Modify: `parking_spot_monitor/detection_lab.py:380-410`
- Modify: `tests/test_operator_decision_memory.py`
- Modify: `tests/test_operator_feedback.py`
- Modify: `tests/test_detection_lab.py`

**Interfaces:**
- Adds internal `_take_bounded(iterable, limit) -> tuple[list[Any], bool]`
- Preserves formatting order, limits, and truncation markers

- [ ] **Step 1: Write failing over-read guard tests**

```python
class CountingList(list[int]):
    def __init__(self, values: range) -> None:
        super().__init__(values)
        self.consumed = 0
    def __iter__(self):
        for item in super().__iter__():
            self.consumed += 1
            yield item

def test_decision_memory_bounding_consumes_only_limit_plus_one() -> None:
    values = CountingList(range(1_000))
    bounded = _bound_value(values, depth=0)
    assert values.consumed == MAX_SEQUENCE_ITEMS + 1
    assert bounded[-1] == "<truncated>"
```

Add equivalent tests for feedback sequence/metadata and detection-lab detail bounding.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_operator_decision_memory.py tests/test_operator_feedback.py tests/test_detection_lab.py -k 'consumes_only or bounded_iterable' -q`

Expected: FAIL because the implementations call `list(...)`.

- [ ] **Step 3: Implement bounded iteration**

```python
def _take_bounded(values: Iterable[_T], limit: int) -> tuple[list[_T], bool]:
    items = list(islice(values, limit + 1))
    return items[:limit], len(items) > limit
```

For mappings, use `islice(value.items(), limit + 1)`. Apply `_take_bounded` only after each existing accepted-type check so arbitrary iterables do not silently become valid input. Keep the current truncation strings.

- [ ] **Step 4: Run focused suites and commit**

Run: `python3 -m pytest tests/test_operator_decision_memory.py tests/test_operator_feedback.py tests/test_detection_lab.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/operator_decision_memory.py parking_spot_monitor/operator_feedback_models.py parking_spot_monitor/detection_lab.py tests/test_operator_decision_memory.py tests/test_operator_feedback.py tests/test_detection_lab.py
git commit -m "perf: bound diagnostic iterable consumption"
```

### Task 5: Remove Tuning JSON Copy Only After Mutation Proof

**Files:**
- Modify: `parking_spot_monitor/tuning.py:1-310`
- Modify: `tests/test_calibration_tuning.py`

**Interfaces:**
- Preserves tuning report dictionaries and Markdown output
- Removes `_json_round_trip` if the read-only contract is proved

- [ ] **Step 1: Add a mutation-proof test**

```python
def test_rendering_tuning_report_does_not_mutate_input() -> None:
    report = build_tuning_comparison_report(
        manifest(
            ReplayFrame(
                frame_id="low-confidence-occupied",
                expected={"left_spot": ExpectedPresence.OCCUPIED, "right_spot": ExpectedPresence.EMPTY},
                detections=[detection((10, 10, 90, 90), confidence=0.42)],
            ),
            ReplayFrame(
                frame_id="empty-frame",
                expected={"left_spot": ExpectedPresence.EMPTY, "right_spot": ExpectedPresence.EMPTY},
                detections=[],
            ),
        ),
        baseline_config=replay_config(confidence_threshold=0.55),
        proposed_config=replay_config(confidence_threshold=0.35),
    )
    before = copy.deepcopy(report)
    markdown = render_tuning_report_markdown(report)
    assert report == before
    assert "apply_shared_tuning" in markdown
```

- [ ] **Step 2: Run the test against current behavior**

Run: `python3 -m pytest tests/test_calibration_tuning.py -k does_not_mutate -q`

Expected: PASS, establishing the ownership contract before removing the copy.

- [ ] **Step 3: Replace JSON round trips with direct read-only access**

Delete `_json_round_trip` and pass newly constructed dictionaries directly. If a caller mutates one nested field in the new test, replace only that field with `dict(field)` or `list(field)`; do not restore whole-report serialization.

- [ ] **Step 4: Run tuning tests and commit**

Run: `python3 -m pytest tests/test_calibration_tuning.py tests/test_calibration_replay.py -q`

Expected: PASS with byte-for-byte identical Markdown fixtures.

```bash
git add parking_spot_monitor/tuning.py tests/test_calibration_tuning.py
git commit -m "perf: remove tuning JSON copy"
```

### Task 6: Verify Slice 2 as an Independent Deliverable

**Files:**
- No planned modifications

**Interfaces:**
- Verifies all interfaces produced by Tasks 1-5

- [ ] **Step 1: Run archive/operator regression suites**

Run: `python3 -m pytest tests/test_vehicle_history.py tests/test_runtime_health_cache.py tests/test_operator_decision_memory.py tests/test_operator_feedback.py tests/test_detection_lab.py tests/test_calibration_tuning.py tests/test_startup.py -q`

Expected: PASS.

- [ ] **Step 2: Run full and structural verification**

Run: `python3 -m pytest -q && python3 -m compileall -q parking_spot_monitor src scripts tests && git diff --check`

Expected: all commands exit 0.

- [ ] **Step 3: Record performance evidence**

Run: `/usr/bin/time -v python3 -m pytest tests/test_vehicle_history.py -k streams_closed_sessions -q`

Record maximum RSS, elapsed time, correction-load call counts, owner-registry load counts, and bounded-generator consumption in the task report. Do not create an empty verification commit.
