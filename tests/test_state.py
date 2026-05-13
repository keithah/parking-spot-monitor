from __future__ import annotations

import json
import math
import os
import stat
from io import StringIO
from pathlib import Path

import pytest

from parking_spot_monitor.logging import setup_logging
from parking_spot_monitor.occupancy import OccupancyStatus, SpotOccupancyState
from parking_spot_monitor.state import RuntimeState, load_runtime_state, save_runtime_state


def logger_records(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_missing_state_file_returns_default_unknown_spots_and_logs_loaded(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)

    state = load_runtime_state(tmp_path / "state.json", ["left_spot", "right_spot"], logger=logger)

    assert set(state.state_by_spot) == {"left_spot", "right_spot"}
    assert state.state_by_spot["left_spot"] == SpotOccupancyState()
    assert state.state_by_spot["right_spot"].status is OccupancyStatus.UNKNOWN
    assert state.active_quiet_window_ids == frozenset()
    assert state.quiet_window_notice_ids == frozenset()
    assert logger_records(stream) == [
        {
            "event": "state-loaded",
            "level": "INFO",
            "path": str(tmp_path / "state.json"),
            "phase": "missing-default",
            "spot_count": 2,
        }
    ]


def test_state_json_round_trips_occupancy_and_quiet_window_markers(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "state.json"
    original = RuntimeState(
        state_by_spot={
            "left_spot": SpotOccupancyState(
                status=OccupancyStatus.OCCUPIED,
                hit_streak=3,
                miss_streak=0,
                last_bbox=(1.0, 2.0, 30.0, 40.0),
                open_event_emitted=False,
            ),
            "right_spot": SpotOccupancyState(
                status=OccupancyStatus.EMPTY,
                hit_streak=0,
                miss_streak=4,
                last_bbox=None,
                open_event_emitted=True,
            ),
        },
        active_quiet_window_ids=frozenset({"street_sweeping:2026-05-18:13:00-15:00"}),
        quiet_window_notice_ids=frozenset(
            {
                "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
                "quiet-window-ended:street_sweeping:2026-05-18:13:00-15:00",
            }
        ),
    )

    save_runtime_state(path, original)
    loaded = load_runtime_state(path, ["left_spot", "right_spot"])

    assert loaded == original
    assert stat.S_IMODE(path.stat().st_mode) == 0o644
    raw = json.loads(path.read_text())
    assert raw == {
        "schema_version": 1,
        "spots": {
            "left_spot": {
                "status": "occupied",
                "hit_streak": 3,
                "miss_streak": 0,
                "last_bbox": [1.0, 2.0, 30.0, 40.0],
                "open_event_emitted": False,
            },
            "right_spot": {
                "status": "empty",
                "hit_streak": 0,
                "miss_streak": 4,
                "last_bbox": None,
                "open_event_emitted": True,
            },
        },
        "active_quiet_window_ids": ["street_sweeping:2026-05-18:13:00-15:00"],
        "quiet_window_notice_ids": [
            "quiet-window-ended:street_sweeping:2026-05-18:13:00-15:00",
            "quiet-window-started:street_sweeping:2026-05-18:13:00-15:00",
        ],
    }


def test_save_uses_temp_file_in_parent_directory_and_os_replace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    calls: list[tuple[str, str]] = []
    real_replace = os.replace

    def recording_replace(src: str | bytes | os.PathLike[str], dst: str | bytes | os.PathLike[str]) -> None:
        calls.append((str(src), str(dst)))
        assert Path(src).parent == tmp_path
        assert Path(src).name.startswith(".state.json.")
        assert Path(dst) == path
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", recording_replace)

    save_runtime_state(path, RuntimeState.default(["left_spot"]))

    assert calls and calls[-1][1] == str(path)
    assert path.exists()
    assert not list(tmp_path.glob(".state.json.*.tmp"))


def test_save_creates_parent_directory_and_logs_saved(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "missing" / "state.json"

    save_runtime_state(path, RuntimeState.default(["left_spot", "right_spot"]), logger=logger)

    assert path.exists()
    assert logger_records(stream) == [
        {
            "event": "state-saved",
            "level": "INFO",
            "path": str(path),
            "spot_count": 2,
            "active_quiet_window_count": 0,
            "quiet_window_notice_count": 0,
        }
    ]


def test_corrupt_json_is_quarantined_with_safe_diagnostics_and_defaults(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    sentinel = "rtsp://camera.local/stream access_token=supersecret Traceback matrix_token=tok raw_image_bytes"
    path.write_text("{not json " + sentinel)

    state = load_runtime_state(path, ["left_spot", "right_spot"], logger=logger)

    assert state == RuntimeState.default(["left_spot", "right_spot"])
    assert not path.exists()
    quarantined = list(tmp_path.glob("state.json.corrupt-*"))
    assert len(quarantined) == 1
    assert sentinel in quarantined[0].read_text()
    records = logger_records(stream)
    assert [record["event"] for record in records] == ["state-corrupt-quarantined", "state-loaded"]
    quarantine = records[0]
    assert quarantine["phase"] == "json-load"
    assert quarantine["error_type"] == "JSONDecodeError"
    assert quarantine["quarantine_path"] == str(quarantined[0])
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "matrix_token=tok" not in rendered
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered
    assert records[1]["phase"] == "quarantined-default"


def test_invalid_schema_is_quarantined_and_unknown_defaults_are_returned(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"schema_version": 1, "spots": {"left_spot": {"status": "maybe"}}}))

    state = load_runtime_state(path, ["left_spot", "right_spot"], logger=logger)

    assert state == RuntimeState.default(["left_spot", "right_spot"])
    assert not path.exists()
    assert len(list(tmp_path.glob("state.json.corrupt-*"))) == 1
    records = logger_records(stream)
    assert records[0]["event"] == "state-corrupt-quarantined"
    assert records[0]["phase"] == "schema-validate"
    assert records[0]["error_type"] == "StateSchemaError"
    assert records[1]["event"] == "state-loaded"
    assert records[1]["phase"] == "quarantined-default"


def test_non_finite_bbox_is_quarantined_and_defaults_are_returned(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "spots": {
                    "left_spot": {
                        "status": "occupied",
                        "hit_streak": 2,
                        "miss_streak": 0,
                        "last_bbox": [1.0, math.inf, 30.0, 40.0],
                        "open_event_emitted": False,
                    }
                },
            }
        )
    )

    state = load_runtime_state(path, ["left_spot"], logger=logger)

    assert state == RuntimeState.default(["left_spot"])
    assert not path.exists()
    assert len(list(tmp_path.glob("state.json.corrupt-*"))) == 1
    records = logger_records(stream)
    assert records[0]["event"] == "state-corrupt-quarantined"
    assert records[0]["phase"] == "schema-validate"
    assert records[0]["error_type"] == "StateSchemaError"


def test_save_rejects_non_finite_json_values_without_replacing_existing_state(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    path.write_text("existing-state")
    state = RuntimeState(
        state_by_spot={
            "left_spot": SpotOccupancyState(
                status=OccupancyStatus.OCCUPIED,
                hit_streak=2,
                miss_streak=0,
                last_bbox=(1.0, math.nan, 30.0, 40.0),
                open_event_emitted=False,
            )
        }
    )

    with pytest.raises(ValueError):
        save_runtime_state(path, state, logger=logger)

    assert path.read_text() == "existing-state"
    assert not list(tmp_path.glob(".state.json.*.tmp"))
    records = logger_records(stream)
    assert records[0]["event"] == "state-save-failed"
    assert records[0]["error_type"] == "ValueError"


def test_oversized_state_file_is_quarantined_before_json_load(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    path.write_text(" " * 1_000_001)

    state = load_runtime_state(path, ["left_spot"], logger=logger)

    assert state == RuntimeState.default(["left_spot"])
    assert not path.exists()
    assert len(list(tmp_path.glob("state.json.corrupt-*"))) == 1
    records = logger_records(stream)
    assert records[0]["event"] == "state-corrupt-quarantined"
    assert records[0]["phase"] == "size-validate"
    assert records[0]["error_type"] == "StateSchemaError"


def test_oversized_state_lists_are_quarantined(tmp_path: Path) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "spots": {},
                "active_quiet_window_ids": [f"window-{index}" for index in range(10_001)],
                "quiet_window_notice_ids": [],
            }
        )
    )

    state = load_runtime_state(path, ["left_spot"], logger=logger)

    assert state == RuntimeState.default(["left_spot"])
    assert not path.exists()
    assert len(list(tmp_path.glob("state.json.corrupt-*"))) == 1
    records = logger_records(stream)
    assert records[0]["event"] == "state-corrupt-quarantined"
    assert records[0]["phase"] == "schema-validate"
    assert records[0]["error_type"] == "StateSchemaError"


def test_save_failure_logs_safe_failure_and_preserves_existing_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = StringIO()
    logger = setup_logging(stream=stream)
    path = tmp_path / "state.json"
    path.write_text("existing-state")

    def failing_replace(src: str | bytes | os.PathLike[str], dst: str | bytes | os.PathLike[str]) -> None:
        raise PermissionError("cannot write rtsp://camera access_token=supersecret Traceback raw_image_bytes")

    monkeypatch.setattr(os, "replace", failing_replace)

    with pytest.raises(PermissionError):
        save_runtime_state(path, RuntimeState.default(["left_spot"]), logger=logger)

    assert path.read_text() == "existing-state"
    assert not list(tmp_path.glob(".state.json.*.tmp"))
    records = logger_records(stream)
    assert records[0]["event"] == "state-save-failed"
    assert records[0]["error_type"] == "PermissionError"
    rendered = json.dumps(records)
    assert "supersecret" not in rendered
    assert "Traceback" not in rendered
    assert "raw_image_bytes" not in rendered
