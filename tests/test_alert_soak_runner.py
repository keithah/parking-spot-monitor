from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from PIL import Image

from scripts import run_docker_alert_soak as runner

SECRET_RTSP = "rtsp://user:camera-secret@example.test/stream"
SECRET_TOKEN = "matrix-secret-token"
ROOM_ID = "!parking-room:example.org"
HOMESERVER = "https://matrix.example.org"
OBSERVED_AT = "2026-05-18T19:00:00Z"
EVENT_ID = f"occupancy-open-event:left_spot:{OBSERVED_AT}"
SNAPSHOT_NAME = "occupancy-open-event-left-spot-2026-05-18t19-00-00z.jpg"


def write_config(path: Path, *, access_token_env: str = "MATRIX_ACCESS_TOKEN", homeserver: str | None = HOMESERVER, room_id: str | None = ROOM_ID) -> None:
    path.write_text(
        "\n".join(
            [
                "stream:",
                "  rtsp_url_env: RTSP_URL",
                "matrix:",
                f"  homeserver: {homeserver or ''}",
                f"  room_id: '{room_id or ''}'",
                f"  access_token_env: {access_token_env}",
            ]
        ),
        encoding="utf-8",
    )


def write_jpeg(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 6), color=(20, 40, 60)).save(path, format="JPEG")


def read_result(data_dir: Path) -> dict[str, Any]:
    return json.loads((data_dir / "alert-soak-result.json").read_text(encoding="utf-8"))


def alert_lines(*, duplicate_txn: bool = False, include_live_proof_marker: bool = False) -> str:
    txn = EVENT_ID
    lines = [
        {"event": "occupancy-open-event", "spot_id": "left_spot", "observed_at": OBSERVED_AT, "snapshot_path": "/data/latest.jpg", "previous_status": "occupied", "new_status": "empty"},
        {"event": "matrix-delivery-attempt", "event_type": "occupancy-open-event", "spot_id": "left_spot", "txn_id": txn, "attempt": 1},
        {"event": "matrix-delivery-succeeded", "event_type": "occupancy-open-event", "spot_id": "left_spot", "txn_id": txn, "attempt": 1},
        {"event": "matrix-snapshot-copied", "event_type": "occupancy-open-event", "spot_id": "left_spot", "txn_id": f"snapshot-{SNAPSHOT_NAME[:-4]}", "snapshot_path": f"/data/snapshots/{SNAPSHOT_NAME}", "width": 8, "height": 6},
    ]
    if duplicate_txn:
        lines.append({"event": "matrix-delivery-attempt", "event_type": "occupancy-open-event", "spot_id": "left_spot", "txn_id": txn, "attempt": 1})
    rendered = "\n".join(json.dumps(line) for line in lines)
    if include_live_proof_marker:
        rendered += "\nLIVE_PROOF / TEST MESSAGE should not count as organic evidence"
    return rendered


class CompletedProcess:
    def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.terminated = False
        self.killed = False

    def communicate(self, timeout: float | None = None) -> tuple[str, str]:
        return self.stdout, self.stderr

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


class TimeoutProcess:
    def __init__(self, stdout_after: str = "", stderr_after: str = "") -> None:
        self.stdout_after = stdout_after
        self.stderr_after = stderr_after
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False
        self.calls = 0

    def communicate(self, timeout: float | None = None) -> tuple[str, str]:
        self.calls += 1
        if self.calls == 1:
            raise subprocess.TimeoutExpired(cmd=["docker"], timeout=timeout, output="partial\n", stderr="")
        self.returncode = -15 if self.terminated else -9
        return self.stdout_after, self.stderr_after

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


def test_preflight_missing_inputs_writes_names_only_and_does_not_run_docker(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_popen(command: list[str], **kwargs: Any) -> CompletedProcess:
        calls.append(command)
        return CompletedProcess()

    exit_code = runner.main(["--config", str(tmp_path / "config.yaml"), "--data-dir", str(tmp_path / "data")], environ={}, popen_factory=fake_popen)

    result = read_result(tmp_path / "data")
    preflight = json.loads((tmp_path / "data" / "alert-soak-input-preflight.json").read_text(encoding="utf-8"))
    rendered = json.dumps(result) + json.dumps(preflight)
    assert exit_code == 2
    assert calls == []
    assert result["status"] == "preflight_failed"
    assert result["phase"] == "preflight"
    assert result["missing_inputs"] == ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"]
    assert preflight["status"] == "preflight_blocked"
    assert preflight["environment"]["matrix_token_env_name"] == "Matrix token env key"
    assert "rtsp://" not in rendered.lower()
    assert SECRET_TOKEN not in rendered


def test_preflight_config_path_directory_reports_name_only_and_does_not_run_docker(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.mkdir()
    calls: list[list[str]] = []

    def fake_popen(command: list[str], **kwargs: Any) -> CompletedProcess:
        calls.append(command)
        return CompletedProcess()

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(tmp_path / "data")], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=fake_popen)

    result = read_result(tmp_path / "data")
    preflight = json.loads((tmp_path / "data" / "alert-soak-input-preflight.json").read_text(encoding="utf-8"))
    rendered = json.dumps(result) + json.dumps(preflight)
    assert exit_code == 2
    assert calls == []
    assert result["status"] == "preflight_failed"
    assert result["phase"] == "preflight"
    assert result["missing_inputs"] == ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"]
    assert result["docker"]["attempted"] is False
    assert preflight["config"]["path"] == str(config_path)
    assert "rtsp://" not in rendered.lower()
    assert SECRET_RTSP not in rendered
    assert SECRET_TOKEN not in rendered


def test_docker_nonzero_preserves_exit_code_and_redacts_logs(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_popen(command: list[str], **kwargs: Any) -> CompletedProcess:
        return CompletedProcess(stdout=f"leaked {SECRET_RTSP}\n", stderr=f"Authorization: Bearer {SECRET_TOKEN}\nTraceback hidden\n", returncode=17)

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(data_dir)], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=fake_popen)

    result = read_result(data_dir)
    rendered = json.dumps(result) + (data_dir / "alert-soak-docker.stdout.log").read_text() + (data_dir / "alert-soak-docker.stderr.log").read_text()
    assert exit_code == 17
    assert result["status"] == "docker_failed"
    assert result["docker"]["exit_code"] == 17
    assert SECRET_RTSP not in rendered
    assert SECRET_TOKEN not in rendered
    assert "Authorization" not in rendered
    assert "Traceback" not in rendered


def test_requested_soak_timeout_is_expected_completion_but_no_alerts_are_coverage_gap(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    process = TimeoutProcess(stdout_after='{"event":"capture-loop-paced","iteration":1}\n')

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(data_dir), "--soak-seconds", "0.01"], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=lambda *args, **kwargs: process)

    result = read_result(data_dir)
    assert exit_code == 1
    assert process.terminated is True
    assert result["status"] == "coverage_gap"
    assert result["phase"] == "alert_detection"
    assert result["docker"]["timed_out"] is True
    assert result["docker"]["expected_timeout_completion"] is True
    assert result["alert_summary"]["organic_alert_count"] == 0
    assert result["room_readback_status"] == "not_applicable"


def test_success_parses_organic_alerts_validates_jpegs_and_matrix_readback(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / SNAPSHOT_NAME)
    (data_dir / "health.json").write_text(json.dumps({"status": "ok", "iteration": 3}), encoding="utf-8")
    (data_dir / "state.json").write_text(json.dumps({"schema_version": 1, "spots": {"left_spot": {}}}), encoding="utf-8")

    def fake_readback(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["homeserver"] == HOMESERVER
        assert kwargs["room_id"] == ROOM_ID
        assert kwargs["access_token"] == SECRET_TOKEN
        return {"chunk": [{"content": {"msgtype": "m.text", "body": "Parking spot open: left_spot at now"}}, {"content": {"msgtype": "m.image", "body": "Raw full-frame snapshot for left_spot at now"}}]}

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(data_dir)], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=lambda *args, **kwargs: CompletedProcess(stdout=alert_lines(include_live_proof_marker=True), returncode=0), readback=fake_readback)

    result = read_result(data_dir)
    assert exit_code == 0
    assert result["status"] == "success"
    assert result["phase"] == "complete"
    assert result["alert_summary"]["organic_alert_count"] == 1
    assert result["log_summary"]["live_proof_ignored_count"] == 1
    assert result["artifact_summary"]["latest_jpeg"]["valid_jpeg"] is True
    assert result["artifact_summary"]["event_snapshot_jpegs"]["valid_count"] == 1
    assert result["room_readback_status"] == "verified"
    assert result["health_summary"]["parse_ok"] is True
    assert result["state_summary"]["parse_ok"] is True
    assert result["aliases"] if False else result["alerts"] == result["alert_summary"]



def test_success_includes_hardware_decode_summary_in_result(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / SNAPSHOT_NAME)
    (data_dir / "health.json").write_text(json.dumps({"status": "ok", "iteration": 3}), encoding="utf-8")
    (data_dir / "state.json").write_text(json.dumps({"schema_version": 1, "spots": {"left_spot": {}}}), encoding="utf-8")

    def fake_hardware_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "verdict": {"accepted": True, "status": "vaapi_supported_qsv_unavailable"},
                    "checks": {
                        "vainfo": {"passed": True, "returncode": 0},
                        "vaapi_ffmpeg_init": {"passed": True, "returncode": 0},
                        "qsv_ffmpeg_init": {"passed": False, "returncode": 171},
                    },
                }
            ),
            stderr="",
        )

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        popen_factory=lambda *args, **kwargs: CompletedProcess(stdout=alert_lines(), returncode=0),
        readback=lambda **kwargs: {
            "chunk": [
                {"content": {"msgtype": "m.text", "body": "Parking spot open: left_spot at now"}},
                {"content": {"msgtype": "m.image", "body": "Raw full-frame snapshot for left_spot at now"}},
            ]
        },
        hardware_run=fake_hardware_run,
    )

    result = read_result(data_dir)
    rendered = json.dumps(result)
    assert exit_code == 0
    assert result["hardware_decode_summary"]["status"] == "vaapi_supported_qsv_unavailable"
    assert result["hardware_decode_summary"]["checks"]["vaapi_ffmpeg_init"] == {"passed": True, "returncode": 0}
    assert result["hardware_decode_summary"]["checks"]["qsv_ffmpeg_init"] == {"passed": False, "returncode": 171}
    report = (data_dir / "alert-soak-evidence.md").read_text(encoding="utf-8")
    assert "Hardware decode: `vaapi_supported_qsv_unavailable` accepted=`True`" in report
    assert "vaapi_ffmpeg_init=True/0" in report
    assert "qsv_ffmpeg_init=False/171" in report
    assert SECRET_RTSP not in rendered + report
    assert SECRET_TOKEN not in rendered + report


def test_duplicate_txns_are_validation_failure(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / SNAPSHOT_NAME)

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(data_dir), "--skip-readback"], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=lambda *args, **kwargs: CompletedProcess(stdout=alert_lines(duplicate_txn=True), returncode=0))

    result = read_result(data_dir)
    assert exit_code == 1
    assert result["status"] == "validation_failed"
    assert result["phase"] == "duplicate_diagnostics"
    assert result["duplicate_summary"]["txn_ids"]


def test_invalid_event_snapshot_is_validation_failure(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    (data_dir / "snapshots").mkdir(parents=True)
    (data_dir / "snapshots" / SNAPSHOT_NAME).write_bytes(b"not a jpeg")

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(data_dir), "--skip-readback"], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=lambda *args, **kwargs: CompletedProcess(stdout=alert_lines(), returncode=0))

    result = read_result(data_dir)
    assert exit_code == 1
    assert result["status"] == "validation_failed"
    assert result["phase"] == "artifact_validation"
    assert result["artifact_summary"]["event_snapshot_jpegs"]["valid_count"] == 0


def test_skip_readback_is_explicit_gap_for_observed_alert(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / SNAPSHOT_NAME)

    exit_code = runner.main(["--config", str(config_path), "--data-dir", str(data_dir), "--skip-readback"], environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, popen_factory=lambda *args, **kwargs: CompletedProcess(stdout=alert_lines(), returncode=0))

    result = read_result(data_dir)
    assert exit_code == 1
    assert result["status"] == "readback_gap"
    assert result["phase"] == "matrix_readback"
    assert result["room_readback_status"] == "skipped"


def test_bare_runner_invocation_imports_package_from_repository_root(tmp_path: Path) -> None:
    completed = subprocess.run([sys.executable, "scripts/run_docker_alert_soak.py", "--data-dir", str(tmp_path / "data")], check=False, capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"})

    result = read_result(tmp_path / "data")
    assert completed.returncode == 2
    assert "ModuleNotFoundError" not in completed.stderr
    assert result["status"] == "preflight_failed"
