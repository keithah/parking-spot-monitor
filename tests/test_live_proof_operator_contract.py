from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from PIL import Image

from scripts import run_docker_live_proof as runner


SECRET_RTSP = "rtsp://user:camera-secret@example.test/stream"
SECRET_TOKEN = "matrix-secret-token"
ROOM_ID = "!parking-room:example.org"
HOMESERVER = "https://matrix.example.org"


def write_config(
    path: Path,
    *,
    access_token_env: str = "MATRIX_ACCESS_TOKEN",
    homeserver: str | None = HOMESERVER,
    room_id: str | None = ROOM_ID,
) -> None:
    lines = [
        "stream:",
        "  rtsp_url_env: RTSP_URL",
        "  frame_width: 8",
        "  frame_height: 6",
        "matrix:",
        f"  homeserver: {homeserver or ''}",
        f"  room_id: '{room_id or ''}'",
        f"  access_token_env: {access_token_env}",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def write_jpeg(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (8, 6), color=(20, 40, 60))
    image.save(path, format="JPEG")


def read_result(data_dir: Path) -> dict[str, Any]:
    return json.loads((data_dir / "live-proof-result.json").read_text(encoding="utf-8"))


def test_preflight_missing_inputs_writes_names_only_and_does_not_run_docker(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    exit_code = runner.main(
        ["--config", str(tmp_path / "config.yaml"), "--data-dir", str(tmp_path / "data")],
        environ={},
        run=fake_run,
    )

    result = read_result(tmp_path / "data")
    preflight = json.loads((tmp_path / "data" / "live-proof-input-preflight.json").read_text(encoding="utf-8"))
    rendered = json.dumps(result) + json.dumps(preflight)
    assert exit_code == 2
    assert calls == []
    assert result["status"] == "preflight_failed"
    assert result["phase"] == "preflight"
    assert result["missing_inputs"] == ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"]
    assert preflight["status"] == "preflight_blocked"
    assert preflight["config"]["exists"] is False
    assert preflight["environment"]["rtsp_env_name"] == "RTSP_URL"
    assert preflight["environment"]["rtsp_env_present"] is False
    assert preflight["environment"]["matrix_token_env_name"] == "Matrix token env key"
    assert preflight["environment"]["matrix_token_env_present"] is False
    assert preflight["missing_inputs"] == result["missing_inputs"]
    assert result["markers"] == result["marker_checks"]
    assert result["artifacts"] == result["artifact_checks"]
    assert result["redaction"] == result["redaction_scan"]
    assert result["matrix_room_readback"] == result["room_readback"]
    assert SECRET_RTSP not in rendered
    assert SECRET_TOKEN not in rendered


def test_preflight_reports_missing_matrix_routing_fields_by_name(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    write_config(config_path, homeserver=None, room_id=None)

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(tmp_path / "data")],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=lambda *args, **kwargs: subprocess.CompletedProcess([], 99, "", ""),
    )

    result = read_result(tmp_path / "data")
    preflight = json.loads((tmp_path / "data" / "live-proof-input-preflight.json").read_text(encoding="utf-8"))
    rendered = json.dumps(result) + json.dumps(preflight)
    assert exit_code == 2
    assert result["status"] == "preflight_failed"
    assert result["phase"] == "preflight"
    assert result["missing_inputs"] == ["matrix.homeserver", "matrix.room_id"]
    assert preflight["status"] == "preflight_blocked"
    assert preflight["config"] == {"exists": True, "parse_ok": True, "path": str(config_path)}
    assert preflight["routing"]["matrix_homeserver_present"] is False
    assert preflight["routing"]["matrix_room_id_present"] is False
    assert preflight["environment"]["rtsp_env_present"] is True
    assert preflight["environment"]["matrix_token_env_present"] is True
    assert preflight["missing_inputs"] == ["matrix.homeserver", "matrix.room_id"]
    assert SECRET_RTSP not in rendered
    assert SECRET_TOKEN not in rendered


def test_alternate_matrix_token_env_is_checked_passed_to_docker_and_used_for_readback(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    token_env = "ALT_MATRIX_TOKEN"
    write_config(config_path, access_token_env=token_env)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / "live-proof-camera-2026-05-18t19-00-00z.jpg")

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert command[:8] == ["docker", "compose", "run", "--rm", "-e", "RTSP_URL", "-e", token_env]
        assert "parking-spot-monitor" in command
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="\n".join(["LIVE_RTSP_CAPTURE_OK", "LIVE_MATRIX_TEXT_OK", "LIVE_MATRIX_IMAGE_OK"]),
            stderr="",
        )

    def fake_readback(*, homeserver: str, room_id: str, access_token: str, timeout_seconds: float, limit: int) -> dict[str, Any]:
        assert access_token == SECRET_TOKEN
        return {
            "chunk": [
                {"content": {"msgtype": "m.text", "body": "LIVE PROOF / TEST MESSAGE"}},
                {"content": {"msgtype": "m.image", "body": "LIVE PROOF / TEST IMAGE"}},
            ]
        }

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={"RTSP_URL": SECRET_RTSP, token_env: SECRET_TOKEN},
        run=fake_run,
        readback=fake_readback,
    )

    result = read_result(data_dir)
    rendered = json.dumps(result)
    assert exit_code == 0
    assert result["status"] == "success"
    assert SECRET_TOKEN not in rendered
    assert "access_token" not in rendered.lower()
    assert "Matrix token env key" in rendered


def test_bare_runner_invocation_imports_package_from_repository_root(tmp_path: Path) -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/run_docker_live_proof.py", "--data-dir", str(tmp_path / "data")],
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )

    result = read_result(tmp_path / "data")
    assert completed.returncode == 2
    assert "ModuleNotFoundError" not in completed.stderr
    assert result["status"] == "preflight_failed"


def test_docker_exit_code_is_preserved_and_logs_are_redacted(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert command == runner.DOCKER_LIVE_PROOF_COMMAND
        return subprocess.CompletedProcess(
            command,
            17,
            stdout=f"LIVE_RTSP_CAPTURE_OK leaked {SECRET_RTSP}\n",
            stderr=f"Authorization: Bearer {SECRET_TOKEN}\nTraceback (most recent call last): hidden\n",
        )

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    result = read_result(data_dir)
    stdout_log = (data_dir / "live-proof-docker.stdout.log").read_text(encoding="utf-8")
    stderr_log = (data_dir / "live-proof-docker.stderr.log").read_text(encoding="utf-8")
    rendered = json.dumps(result) + stdout_log + stderr_log
    assert exit_code == 17
    assert result["status"] == "docker_failed"
    assert result["phase"] == "docker"
    assert result["docker_exit_code"] == 17
    assert SECRET_RTSP not in rendered
    assert SECRET_TOKEN not in rendered
    assert "Authorization" not in rendered
    assert "Traceback" not in rendered


def test_success_validates_markers_jpegs_and_matrix_readback(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / "live-proof-camera-2026-05-18t19-00-00z.jpg")

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="\n".join(["LIVE_RTSP_CAPTURE_OK", "LIVE_MATRIX_TEXT_OK", "LIVE_MATRIX_IMAGE_OK"]),
            stderr="",
        )

    def fake_readback(*, homeserver: str, room_id: str, access_token: str, timeout_seconds: float, limit: int) -> dict[str, Any]:
        assert homeserver == HOMESERVER
        assert room_id == ROOM_ID
        assert access_token == SECRET_TOKEN
        assert limit == 20
        return {
            "chunk": [
                {"content": {"msgtype": "m.text", "body": "LIVE PROOF / TEST MESSAGE: RTSP capture succeeded at now."}},
                {"content": {"msgtype": "m.image", "body": "LIVE PROOF / TEST IMAGE: raw full-frame camera snapshot captured at now."}},
            ]
        }

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
        readback=fake_readback,
    )

    result = read_result(data_dir)
    assert exit_code == 0
    assert result["status"] == "success"
    assert result["markers"] == result["marker_checks"]
    assert result["artifacts"] == result["artifact_checks"]
    assert result["redaction"] == result["redaction_scan"]
    assert result["matrix_room_readback"] == result["room_readback"]
    assert result["marker_checks"]["required_present"] is True
    assert result["marker_checks"]["forbidden_present"] == []
    assert result["artifact_checks"]["latest_jpeg"]["valid_jpeg"] is True
    assert result["artifact_checks"]["snapshot_jpegs"]["valid_count"] == 1
    assert result["room_readback_status"] == "verified"
    assert result["room_readback"]["text_found"] is True
    assert result["room_readback"]["image_found"] is True


def test_skip_marker_and_invalid_jpeg_make_wrapper_non_validation(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    (data_dir / "snapshots").mkdir(parents=True)
    (data_dir / "latest.jpg").write_bytes(b"not a jpeg")
    (data_dir / "snapshots" / "live-proof-bad.jpg").write_bytes(b"not a jpeg")

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="\n".join(
                [
                    "LIVE_PROOF_SKIPPED_RTSP_ENV_ABSENT",
                    "LIVE_RTSP_CAPTURE_OK",
                    "LIVE_MATRIX_TEXT_OK",
                    "LIVE_MATRIX_IMAGE_OK",
                ]
            ),
            stderr="",
        )

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir), "--skip-readback"],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    result = read_result(data_dir)
    assert exit_code == 1
    assert result["status"] == "validation_failed"
    assert result["marker_checks"]["forbidden_present"] == ["LIVE_PROOF_SKIPPED_RTSP_ENV_ABSENT"]
    assert result["artifact_checks"]["latest_jpeg"]["valid_jpeg"] is False
    assert result["artifact_checks"]["snapshot_jpegs"]["valid_count"] == 0
    assert result["room_readback_status"] == "skipped"


def test_matrix_readback_unavailable_is_recorded_as_explicit_gap(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / "live-proof-camera-2026-05-18t19-00-00z.jpg")

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="\n".join(["LIVE_RTSP_CAPTURE_OK", "LIVE_MATRIX_TEXT_OK", "LIVE_MATRIX_IMAGE_OK"]),
            stderr="",
        )

    def fake_readback(**kwargs: Any) -> dict[str, Any]:
        raise TimeoutError("timed out with token matrix-secret-token")

    exit_code = runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
        readback=fake_readback,
    )

    result = read_result(data_dir)
    rendered = json.dumps(result)
    assert exit_code == 1
    assert result["status"] == "readback_gap"
    assert result["room_readback_status"] == "gap"
    assert result["room_readback"]["reason"] == "TimeoutError"
    assert SECRET_TOKEN not in rendered


def test_verify_live_proof_writes_preflight_blocker_report(tmp_path: Path) -> None:
    from scripts import verify_live_proof

    data_dir = tmp_path / "data"
    result_path = data_dir / "live-proof-result.json"
    evidence_path = data_dir / "live-proof-evidence.md"
    runner.main(
        ["--config", str(tmp_path / "config.yaml"), "--data-dir", str(data_dir)],
        environ={},
        run=lambda *args, **kwargs: subprocess.CompletedProcess([], 99, "", ""),
    )

    exit_code = verify_live_proof.main(
        ["--result", str(result_path), "--evidence", str(evidence_path), "--allow-preflight-blocker"]
    )

    report = evidence_path.read_text(encoding="utf-8")
    assert exit_code == 0
    assert "Status: `preflight_failed`" in report
    assert "Docker exit code: `None`" in report
    assert "Matrix room readback: not_attempted" in report
    assert "Redaction secret occurrences: `0`" in report
    assert "config.yaml, RTSP_URL, Matrix token env key" in report
    assert "R003/R015 remain unvalidated" in report
    for forbidden in ["rtsp://", "Authorization", "Bearer", "access_token", "Traceback"]:
        assert forbidden.lower() not in report.lower()


def test_verify_live_proof_accepts_names_only_matrix_routing_preflight_blocker(tmp_path: Path) -> None:
    from scripts import verify_live_proof

    data_dir = tmp_path / "data"
    config_path = tmp_path / "config.yaml"
    result_path = data_dir / "live-proof-result.json"
    evidence_path = data_dir / "live-proof-evidence.md"
    write_config(config_path, homeserver=None, room_id=None)

    runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={},
        run=lambda *args, **kwargs: subprocess.CompletedProcess([], 99, "", ""),
    )

    exit_code = verify_live_proof.main(
        ["--result", str(result_path), "--evidence", str(evidence_path), "--allow-preflight-blocker"]
    )

    report = evidence_path.read_text(encoding="utf-8")
    assert exit_code == 0
    assert "matrix.homeserver" in report
    assert "matrix.room_id" in report
    assert "R003/R015 remain unvalidated" in report
    assert "## Operator Handoff" in report
    assert "No Docker/live proof was attempted" in report
    assert "Future strict run command" in report
    assert "python scripts/run_docker_live_proof.py" in report
    assert "RTSP_URL" in report
    assert "matrix.homeserver" in report
    assert "matrix.room_id" in report
    assert "Matrix token env key" in report
    for forbidden in ["rtsp://", "Authorization", "Bearer", "access_token", "Traceback"]:
        assert forbidden.lower() not in report.lower()


def test_verify_live_proof_accepts_only_room_readback_verified_success(tmp_path: Path) -> None:
    from scripts import verify_live_proof

    data_dir = tmp_path / "data"
    config_path = tmp_path / "config.yaml"
    result_path = data_dir / "live-proof-result.json"
    evidence_path = data_dir / "live-proof-evidence.md"
    write_config(config_path)
    write_jpeg(data_dir / "latest.jpg")
    write_jpeg(data_dir / "snapshots" / "live-proof-camera-2026-05-18t19-00-00z.jpg")

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="\n".join(["LIVE_RTSP_CAPTURE_OK", "LIVE_MATRIX_TEXT_OK", "LIVE_MATRIX_IMAGE_OK"]),
            stderr="",
        )

    runner.main(
        ["--config", str(config_path), "--data-dir", str(data_dir)],
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
        readback=lambda **kwargs: {
            "chunk": [
                {"content": {"msgtype": "m.text", "body": "LIVE PROOF / TEST MESSAGE"}},
                {"content": {"msgtype": "m.image", "body": "LIVE PROOF / TEST IMAGE"}},
            ]
        },
    )

    exit_code = verify_live_proof.main(["--result", str(result_path), "--evidence", str(evidence_path)])

    report = evidence_path.read_text(encoding="utf-8")
    assert exit_code == 0
    assert "R003/R015 validated" in report
    assert "Matrix room readback: verified" in report

    result = json.loads(result_path.read_text(encoding="utf-8"))
    result["status"] = "success"
    result["room_readback_status"] = "gap"
    result["room_readback"] = {"status": "gap", "text_found": False, "image_found": False}
    result["matrix_room_readback"] = result["room_readback"]
    result_path.write_text(json.dumps(result), encoding="utf-8")

    failed_exit_code = verify_live_proof.main(["--result", str(result_path), "--evidence", str(evidence_path)])
    failed_report = evidence_path.read_text(encoding="utf-8")
    assert failed_exit_code == 1
    assert "R003/R015 remain unvalidated" in failed_report
