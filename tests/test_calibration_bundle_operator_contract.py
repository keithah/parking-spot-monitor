from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from scripts import capture_calibration_bundle as runner


SECRET_RTSP = "rtsp://user:camera-secret@example.test/stream"
SECRET_TOKEN = "matrix-secret-token"
SECRET_AUTH_HEADER = f"Authorization: Bearer {SECRET_TOKEN}"
ROOM_ID = "!parking-room:example.org"
HOMESERVER = "https://matrix.example.org"


class FakeClock:
    def __init__(self, value: str = "2026-05-18T19:00:00Z") -> None:
        self.value = value

    def __call__(self) -> str:
        return self.value


def write_config(
    path: Path,
    *,
    rtsp_env: str = "RTSP_URL",
    access_token_env: str = "MATRIX_ACCESS_TOKEN",
    include_rtsp_env: bool = True,
    include_matrix_token_env: bool = True,
) -> None:
    lines = [
        "stream:",
        *([f"  rtsp_url_env: {rtsp_env}"] if include_rtsp_env else []),
        "  frame_width: 8",
        "  frame_height: 6",
        "matrix:",
        f"  homeserver: {HOMESERVER}",
        f"  room_id: '{ROOM_ID}'",
        *([f"  access_token_env: {access_token_env}"] if include_matrix_token_env else []),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_jpeg(path: Path, *, color: tuple[int, int, int] = (20, 40, 60)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (8, 6), color=color)
    image.save(path, format="JPEG")


def read_manifest(data_dir: Path) -> dict[str, Any]:
    bundle_dirs = sorted((data_dir / "calibration-bundles").iterdir())
    assert len(bundle_dirs) == 1
    manifest_path = bundle_dirs[0] / "manifest.json"
    assert manifest_path.is_file()
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def read_preflight(data_dir: Path) -> dict[str, Any]:
    return json.loads((data_dir / "calibration-input-preflight.json").read_text(encoding="utf-8"))


def bundle_text(data_dir: Path) -> str:
    root = data_dir / "calibration-bundles"
    if not root.exists():
        return ""
    return "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in sorted(root.glob("*/*"))
        if path.is_file() and path.suffix.lower() in {".json", ".md", ".log", ".txt"}
    )


def invoke(
    tmp_path: Path,
    *,
    config_path: Path | None = None,
    data_dir: Path | None = None,
    environ: dict[str, str] | None = None,
    run: runner.RunCallable | None = None,
    extra_args: list[str] | None = None,
    now: FakeClock | None = None,
) -> int:
    args = [
        "--config",
        str(config_path or tmp_path / "config.yaml"),
        "--data-dir",
        str(data_dir or tmp_path / "data"),
    ]
    if extra_args:
        args.extend(extra_args)
    return runner.main(
        args,
        environ={} if environ is None else environ,
        run=(lambda command, **kwargs: subprocess.CompletedProcess(command, 0, "", "")) if run is None else run,
        now=FakeClock() if now is None else now,
    )


def assert_publication_safe(text: str) -> None:
    for forbidden in [
        SECRET_RTSP,
        SECRET_TOKEN,
        SECRET_AUTH_HEADER,
        "Authorization: Bearer",
        "Traceback",
        "camera-secret",
        "matrix-secret-token",
        "raw_secret_line",
    ]:
        assert forbidden.lower() not in text.lower()


def assert_capture_once_docker_command(command: list[str], *, matrix_token_env: str = "MATRIX_ACCESS_TOKEN") -> None:
    assert command[:4] == ["docker", "compose", "run", "--rm"]
    assert command[4:8] == ["-e", "RTSP_URL", "-e", matrix_token_env]
    assert all("=" not in item for item in command[:8])
    assert SECRET_RTSP not in json.dumps(command)
    assert SECRET_TOKEN not in json.dumps(command)
    assert command[-8:] == [
        "python",
        "-m",
        "parking_spot_monitor",
        "--config",
        "/config/config.yaml",
        "--data-dir",
        "/data",
        "--capture-once",
    ]


def test_success_creates_timestamped_redacted_calibration_bundle(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        assert kwargs["check"] is False
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        write_jpeg(data_dir / "latest.jpg", color=(20, 40, 60))
        write_jpeg(data_dir / "debug_latest.jpg", color=(80, 100, 120))
        (data_dir / "health.json").write_text(
            json.dumps({"status": "ok", "last_frame_at": "2026-05-18T19:00:00Z", "selected_decode_mode": "tcp"}),
            encoding="utf-8",
        )
        (data_dir / "state.json").write_text(json.dumps({"state_by_spot": {"left_spot": {"status": "open"}}}), encoding="utf-8")
        stdout = "\n".join(
            [
                json.dumps({"event": "capture-frame-written", "path": "/data/latest.jpg", "timestamp": "2026-05-18T19:00:00Z", "decode_mode": "tcp"}),
                json.dumps({"event": "debug-overlay-written", "path": "/data/debug_latest.jpg"}),
                json.dumps(
                    {
                        "event": "detection-frame-processed",
                        "mode": "capture-once",
                        "detection_count": 2,
                        "accepted_count": 1,
                        "spot_ids": ["left_spot", "right_spot"],
                        "candidate_summaries": [{"spot_id": "left_spot", "class_name": "car", "confidence": 0.91}],
                    }
                ),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 0
    assert len(calls) == 1
    assert_capture_once_docker_command(calls[0])
    manifest = read_manifest(data_dir)
    bundle_dir = Path(manifest["bundle_dir"])
    assert bundle_dir.name == "2026-05-18T19-00-00Z"
    assert manifest["status"] == "success"
    assert manifest["phase"] == "complete"
    assert manifest["docker_exit_code"] == 0
    assert manifest["docker_command"][:4] == ["docker", "compose", "run", "--rm"]
    assert manifest["artifacts"]["raw_frame"]["valid_jpeg"] is True
    assert manifest["artifacts"]["raw_frame"]["bundle_path"].endswith("latest.jpg")
    assert manifest["artifacts"]["debug_overlay"]["valid_jpeg"] is True
    assert manifest["detection_summary"]["event"] == "detection-frame-processed"
    assert manifest["detection_summary"]["detection_count"] == 2
    assert manifest["detection_summary"]["accepted_count"] == 1
    assert manifest["capture"]["decode_mode"] == "tcp"
    assert manifest["capture"]["frame_timestamp"] == "2026-05-18T19:00:00Z"
    assert manifest["context"]["health_json_present"] is True
    assert manifest["context"]["state_json_present"] is True
    assert manifest["redaction_scan"]["secret_occurrences"] == 0
    assert (bundle_dir / "latest.jpg").is_file()
    assert (bundle_dir / "debug_latest.jpg").is_file()
    assert (bundle_dir / "calibration-report.md").is_file()
    assert_publication_safe(bundle_text(data_dir))


@pytest.mark.parametrize(
    ("config_exists", "environ", "expected_missing"),
    [
        (False, {}, ["config.yaml", "RTSP_URL", "MATRIX_TOKEN_ENV"]),
        (True, {"MATRIX_ACCESS_TOKEN": SECRET_TOKEN}, ["RTSP_URL"]),
        (True, {"RTSP_URL": SECRET_RTSP}, ["MATRIX_TOKEN_ENV"]),
    ],
)
def test_preflight_blockers_are_names_only_and_do_not_run_docker(
    tmp_path: Path,
    config_exists: bool,
    environ: dict[str, str],
    expected_missing: list[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    if config_exists:
        write_config(config_path)
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 99, "", "")

    exit_code = invoke(tmp_path, config_path=config_path, data_dir=data_dir, environ=environ, run=fake_run)

    assert exit_code == 2
    assert calls == []
    preflight = read_preflight(data_dir)
    rendered = json.dumps(preflight) + bundle_text(data_dir)
    assert preflight["status"] == "preflight_blocked"
    assert preflight["missing_inputs"] == expected_missing
    assert preflight["environment"]["rtsp_env_name"] == "RTSP_URL"
    assert preflight["environment"]["matrix_token_env_name"] == "Matrix token env key"
    assert_publication_safe(rendered)


def test_invalid_yaml_config_is_names_only_preflight_blocker(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    config_path.write_text("stream: [not: valid: yaml", encoding="utf-8")

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
    )

    assert exit_code == 2
    preflight = read_preflight(data_dir)
    assert preflight["status"] == "preflight_blocked"
    assert preflight["config"]["exists"] is True
    assert preflight["config"]["parse_ok"] is False
    assert "config.yaml" in preflight["missing_inputs"]
    assert_publication_safe(json.dumps(preflight) + bundle_text(data_dir))


def test_alternate_configured_matrix_token_env_is_checked_and_passed_by_name(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    token_env = "ALT_MATRIX_TOKEN"
    write_config(config_path, access_token_env=token_env)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert_capture_once_docker_command(command, matrix_token_env=token_env)
        write_jpeg(data_dir / "latest.jpg")
        write_jpeg(data_dir / "debug_latest.jpg")
        return subprocess.CompletedProcess(command, 0, json.dumps({"event": "detection-frame-processed", "detection_count": 0, "accepted_count": 0}), "")

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, token_env: SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 0
    manifest = read_manifest(data_dir)
    rendered = json.dumps(manifest) + bundle_text(data_dir)
    assert "Matrix token env key" in rendered
    assert token_env not in json.dumps(manifest["docker_command"])
    assert_publication_safe(rendered)


def test_docker_nonzero_preserves_exit_code_and_writes_redacted_partial_bundle(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            17,
            stdout=f"capture-frame-written leaked {SECRET_RTSP}\nraw_secret_line token={SECRET_TOKEN}\n",
            stderr=f"{SECRET_AUTH_HEADER}\nTraceback (most recent call last): hidden\n",
        )

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 17
    manifest = read_manifest(data_dir)
    assert manifest["status"] == "docker_failed"
    assert manifest["phase"] == "docker"
    assert manifest["docker_exit_code"] == 17
    assert manifest["redaction_scan"]["secret_occurrences"] == 0
    assert_publication_safe(json.dumps(manifest) + bundle_text(data_dir))


def test_docker_timeout_returns_124_and_writes_redacted_partial_bundle(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(
            command,
            timeout=3,
            output=f"partial stdout {SECRET_RTSP}",
            stderr=f"{SECRET_AUTH_HEADER}\nTraceback (most recent call last): hidden",
        )

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
        extra_args=["--docker-timeout-seconds", "3"],
    )

    assert exit_code == 124
    manifest = read_manifest(data_dir)
    assert manifest["status"] == "docker_timeout"
    assert manifest["phase"] == "docker"
    assert manifest["docker_exit_code"] is None
    assert manifest["timeout_seconds"] == 3
    assert_publication_safe(json.dumps(manifest) + bundle_text(data_dir))


def test_invalid_jpeg_causes_validation_failure_with_explicit_artifact_error(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "latest.jpg").write_bytes(b"not a jpeg")
        write_jpeg(data_dir / "debug_latest.jpg")
        return subprocess.CompletedProcess(command, 0, json.dumps({"event": "detection-frame-processed", "detection_count": 1, "accepted_count": 1}), "")

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 1
    manifest = read_manifest(data_dir)
    assert manifest["status"] == "validation_failed"
    assert manifest["phase"] == "validation"
    assert manifest["artifacts"]["raw_frame"]["exists"] is True
    assert manifest["artifacts"]["raw_frame"]["valid_jpeg"] is False
    assert manifest["artifacts"]["raw_frame"]["error_type"] in {"UnidentifiedImageError", "OSError"}


def test_detection_failure_event_is_failed_partial_bundle_not_empty_detection_summary(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        write_jpeg(data_dir / "latest.jpg")
        write_jpeg(data_dir / "debug_latest.jpg")
        return subprocess.CompletedProcess(
            command,
            0,
            "\n".join(
                [
                    json.dumps({"event": "capture-frame-written", "path": "/data/latest.jpg"}),
                    "malformed non-json docker line should be retained only in redacted logs",
                    json.dumps({"event": "detection-frame-failed", "error_type": "DetectionError", "message": f"failed near {SECRET_RTSP}"}),
                ]
            ),
            "",
        )

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 1
    manifest = read_manifest(data_dir)
    assert manifest["status"] == "partial_bundle"
    assert manifest["phase"] == "detection"
    assert manifest["detection_summary"] is None
    assert manifest["detection_failure"]["event"] == "detection-frame-failed"
    assert manifest["redaction_scan"]["secret_occurrences"] == 0
    assert_publication_safe(json.dumps(manifest) + bundle_text(data_dir))


def test_missing_detection_summary_is_validation_failure(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        write_jpeg(data_dir / "latest.jpg")
        write_jpeg(data_dir / "debug_latest.jpg")
        return subprocess.CompletedProcess(command, 0, json.dumps({"event": "capture-frame-written", "path": "/data/latest.jpg"}), "")

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 1
    manifest = read_manifest(data_dir)
    assert manifest["status"] == "validation_failed"
    assert manifest["phase"] == "validation"
    assert manifest["detection_summary"] is None
    assert "detection-frame-processed" in manifest["validation_errors"]


def test_absent_health_and_state_are_context_gaps_not_capture_failure(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        write_jpeg(data_dir / "latest.jpg")
        write_jpeg(data_dir / "debug_latest.jpg")
        return subprocess.CompletedProcess(command, 0, json.dumps({"event": "detection-frame-processed", "detection_count": 0, "accepted_count": 0}), "")

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN},
        run=fake_run,
    )

    assert exit_code == 0
    manifest = read_manifest(data_dir)
    assert manifest["status"] == "success"
    assert manifest["context"]["health_json_present"] is False
    assert manifest["context"]["state_json_present"] is False
    assert "health.json missing" in manifest["context"]["gaps"]
    assert "state.json missing" in manifest["context"]["gaps"]


def test_redaction_scan_blocks_publication_when_secret_reaches_text_artifacts(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    data_dir = tmp_path / "data"
    write_config(config_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        write_jpeg(data_dir / "latest.jpg")
        write_jpeg(data_dir / "debug_latest.jpg")
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"event": "detection-frame-processed", "detection_count": 1, "accepted_count": 1}),
            "this line contains raw_secret_line and must be redacted",
        )

    exit_code = invoke(
        tmp_path,
        config_path=config_path,
        data_dir=data_dir,
        environ={"RTSP_URL": SECRET_RTSP, "MATRIX_ACCESS_TOKEN": SECRET_TOKEN, "EXTRA_SECRET": "raw_secret_line"},
        run=fake_run,
    )

    assert exit_code == 0
    manifest = read_manifest(data_dir)
    rendered = json.dumps(manifest) + bundle_text(data_dir)
    assert manifest["redaction_scan"]["secret_occurrences"] == 0
    assert_publication_safe(rendered)
