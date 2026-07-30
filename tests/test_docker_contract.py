from __future__ import annotations

import fnmatch
import importlib.util
import re
import sys
import tomllib
from pathlib import Path

import yaml


PYTHON_BASE_IMAGE = (
    "python:3.12-slim@sha256:"
    "090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203"
)


def _docker_stages(dockerfile: str) -> list[tuple[str, str, str]]:
    matches = list(
        re.finditer(
            r"^FROM ([^\s]+) AS ([A-Za-z0-9._-]+)\s*$",
            dockerfile,
            flags=re.MULTILINE,
        )
    )
    return [
        (
            match.group(1),
            match.group(2),
            dockerfile[match.end() : matches[index + 1].start()]
            if index + 1 < len(matches)
            else dockerfile[match.end() :],
        )
        for index, match in enumerate(matches)
    ]


def _docker_context_path_is_allowed(path: str, patterns: list[str]) -> bool:
    allowed = True
    for pattern in patterns:
        negated = pattern.startswith("!")
        candidate = pattern[1:] if negated else pattern
        matches = fnmatch.fnmatchcase(path, candidate)
        if candidate.endswith("/**"):
            directory = candidate.removesuffix("/**")
            if path == directory or path.startswith(directory + "/"):
                matches = True
        elif candidate.endswith("/"):
            directory = candidate.rstrip("/")
            if path == directory or path.startswith(directory + "/"):
                matches = True
        if matches:
            allowed = negated
    return allowed


SECRET_LIKE_STRINGS = [
    "rtsp://",
    "camera-secret",
    "matrix-secret",
    "should-not-leak",
]

FORBIDDEN_SPAM_SENTINELS = [
    "Traceback (most recent call last)",
    "BEGIN RAW IMAGE BYTES",
    "END RAW IMAGE BYTES",
]


def test_example_config_uses_mount_relative_runtime_paths() -> None:
    config = yaml.safe_load(Path("config.yaml.example").read_text(encoding="utf-8"))

    assert config["storage"]["data_dir"] == "./data"
    assert config["storage"]["snapshots_dir"] == "snapshots"
    assert config["runtime"]["health_file"] == "health.json"


def test_readme_documents_mount_relative_runtime_path_contract() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "snapshots_dir: snapshots" in readme
    assert "health_file: health.json" in readme
    assert "relative to the effective `--data-dir`" in readme
    assert "/data/snapshots" in readme
    assert "/data/health.json" in readme
    assert "./data/snapshots" in readme
    assert "./data/health.json" in readme


def test_dockerfile_installs_runtime_and_defaults_to_package_entrypoint() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    requirements = Path("requirements.txt").read_text(encoding="utf-8")
    detector_requirements = Path("requirements-detector.txt").read_text(encoding="utf-8")
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    detector_extra = pyproject["project"]["optional-dependencies"]["detector"]

    assert "FROM python:3.12-slim@sha256:" in dockerfile or "FROM python:3.11-slim@sha256:" in dockerfile
    assert "ffmpeg" in dockerfile
    assert "intel-media-va-driver" in dockerfile
    assert "vainfo" not in dockerfile
    assert "LIBVA_DRIVER_NAME=iHD" in dockerfile
    assert "FROM python:3.12-slim@sha256:" in dockerfile or "FROM python:3.11-slim@sha256:" in dockerfile
    assert " AS python-base" in dockerfile
    assert "FROM capture-base AS runtime-app" in dockerfile
    assert "FROM capture-base AS runtime-detector" in dockerfile
    assert "COPY requirements-runtime.lock ./" in dockerfile
    assert "pip install --require-hashes -r requirements-runtime.lock" in dockerfile
    assert "ultralytics>=8" not in requirements
    assert "ultralytics==" in detector_requirements
    assert "--extra-index-url https://download.pytorch.org/whl/cpu" in detector_requirements
    assert "torch==2.7.1+cpu" in detector_requirements
    assert "torchvision==0.22.1+cpu" in detector_requirements
    detector_requirement_packages = [
        line.strip()
        for line in detector_requirements.splitlines()
        if line.strip() and not line.startswith("--")
    ]
    assert detector_extra == ["torch==2.7.1+cpu", "torchvision==0.22.1+cpu", "ultralytics==8.4.60"]
    assert "ultralytics==8.4.60" in detector_requirement_packages
    base_stage, detector_stage = dockerfile.split("FROM capture-base AS runtime-detector", 1)
    assert "requirements-detector.lock" not in base_stage
    assert "COPY requirements-detector.lock ./" in detector_stage
    assert "pip install --require-hashes -r requirements-detector.lock" in detector_stage
    assert "COPY parking_spot_monitor ./parking_spot_monitor" in dockerfile
    assert 'CMD ["python", "-m", "parking_spot_monitor", "--config", "/config/config.yaml"]' in dockerfile


def test_dockerfile_uses_buildkit_cache_hash_locks_and_compileall() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert dockerfile.startswith("# syntax=docker/dockerfile:1.7\n")
    assert "--mount=type=cache,target=/root/.cache/pip" in dockerfile
    assert "pip install --require-hashes -r requirements-runtime.lock" in dockerfile
    assert "pip install --require-hashes -r requirements-detector.lock" in dockerfile
    assert "python -m compileall -q /app/parking_spot_monitor /app/src" in dockerfile


def test_dockerfile_has_exact_ordered_stage_graph_and_pinned_base() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    stages = _docker_stages(dockerfile)

    assert [(base, alias) for base, alias, _body in stages] == [
        (PYTHON_BASE_IMAGE, "python-base"),
        ("python-base", "tooling"),
        ("python-base", "capture-base"),
        ("capture-base", "runtime-app"),
        ("capture-base", "runtime-detector"),
    ]


def test_lock_installs_are_exactly_owned_by_their_intended_stages() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    stages = {alias: body for _base, alias, body in _docker_stages(dockerfile)}
    cache_mount = "--mount=type=cache,target=/root/.cache/pip,sharing=locked"

    assert dockerfile.count(cache_mount) == 2
    assert stages["python-base"].count(cache_mount) == 1
    assert stages["python-base"].count("COPY requirements-runtime.lock ./") == 1
    assert stages["python-base"].count(
        "pip install --require-hashes -r requirements-runtime.lock"
    ) == 1
    assert stages["runtime-detector"].count(cache_mount) == 1
    assert stages["runtime-detector"].count("COPY requirements-detector.lock ./") == 1
    assert stages["runtime-detector"].count(
        "pip install --require-hashes -r requirements-detector.lock"
    ) == 1
    for alias in ("tooling", "capture-base", "runtime-app"):
        assert "requirements-runtime.lock" not in stages[alias]
        assert "requirements-detector.lock" not in stages[alias]
        assert cache_mount not in stages[alias]


def test_dockerfile_has_lightweight_tooling_and_capture_stage_boundary() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert " AS python-base" in dockerfile
    assert "FROM python-base AS tooling" in dockerfile
    assert "FROM python-base AS capture-base" in dockerfile
    assert "FROM capture-base AS runtime-app" in dockerfile
    assert "FROM capture-base AS runtime-detector" in dockerfile
    tooling = dockerfile.split("FROM python-base AS tooling", 1)[1].split(
        "FROM python-base AS capture-base", 1
    )[0]
    assert "ffmpeg" not in tooling
    assert "intel-media-va-driver" not in tooling


def test_each_final_docker_stage_copies_source_once_and_compiles_it() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    stages = {alias: body for _base, alias, body in _docker_stages(dockerfile)}
    app_stage = stages["runtime-app"]
    detector_stage = stages["runtime-detector"]

    for stage in (app_stage, detector_stage):
        assert stage.count("COPY parking_spot_monitor ./parking_spot_monitor") == 1
        assert stage.count("COPY src ./src") == 1
        assert stage.count("COPY main.py config.yaml.example ./") == 1
        assert stage.count(
            "python -m compileall -q /app/parking_spot_monitor /app/src"
        ) == 1
    for alias in ("python-base", "tooling", "capture-base"):
        assert "COPY parking_spot_monitor" not in stages[alias]
        assert "COPY src" not in stages[alias]
        assert "COPY main.py" not in stages[alias]
        assert "COPY config.yaml.example" not in stages[alias]


def test_dockerignore_default_deny_excludes_non_build_context_paths() -> None:
    patterns = Path(".dockerignore").read_text(encoding="utf-8").splitlines()

    for excluded in [
        ".git/config",
        ".gsd/state.json",
        ".worktrees/review/file.py",
        ".superpowers/sdd/report.md",
        ".env",
        "config.yaml",
        "data/latest.jpg",
        "docs/deployment.md",
        "models/yolov8n.pt",
        "README.md",
        "scripts/lock_dependencies.py",
        "tests/test_docker_contract.py",
        "untracked.tmp",
        "parking_spot_monitor/debug.tmp",
        "parking_spot_monitor/__pycache__/config.cpython-312.pyc",
        "src/parking_monitor/__pycache__/outbox.cpython-312.pyc",
    ]:
        assert _docker_context_path_is_allowed(excluded, patterns) is False


def test_dockerignore_allows_every_consumed_build_input() -> None:
    patterns = Path(".dockerignore").read_text(encoding="utf-8").splitlines()

    for included in [
        "Dockerfile",
        ".dockerignore",
        "requirements-runtime.lock",
        "requirements-detector.lock",
        "parking_spot_monitor/__main__.py",
        "src/parking_monitor/__init__.py",
        "main.py",
        "config.yaml.example",
    ]:
        assert _docker_context_path_is_allowed(included, patterns) is True


def test_dockerignore_is_exact_default_deny_build_input_allowlist() -> None:
    dockerignore = Path(".dockerignore").read_text(encoding="utf-8").splitlines()

    assert dockerignore == [
        "**",
        "!Dockerfile",
        "!.dockerignore",
        "!requirements-runtime.lock",
        "!requirements-detector.lock",
        "!parking_spot_monitor/",
        "!parking_spot_monitor/**",
        "!src/",
        "!src/**",
        "!main.py",
        "!config.yaml.example",
        "**/__pycache__/",
        "**/*.py[cod]",
        "**/*.tmp",
    ]
    assert not any(
        forbidden in pattern
        for pattern in dockerignore[1:]
        if pattern.startswith("!")
        for forbidden in (
            "scripts",
            "docs",
            "README",
            ".env",
            "models",
            "data",
            "tests",
            "tmp",
        )
    )


def test_compose_contract_mounts_config_data_and_uses_capture_runtime() -> None:
    compose_text = Path("docker-compose.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)
    service = compose["services"]["parking-spot-monitor"]

    assert "./config.yaml:/config/config.yaml:ro" in service["volumes"]
    assert "./data:/data" in service["volumes"]
    assert "env_file" not in service
    assert service["environment"] == ["RTSP_URL", "RTSP_URL_4K", "RTSP_URL_360P", "MATRIX_ACCESS_TOKEN", "TZ=America/Los_Angeles"]
    assert service["command"] == [
        "python",
        "-m",
        "parking_spot_monitor",
        "--config",
        "/config/config.yaml",
        "--data-dir",
        "/data",
    ]
    assert "--validate-config" not in service["command"]
    assert service["devices"] == ["/dev/dri:/dev/dri"]
    assert service["restart"] == "unless-stopped"
    assert "/dev/dri:/dev/dri" in compose_text
    assert "#   - ./models:/models:ro" in compose_text


def test_readme_documents_final_operator_verification_contract() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    for required in [
        "Model storage policy",
        "detection.model` accepts local model names",
        "rejects URL-like values",
        "/models/yolov8n.pt",
        "First-run Ultralytics downloads are allowed",
        "can block startup",
        "./models:/models:ro",
        "M001 keeps the container running as root",
        "non-root container hardening",
        "python -m parking_spot_monitor --config config.yaml --validate-config",
        "python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once",
        "python scripts/verify_live_proof.py",
        "python scripts/verify_hardware_decode.py --json",
        "python -m json.tool data/health.json",
        "find data/snapshots",
        "docker build -t parking-spot-monitor:test .",
        "docker compose config",
        "R015 evidence",
        "VAAPI should initialize on Intel Iris Xe",
        "QSV may still fail",
        "selected_mode=vaapi",
        "hardware_decode_status=vaapi_supported_qsv_unavailable",
        "qsv_required_but_unavailable",
        "verifier_timeout",
    ]:
        assert required in readme


def test_readme_pins_health_shape_retention_and_live_proof_markers() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    for required in [
        "status",
        "iteration",
        "last_frame_at",
        "selected_decode_mode",
        '"capture"',
        "last_success_at",
        "consecutive_capture_failures",
        "consecutive_detection_failures",
        "last_matrix_error",
        "retention_failure_count",
        "state_save_error",
        "last_error",
        "snapshot_retention_count: 50",
        "LIVE_PROOF_SKIPPED_CONFIG_ABSENT",
        "LIVE_RTSP_CAPTURE_OK",
        "LIVE_MATRIX_TEXT_OK",
        "LIVE_MATRIX_IMAGE_OK",
        "LIVE_RTSP_CAPTURE_FAILED",
        "LIVE_MATRIX_TEXT_FAILED",
        "LIVE_MATRIX_IMAGE_FAILED",
    ]:
        assert required in readme


def test_readme_documents_finite_validation_and_capture_smoke_commands() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once" in readme
    assert "docker compose run --rm parking-spot-monitor" in readme
    assert "--capture-once" in readme
    assert "finite capture proof" in readme
    assert "/data/latest.jpg" in readme
    assert "./data/latest.jpg" in readme
    assert "latest.jpg` is the raw full-frame camera evidence" in readme
    assert "Keep it unannotated" in readme
    assert "structured" in readme
    assert "fallback" in readme


def test_readme_documents_local_yolo_detection_and_deferred_live_tuning() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "## Local YOLO detection" in readme
    assert "pins Ultralytics" in readme
    assert "python-base" in readme
    assert "capture-base" in readme
    assert "runtime-app" in readme
    assert "runtime-detector" in readme
    assert "YOLO nano" in readme
    assert "detection-frame-processed" in readme
    assert "detection-frame-failed" in readme
    assert "accepted candidate summaries" in readme
    assert "rejection reason counts" in readme
    assert "Unit tests use fake YOLO result objects" in readme
    assert "normal test runs do not download weights or run real inference" in readme
    assert "Live camera accuracy proof" in readme
    assert "detection.model allowlisting" in readme
    assert "non-root container hardening" in readme
    assert "deferred to S07" in readme


def test_readme_documents_runtime_occupancy_state_and_schedule_events() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "/data/state.json" in readme
    assert "street_sweeping" in readme
    assert "first and third Monday" in readme
    assert "13:00" in readme and "15:00" in readme
    for event_name in [
        "occupancy-state-changed",
        "occupancy-open-event",
        "occupancy-open-suppressed",
        "quiet-window-started",
        "quiet-window-ended",
        "state-loaded",
        "state-saved",
        "state-corrupt-quarantined",
    ]:
        assert event_name in readme
    assert "S06" in readme
    assert "Matrix messages from these S05 event objects" in readme


def test_docker_contract_docs_and_compose_do_not_embed_secret_values() -> None:
    rendered = "\n".join(
        Path(path).read_text(encoding="utf-8") for path in ["Dockerfile", "docker-compose.yml", "README.md", ".gitignore"]
    )

    assert "config.yaml" in Path(".gitignore").read_text(encoding="utf-8")
    assert "RTSP_URL" in rendered
    assert "MATRIX_ACCESS_TOKEN" in rendered
    for secret_like in SECRET_LIKE_STRINGS:
        assert secret_like not in rendered
    for sentinel in FORBIDDEN_SPAM_SENTINELS:
        assert sentinel not in rendered


def test_readme_does_not_claim_production_image_installs_vainfo() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "image installs `intel-media-va-driver` and `vainfo`" not in readme
    assert "image installs `intel-media-va-driver`" in readme
    assert "`vainfo`" in readme
    assert "diagnostic" in readme


def _load_closeout_script_module():
    script_path = Path("scripts/verify_s05_operator_cockpit_closeout.py")
    spec = importlib.util.spec_from_file_location("verify_s05_operator_cockpit_closeout", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_s05_closeout_smoke_script_contract_is_bounded_and_redacted() -> None:
    script_path = Path("scripts/verify_s05_operator_cockpit_closeout.py")
    script = script_path.read_text(encoding="utf-8")
    module = _load_closeout_script_module()

    assert script_path.exists()
    assert "shell=True" not in script
    assert "subprocess.run(" in script
    assert "timeout=" in script
    assert "stdout=subprocess.PIPE" in script
    assert "stderr=subprocess.PIPE" in script
    assert "S05_CLOSEOUT_START" in script
    assert "S05_CLOSEOUT_PASS" in script
    assert "S05_CLOSEOUT_FAIL" in script
    assert "S05_CLOSEOUT_RESULT" in script

    commands = {command.label: command.argv for command in module.COMMANDS}
    assert commands["pytest-docs-matrix"] == (
        module.sys.executable,
        "-m",
        "pytest",
        "tests/test_operator_docs.py",
        "tests/test_matrix.py",
        "-q",
    )
    assert "tests/test_matrix_operator_cockpit.py" in commands["pytest-cockpit-lab-memory"]
    assert "tests/test_detection_lab.py" in commands["pytest-cockpit-lab-memory"]
    assert "tests/test_operator_decision_memory.py" in commands["pytest-cockpit-lab-memory"]
    assert "tests/test_startup.py" in commands["pytest-runtime-docker-config-state"]
    assert "tests/test_docker_contract.py" in commands["pytest-runtime-docker-config-state"]
    assert "tests/test_config.py" in commands["pytest-runtime-docker-config-state"]
    assert "tests/test_health.py" in commands["pytest-runtime-docker-config-state"]
    assert "tests/test_state.py" in commands["pytest-runtime-docker-config-state"]
    assert commands["validate-config-entrypoint"] == (
        module.sys.executable,
        "-m",
        "parking_spot_monitor",
        "--config",
        "config.yaml.example",
        "--validate-config",
    )
    assert commands["docker-compose-config"] == ("docker", "compose", "config", "--quiet")

    env = module._smoke_env({})
    assert env["RTSP_URL"] == module.PLACEHOLDER_RTSP_URL
    assert env["MATRIX_ACCESS_TOKEN"] == module.PLACEHOLDER_MATRIX_TOKEN

    redacted = module._safe_output(
        "rtsp://camera.local/stream MATRIX_ACCESS_TOKEN=matrix-secret Authorization: bearer-secret",
        f"{module.PLACEHOLDER_RTSP_URL} {module.PLACEHOLDER_MATRIX_TOKEN} Traceback (most recent call last)",
    )
    assert module.PLACEHOLDER_RTSP_URL not in redacted
    assert module.PLACEHOLDER_MATRIX_TOKEN not in redacted
    assert "rtsp://camera.local" not in redacted
    assert "matrix-secret" not in redacted
    assert "bearer-secret" not in redacted
    assert "Traceback (most recent call last)" not in redacted
