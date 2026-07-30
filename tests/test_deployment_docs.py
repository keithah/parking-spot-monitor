from __future__ import annotations

from pathlib import Path
import re
import subprocess

import pytest


def test_deployment_runbook_is_discoverable_and_actionable() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")

    assert "[Docker deployment and operations](docs/deployment.md)" in readme
    for required in [
        "docker compose build parking-spot-monitor",
        "docker compose run --rm parking-spot-monitor",
        "--validate-config",
        "docker compose up -d --build parking-spot-monitor",
        "docker compose ps",
        "docker compose logs --tail 100 parking-spot-monitor",
        "docker stats --no-stream",
        "$(docker compose ps -q parking-spot-monitor)",
        "docker compose config --quiet",
        "scripts/deployment_operations.py upgrade",
        "scripts/deployment_operations.py rollback",
        "data/health.json",
        "data/state.json",
        "data/matrix-outbox.json",
        "config.yaml",
        ".env",
        "/dev/dri",
        "frame_interval_seconds",
        "stable_frame_interval_seconds",
        "--approved-model-sha256",
        "trusted artifact source",
    ]:
        assert required in runbook


def test_operator_docs_name_current_locked_docker_stages() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")

    for stage in ("python-base", "tooling", "capture-base", "runtime-app", "runtime-detector"):
        assert f"`{stage}`" in readme
    assert "Docker uses `requirements-runtime.lock`" in readme
    assert "`requirements-detector.lock`" in readme
    assert "smaller `runtime-app` target" in runbook
    assert "`runtime-base` target" not in readme
    assert "--target runtime-base" not in readme
    assert "`runtime-base` target" not in runbook


def test_environment_template_names_only_supported_compose_variables() -> None:
    template = Path(".env.example").read_text(encoding="utf-8")
    assignments = {
        line.split("=", 1)[0]
        for line in template.splitlines()
        if line and not line.startswith("#") and "=" in line
    }

    assert assignments == {
        "RTSP_URL",
        "RTSP_URL_4K",
        "RTSP_URL_360P",
        "MATRIX_ACCESS_TOKEN",
        "MODEL_DIR",
    }
    assert "rtsp://" not in template
    assert "Bearer " not in template
    assert "syt_" not in template


def test_compose_mounts_read_only_model_directory() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    config = Path("config.yaml.example").read_text(encoding="utf-8")
    template = Path(".env.example").read_text(encoding="utf-8")

    assert "${MODEL_DIR:-./models}:/models:ro" in compose
    assert "model: /models/yolov8n.pt" in config
    assert "MODEL_DIR=./models" in template


def test_deployment_documents_model_preflight_before_start() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")

    checksum = runbook.index('sha256sum "$model_dir/yolov8n.pt"')
    validation = runbook.index("--validate-config", checksum)
    deployment = runbook.index("docker compose up -d", validation)

    assert checksum < validation < deployment
    assert "compare" in runbook[checksum:validation].lower()


def test_model_directory_resolution_supports_default_and_custom_environment() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    resolution = 'model_dir="${MODEL_DIR:-./models}"'

    assert resolution in runbook
    for environ, expected in [({}, "./models"), ({"MODEL_DIR": "/srv/models"}, "/srv/models")]:
        resolved = subprocess.run(
            ["bash", "-c", f'{resolution}\nprintf "%s" "$model_dir"'],
            text=True,
            capture_output=True,
            check=False,
            env=environ,
        )
        assert resolved.returncode == 0, resolved.stderr
        assert resolved.stdout == expected

def test_recovery_operations_are_helper_backed_and_ordered() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")

    headings = [
        "## Backup and recovery",
        "### Create the protected pre-upgrade bundle",
        "### Upgrade the exact reviewed revision",
        "### Roll back image, config, environment, and model",
        "### Restore the complete data recovery point",
        "## Troubleshooting deployment failures",
    ]
    positions = [runbook.index(heading) for heading in headings]
    assert positions == sorted(positions)
    for operation in ("backup", "upgrade", "rollback", "restore-data"):
        assert f"scripts/deployment_operations.py {operation}" in runbook
    assert "running service container" in runbook
    assert "operator-supplied digest establishes provenance" in runbook
    assert "co-created manifests as signatures or authentication" in runbook
    assert "immediately before recreation" in runbook
    assert "Only after that freshness gate" in runbook


def test_deployment_helper_exposes_all_documented_operations() -> None:
    for operation in ("backup", "upgrade", "rollback", "restore-data"):
        result = subprocess.run(
            ["python3", "scripts/deployment_operations.py", operation, "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr


def test_runbook_does_not_duplicate_transactional_implementation() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    operations = runbook.split("## Backup and recovery", 1)[1].split(
        "## Troubleshooting deployment failures", 1
    )[0]

    assert len(operations.splitlines()) < 120
    assert "def wait_for_fresh_health" not in operations
    assert "recover_failed_rollback" not in operations
    assert "restore_on_failure" not in operations


@pytest.mark.parametrize(
    "artifact",
    [
        "models/custom.bin",
        "weights/yolo.pt",
        "weights/yolo.PT",
        "weights/yolo.pTh",
        "weights/yolo.ONNX",
        "weights/yolo.SafeTensors",
    ],
)
def test_git_ignores_operator_model_artifacts(artifact: str) -> None:
    ignored = subprocess.run(
        ["git", "check-ignore", "--no-index", "--quiet", "--", artifact],
        text=True,
        capture_output=True,
        check=False,
    )

    assert ignored.returncode == 0, artifact


def test_deployment_docs_do_not_embed_live_secret_or_traceback_markers() -> None:
    rendered = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in ["docs/deployment.md", ".env.example"]
    )

    for forbidden in [
        "rtsp://",
        "Authorization: Bearer",
        "syt_",
        "mxc://",
        "Traceback (most recent call last)",
    ]:
        assert forbidden not in rendered


def test_dependency_lock_workflow_is_one_cleanup_scoped_subshell() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("## Dependency lock maintenance", 1)[1].split(
        "### Check freshness without package-index access", 1
    )[0]
    blocks = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)

    assert len(blocks) == 1
    workflow = blocks[0]
    assert workflow.startswith("(\n")
    assert workflow.rstrip().endswith(")")
    assert "trap cleanup_lock_tools EXIT" in workflow
    assert workflow.count("mktemp -d") == 2
    assert "--stage-build-lock" in workflow
    assert "requirements-build.next.lock" in workflow
    assert "all three locks" in section.lower()
    assert "two generated manifests" not in section.lower()
    assert "both locks" not in section.lower()
    syntax = subprocess.run(
        ["bash", "-n"], input=workflow, text=True, capture_output=True, check=False
    )
    assert syntax.returncode == 0, syntax.stderr
