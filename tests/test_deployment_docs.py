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
        "git pull --ff-only",
        "docker compose up -d --no-build --force-recreate parking-spot-monitor",
        "data/health.json",
        "data/state.json",
        "data/matrix-outbox.json",
        "config.yaml",
        ".env",
        "/dev/dri",
        "frame_interval_seconds",
        "stable_frame_interval_seconds",
        "mkdir -p models",
        "sha256sum models/yolov8n.pt",
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

    checksum = runbook.index("sha256sum models/yolov8n.pt")
    validation = runbook.index("--validate-config", checksum)
    deployment = runbook.index("docker compose up -d", validation)

    assert checksum < validation < deployment
    assert "compare" in runbook[checksum:validation].lower()


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


def test_rollback_restores_and_validates_model_pair_before_recreate() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("## Rollback", 1)[1].split(
        "## Troubleshooting deployment failures", 1
    )[0]
    workflow = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)[0]

    restore_config = workflow.index('cp -- "$ROLLBACK_DIR/config.yaml" config.yaml')
    restore_model = workflow.index('cp -- "$ROLLBACK_DIR/yolov8n.pt" models/yolov8n.pt')
    validate = workflow.index("--validate-config")
    recreate = workflow.index("docker compose up -d --no-build --force-recreate")

    assert max(restore_config, restore_model) < validate < recreate
    syntax = subprocess.run(
        ["bash", "-n"], input=workflow, text=True, capture_output=True, check=False
    )
    assert syntax.returncode == 0, syntax.stderr


def test_backup_precedes_upgrade_rollback_and_troubleshooting() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")

    backup = runbook.index("## Backup and recovery")
    upgrade = runbook.index("## Safe upgrade")
    rollback = runbook.index("## Rollback")
    troubleshooting = runbook.index("## Troubleshooting deployment failures")

    assert backup < upgrade < rollback < troubleshooting


def test_backup_workflow_captures_model_checksum_config_data_and_image_identity() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("## Backup and recovery", 1)[1].split(
        "## Troubleshooting deployment failures", 1
    )[0]
    workflow = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)[0]

    for required in [
        "cp -- config.yaml",
        "cp -- models/yolov8n.pt",
        "sha256sum yolov8n.pt",
        "docker image inspect parking-spot-monitor:local",
        "cp -a -- data",
    ]:
        assert required in workflow
    syntax = subprocess.run(
        ["bash", "-n"], input=workflow, text=True, capture_output=True, check=False
    )
    assert syntax.returncode == 0, syntax.stderr


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
