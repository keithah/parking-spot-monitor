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
        'git checkout --detach "$REVIEWED_REVISION"',
        "docker compose up -d --no-build --force-recreate parking-spot-monitor",
        "data/health.json",
        "data/state.json",
        "data/matrix-outbox.json",
        "config.yaml",
        ".env",
        "/dev/dri",
        "frame_interval_seconds",
        "stable_frame_interval_seconds",
        'mkdir -p "$model_dir"',
        'sha256sum "$model_dir/yolov8n.pt"',
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


def test_custom_model_directory_flows_through_stage_backup_and_rollback() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    deployment = runbook.split("## First deployment", 1)[1].split("## Routine operations", 1)[0]
    deployment_workflows = re.findall(r"```sh\n(.*?)```", deployment, flags=re.DOTALL)
    staging = deployment_workflows[1]
    backup = re.findall(
        r"```sh\n(.*?)```",
        runbook.split("## Backup and recovery", 1)[1].split("## Safe upgrade", 1)[0],
        flags=re.DOTALL,
    )[0]
    rollback = re.findall(
        r"```sh\n(.*?)```",
        runbook.split("## Rollback", 1)[1].split(
            "## Troubleshooting deployment failures", 1
        )[0],
        flags=re.DOTALL,
    )[0]
    upgrade = re.findall(
        r"```sh\n(.*?)```",
        runbook.split("## Safe upgrade", 1)[1].split("## Rollback", 1)[0],
        flags=re.DOTALL,
    )[0]

    for workflow in (*deployment_workflows[:5], backup, upgrade, rollback):
        assert workflow.startswith("(\n")
        assert workflow.rstrip().endswith(")")
        assert 'model_dir="${MODEL_DIR:-./models}"' in workflow
        assert 'export MODEL_DIR="$model_dir"' in workflow
        assert "source .env" not in workflow
        assert "eval " not in workflow
        syntax = subprocess.run(
            ["bash", "-n"], input=workflow, text=True, capture_output=True, check=False
        )
        assert syntax.returncode == 0, syntax.stderr

    assert 'test -f "$model_dir/yolov8n.pt"' in staging
    assert 'sha256sum "$model_dir/yolov8n.pt"' in staging
    assert 'test -f "$model_dir/yolov8n.pt"' in backup
    assert 'install -m 0600 -- "$model_dir/yolov8n.pt" "$staging_dir/yolov8n.pt"' in backup
    assert "sha256sum yolov8n.pt > yolov8n.pt.sha256" in backup
    assert 'mkdir -p "$model_dir"' in rollback
    assert 'install_atomic "$stage_dir/models/yolov8n.pt" "$model_dir/yolov8n.pt" 0644' in rollback
    assert "models/yolov8n.pt" not in staging
    assert "models/yolov8n.pt" not in backup
    assert "./models/yolov8n.pt" not in rollback


def test_custom_model_restore_precedes_container_validation_and_recreation() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("## Rollback", 1)[1].split(
        "## Troubleshooting deployment failures", 1
    )[0]
    workflow = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)[0]

    resolve = workflow.index('model_dir="${MODEL_DIR:-./models}"')
    stage = workflow.index(
        'install -m 0644 -- "$ROLLBACK_DIR/yolov8n.pt" "$stage_dir/models/yolov8n.pt"'
    )
    validate = workflow.index("--validate-config")
    stop = workflow.index("docker compose stop parking-spot-monitor", validate)
    restore = workflow.index(
        'install_atomic "$stage_dir/models/yolov8n.pt" "$model_dir/yolov8n.pt" 0644'
    )
    recreate = workflow.index(
        "docker compose up -d --no-build --force-recreate", restore
    )

    assert resolve < stage < validate < stop < restore < recreate


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

    stage_config = workflow.index(
        'install -m 0600 -- "$ROLLBACK_DIR/config.yaml" "$stage_dir/config.yaml"'
    )
    validate = workflow.index("--validate-config")
    stop = workflow.index("docker compose stop parking-spot-monitor", validate)
    restore_config = workflow.index(
        'install_atomic "$stage_dir/config.yaml" config.yaml 0600'
    )
    restore_model = workflow.index(
        'install_atomic "$stage_dir/models/yolov8n.pt" "$model_dir/yolov8n.pt" 0644'
    )
    recreate = workflow.index(
        "docker compose up -d --no-build --force-recreate",
        max(restore_config, restore_model),
    )

    assert stage_config < validate < stop < min(restore_config, restore_model) < recreate
    assert "recover_failed_rollback" in workflow
    assert 'docker image tag "$active_image_id" parking-spot-monitor:local' in workflow
    assert 'require_fresh_frame "$started_at"' in workflow
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
        'install -m 0600 -- config.yaml "$staging_dir/config.yaml"',
        'install -m 0600 -- "$env_file" "$staging_dir/.env"',
        'install -m 0600 -- "$model_dir/yolov8n.pt" "$staging_dir/yolov8n.pt"',
        "sha256sum yolov8n.pt > yolov8n.pt.sha256",
        "docker image inspect \"$ROLLBACK_TAG\"",
        'tar --format=pax -C data -cpf "$staging_dir/data.tar" .',
        "sha256sum data.tar > data.tar.sha256",
    ]:
        assert required in workflow
    syntax = subprocess.run(
        ["bash", "-n"], input=workflow, text=True, capture_output=True, check=False
    )
    assert syntax.returncode == 0, syntax.stderr


def test_backup_is_private_retry_safe_and_restarts_when_stop_is_interrupted() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("## Backup and recovery", 1)[1].split(
        "## Safe upgrade", 1
    )[0]
    workflow = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)[0]

    assert "umask 077" in workflow
    assert 'mkdir -m 0700 -- "$staging_dir"' in workflow
    assert 'chmod 0600 "$staging_dir"/* "$staging_dir/.env"' in workflow
    assert workflow.index("trap restart_after_backup EXIT") < workflow.index(
        "docker compose stop parking-spot-monitor"
    )
    assert "backup finished but the service did not restart" in workflow
    assert "docker compose start parking-spot-monitor >/dev/null 2>&1 || true" not in workflow
    assert workflow.index('mv -- "$staging_dir" "$BACKUP_DIR"') > workflow.index(
        "sha256sum -c bundle-files.sha256"
    )


def test_upgrade_checks_out_and_tests_exact_reviewed_revision_before_recreate() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("## Safe upgrade", 1)[1].split("## Rollback", 1)[0]
    workflow = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)[0]

    clean = workflow.index('test -z "$(git status --porcelain)"')
    fetch = workflow.index("git fetch --prune origin")
    checkout = workflow.index('git checkout --detach "$REVIEWED_REVISION"')
    tests = workflow.index("python3 -m pytest -q")
    build = workflow.index("docker compose build parking-spot-monitor")
    validate = workflow.index("--validate-config")
    recreate = workflow.index("docker compose up -d --no-build --force-recreate")

    assert clean < fetch < checkout < tests < build < validate < recreate
    assert 'docker image inspect "$ROLLBACK_TAG"' in workflow
    assert "last_frame_at" in workflow
    assert "no successful frame newer than this container start" in workflow


def test_data_restore_verifies_archive_and_preserves_current_tree() -> None:
    runbook = Path("docs/deployment.md").read_text(encoding="utf-8")
    section = runbook.split("### Restore the complete data archive", 1)[1].split(
        "## Troubleshooting deployment failures", 1
    )[0]
    workflow = re.findall(r"```sh\n(.*?)```", section, flags=re.DOTALL)[0]

    checksum = workflow.index("sha256sum -c data.tar.sha256")
    extract = workflow.index('tar --no-same-owner -C "$restore_dir" -xpf')
    stop = workflow.index("docker compose stop parking-spot-monitor")
    preserve = workflow.index('mv -- "$data_dir" "$preserved_dir"')
    activate = workflow.index('mv -- "$restore_dir" "$data_dir"')

    assert checksum < extract < stop < preserve < activate
    assert "restore_on_failure" in workflow
    assert 'mv -- "$data_dir" "$failed_dir"' in workflow
    assert 'mv -- "$preserved_dir" "$data_dir"' in workflow
    assert 'require_fresh_frame "$started_at"' in workflow
    assert workflow.index('require_fresh_frame "$started_at"') < workflow.index(
        "trap - EXIT HUP INT TERM"
    )
    assert "preserved service did not restart" in workflow
    assert "rm -rf" not in workflow


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
