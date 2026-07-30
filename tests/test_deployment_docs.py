from __future__ import annotations

from pathlib import Path
import re
import subprocess


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
    }
    assert "rtsp://" not in template
    assert "Bearer " not in template
    assert "syt_" not in template


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
