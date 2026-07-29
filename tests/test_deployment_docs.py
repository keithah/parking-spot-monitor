from __future__ import annotations

from pathlib import Path


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
