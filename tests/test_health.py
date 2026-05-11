from __future__ import annotations

import json
import stat
from pathlib import Path

from parking_spot_monitor.health import HealthStatus, write_health_status


def test_write_health_status_creates_host_readable_json(tmp_path: Path) -> None:
    path = tmp_path / "health.json"

    write_health_status(path, HealthStatus(status="ok", updated_at="2026-05-18T19:00:00Z", iteration=7))

    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "ok"
    assert stat.S_IMODE(path.stat().st_mode) == 0o644
