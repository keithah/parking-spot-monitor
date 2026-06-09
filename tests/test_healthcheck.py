from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from parking_spot_monitor.healthcheck import check_health_file


def test_healthcheck_accepts_fresh_ok_health_file(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(
        json.dumps({"status": "ok", "updated_at": datetime.now(timezone.utc).isoformat()}),
        encoding="utf-8",
    )

    assert check_health_file(path, max_age_seconds=60) == 0


def test_healthcheck_rejects_stale_health_file(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(
        json.dumps({"status": "ok", "updated_at": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()}),
        encoding="utf-8",
    )

    assert check_health_file(path, max_age_seconds=60) == 1
