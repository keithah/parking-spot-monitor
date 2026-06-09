from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def check_health_file(path: str | Path, *, max_age_seconds: float) -> int:
    health_path = Path(path)
    try:
        payload = json.loads(health_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 1
    if not isinstance(payload, dict):
        return 1
    status = payload.get("status")
    if status not in {"ok", "starting", "degraded"}:
        return 1
    updated_at = _parse_timestamp(payload.get("updated_at"))
    if updated_at is None:
        return 1
    age_seconds = (datetime.now(timezone.utc) - updated_at).total_seconds()
    if age_seconds < 0:
        return 1
    return 0 if age_seconds <= max_age_seconds else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate parking monitor health JSON freshness.")
    parser.add_argument("--health-file", default="/data/health.json")
    parser.add_argument("--max-age-seconds", type=float, default=120.0)
    args = parser.parse_args(argv)
    return check_health_file(args.health_file, max_age_seconds=args.max_age_seconds)


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
