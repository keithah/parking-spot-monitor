from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from parking_spot_monitor.vehicle_history import (
    ArchiveSchemaError,
    ArchiveWriteError,
    VehicleHistoryArchive,
    cutoff_older_than_days,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m parking_spot_monitor.vehicle_history_cli",
        description="Manage the local vehicle-history archive without exposing raw diagnostics.",
    )
    parser.add_argument("--data-dir", default="./data", help="Runtime data directory containing vehicle-history/ (default: ./data).")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Write a tar.gz bundle plus metadata-only maintenance manifest.")
    export_parser.add_argument("--output", required=True, help="Destination .tar.gz path for the operator-owned bundle.")

    prune_parser = subparsers.add_parser("prune", help="Dry-run or apply pruning for closed sessions older than a cutoff.")
    cutoff = prune_parser.add_mutually_exclusive_group(required=True)
    cutoff.add_argument("--older-than", help="ISO timestamp cutoff; closed sessions ending before this are candidates.")
    cutoff.add_argument("--older-than-days", type=int, help="Candidate cutoff as whole days before the current UTC time.")
    mode = prune_parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report what would be pruned without deleting files.")
    mode.add_argument("--apply", action="store_true", help="Delete candidate closed-session metadata and unreferenced images.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    archive = VehicleHistoryArchive(Path(args.data_dir))
    try:
        if args.command == "export":
            result = archive.export_archive(args.output)
        elif args.command == "prune":
            if args.older_than_days is not None:
                cutoff = cutoff_older_than_days(args.older_than_days, now=datetime.now(timezone.utc))
            else:
                cutoff = args.older_than
            result = archive.prune_closed_sessions(older_than=cutoff, dry_run=not args.apply)
        else:  # pragma: no cover - argparse prevents this branch.
            parser.error("unsupported command")
    except (ArchiveSchemaError, ArchiveWriteError, OSError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error_message": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result.to_json_dict(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
