from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_OPERATOR_EVIDENCE_PATHS = [
    "data/live-proof-result.json",
    "data/live-proof-input-preflight.json",
    "data/live-proof-evidence.md",
    "data/alert-soak-result.json",
    "data/alert-soak-input-preflight.json",
    "data/alert-soak-evidence.md",
    "data/replay-report.json",
    "data/replay-report.md",
    "data/tuning-report.json",
    "data/tuning-report.md",
    "data/m002-validation/m002-validation-package.json",
    "data/m002-validation/m002-validation-package.md",
]
S07_EVIDENCE_PATHS = [
    "data/s07-replay-evidence/real-traffic-labels.yaml",
    "data/s07-replay-evidence/proposed-config.yaml",
    "data/s07-replay-evidence/replay/replay-report.json",
    "data/s07-replay-evidence/replay/replay-report.md",
    "data/s07-replay-evidence/tuning/tuning-report.json",
    "data/s07-replay-evidence/tuning/tuning-report.md",
    "data/s07-replay-evidence/coverage/s07-evidence-report.json",
    "data/s07-replay-evidence/coverage/s07-evidence-report.md",
]
S07_NON_EVIDENCE_PATHS = [
    "data/s07-replay-evidence-notes.md",
    "data/s07-replay-evidence-public/replay-report.json",
]


def assert_paths_are_gitignored(paths: list[str], description: str) -> None:
    publishable_paths: list[str] = []

    for relative_path in paths:
        completed = subprocess.run(
            ["git", "check-ignore", "--quiet", relative_path],
            cwd=REPO_ROOT,
            check=False,
        )
        if completed.returncode != 0:
            publishable_paths.append(relative_path)

    assert not publishable_paths, f"{description} must be ignored by Git: " + ", ".join(
        publishable_paths
    )


def assert_paths_are_not_gitignored(paths: list[str], description: str) -> None:
    ignored_paths: list[str] = []

    for relative_path in paths:
        completed = subprocess.run(
            ["git", "check-ignore", "--quiet", relative_path],
            cwd=REPO_ROOT,
            check=False,
        )
        if completed.returncode == 0:
            ignored_paths.append(relative_path)

    assert not ignored_paths, f"{description} must not be ignored by Git: " + ", ".join(
        ignored_paths
    )


def test_local_operator_evidence_artifacts_are_gitignored() -> None:
    assert_paths_are_gitignored(LOCAL_OPERATOR_EVIDENCE_PATHS, "Local operator evidence artifacts")


def test_s07_private_evidence_workspace_is_gitignored() -> None:
    assert_paths_are_gitignored(S07_EVIDENCE_PATHS, "Private S07 evidence artifacts")


def test_s07_ignore_rule_is_narrow() -> None:
    assert_paths_are_not_gitignored(S07_NON_EVIDENCE_PATHS, "Paths outside the private S07 workspace")
