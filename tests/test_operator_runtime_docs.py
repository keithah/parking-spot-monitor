from __future__ import annotations

import re

from tests.operator_docs_helpers import (
    assert_contains_all,
    assert_section_case,
    read_readme_section,
    read_tracked,
)


def test_first_check_artifact_guidance_and_structured_events_are_documented() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "/data/latest.jpg",
            "./data/latest.jpg",
            "data/health.json",
            "python -m json.tool data/health.json",
            "find data/snapshots",
        ],
    )
    assert "startup-ready" in readme or "capture-frame-written" in readme


def test_readme_local_configuration_documents_adaptive_resource_semantics() -> None:
    section = read_readme_section("Local configuration")
    normalized = " ".join(section.split())

    assert_contains_all(
        normalized,
        [
            "frame_interval_seconds: 30",
            "adaptive_polling_enabled: true",
            "stable_frame_interval_seconds: 60",
            "stable_settle_frames: 3",
            "debug_overlay_interval_seconds: 60",
            "escalation_verification_seconds: 600",
            "consecutive successful stable frames (default `3`)",
            "`adaptive_polling_enabled: false`",
            "`stable_frame_interval_seconds` equal to `frame_interval_seconds`",
            "second fixed-cadence rollback",
            "`debug_overlay_interval_seconds: 0` to disable periodic debug overlays",
            "transition-driven overlays remain enabled",
            "`escalation_verification_seconds: 0` to disable periodic high-resolution verification",
            "transition-driven escalation remains enabled",
            "`latest-high_resolution.jpg`",
            "authoritative frame for that iteration's detection, state transition, and event snapshot",
            "Escalation is transition-driven for weak evidence that could change occupancy and is also "
            "periodic after state has settled",
            "A successful transition-driven verification resets that periodic deadline",
            "primary frames own routine timeline retention and debug overlays",
        ],
    )


def test_readme_distinguishes_primary_publication_from_cadence_limited_runtime_overlays() -> None:
    readme = read_tracked("README.md")
    normalized = " ".join(readme.split())

    assert_contains_all(
        normalized,
        [
            "Every successful primary capture publishes `latest.jpg` in the data directory.",
            "`--capture-once` also refreshes `debug_latest.jpg`.",
            "In the runtime loop, `debug_latest.jpg` is cadence-limited: it is refreshed only when "
            "`runtime.debug_overlay_interval_seconds` is due or a state transition forces an overlay.",
        ],
    )
    assert "A successful capture writes two files in the data directory" not in readme

def test_readme_troubleshooting_covers_s04_failure_classes_with_evidence_surfaces() -> None:
    section = read_readme_section("Troubleshooting and cleanup runbook")

    required_cases = {
        "RTSP/capture failures or reconnect symptoms": [
            "RTSP/capture failures",
            "stream.reconnect_seconds",
            "docker compose logs -f parking-spot-monitor",
            "data/latest.jpg",
            "data/health.json",
            "capture-frame-written",
            "capture-all-modes-failed",
        ],
        "hardware decode/device passthrough issues": [
            "hardware decode",
            "/dev/dri:/dev/dri",
            "docker compose ps",
            "data/health.json",
            "selected_decode_mode",
        ],
        "Matrix send/upload failures": [
            "Matrix send/upload failures",
            "docker compose logs -f parking-spot-monitor",
            "data/health.json",
            "last_matrix_error",
            "matrix-send-failed",
            "matrix-delivery-failed",
        ],
        "detector misses/false negatives": [
            "detector misses",
            "false negatives",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "detection-frame-processed",
            "detection-frame-failed",
        ],
        "false positives/passing traffic": [
            "false positives",
            "passing traffic",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "data/state.json",
            "detection-frame-processed",
        ],
        "street-sweeping or quiet-window behavior": [
            "street-sweeping",
            "quiet-window",
            "data/state.json",
            "quiet-window-started",
            "quiet-window-ended",
            "occupancy-open-suppressed",
        ],
        "restart/state corruption recovery": [
            "restart/state corruption recovery",
            "docker compose restart parking-spot-monitor",
            "data/state.json",
            "quarantined",
            "state-corrupt-quarantined",
        ],
        "permissions/disk write failures": [
            "permissions/disk write failures",
            "./data:/data",
            "data/health.json",
            "health-write-failed",
            "state-save-failed",
            "debug-overlay-failed",
        ],
        "snapshot/disk cleanup": [
            "snapshot/disk cleanup",
            "data/snapshots/",
            "storage.snapshot_retention_count",
            "snapshot-retention-pruned",
            "snapshot-retention-failed",
        ],
    }

    for case_name, required in required_cases.items():
        assert_section_case(section, case_name, required)

def test_readme_non_goals_are_explicit_and_distinguished_from_local_docs_validation() -> None:
    section = read_readme_section("Non-goals and deferred capabilities")

    assert_contains_all(
        section,
        [
            "no supported web UI",
            "NVR/video archive",
            "license-plate recognition",
            "cloud AI dependency",
            "encrypted Matrix-room hardening guarantee",
            "driveway-car monitoring",
            "live-camera proof",
            "live Matrix delivery guarantee",
            "local docs alone",
        ],
    )

def test_s04_docs_contract_stays_grounded_in_tracked_source_events() -> None:
    readme = read_tracked("README.md")
    tracked_sources = "\n".join(
        read_tracked(path)
        for path in [
            "docker-compose.yml",
            "config.yaml.example",
            "parking_spot_monitor/__main__.py",
            "parking_spot_monitor/matrix_dispatch.py",
            "parking_spot_monitor/capture_loop.py",
            "parking_spot_monitor/runtime_detection.py",
            "parking_spot_monitor/runtime_health.py",
            "parking_spot_monitor/runtime_lifecycle.py",
            "parking_spot_monitor/runtime_overlay.py",
            "parking_spot_monitor/runtime_state_update.py",
            "parking_spot_monitor/runtime_frame_plan.py",
            "parking_spot_monitor/capture.py",
            "parking_spot_monitor/matrix.py",
            "parking_spot_monitor/matrix_commands.py",
            "parking_spot_monitor/matrix_delivery.py",
            "parking_spot_monitor/matrix_snapshots.py",
            "parking_spot_monitor/matrix_alerts.py",
            "parking_spot_monitor/state.py",
            "parking_spot_monitor/health.py",
            "parking_spot_monitor/debug_overlay.py",
            "parking_spot_monitor/occupancy.py",
            "parking_spot_monitor/operator_decision_memory.py",
        ]
    )

    source_backed_tokens = [
        "startup-ready",
        "capture-frame-written",
        "capture-decode-fallback",
        "capture-all-modes-failed",
        "debug-overlay-written",
        "debug-overlay-failed",
        "detection-frame-processed",
        "matrix-send-failed",
        "matrix-delivery-failed",
        "state-corrupt-quarantined",
        "state-save-failed",
        "health-write-failed",
        "snapshot-retention-pruned",
        "snapshot-retention-failed",
        "quiet-window-started",
        "quiet-window-ended",
        "occupancy-open-suppressed",
        "/dev/dri:/dev/dri",
        "snapshot_retention_count",
        "operator-decision-memory-quarantined",
    ]
    for token in source_backed_tokens:
        assert token in tracked_sources, f"tracked source no longer backs documented token: {token}"
        assert token in readme, f"README.md missing source-backed operator token: {token}"

def test_docs_and_wiring_remain_secret_safe_and_do_not_embed_raw_artifact_spam() -> None:
    scanned_paths = [
        "README.md",
        "Dockerfile",
        "docker-compose.yml",
        "config.yaml.example",
        "tests/test_operator_docs.py",
    ]
    rendered = "\n".join(read_tracked(path) for path in scanned_paths)

    forbidden_live_value_markers = [
        "rt" "sp://",
        "camera-" "secret",
        "matrix-" "secret",
        "should-not-" "leak",
        "mxc" "://",
        "Authorization" ": " "Bearer",
        "Bear" "er " "syt_",
        "Trace" "back (most recent call last)",
        "BEGIN RAW " "IMAGE BYTES",
        "END RAW " "IMAGE BYTES",
        ("raw " "image bytes").upper(),
    ]
    for marker in forbidden_live_value_markers:
        assert marker not in rendered

    forbidden_live_value_patterns = {
        "concrete RTSP URL": r"rt" r"sp://[^\s)>'\"]+",
        "Matrix access token": r"(?:syt|spa|map)_[-A-Za-z0-9._=]{20,}",
        "Authorization/Bear" "er example": r"Authorization\s*:\s*Bearer\s+\S+",
        "Matrix content URI": r"mxc" r"://[^\s)>'\"]+",
        "private Matrix room id": r"![A-Za-z0-9_-]{20,}:[A-Za-z0-9.-]+",
        "traceback spam": r"Traceback \(most recent call last\)",
    }
    for marker_class, pattern in forbidden_live_value_patterns.items():
        assert re.search(pattern, rendered) is None, f"forbidden {marker_class} marker found in docs/config/wiring"

def test_documented_artifact_paths_and_debug_events_stay_wired_to_tracked_code() -> None:
    combined_sources = "\n".join(
        read_tracked(path)
        for path in [
            "parking_spot_monitor/paths.py",
            "parking_spot_monitor/capture.py",
            "parking_spot_monitor/debug_overlay.py",
            "parking_spot_monitor/__main__.py",
            "parking_spot_monitor/runtime_detection.py",
            "parking_spot_monitor/runtime_overlay.py",
            "parking_spot_monitor/runtime_frame_plan.py",
            "parking_spot_monitor/matrix.py",
        ]
    )

    assert_contains_all(
        combined_sources,
        [
            "latest.jpg",
            "debug_latest.jpg",
            "snapshots",
            "capture-frame-written",
            "debug-overlay-written",
            "debug-overlay-failed",
            "detection-frame-processed",
            "detection-frame-failed",
        ],
    )

def test_readme_calibration_artifact_and_safety_contract_is_grounded() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "config.yaml.example",
            "config.yaml",
            "RTSP_URL",
            "MATRIX_ACCESS_TOKEN",
            "data/latest.jpg",
            "data/debug_latest.jpg",
            "data/snapshots/",
            "data/health.json",
            "data/state.json",
            "data/operator-decision-memory.json",
            "debug-overlay-written",
            "capture-frame-written",
            "detection-frame-processed",
            "detection-frame-failed",
            "image payload bytes",
            "raw frames, snapshots, health/state, and redacted runtime logs local",
            "does not prove a live camera or Matrix room",
            "per-spot threshold schema",
        ],
    )

    unsupported_claim_markers = [
        "visual calibration UI",
        "validated live camera",
        "validated live Matrix",
    ]
    for marker in unsupported_claim_markers:
        assert marker not in readme
