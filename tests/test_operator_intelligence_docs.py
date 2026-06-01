from __future__ import annotations

from tests.operator_docs_helpers import assert_contains_all, read_matrix_command_contract, read_tracked


def test_operator_docs_include_feedback_correction_and_who_snapshot_contract() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
    cockpit_snapshot_source = read_tracked("parking_spot_monitor/operator_cockpit_snapshots.py")
    feedback_source = read_tracked("parking_spot_monitor/operator_feedback.py")
    feedback_model_source = read_tracked("parking_spot_monitor/operator_feedback_models.py")

    assert_contains_all(
        readme,
        [
            "Use `!parking correct <spot_id> <open|occupied>`",
            "Use `!parking learn <spot_id> <open|occupied> at <time>`",
            "records a bounded local label in `data/operator-feedback-labels.json`",
            "does not store image bytes, camera URLs, Matrix tokens, raw Matrix bodies, or tracebacks",
            "Feedback labels are training and replay evidence only",
            "does not mutate live occupancy state",
            "does not automatically change detector thresholds or train a model",
            "Use `!parking who` to list active parking sessions by spot and request a fresh current snapshot",
            "Snapshot: fresh capture unavailable",
            "does not run detector/model inference or mutate live occupancy state",
        ],
    )
    assert_contains_all(
        matrix_contract,
        [
            "usage: !parking correct <spot_id> <open|occupied>",
            "usage: !parking learn <spot_id> <open|occupied> at <time>",
            "{command_prefix} correct <spot_id> <open|occupied> — record the actual spot state for a wrong alert",
            "{command_prefix} learn <spot_id> <open|occupied> at <time> — record a retained-timeline calibration label for review",
            "usage: !parking who",
            "{command_prefix} who — list active parking sessions by spot and attach a fresh current snapshot when configured",
        ],
    )
    assert_contains_all(
        cockpit_snapshot_source,
        [
            "Build a Matrix who reply enriched by one fresh raw capture when available.",
            "not run detector/model inference and does not read or mutate occupancy",
            "Snapshot: fresh capture unavailable",
            "no live state was changed",
        ],
    )
    assert_contains_all(
        feedback_model_source,
        [
            "FEEDBACK_LABELS_FILENAME = \"operator-feedback-labels.json\"",
            "reported_state",
            "actual_state",
        ],
    )
    assert_contains_all(
        feedback_source,
        [
            "operator correction recorded",
            "record_learn_label",
            "operator learn label recorded",
        ],
    )

def test_operator_intelligence_docs_cover_feedback_aliases_analytics_and_live_uat_limits() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
    feedback_source = read_tracked("parking_spot_monitor/operator_feedback.py")
    feedback_model_source = read_tracked("parking_spot_monitor/operator_feedback_models.py")
    feedback_store_source = read_tracked("parking_spot_monitor/operator_feedback_store.py")

    assert_contains_all(
        readme,
        [
            "!parking explain <spot_id>",
            "`!parking analytics` is a read-only historical occupancy summary over local vehicle-history artifacts",
            "Use `!parking analytics` or `!parking analytics 7d`",
            "`!parking analytics today`",
            "`!parking analytics 30d`",
            "`!parking analytics all`",
            "Analytics does not start capture, run detector/model inference, upload media, emit alerts, mutate feedback labels, mutate vehicle-history records, change thresholds, prove a live camera, or prove live Matrix room delivery",
            "Use `!parking correct <spot_id> <open|occupied>` or its explicit false-alert alias `!parking false-alert <spot_id> <open|occupied>`",
            "Use `!parking learn <spot_id> <open|occupied> at <time>` or its explicit missed-alert alias `!parking missed-alert <spot_id> <open|occupied> at <time>`",
            "records a bounded local label in `data/operator-feedback-labels.json`",
            "They append local feedback labels only after validating the bounded alert or timeline evidence they can access",
            "does not mutate live occupancy state",
            "does not provide live-camera proof from repository tests, README examples, or local docs alone",
            "does not provide a live Matrix delivery guarantee from send attempts or docs alone",
            "Room-visible Matrix delivery requires explicit live proof/readback evidence",
        ],
    )
    assert_contains_all(
        matrix_contract,
        [
            "usage: !parking explain <spot_id>",
            "usage: !parking analytics [today|7d|30d|all]",
            "{command_prefix} false-alert <spot_id> <open|occupied> — explicit alias for correcting a false alert",
            "{command_prefix} missed-alert <spot_id> <open|occupied> at <time> — explicit alias for recording missed timeline evidence",
            "{command_prefix} analytics [today|7d|30d|all] — show spot-level historical occupancy metrics from local vehicle-history sessions",
            "false-alert action=correct_spot_state",
            "missed-alert action=learn_label",
        ],
    )
    assert_contains_all(
        cockpit_source,
        [
            "Format bounded spot-level historical occupancy analytics from local vehicle-history JSON only.",
            "Parking occupancy analytics",
            "Parking occupancy analytics unavailable",
        ],
    )
    assert_contains_all(
        feedback_model_source,
        [
            "FEEDBACK_LABELS_FILENAME = \"operator-feedback-labels.json\"",
        ],
    )
    assert_contains_all(
        feedback_source,
        [
            "feedback_category=\"false_alert\"",
            "feedback_category=\"missed_alert\"",
        ],
    )
    assert_contains_all(
        feedback_store_source,
        [
            "operator-feedback-label-appended",
        ],
    )

    forbidden_operator_intelligence_claims = [
        "analytics proves live Matrix delivery",
        "analytics starts a capture",
        "false-alert mutates live occupancy state",
        "missed-alert changes detector thresholds",
        "local docs prove live Matrix delivery",
    ]
    for marker in forbidden_operator_intelligence_claims:
        assert marker not in readme

def test_operator_docs_include_timeline_frame_buffer_contract() -> None:
    readme = read_tracked("README.md")
    timeline_source = read_tracked("parking_spot_monitor/timeline_buffer.py")
    startup_source = read_tracked("parking_spot_monitor/__main__.py")

    assert_contains_all(
        readme,
        [
            "data/timeline/frames/",
            "one raw full-frame JPEG per UTC minute",
            "12 hours",
            "missed-alert review",
            "about 150–250 MiB",
            "not a continuous video archive",
        ],
    )
    assert_contains_all(
        timeline_source,
        [
            "DEFAULT_TIMELINE_INTERVAL_SECONDS = 60",
            "DEFAULT_TIMELINE_RETENTION_HOURS = 12",
            "already-sampled",
            "timeline-unavailable",
        ],
    )
    capture_loop_source = read_tracked("parking_spot_monitor/capture_loop.py")
    assert_contains_all(
        startup_source + capture_loop_source,
        [
            "record_timeline_frame",
            "timeline-frame-retained",
        ],
    )

def test_detection_lab_command_docs_cover_bounded_authorized_local_artifact_boundary() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()
    cockpit_source = read_tracked("parking_spot_monitor/operator_cockpit.py")
    lab_source = read_tracked("parking_spot_monitor/detection_lab.py")
    startup_source = read_tracked("parking_spot_monitor/__main__.py")

    assert_contains_all(
        readme,
        [
            "`!parking lab run replay`",
            "`!parking lab run tuning`",
            "`!parking lab status`",
            "`!parking lab status <job_id|latest>`",
            "authorized cockpit commands",
            "authorization/default-deny boundary",
            "empty `matrix.command_authorized_senders` list denies lab starts",
            "exact lab grammar",
            "malformed job IDs",
            "path traversal strings",
            "shell snippets",
            "Matrix-supplied filesystem paths",
            "asynchronous, non-blocking local replay job",
            "returns immediately with a bounded job ID",
            "data/detection-lab/labels.json",
            "data/detection-lab/replay-config.json",
            "data/detection-lab/baseline-config.json",
            "data/detection-lab/proposed-config.json",
            "data/detection-lab/jobs/<job_id>/",
            "persisted redacted `status.json`",
            "replay-report.json",
            "tuning-report.json",
            "`!parking lab status` is the same as `!parking lab status latest`",
            "Detection lab status unavailable",
            "missing_fixed_inputs",
            "status_unreadable",
            "malformed_report",
            "runner_unavailable",
            "text-only and do not upload media",
            "does not mutate live occupancy",
            "camera capture",
            "live detector/model execution",
            "live Matrix delivery",
            "safe `lab_outcome` records",
            "`!parking recent` may show lab outcomes",
            "RTSP URLs",
            "Matrix tokens",
            "Authorization headers",
            "raw Matrix response bodies",
            "tracebacks",
            "image bytes",
        ],
    )
    assert_contains_all(
        matrix_contract,
        [
            "{command_prefix} lab run replay — start a bounded local replay lab job using fixed inputs",
            "{command_prefix} lab run tuning — start a bounded local tuning lab job using fixed inputs",
            "{command_prefix} lab status [job_id|latest] — show the latest or selected redacted lab job status",
            "usage: !parking lab run <replay|tuning>",
            "usage: !parking lab status [job_id|latest]",
            "invalid lab job kind",
            "invalid lab job id",
        ],
    )
    assert_contains_all(
        cockpit_source,
        [
            "Detection lab job started",
            "use !parking lab status latest",
            "Inputs: fixed local detection-lab files under the runtime data directory.",
            "Detection lab status unavailable",
            "No detector, camera, shell, or live occupancy work was run by this reply path.",
            "Report:",
            "missing fixed inputs",
        ],
    )
    assert_contains_all(
        lab_source,
        [
            "LAB_DIR_NAME = \"detection-lab\"",
            "JOBS_DIR_NAME = \"jobs\"",
            "STATUS_FILENAME = \"status.json\"",
            "REPLAY_REPORT_FILENAME = \"replay-report.json\"",
            "TUNING_REPORT_FILENAME = \"tuning-report.json\"",
            "REPLAY_LABELS_FILENAME = \"labels.json\"",
            "REPLAY_CONFIG_FILENAME = \"replay-config.json\"",
            "TUNING_BASELINE_CONFIG_FILENAME = \"baseline-config.json\"",
            "TUNING_PROPOSED_CONFIG_FILENAME = \"proposed-config.json\"",
            "missing_fixed_inputs",
            "runner_unavailable",
            "malformed_report",
            "status_unreadable",
            "path_outside_lab",
            "outcome_recorder",
        ],
    )
    assert "record_outcome" in startup_source
    assert "_append_lab_outcome_memory" in startup_source

    forbidden_lab_doc_claims = [
        "lab command accepts a path",
        "lab command uploads media",
        "lab command mutates live occupancy",
        "lab command runs a live camera",
        "lab command changes production thresholds",
        "lab status reads arbitrary paths",
    ]
    for marker in forbidden_lab_doc_claims:
        assert marker not in readme

def test_why_recent_command_docs_cover_memory_boundaries_and_safe_failures() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()
    memory_source = read_tracked("parking_spot_monitor/operator_decision_memory.py")

    assert_contains_all(
        readme,
        [
            "`!parking why <spot_id>` and `!parking explain <spot_id>` explain the bounded recent decision memory",
            "`!parking recent` returns a compact bounded timeline",
            "accepted/rejected evidence",
            "hit/miss streak context",
            "quiet-window or weak-open suppression",
            "alert outcomes",
            "command/lab outcomes",
            "Invalid spot IDs or extra arguments are rejected",
            "Missing, corrupt, oversized, unsupported, or unreadable `operator-decision-memory.json`",
            "Decision memory unavailable",
            "no detector or camera work was run",
            "No recent decision memory for this spot",
            "bounded local `operator-decision-memory.json` under the effective runtime data directory",
            "why/explain/recent memory boundary",
            "Matrix arguments cannot choose arbitrary files",
            "They are text-only commands",
            "do not upload media",
            "mutate archive corrections",
            "start capture",
            "run the detector/model",
            "invoke detection-lab work",
            "raw JPEG bytes",
            "raw Matrix event bodies",
            "unbounded detector payloads",
        ],
    )
    assert_contains_all(
        matrix_contract,
        [
            "usage: !parking why <spot_id>",
            "usage: !parking explain <spot_id>",
            "usage: !parking recent",
            "invalid spot id",
            "{command_prefix} why <spot_id> — explain recent parking decisions for one spot from bounded local memory",
            "{command_prefix} recent — show recent decision, alert, suppression, command, and lab records from bounded local memory",
        ],
    )
    assert_contains_all(
        memory_source,
        [
            "operator-decision-memory.json",
            "Decision memory unavailable",
            "No recent decision memory for this spot",
            "no detector or camera work was run",
            "operator-decision-memory-quarantined",
            "MAX_REPLY_BYTES",
        ],
    )

    forbidden_why_recent_claims = [
        "why starts a capture",
        "recent starts a capture",
        "why uploads media",
        "recent uploads media",
        "why runs the detector",
        "recent runs detection-lab",
    ]
    for marker in forbidden_why_recent_claims:
        assert marker not in readme

def test_latest_command_docs_cover_raw_image_failure_and_retention_boundaries() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()

    assert_contains_all(
        readme,
        [
            "`!parking latest` sends a concise runtime summary plus one Matrix image",
            "already-existing local `latest.jpg` passes validation",
            "Parking monitor latest unavailable",
            "Snapshot: unavailable",
            "missing",
            "too large",
            "invalid JPEG",
            "health freshness including `stale`",
            "capture/detection failure counts",
            "per-spot decisions",
            "raw full-frame `data/latest.jpg`",
            "must not use `data/debug_latest.jpg`",
            "polygon overlays",
            "not invoke a new capture",
            "detector/model run",
            "Raw full-frame latest.jpg evidence",
            "does not create or prune retained files under `data/snapshots/`",
            "retention boundaries remain for Matrix event/live-proof snapshots",
            "read-only cockpit commands never mutate live spot state",
            "snapshot retention",
            "runtime artifacts",
        ],
    )
    assert_contains_all(
        matrix_contract,
            [
                "{command_prefix} latest — show latest runtime summary and raw full-frame image evidence",
                "Raw full-frame latest.jpg evidence",
                "command:$event:image",
            ],
        )

    forbidden_latest_claims = [
        "latest uses debug_latest.jpg",
        "latest creates retained snapshots",
        "latest starts a capture",
        "latest mutates live spot state",
    ]
    for marker in forbidden_latest_claims:
        assert marker not in readme
