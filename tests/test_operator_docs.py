from __future__ import annotations

from tests.operator_docs_helpers import assert_contains_all, read_matrix_command_contract, read_tracked


def test_readme_documents_clean_machine_setup_sequence_and_operator_commands() -> None:
    readme = read_tracked("README.md")

    assert_contains_all(
        readme,
        [
            "cp config.yaml.example config.yaml",
            "RTSP_URL",
            "MATRIX_ACCESS_TOKEN",
            "python -m parking_spot_monitor --config config.yaml --validate-config",
            "docker build -t parking-spot-monitor:test .",
            "docker compose config --no-interpolate",
            "docker compose up parking-spot-monitor",
            "docker compose logs -f parking-spot-monitor",
            "docker compose restart parking-spot-monitor",
            "docker compose down",
            "!parking help",
            "!parking status",
            "!parking config",
            "!parking latest",
            "!parking why <spot_id>",
            "!parking explain <spot_id>",
            "!parking learn <spot_id> <open|occupied> at <time>",
            "!parking recent",
            "!parking confidence",
            "!parking at <time> <spot_id>",
            "!parking lab run replay",
            "!parking lab run tuning",
            "!parking lab status",
            "!parking lab status <job_id|latest>",
            "!parking who",
            "!parking owner <spot_id>",
            "!parking wrong <spot_id|session_id>",
            "!parking profile summary <profile_id>",
            "matrix.command_authorized_senders",
        ],
    )

    sequence = [
        "cp config.yaml.example config.yaml",
        "python -m parking_spot_monitor --config config.yaml --validate-config",
        "docker build -t parking-spot-monitor:test .",
        "docker compose config --no-interpolate",
        "docker compose up parking-spot-monitor",
        "docker compose logs -f parking-spot-monitor",
        "docker compose restart parking-spot-monitor",
        "docker compose down",
    ]
    positions = [readme.index(token) for token in sequence]
    assert positions == sorted(positions)

def test_operator_cockpit_commands_are_documented_as_authorized_read_only_and_secret_safe() -> None:
    readme = read_tracked("README.md")
    matrix_contract = read_matrix_command_contract()

    assert_contains_all(
        readme,
        [
            "The read-only cockpit commands are `!parking status`, `!parking config`, `!parking latest`, `!parking why <spot_id>`, `!parking explain <spot_id>`, `!parking recent`, `!parking confidence`, and `!parking at <time> <spot_id>`",
            "Empty `matrix.command_authorized_senders` default-denies all Matrix commands",
            "Missing, corrupt, or unreadable health/state files are reported as `unavailable`",
            "`!parking why <spot_id>` and `!parking explain <spot_id>` explain the bounded recent decision memory",
            "`!parking recent` returns a compact bounded timeline",
            "operator-decision-memory.json",
            "Decision memory unavailable",
            "No recent decision memory for this spot",
            "old health timestamps are called `stale`",
            "safe error classes such as `error_type`",
            "redact resolved camera URLs",
            "Matrix access-token values",
            "raw Matrix response bodies",
            "tracebacks",
            "YAML dumps",
            "image bytes",
            "raw JPEG bytes",
            "raw Matrix event bodies",
            "unbounded detector payloads",
            "do not start capture, detector, model, upload preparation, Matrix-sync, shell, browser, dashboard, detection-lab, or live-proof work",
            "never mutate live spot state or vehicle-history records",
            "owner/wrong/profile commands are the explicit mutation boundary",
            "!parking latest",
        ],
    )
    assert_contains_all(
        matrix_contract,
        [
            "{command_prefix} status — show runtime health and spot status",
            "{command_prefix} config — show safe monitor configuration",
            "{command_prefix} latest — show latest runtime summary and raw full-frame image evidence",
            "{command_prefix} why <spot_id> — explain recent parking decisions for one spot from bounded local memory",
            "{command_prefix} explain <spot_id> — alias for why with the same bounded local-memory explanation",
            "{command_prefix} recent — show recent decision, alert, suppression, command, and lab records from bounded local memory",
        ],
    )

    forbidden_promises = [
        "run shell commands",
        "open a dashboard",
        "unredacted secret",
        "mutate live spot state with `!parking status`",
        "mutate live spot state with `!parking config`",
    ]
    for marker in forbidden_promises:
        assert marker not in readme
