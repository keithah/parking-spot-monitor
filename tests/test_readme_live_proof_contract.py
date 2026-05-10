from __future__ import annotations

from pathlib import Path


README_PATH = Path("README.md")
RUNNER_COMMAND = "python scripts/run_docker_live_proof.py"
VERIFIER_COMMAND = "python scripts/verify_live_proof.py"
RESULT_PATH = "data/live-proof-result.json"
EVIDENCE_PATH = "data/live-proof-evidence.md"
ALERT_SOAK_RUNNER_COMMAND = "python scripts/run_docker_alert_soak.py"
ALERT_SOAK_VERIFIER_COMMAND = "python scripts/verify_alert_soak.py"
ALERT_SOAK_RESULT_PATH = "data/alert-soak-result.json"
ALERT_SOAK_EVIDENCE_PATH = "data/alert-soak-evidence.md"


def _assert_live_proof_contract(readme: str) -> None:
    runner_index = readme.find(RUNNER_COMMAND)
    verifier_index = readme.find(VERIFIER_COMMAND)

    assert runner_index != -1, f"README must name the live-proof producer: {RUNNER_COMMAND}"
    assert verifier_index != -1, f"README must name the strict verifier: {VERIFIER_COMMAND}"
    assert runner_index < verifier_index, "README must instruct operators to run the Docker live-proof producer before the strict verifier"

    for required in [RESULT_PATH, EVIDENCE_PATH]:
        assert required in readme, f"README must name retained proof artifact {required}"

    normalized = " ".join(readme.lower().split())
    assert "preflight" in normalized and "blocker" in normalized, "README must describe missing live inputs as preflight blockers"
    assert "r003/r015 remain unvalidated" in normalized, "README must say preflight blockers leave R003/R015 unvalidated"
    assert "preflight blockers validate" not in normalized, "README must not claim preflight blockers validate R003/R015"
    assert "preflight blocker evidence validates" not in normalized, "README must not claim preflight evidence validates R003/R015"
    assert "--skip-readback may be used for validation" not in normalized, "README must not claim skipped readback validates R003/R015"
    assert "--skip-readback" in readme, "README must document skipped readback as non-validation"
    assert "strict verifier" in normalized, "README must identify verify_live_proof.py as the strict verifier"
    assert "strict verifier exits non-zero" in normalized, "README must document strict verifier failure behavior"


def _assert_alert_soak_contract(readme: str) -> None:
    runner_index = readme.find(ALERT_SOAK_RUNNER_COMMAND)
    verifier_index = readme.find(ALERT_SOAK_VERIFIER_COMMAND)

    assert runner_index != -1, f"README must name the alert-soak producer: {ALERT_SOAK_RUNNER_COMMAND}"
    assert verifier_index != -1, f"README must name the alert-soak strict verifier: {ALERT_SOAK_VERIFIER_COMMAND}"
    assert runner_index < verifier_index, "README must instruct operators to run the alert-soak producer before the strict verifier"

    for required in [
        ALERT_SOAK_RESULT_PATH,
        ALERT_SOAK_EVIDENCE_PATH,
        "data/alert-soak-docker.stdout.log",
        "data/alert-soak-docker.stderr.log",
        "data/health.json",
        "data/state.json",
    ]:
        assert required in readme, f"README must name alert-soak publication boundary artifact {required}"

    normalized = " ".join(readme.lower().split())
    for required in [
        "coverage_gap_no_alert",
        "preflight blocker",
        "docker_failed",
        "readback_gap",
        "duplicate-spam failure",
        "artifact_validation",
        "redaction",
        "matrix readback is `verified`",
        "at least one organic `occupancy-open-event`",
        "raw occupancy-open-event snapshot",
        "redaction scans report zero secret or forbidden-pattern occurrences",
        "not full s08 strict live soak validation unless final closure explicitly accepts the residual risk",
        "scripts/compare_calibration_tuning.py",
        "does not tune polygons/thresholds",
        "does not add per-spot runtime schema",
    ]:
        assert required in normalized, f"README must document alert-soak contract language: {required}"

    for forbidden in [
        "coverage_gap_no_alert validates s04",
        "no organic open event validates s04",
        "send responses alone prove room-visible delivery",
        "commit raw snapshots",
        "raw matrix response bodies may be included",
    ]:
        assert forbidden not in normalized, f"README must not overclaim unsafe alert-soak behavior: {forbidden}"


def test_readme_documents_strict_live_proof_flow_and_artifacts() -> None:
    _assert_live_proof_contract(README_PATH.read_text(encoding="utf-8"))


def test_readme_documents_unattended_alert_soak_flow_and_publication_boundary() -> None:
    _assert_alert_soak_contract(README_PATH.read_text(encoding="utf-8"))


def test_alert_soak_contract_rejects_missing_runner_or_reversed_order() -> None:
    verifier_only = (
        f"Run {ALERT_SOAK_VERIFIER_COMMAND} and inspect {ALERT_SOAK_RESULT_PATH}; "
        "coverage_gap_no_alert is not full S04 validation unless S05 explicitly accepts the residual risk."
    )
    reversed_order = (
        f"Run {ALERT_SOAK_VERIFIER_COMMAND} as the strict verifier, then {ALERT_SOAK_RUNNER_COMMAND}. "
        f"It writes {ALERT_SOAK_RESULT_PATH} and {ALERT_SOAK_EVIDENCE_PATH}; "
        "strict success needs at least one organic `occupancy-open-event`, raw occupancy-open-event snapshot, "
        "Matrix readback is `verified`, and redaction scans report zero secret or forbidden-pattern occurrences. "
        "Also document preflight blocker, docker_failed, readback_gap, duplicate-spam failure, artifact_validation, redaction, "
        "data/alert-soak-docker.stdout.log, data/alert-soak-docker.stderr.log, data/health.json, data/state.json, "
        "scripts/compare_calibration_tuning.py, does not tune polygons/thresholds, and does not add per-spot runtime schema. "
        "It is not full S04 validation unless S05 explicitly accepts the residual risk."
    )

    for bad_readme in [verifier_only, reversed_order]:
        try:
            _assert_alert_soak_contract(bad_readme)
        except AssertionError:
            pass
        else:  # pragma: no cover - failure message is clearer than pytest.raises here
            raise AssertionError("contract accepted a README with a missing or reversed alert-soak flow")


def test_alert_soak_contract_rejects_success_or_publication_safety_overclaims() -> None:
    overclaiming_readme = (
        f"Run {ALERT_SOAK_RUNNER_COMMAND}, then run {ALERT_SOAK_VERIFIER_COMMAND}. "
        f"Inspect {ALERT_SOAK_RESULT_PATH}, {ALERT_SOAK_EVIDENCE_PATH}, data/alert-soak-docker.stdout.log, "
        "data/alert-soak-docker.stderr.log, data/health.json, and data/state.json. "
        "Strict success needs at least one organic `occupancy-open-event`, raw occupancy-open-event snapshot, "
        "Matrix readback is `verified`, and redaction scans report zero secret or forbidden-pattern occurrences. "
        "Document coverage_gap_no_alert, preflight blocker, docker_failed, readback_gap, duplicate-spam failure, "
        "artifact_validation, redaction, scripts/compare_calibration_tuning.py, does not tune polygons/thresholds, "
        "and does not add per-spot runtime schema. "
        "coverage_gap_no_alert validates S04. raw Matrix response bodies may be included."
    )

    try:
        _assert_alert_soak_contract(overclaiming_readme)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text that overclaims alert-soak validation or publication safety")


def test_contract_rejects_missing_runner_or_reversed_order() -> None:
    verifier_only = f"Run {VERIFIER_COMMAND} and inspect {RESULT_PATH}; R003/R015 remain unvalidated on preflight blockers."
    reversed_order = (
        f"Run {VERIFIER_COMMAND} as the strict verifier, then {RUNNER_COMMAND}. "
        f"It writes {RESULT_PATH} and {EVIDENCE_PATH}; preflight blockers mean R003/R015 remain unvalidated. "
        "The strict verifier exits non-zero on non-success. Do not use --skip-readback for validation."
    )

    for bad_readme in [verifier_only, reversed_order]:
        try:
            _assert_live_proof_contract(bad_readme)
        except AssertionError:
            pass
        else:  # pragma: no cover - failure message is clearer than pytest.raises here
            raise AssertionError("contract accepted a README with a missing or reversed live-proof flow")


def test_contract_rejects_preflight_or_skipped_readback_overclaiming_validation() -> None:
    overclaiming_readme = (
        f"Run {RUNNER_COMMAND}, then run {VERIFIER_COMMAND} as the strict verifier. "
        f"Inspect {RESULT_PATH} and {EVIDENCE_PATH}. "
        "Preflight blockers validate R003/R015, even though preflight blockers normally mean R003/R015 remain unvalidated. "
        "The --skip-readback may be used for validation. "
        "The strict verifier exits non-zero on non-success."
    )

    try:
        _assert_live_proof_contract(overclaiming_readme)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text that overclaims preflight/skipped-readback validation")
