from __future__ import annotations

from pathlib import Path


README_PATH = Path("README.md")
ASSEMBLER_COMMAND = "python scripts/assemble_m002_validation_package.py"
REPLAY_ARG = "--replay-report"
TUNING_ARG = "--tuning-report"
LIVE_PROOF_DEFAULT = "data/live-proof-result.json"
ALERT_SOAK_DEFAULT = "data/alert-soak-result.json"
OUTPUT_DIR = "data/m002-validation"
PACKAGE_JSON = "data/m002-validation/m002-validation-package.json"
PACKAGE_MARKDOWN = "data/m002-validation/m002-validation-package.md"


def _normalized(text: str) -> str:
    return " ".join(text.lower().split())


def _assert_m002_closure_contract(readme: str) -> None:
    normalized = _normalized(readme)

    assert "m002 final closure" in normalized, "README must include an M002 final closure section"
    assert ASSEMBLER_COMMAND in readme, f"README must name the package assembler: {ASSEMBLER_COMMAND}"
    for required in [REPLAY_ARG, TUNING_ARG, "--output-dir", OUTPUT_DIR]:
        assert required in readme, f"README must show required final package argument/output: {required}"
    for default_path in [LIVE_PROOF_DEFAULT, ALERT_SOAK_DEFAULT]:
        assert default_path in readme, f"README must document default evidence path {default_path}"
    for artifact in [PACKAGE_JSON, PACKAGE_MARKDOWN]:
        assert artifact in readme, f"README must explain produced final package artifact {artifact}"

    for status in ["validated", "coverage_gap", "blocked", "failed", "residual_risk_accepted"]:
        assert status in readme, f"README must define final closure status `{status}`"

    for scope_status in ["coverage_gap", "blocked", "deferred", "out-of-scope"]:
        assert scope_status in normalized, f"README must distinguish requirement scope/status term: {scope_status}"

    for requirement_id in ["R018", "R019", "R020", "R021", "R022", "R028"]:
        assert requirement_id in readme, f"README must name requirement reconciliation entry {requirement_id}"

    for evidence_phrase in [
        "requirement reconciliation",
        "r018, r019, and r028 are evidence-derived",
        "r018 real-traffic replay",
        "r019 shared-threshold sufficiency",
        "r028 bottom-driveway exclusion",
        "strict s07 semantic evidence",
        "real_capture",
        "bottom_driveway",
        "passing_traffic",
        "threshold_decision",
        "s07 workflow-smoke artifacts do not validate r018/r019/r028",
    ]:
        assert evidence_phrase in normalized, f"README must document S07 requirement evidence boundary: {evidence_phrase}"

    for deferred_phrase in [
        "r020, r021, and r022 are deferred/out-of-scope for m002",
        "r020 setup/matrix/docker/gpu/troubleshooting documentation is deferred",
        "r021 encrypted matrix room support remains out-of-scope",
        "r022 historical occupancy query/storage is out-of-scope",
    ]:
        assert deferred_phrase in normalized, f"README must document R020-R022 scope boundary: {deferred_phrase}"

    for non_validation in [
        "coverage_gap_no_alert",
        "preflight blockers",
        "skipped readback",
        "malformed inputs",
        "redaction hits",
        "matrix send-only evidence",
        "send responses alone do not prove room-visible delivery",
        "do not validate m002",
        "s08 strict live soak validation",
    ]:
        assert non_validation in normalized, f"README must reject known non-validation evidence: {non_validation}"

    for tuning_phrase in [
        "s05 does not tune polygons/thresholds",
        "does not add per-spot threshold schema",
        "no-change/shared-threshold closure",
        "keep_shared_thresholds",
        "apply_shared_tuning",
        "needs_per_spot_thresholds blocks closure",
        "explicitly accepted residual risk",
    ]:
        assert tuning_phrase in normalized, f"README must document tuning/no-change contract: {tuning_phrase}"

    for ignored_artifact in [
        "raw snapshots",
        "logs",
        "health/state/latest frames",
        "live-proof/alert-soak results",
        "replay/tuning reports",
        "final package outputs",
        "local/ignored until reviewed",
    ]:
        assert ignored_artifact in normalized, f"README must align publication boundary with .gitignore: {ignored_artifact}"

    forbidden_claims = [
        "coverage_gap_no_alert validates",
        "no-alert soak validates live alerts",
        "preflight blockers validate",
        "send responses alone prove room-visible delivery",
        "matrix send-only evidence validates",
        "raw snapshots should be committed",
        "raw logs should be committed",
        "per-spot threshold schema was added",
        "s07 workflow-smoke artifacts validate r018",
        "s07 workflow-smoke artifacts validate r019",
        "s07 workflow-smoke artifacts validate r028",
        "workflow-smoke evidence validates r018/r019/r028",
        "m002 implements encrypted matrix rooms",
        "encrypted matrix rooms are implemented by m002",
        "m002 implements historical occupancy storage",
        "historical occupancy storage is implemented by m002",
    ]
    for forbidden in forbidden_claims:
        assert forbidden not in normalized, f"README must not overclaim M002 closure: {forbidden}"


def test_readme_documents_m002_final_closure_command_statuses_and_publication_boundary() -> None:
    _assert_m002_closure_contract(README_PATH.read_text(encoding="utf-8"))


def test_contract_rejects_missing_final_package_command_or_artifacts() -> None:
    missing_command = (
        "## M002 final closure\n"
        "Define validated coverage_gap blocked failed residual_risk_accepted. "
        "coverage_gap_no_alert, preflight blockers, skipped readback, malformed inputs, redaction hits, "
        "Matrix send-only evidence, and send responses alone do not prove room-visible delivery do not validate M002; S08 strict live soak validation requires strict alert-soak success. "
        "S05 does not tune polygons/thresholds and does not add per-spot threshold schema. "
        "A no-change/shared-threshold closure cites keep_shared_thresholds, apply_shared_tuning, explicitly accepted residual risk; "
        "needs_per_spot_thresholds blocks closure. raw snapshots, logs, health/state/latest frames, live-proof/alert-soak results, "
        "replay/tuning reports, and final package outputs stay local/ignored until reviewed."
    )

    try:
        _assert_m002_closure_contract(missing_command)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text without the final package command/artifact paths")


def test_contract_rejects_missing_r020_r021_r022_scope_language() -> None:
    missing_scope = README_PATH.read_text(encoding="utf-8").replace(
        "R020, R021, and R022 are deferred/out-of-scope for M002",
        "R020, R021, and R022 are discussed for M002",
    )

    try:
        _assert_m002_closure_contract(missing_scope)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text without explicit R020/R021/R022 deferral scope")


def test_contract_rejects_s07_workflow_smoke_validation_claims() -> None:
    smoke_overclaim = (
        README_PATH.read_text(encoding="utf-8")
        + "\nS07 workflow-smoke artifacts validate R018. "
        + "S07 workflow-smoke artifacts validate R019. "
        + "S07 workflow-smoke artifacts validate R028.\n"
    )

    try:
        _assert_m002_closure_contract(smoke_overclaim)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text that validates R018/R019/R028 from S07 smoke evidence")


def test_contract_rejects_claimed_encrypted_matrix_or_historical_storage_implementation() -> None:
    implementation_overclaim = (
        README_PATH.read_text(encoding="utf-8")
        + "\nM002 implements encrypted Matrix rooms. "
        + "M002 implements historical occupancy storage.\n"
    )

    try:
        _assert_m002_closure_contract(implementation_overclaim)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text claiming deferred R021/R022 capabilities are implemented")


def test_contract_rejects_known_m002_closure_overclaims() -> None:
    overclaiming_readme = f"""
    ## M002 final closure
    Run {ASSEMBLER_COMMAND} {REPLAY_ARG} data/replay-report.json {TUNING_ARG} data/tuning-report.json --output-dir {OUTPUT_DIR}.
    Defaults include {LIVE_PROOF_DEFAULT} and {ALERT_SOAK_DEFAULT}.
    Review {PACKAGE_JSON} and {PACKAGE_MARKDOWN}.
    Statuses: validated coverage_gap blocked failed residual_risk_accepted.
    coverage_gap_no_alert, preflight blockers, skipped readback, malformed inputs, redaction hits,
    Matrix send-only evidence, and send responses alone do not prove room-visible delivery do not validate M002.
    S05 does not tune polygons/thresholds and does not add per-spot threshold schema.
    A no-change/shared-threshold closure cites keep_shared_thresholds, apply_shared_tuning, explicitly accepted residual risk;
    needs_per_spot_thresholds blocks closure.
    raw snapshots, logs, health/state/latest frames, live-proof/alert-soak results, replay/tuning reports,
    and final package outputs stay local/ignored until reviewed.
    no-alert soak validates live alerts. preflight blockers validate live proof. raw snapshots should be committed.
    per-spot threshold schema was added without tuning evidence.
    """

    try:
        _assert_m002_closure_contract(overclaiming_readme)
    except AssertionError:
        pass
    else:  # pragma: no cover - failure message is clearer than pytest.raises here
        raise AssertionError("contract accepted README text that overclaims M002 closure")
