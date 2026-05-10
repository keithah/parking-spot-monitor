from __future__ import annotations

from pathlib import Path


README_PATH = Path("README.md")
RUNNER_COMMAND = "python scripts/run_docker_live_proof.py"
VERIFIER_COMMAND = "python scripts/verify_live_proof.py"
RESULT_PATH = "data/live-proof-result.json"
EVIDENCE_PATH = "data/live-proof-evidence.md"


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


def test_readme_documents_strict_live_proof_flow_and_artifacts() -> None:
    _assert_live_proof_contract(README_PATH.read_text(encoding="utf-8"))


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
