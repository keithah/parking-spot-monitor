from __future__ import annotations

from pathlib import Path


TESTS_DIR = Path(__file__).parent
SPLIT_DOMAIN_PREFIXES = (
    "test_config_",
    "test_matrix_",
    "test_operator_feedback_",
    "test_outbox_persistence_",
    "test_startup_",
    "test_vehicle_history_",
)
MAX_TEST_MODULE_LINES = 999


def test_split_domain_modules_stay_below_monolith_threshold() -> None:
    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in TESTS_DIR.glob("test_*.py")
        if path.name.startswith(SPLIT_DOMAIN_PREFIXES)
        and len(path.read_text(encoding="utf-8").splitlines()) > MAX_TEST_MODULE_LINES
    }

    assert oversized == {}


def test_split_domain_support_modules_stay_below_monolith_threshold() -> None:
    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in (TESTS_DIR / "support").glob("_*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > MAX_TEST_MODULE_LINES
    }

    assert oversized == {}
