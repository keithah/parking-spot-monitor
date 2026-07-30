from __future__ import annotations

from pathlib import Path

from scripts.test_suite_paths import (
    CONFIG_TEST_MODULES,
    MATRIX_COCKPIT_TEST_MODULES,
    MATRIX_OUTBOX_TEST_MODULES,
    MATRIX_TEST_MODULES,
    OPERATOR_FEEDBACK_TEST_MODULES,
    OUTBOX_PERSISTENCE_TEST_MODULES,
    STARTUP_TEST_MODULES,
    VEHICLE_HISTORY_TEST_MODULES,
)


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
NON_SPLIT_DOMAIN_MODULES = {"test_vehicle_history_cli.py"}
FOCUSED_TEST_MODULE_GROUPS = (
    CONFIG_TEST_MODULES,
    MATRIX_TEST_MODULES,
    MATRIX_COCKPIT_TEST_MODULES,
    MATRIX_OUTBOX_TEST_MODULES,
    OPERATOR_FEEDBACK_TEST_MODULES,
    OUTBOX_PERSISTENCE_TEST_MODULES,
    STARTUP_TEST_MODULES,
    VEHICLE_HISTORY_TEST_MODULES,
)


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


def test_closeout_smoke_groups_reference_every_split_module_once() -> None:
    paths = [path for group in FOCUSED_TEST_MODULE_GROUPS for path in group]
    expected = {
        str(path.relative_to(TESTS_DIR.parent))
        for path in TESTS_DIR.glob("test_*.py")
        if path.name.startswith(SPLIT_DOMAIN_PREFIXES)
        and path.name not in NON_SPLIT_DOMAIN_MODULES
    }

    assert len(paths) == len(set(paths))
    assert set(paths) == expected
