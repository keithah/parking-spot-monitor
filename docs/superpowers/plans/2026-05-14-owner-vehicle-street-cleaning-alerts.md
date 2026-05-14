# Owner Vehicle Street-Cleaning Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Mark the currently active right-spot Tesla profile as the owner vehicle and send an immediate Matrix alert if that vehicle is parked in either monitored spot during street cleaning.

**Architecture:** Store owner-vehicle metadata under the local vehicle-history data directory so it remains private/operator-owned. The runtime reads that metadata each frame, joins it with active vehicle-history sessions, and emits a deduped Matrix alert when an owner vehicle is active during a quiet window. Owner-vehicle alert text omits dwell/history estimates.

**Tech Stack:** Python 3.12, pytest, Docker Compose, existing MatrixDelivery and vehicle-history JSON archive.

---

### Task 1: Owner metadata model and alert formatting

**Files:**
- Create: `parking_spot_monitor/owner_vehicles.py`
- Modify: `parking_spot_monitor/matrix.py`
- Test: `tests/test_owner_vehicles.py`, `tests/test_matrix.py`

- [x] Write failing tests for loading `owner-vehicles.json` and formatting an owner street-cleaning alert.
- [x] Run targeted tests and confirm they fail because the module/formatter does not exist.
- [x] Implement `OwnerVehicleRegistry` with safe JSON parsing and profile matching.
- [x] Add Matrix text/event ID formatter for `owner-vehicle-quiet-window-alert`.
- [x] Run targeted tests and confirm they pass.

### Task 2: Runtime detection and dedupe

**Files:**
- Modify: `parking_spot_monitor/state.py`
- Modify: `parking_spot_monitor/__main__.py`
- Test: `tests/test_startup.py`, `tests/test_state.py`

- [x] Write failing tests that an active owner profile in a quiet window sends exactly one owner alert and persists a dedupe ID.
- [x] Run targeted tests and confirm they fail.
- [x] Add `owner_quiet_window_alert_ids` to runtime state with backward-compatible loading.
- [x] Add runtime owner-alert evaluation after vehicle-history processing and before state save.
- [x] Run targeted tests and confirm they pass.

### Task 3: Mark current Tesla profile, docs, full verification, deploy

**Files:**
- Create local ignored data: `data/vehicle-history/owner-vehicles.json`
- Append local correction event through `VehicleHistoryArchive.rename_profile`.
- Modify: `README.md`, `CHANGELOG.md`

- [x] Mark profile `prof_sess-right-spot-2026-05-12t16-14-03-187234-00-00` as `Keith's black Tesla` with description `black Tesla, tinted windows, roof rack`.
- [x] Update docs/changelog with owner-vehicle street-cleaning behavior.
- [x] Run full `python -m pytest -q`.
- [x] Rebuild and force-recreate Docker Compose service.
- [x] Verify container startup, capture, detection, and state-save logs.
