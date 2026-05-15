# Matrix Owner Vehicle Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add Matrix commands that let an authorized operator ask who is parked where and assign the active vehicle in a spot to the configured owner profile.

**Architecture:** Extend the existing `MatrixCommandService` and `VehicleHistoryArchive` correction patterns instead of adding a separate command channel. Spot-based commands resolve active sessions inside the archive, mutate only the vehicle-history archive, and reply through the existing Matrix text reply path.

**Tech Stack:** Python, pytest, Docker Compose runtime, Matrix Client-Server API via existing `MatrixClient`/`MatrixCommandService`.

---

### Task 1: Add archive owner-assignment primitive

**Files:**
- Modify: `parking_spot_monitor/vehicle_history.py`
- Test: `tests/test_vehicle_history.py`

- [x] Write a failing test that creates an active right-spot session, an owner profile registry, calls `assign_owner_profile_to_active_spot("right_spot")`, and expects the session to get the owner profile with confidence `1.0` while the profile sample count increments.
- [x] Run `python -m pytest tests/test_vehicle_history.py::test_assign_owner_profile_to_active_spot_updates_session_and_profile_sample -q` and confirm it fails because the method is missing.
- [x] Implement `VehicleHistoryArchive.assign_owner_profile_to_active_spot(spot_id, ...)` by resolving exactly one active session for the spot, resolving exactly one configured owner profile, reusing `_profile_with_sample`, updating the session, and logging `vehicle-session-owner-profile-assigned`.
- [x] Run the new test and nearby vehicle-history tests.

### Task 2: Add Matrix parsing and command application

**Files:**
- Modify: `parking_spot_monitor/matrix.py`
- Test: `tests/test_matrix.py`

- [x] Write failing parser tests for `!parking owner right_spot` and `!parking who`.
- [x] Write failing command-service tests showing authorized `!parking owner right_spot` calls the archive assignment method and sends a reply.
- [x] Write failing command-service tests showing `!parking who` calls an archive summary method and sends a readable spot summary.
- [x] Implement Matrix command parsing/actions with existing authorization and reply path.
- [x] Run focused Matrix tests.

### Task 3: Add who-is-parked archive summary and diagnostics

**Files:**
- Modify: `parking_spot_monitor/vehicle_history.py`
- Test: `tests/test_vehicle_history.py`, `tests/test_matrix.py`

- [x] Write failing test for `VehicleHistoryArchive.active_spot_assignments()` returning active sessions with spot, session, profile, label, owner flag, confidence, sample count.
- [x] Implement the summary using active sessions, owner registry, active profiles, and effective labels.
- [x] Format `!parking who` reply from the summary.
- [x] Run focused vehicle-history and Matrix tests.

### Task 4: Document commands and verify runtime

**Files:**
- Modify: `README.md` or operator docs section used by tests
- Test: `tests/test_operator_docs.py` if affected, plus full suite

- [x] Add docs for `!parking who`, `!parking owner <spot_id>`, existing `!parking wrong <spot_id|session_id>`, and `!parking profile summary <profile_id>`.
- [x] Run `python -m pytest tests/test_vehicle_history.py tests/test_matrix.py tests/test_operator_docs.py -q`.
- [x] Run `python -m pytest -q`.
- [x] Rebuild/recreate Docker service and verify live logs/state.
