# Passing Traffic Candidate Capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture every live calibration check as reviewable S07 passing-traffic evidence and promote only valid passing-traffic frames into the strict replay labels.

**Architecture:** Add a small script that wraps `capture_calibration_bundle.py`, indexes each ignored bundle as a candidate, and updates the private S07 label manifest only for accepted candidates. Candidate metadata stays publication-safe and uses `replay://` references instead of raw paths.

**Tech Stack:** Python stdlib, PyYAML, existing calibration bundle runner, existing S07 label/replay/tuning/package scripts.

---

### Task 1: Candidate index and promotion helper

**Files:**
- Create: `scripts/capture_passing_traffic_candidates.py`
- Create: `tests/test_passing_traffic_candidate_capture.py`

- [ ] Write tests for candidate indexing from bundle manifests.
- [ ] Implement minimal index builder and JSON writer.
- [ ] Write tests for strict-label promotion only when status is `accepted`.
- [ ] Implement label manifest append with publication-safe `replay://` snapshot references.
- [ ] Verify with focused pytest.

### Task 2: CLI capture loop

**Files:**
- Modify: `scripts/capture_passing_traffic_candidates.py`
- Modify: `tests/test_passing_traffic_candidate_capture.py`

- [ ] Write tests that CLI invokes capture runner for N attempts and records candidates.
- [ ] Implement CLI arguments for attempts, interval, candidate index, labels, and accept-latest.
- [ ] Verify focused pytest and real command smoke.
