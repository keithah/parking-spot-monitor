# Live Proof Evidence

- Status: `preflight_failed`
- Verification state: `preflight_failed`
- Docker exit code: `None`
- Required marker gaps: `not checked`
- Forbidden markers present: `not checked`
- Latest JPEG: `data/latest.jpg` exists=None valid=None
- Snapshot JPEG count: `0` valid_count=`0`
- Matrix room readback: not_attempted text_found=None image_found=None
- Redaction secret occurrences: `0`
- Redaction replacements: `0`
- Missing inputs: RTSP_URL, matrix.homeserver, matrix.room_id, Matrix token env key
- Requirement status: R003/R015 remain unvalidated

## Operator Handoff
- Outcome: Blocked during preflight. No Docker/live proof was attempted.
- Missing input names: RTSP_URL, matrix.homeserver, matrix.room_id, Matrix token env key
- Required future inputs: RTSP_URL, matrix.homeserver, matrix.room_id, and the configured Matrix token env key.
- Future strict run command: `python scripts/run_docker_live_proof.py` followed by `python scripts/verify_live_proof.py`.
- Validation rule: keep R003/R015 active until strict success includes Docker exit 0, valid JPEG artifacts, Matrix room readback, and zero redaction hits.

## Findings
- missing inputs: RTSP_URL, matrix.homeserver, matrix.room_id, Matrix token env key
- R003/R015 remain unvalidated
