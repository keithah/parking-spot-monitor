# Live Proof Operator Blocker Handoff

## Status

Strict live proof is blocked at preflight because required live inputs are not configured. The canonical wrapper exits with code `2` before invoking Docker proof execution.

## Missing input names

- `RTSP_URL`
- `matrix.homeserver`
- `matrix.room_id`
- `MATRIX_TOKEN_ENV`

## Operator remediation

Provide the real deployment inputs through the project’s normal configuration and secret-management path:

1. Configure `RTSP_URL` in the live runtime environment.
2. Configure `matrix.homeserver` in tracked or deployed configuration.
3. Configure `matrix.room_id` in tracked or deployed configuration.
4. Configure the Matrix token using the environment variable named by the Matrix token env key (`MATRIX_TOKEN_ENV`).

This handoff intentionally records names only. It must not contain RTSP connection strings, Matrix token values, auth headers, raw Matrix API responses, stack traces, image bytes, or invented routing.

## Proof boundary

- Strict Docker proof was not attempted after preflight.
- Docker stdout/stderr logs may be absent because Docker was not invoked.
- Matrix readback status is `not_attempted`.
- JPEG validation has not run against a live capture in this blocked state.
- R003 and R015 remain active and unvalidated.

## Next valid proof attempt

After the missing inputs are configured, run the strict path without readback skips:

1. `python scripts/run_docker_live_proof.py`
2. `python scripts/verify_live_proof.py`

Do not use `--skip-readback`. Do not verify the blocked state as final success. R003/R015 require Docker exit `0`, valid JPEG artifacts, Matrix text/image readback, and zero redaction hits before validation can be claimed.
