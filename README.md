# Parking Spot Monitor

Local Python service for monitoring configured street-parking regions in a UniFi Protect RTSP stream and notifying Matrix when a spot changes from occupied to empty.

The working product spec is in [`parking-spot-monitor-spec.md`](parking-spot-monitor-spec.md).

## Local configuration

Start from the tracked example and keep real secrets out of YAML and committed files:

```sh
cp config.yaml.example config.yaml
```

`config.yaml` should contain environment variable names only. The service resolves these required variables at startup; provide them through your shell, service manager, or ignored `.env` file used by the live-proof wrapper:

- `RTSP_URL`
- `MATRIX_ACCESS_TOKEN`

Runtime artifact paths are anchored by the effective `--data-dir` value. Keep `storage.snapshots_dir` and `runtime.health_file` relative to the effective `--data-dir` unless you intentionally want an absolute operator override:

```yaml
storage:
  data_dir: ./data
  snapshots_dir: snapshots
  snapshot_retention_count: 50
runtime:
  health_file: health.json
  frame_interval_seconds: 30
```

With the Docker default `--data-dir /data`, those relative values resolve to `/data/snapshots` and `/data/health.json`; with local `--data-dir ./data`, they resolve to `./data/snapshots` and `./data/health.json`. `storage.snapshot_retention_count` bounds Matrix event-specific JPEG snapshots in the snapshots directory (default `50`) while leaving `latest.jpg`, `debug_latest.jpg`, `state.json`, `health.json`, malformed filenames, and unrelated operator files untouched. `runtime.frame_interval_seconds` paces successful runtime-loop iterations (default `30`) while capture failures continue to use `stream.reconnect_seconds` backoff.

Never paste camera URLs, usernames, passwords, Matrix tokens, or other secret values into `config.yaml`, `docker-compose.yml`, README examples, logs, or committed files.

## Operator calibration and debug artifact guide

Use this section when the parking spots need tuning after a camera move, frame-size change, detector upgrade, or false-positive/missed-detection report. The supported operator boundary is deliberately narrow: edit configured spot polygons and shared detection/occupancy thresholds in `config.yaml`; do not edit Python code for routine calibration. The runtime does not currently implement a per-spot threshold schema, so do not add or rely on fields such as per-spot confidence, area, overlap, IoU, confirm, or release thresholds.

### 1. Copy and validate the operator config

Start from the tracked example and keep secret values in environment variables, not YAML:

```sh
cp config.yaml.example config.yaml
RTSP_URL="<operator-provided-camera-stream>" \
MATRIX_ACCESS_TOKEN="<operator-provided-matrix-token>" \
python -m parking_spot_monitor --config config.yaml --validate-config
```

Validation resolves `stream.rtsp_url_env` and `matrix.access_token_env` by name, then checks the schema before runtime. Failure diagnostics should name fields or environment variable names such as `RTSP_URL` and `MATRIX_ACCESS_TOKEN`; they must not include resolved camera URLs, Matrix access tokens, Authorization headers, Matrix response bodies, tracebacks, YAML dumps, or image bytes.

### 2. Edit spot polygons safely

`spots.left_spot.polygon` and `spots.right_spot.polygon` are YAML lists of `[x, y]` coordinate pairs in the captured frame coordinate system. Coordinates are relative to `stream.frame_width` and `stream.frame_height`, where `x=0, y=0` is the top-left of the frame and `x=stream.frame_width, y=stream.frame_height` is the bottom-right edge. Each spot needs at least three points so the polygon has an area.

Keep every polygon point inside the configured frame. Config validation rejects out-of-frame points and reports the offending polygon/index instead of starting the monitor with ambiguous geometry. After editing, rerun `--validate-config` before any capture attempt.

A safe local calibration loop is:

```sh
cp config.yaml.example config.yaml
# Edit only config.yaml: spot polygons plus shared detection/occupancy thresholds.
RTSP_URL="<operator-provided-camera-stream>" \
MATRIX_ACCESS_TOKEN="<operator-provided-matrix-token>" \
python -m parking_spot_monitor --config config.yaml --validate-config
mkdir -p data
python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once
ls -l data/latest.jpg data/debug_latest.jpg
```

For the same bounded capture through Docker, use the Compose service and mounted `/data` path:

```sh
mkdir -p data
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor --config /config/config.yaml --data-dir /data --capture-once
ls -l data/latest.jpg data/debug_latest.jpg
```

### 3. Operator-level config map

- `stream` / camera: `rtsp_url_env` names the camera URL environment variable, `frame_width` and `frame_height` define the coordinate system for polygons, and `reconnect_seconds` controls capture retry backoff after capture failures.
- `spots`: each configured spot has a human-readable `name` and a polygon. Tune these polygons to cover only the street parking regions being monitored; exclude driveway cars or unrelated curb areas by moving polygon points, not by adding Python logic.
- `detection`: `model` selects the local YOLO model name/path, `confidence_threshold` is the shared minimum detector confidence, `inference_image_size` sets the Ultralytics YOLO `imgsz` used for prediction, `spot_crop_inference` optionally runs additional YOLO passes on configured spot crops, `spot_crop_margin_px` controls the crop padding, `open_suppression_min_confidence` keeps lower-confidence detector output available for false-open suppression, `open_suppression_classes` lists class names that may suppress open alerts when they overlap a spot, `min_bbox_area_px` filters tiny detections, `min_polygon_overlap_ratio` requires overlap with a configured spot polygon, and `vehicle_classes` lists accepted vehicle class names. These thresholds are shared across configured spots.
- `occupancy`: `iou_threshold`, `confirm_frames`, and `release_frames` debounce detector output into stable occupied/empty state. These are shared occupancy controls, not spot-specific schema. Weak in-spot evidence can suppress open alerts without confirming a new occupied state; this uses `detection.open_suppression_min_confidence` and `detection.open_suppression_classes` so visible low-confidence vehicles do not become false open alerts.
- `matrix`: `homeserver`, `room_id`, and related routing fields define delivery targets; `access_token_env` must remain the environment variable name `MATRIX_ACCESS_TOKEN`, not a token value.
- `quiet_windows`: configured quiet periods such as street sweeping suppress open-spot alerts while still emitting diagnostic quiet-window events.
- `storage`: `data_dir` anchors local artifacts, `snapshots_dir` stores retained Matrix event/live-proof snapshots relative to the data directory unless absolute, and `snapshot_retention_count` bounds retained event snapshots.
- `runtime`: `health_file`, `log_level`, `startup_timeout_seconds`, and `frame_interval_seconds` control operator health output, structured logging level, startup guardrails, and successful-loop pacing.

### 4. Interpret local artifacts

- `data/latest.jpg` is raw full-frame evidence from the most recent successful capture. It is intentionally unannotated so Matrix alert snapshots and later evidence retain the original camera frame.
- `data/debug_latest.jpg` is a local RGB JPEG polygon overlay for tuning only. Use it to confirm configured street polygons line up with the raw frame. Do not treat the overlay as Matrix alert evidence and do not publish it without review.
- `data/snapshots/` retains Matrix event/live-proof snapshots according to `storage.snapshot_retention_count`. Retention applies to event snapshots, not to `latest.jpg`, `debug_latest.jpg`, `health.json`, `state.json`, malformed filenames, or unrelated operator files.
- `data/health.json` summarizes current runtime status, selected decode mode, timestamps, failure counters, Matrix delivery errors, retention failures, state-save errors, and last sanitized error. Inspect it with `python -m json.tool data/health.json`.
- `data/state.json` stores conservative restart state: spot status, hit/miss streaks, duplicate-open suppression markers, quiet-window state, and related occupancy context. Inspect it with `python -m json.tool data/state.json` when state transitions look surprising.
- Calibration bundles, replay reports, and tuning reports are local evidence gates. They summarize safe metadata and decisions; they do not embed raw images, image payload bytes, camera URLs, Matrix tokens, Authorization headers, Matrix response bodies, or tracebacks.

### 5. Debug false positives and missed detections

When a spot reports occupied while it should be empty, or misses a real parked vehicle, debug one layer at a time:

1. Compare `data/latest.jpg` with `data/debug_latest.jpg`. If the overlay polygon covers the wrong curb area, adjust the relevant polygon in `config.yaml`, validate config, and rerun a bounded capture.
2. Inspect JSON-line logs for `capture-frame-written` to confirm the raw frame was refreshed and for `debug-overlay-written` or `debug-overlay-failed` to confirm overlay generation succeeded or failed safely.
3. Inspect `detection-frame-processed`. Its accepted candidate summaries and rejection counts show whether detections were rejected by confidence, class, area, polygon overlap, or spot filtering.
4. Treat `detection-frame-failed` as an inference/model/runtime failure, not as evidence that the spot is empty. Fix the model/config/runtime issue before changing thresholds.
5. For threshold changes, use `scripts/replay_calibration_cases.py` and `scripts/compare_calibration_tuning.py` with operator-labeled evidence. Apply only shared threshold changes supported by replay/tuning reports; a `needs_per_spot_thresholds` tuning decision is follow-up design evidence, not permission to invent runtime schema in `config.yaml`.

This guide does not prove a live camera or Matrix room without real operator-provided secrets. It also does not add a browser calibration surface, NVR/retention management surface, cloud AI processing, license plate recognition, driveway-car monitoring support, encrypted Matrix room support, or non-root container hardening. Keep raw frames, snapshots, health/state, calibration bundles, replay/tuning reports, and redacted runtime logs local unless they have been deliberately reviewed for safe publication.

## Clean-machine setup and Docker Compose operator guide

This guide is the fresh-clone path for an operator who wants to validate configuration, launch the Docker Compose service, inspect logs/restarts, and confirm the first local artifacts without relying on private session context.

### 1. Prerequisites

Install these on the host before running the commands below:

- Python 3.11 or newer for local config checks and optional validation commands.
- Docker with the Compose plugin (`docker compose ...`) for container build/run operations.
- Optional Intel hardware decode access at `/dev/dri` if the host supports VAAPI/QSV passthrough.
- `pytest` only if you are running repository validation tests such as `python -m pytest tests/test_docker_contract.py -q`.

Docker unavailable is an environment/setup blocker, not a different application contract. The commands below remain the operator contract; install Docker/Compose before exercising the container path.

### 2. Create operator config and provide secrets by environment name

Start from the tracked example and keep real secret values outside committed files:

```sh
cp config.yaml.example config.yaml
```

`config.yaml` names the required environment variables; it must not contain the camera stream value or Matrix access-token value. Provide the values through your shell, service manager, or another local secret mechanism that is not committed:

```sh
export RTSP_URL="<operator-provided-camera-stream>"
export MATRIX_ACCESS_TOKEN="<operator-provided-matrix-token>"
python -m parking_spot_monitor --config config.yaml --validate-config
```

Missing or empty values should fail validation with diagnostics naming `RTSP_URL` and/or `MATRIX_ACCESS_TOKEN` only. Do not paste resolved camera URLs, Matrix tokens, room-private responses, or raw logs into docs, tickets, or committed files.

The tracked Compose file does not define an `env_file`. It passes `RTSP_URL` and `MATRIX_ACCESS_TOKEN` by name from the environment where `docker compose` is invoked.

### 3. Inspect the actual Compose contract

The Compose service is named `parking-spot-monitor`. It builds the local Dockerfile and tags the local image as `parking-spot-monitor:local`; you can also build an explicit test tag for clean-machine verification:

```sh
docker build -t parking-spot-monitor:test .
docker compose config --no-interpolate
```

Use `docker compose config --no-interpolate` for structure-only inspection because it avoids expanding secret values. The service contract is:

- Service name: `parking-spot-monitor`.
- Runtime command: `python -m parking_spot_monitor --config /config/config.yaml --data-dir /data`.
- Config mount: `./config.yaml:/config/config.yaml:ro`.
- Data mount: `./data:/data`.
- Environment names passed through by name: `RTSP_URL` and `MATRIX_ACCESS_TOKEN`.
- No `env_file` contract in `docker-compose.yml`.
- Optional model mount shown as a commented example: `./models:/models:ro`.

### 4. Launch, inspect, restart, and stop the service

Create the host data directory first so artifacts written to `/data` in the container appear under `./data` on the host:

```sh
mkdir -p data
docker compose up parking-spot-monitor
```

For unattended operation, run the same service detached and inspect it with Compose:

```sh
docker compose up -d parking-spot-monitor
docker compose logs -f parking-spot-monitor
docker compose ps
docker compose restart parking-spot-monitor
docker compose down
```

`docker compose logs -f parking-spot-monitor` is the primary runtime log surface. Expect structured event names for startup/config/capture/detection/state/Matrix diagnostics, including `startup-ready`, `startup-config-invalid`, `capture-frame-written`, capture attempt/write/failure events, `detection-frame-processed`, `detection-frame-failed`, `state-loaded`, `state-saved`, `state-corrupt-quarantined`, `health-write-failed`, `occupancy-state-changed`, `occupancy-open-event`, `occupancy-open-suppressed`, Matrix delivery success/failure diagnostics, and quiet-window events such as `quiet-window-started` and `quiet-window-ended`.

### 5. Run a finite Docker capture smoke and inspect first artifacts

Use this bounded command when you want one capture attempt instead of the continuous monitoring loop:

```sh
mkdir -p data
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor --config /config/config.yaml --data-dir /data --capture-once
```

With real operator secrets and a reachable camera, a successful capture writes `/data/latest.jpg` in the container, visible as `./data/latest.jpg` on the host. After the runtime has written health and state, inspect the operator surfaces with:

```sh
ls -l data/latest.jpg
python -m json.tool data/health.json
python -m json.tool data/state.json
find data/snapshots -maxdepth 1 -type f | sort
```

`data/latest.jpg`, `data/debug_latest.jpg`, `data/health.json`, `data/state.json`, and `data/snapshots/` are local operator artifacts. Keep raw frames, snapshots, health/state, and redacted runtime logs local unless they have been deliberately reviewed for publication.

### 6. Optional `/dev/dri` hardware decode passthrough

`docker-compose.yml` maps `/dev/dri:/dev/dri` so Intel VAAPI/QSV hardware decode can be used when the host exposes the device and permissions allow it. Hosts without `/dev/dri` should remove or override that device mapping and rely on software fallback.

Do not claim QSV success from the presence of the mapping alone. Verify the active hardware surface with:

```sh
python scripts/verify_hardware_decode.py --json
```

QSV may be unavailable even when VAAPI works; the runtime should fall back safely and report the selected decode mode in logs and `data/health.json`.

### 7. Scope and safety boundaries

This guide documents setup and inspection surfaces; it does not prove a live camera or Matrix room without operator-supplied real secrets. The project does not currently claim encrypted Matrix room support, non-root container hardening, per-spot threshold schema, or a historical occupancy query UI/API. Missing live inputs, skipped Matrix readback, send-only Matrix responses, Docker startup failures, redaction hits, and no-alert observation windows are gaps or blockers, not validation success.

## Troubleshooting and cleanup runbook

Use this runbook after the clean-machine setup is complete and the service is being operated through the documented local surfaces. Each case follows the same pattern: symptom, evidence to inspect, and safe remediation. Keep evidence local while investigating. Do not paste camera stream values, credentials, Matrix tokens, private room IDs, Matrix content URIs, raw response bodies, tracebacks, or image payload bytes into docs, tickets, commits, or shared logs.

### RTSP/capture failures or reconnect symptoms

**Symptom:** `data/latest.jpg` stops updating, the service repeatedly waits before the next capture attempt, or no first frame appears after startup.

**Evidence to inspect:**

- `docker compose logs -f parking-spot-monitor` for capture diagnostics such as `capture-frame-written`, `capture-decode-fallback`, and `capture-all-modes-failed`.
- `data/health.json` with `python -m json.tool data/health.json` for capture failure counters, selected decode mode, timestamps, and sanitized last-error fields.
- `data/latest.jpg` file timestamp and size to confirm whether a new frame was actually written.
- `config.yaml` field names only: `stream.rtsp_url_env`, `stream.reconnect_seconds`, `stream.frame_width`, and `stream.frame_height`.

**Safe remediation:**

- Confirm the configured environment variable name is still `RTSP_URL`, then use `docker compose config --no-interpolate` to inspect wiring without expanding the value.
- Validate local config shape with `RTSP_URL="<operator-provided-camera-stream>" MATRIX_ACCESS_TOKEN="<operator-provided-matrix-token>" python -m parking_spot_monitor --config config.yaml.example --validate-config` when you only need schema evidence from the tracked example.
- Adjust `stream.reconnect_seconds` only if the retry cadence is too aggressive for the camera or network.
- Restart the service boundary with `docker compose restart parking-spot-monitor`; use `docker compose down` followed by `docker compose up -d parking-spot-monitor` only when you need a clean container start.

### Hardware decode/device passthrough issues

**Symptom:** Capture works only after fallback, startup logs mention unavailable hardware decode, or the host has no usable `/dev/dri` device.

**Evidence to inspect:**

- `docker compose ps` to confirm the container is running rather than crash-looping.
- `docker compose logs -f parking-spot-monitor` for `capture-decode-fallback` and final capture status.
- `data/health.json` for `selected_decode_mode` and capture failure counters.
- `docker-compose.yml` for the `/dev/dri:/dev/dri` device mapping.

**Safe remediation:**

- On hosts without `/dev/dri`, remove or override the `/dev/dri:/dev/dri` mapping and rely on software decode.
- If hardware decode intermittently fails, keep the current config and verify fallback still produces `capture-frame-written` and a refreshed `data/latest.jpg` before changing detector or occupancy thresholds.
- Recreate the service with `docker compose up -d parking-spot-monitor` after changing Compose device wiring.

### Matrix send/upload failures

**Symptom:** Occupancy transitions are observed locally, but Matrix text or image delivery fails or is not visible to the operator.

**Evidence to inspect:**

- `docker compose logs -f parking-spot-monitor` for safe Matrix event names such as `matrix-send-failed`, `matrix-delivery-failed`, and upload/send phase summaries.
- `data/health.json` for `last_matrix_error`, delivery failure counters, and last successful delivery timestamps when present.
- `data/snapshots/` for retained local event snapshots that prove the service produced local evidence before delivery.
- `config.yaml` Matrix routing fields by name only: `matrix.homeserver`, `matrix.room_id`, `matrix.access_token_env`, timeout, retry count, and retry backoff.

**Safe remediation:**

- Confirm `matrix.access_token_env` still names `MATRIX_ACCESS_TOKEN`; do not paste the token into YAML or logs.
- Use `docker compose config --no-interpolate` to verify `MATRIX_ACCESS_TOKEN` is passed by name.
- Tune only documented Matrix timeout/retry fields in `config.yaml`, then run `docker compose restart parking-spot-monitor`.
- Treat send responses without room readback as delivery-attempt evidence only, not a live Matrix delivery guarantee.

### detector misses/false negatives

**Symptom:** A parked vehicle is visible in the monitored street spot, but the service does not reach an occupied state or misses an opening transition.

**Evidence to inspect:**

- `data/latest.jpg` for the raw frame and `data/debug_latest.jpg` for local polygon alignment.
- `docker compose logs -f parking-spot-monitor` for `detection-frame-processed` accepted/rejected summaries and `detection-frame-failed` failures.
- `data/state.json` with `python -m json.tool data/state.json` for hit/miss streaks and the last stable occupancy state.
- Shared config fields under `detection` and `occupancy`, especially confidence, minimum area, polygon-overlap ratio, IoU, confirm frames, and release frames.

**Safe remediation:**

- Fix polygon alignment first by editing only `spots.*.polygon` in `config.yaml`, then validate config and rerun a bounded capture.
- Change shared detector/occupancy thresholds only after comparing local evidence; the current schema does not support per-spot thresholds.
- Do not treat `detection-frame-failed` as empty-spot evidence. Fix the model/runtime/config failure before tuning occupancy behavior.

### false positives/passing traffic

**Symptom:** Passing traffic, driveway cars, or unrelated curb activity causes a spot to be marked occupied or opened incorrectly.

**Evidence to inspect:**

- `data/latest.jpg` and `data/debug_latest.jpg` to compare raw evidence with the configured street polygons.
- `data/state.json` for stable state, duplicate-open suppression markers, and transition context.
- `docker compose logs -f parking-spot-monitor` for `detection-frame-processed`, rejection counts, and `occupancy-state-changed` / `occupancy-open-event` timing.

**Safe remediation:**

- Move polygon points to cover only the supported street-parking region; do not expand the product scope to driveway-car monitoring.
- Increase shared overlap/area/confidence thresholds only when local evidence shows passing traffic is being counted inside the polygon.
- After config edits, run `python -m parking_spot_monitor --config config.yaml --validate-config`, then restart with `docker compose restart parking-spot-monitor`.

### Street-sweeping or quiet-window behavior

**Symptom:** A real opening occurs but no Matrix open-spot alert is sent, or quiet-window notices appear around the configured street-sweeping window.

**Evidence to inspect:**

- `data/state.json` for active quiet-window IDs and duplicate-open suppression context.
- `docker compose logs -f parking-spot-monitor` for `quiet-window-started`, `quiet-window-ended`, and `occupancy-open-suppressed`.
- `config.yaml` under `quiet_windows` for the street-sweeping timezone, recurrence, weekday, ordinal, start, and end values.

**Safe remediation:**

- Correct only the documented quiet-window config fields, then validate config and restart the service.
- Treat `occupancy-open-suppressed` during an active quiet-window as expected suppression, not Matrix failure.
- Preserve `data/state.json` before deleting or resetting state so the quiet-window/restart evidence is not lost.

### restart/state corruption recovery

**Symptom:** After restart, spot state looks unknown, duplicate suppression changed, or startup reports corrupt state recovery.

**Evidence to inspect:**

- `docker compose logs -f parking-spot-monitor` for `state-loaded`, `state-saved`, and `state-corrupt-quarantined`.
- `data/state.json` and any quarantined corrupt state file beside it.
- `data/health.json` for state-save failure counters and sanitized last-error fields.

**Safe remediation:**

- Use `docker compose restart parking-spot-monitor` for ordinary restart checks.
- If `data/state.json` was quarantined, keep the quarantined file as local evidence and let the service continue from conservative unknown defaults.
- Stop the service with `docker compose down` before manually moving generated state evidence, and never delete `config.yaml` or secret-management files as part of state recovery.

### permissions/disk write failures

**Symptom:** Health, state, latest frame, debug overlay, or snapshot artifacts are missing even though the container is running.

**Evidence to inspect:**

- `docker compose ps` for container status and `docker compose logs -f parking-spot-monitor` for `health-write-failed`, `state-save-failed`, `debug-overlay-failed`, capture write failures, and snapshot retention failures.
- The Compose mount `./data:/data` and host directory permissions on `data/`.
- `data/health.json` when present for write failure counters.

**Safe remediation:**

- Stop the service with `docker compose down`, fix ownership/permissions or recreate the generated `data/` directory, then start with `docker compose up -d parking-spot-monitor`.
- Preserve existing `data/latest.jpg`, `data/debug_latest.jpg`, `data/health.json`, `data/state.json`, and `data/snapshots/` evidence before pruning or recreating generated data directories.
- Do not broaden container privileges unless the operator intentionally changes the deployment boundary.

### snapshot/disk cleanup

**Symptom:** Local disk usage grows under generated runtime data, especially retained event snapshots or logs.

**Evidence to inspect:**

- `data/snapshots/` for Matrix event/live-proof snapshots.
- `storage.snapshot_retention_count` in `config.yaml` and source-backed retention events `snapshot-retention-pruned` and `snapshot-retention-failed` in logs.
- `data/health.json` for retention failure counters.
- `docker compose logs -f parking-spot-monitor` for log volume and repeated failure loops.

**Safe remediation:**

- Tune `storage.snapshot_retention_count` to a bounded value appropriate for the host disk, validate config, and restart the service.
- Before deleting generated artifacts, stop the service with `docker compose down` and preserve any evidence needed for debugging.
- Prune only generated local artifacts such as old files under `data/snapshots/` after review. Do not delete `config.yaml`, Compose files, model files, secret stores, or current evidence files unless the operator has deliberately backed them up.

## Non-goals and deferred capabilities

These boundaries are current-state documentation, not future-work claims:

- There is no supported web UI for calibration, operations, historical queries, or live monitoring; local docs alone do not provide a browser surface.
- This project is not an NVR/video archive. It writes bounded/current local artifacts and documented snapshots, not continuous video history.
- It does not implement license-plate recognition, person identification, or other identity extraction.
- It has no cloud AI dependency in the runtime contract; detection is local model execution from the configured model name/path.
- It does not provide an encrypted Matrix-room hardening guarantee. Matrix routing is send-oriented and depends on the operator-provided homeserver, room, and token environment.
- It does not support driveway-car monitoring as a product goal. Operators should tune polygons to street-parking regions only.
- It does not support per-spot threshold configuration when the runtime schema is shared-only; use shared `detection` and `occupancy` fields unless a later evidence-gated design changes the schema.
- It does not provide live-camera proof from repository tests, README examples, or local docs alone. Live-camera proof requires operator-provided runtime inputs and the documented live-proof workflows.
- It does not provide a live Matrix delivery guarantee from send attempts or docs alone. Room-visible Matrix delivery requires explicit live proof/readback evidence.

## Finite validation and capture smoke checks

Use `--validate-config` for finite startup/configuration checks. Against an operator config with required environment variables already supplied, the direct validation command is `python -m parking_spot_monitor --config config.yaml --validate-config`. Use `--capture-once` for the S02/S03 finite capture proof: it attempts one frame capture, writes `latest.jpg`, refreshes the local debug overlay at `debug_latest.jpg`, and exits instead of starting the continuous monitoring loop. Live R003 acceptance requires a real operator RTSP environment supplied through environment variables; do not commit those values or paste them into examples.

Local validation against the tracked example can use non-secret placeholder values because it does not connect to the camera or Matrix:

```sh
python - <<'PY'
import os
import subprocess
import sys

os.environ["RTSP_URL"] = "placeholder"
os.environ["MATRIX_ACCESS_TOKEN"] = "placeholder"
sys.exit(subprocess.call([
    sys.executable,
    "-m",
    "parking_spot_monitor",
    "--config",
    "config.yaml.example",
    "--validate-config",
]))
PY
```

Local one-frame capture proof against the operator config uses the real environment already present in your shell or service manager:

```sh
mkdir -p data
python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once
```

Run the finite live RTSP + Matrix proof when you need evidence that the assembled camera-to-room boundary works without waiting for an organic parking event. This is a strict two-step flow: first run the Docker live-proof producer, then run the verifier/report writer.

```sh
python scripts/run_docker_live_proof.py
python scripts/verify_live_proof.py
```

`scripts/run_docker_live_proof.py` runs the Dockerized `--live-proof-once` command, passes `RTSP_URL` and the configured Matrix token environment key to `docker compose run`, captures redacted Docker stdout/stderr logs, performs Matrix room readback, and produces `data/live-proof-result.json`. It requires real live inputs by name/path only: `config.yaml`, `RTSP_URL`, Matrix access-token environment routing such as `MATRIX_ACCESS_TOKEN`, and the Matrix homeserver/room routing in operator config. Do not paste actual RTSP URLs, Matrix tokens, room response bodies, or other secret values into examples, logs, or reports.

Runner exit codes are part of the proof contract: `0` means the runner completed strict live proof, `2` means preflight blockers such as missing `config.yaml`, `RTSP_URL`, or Matrix token routing prevented live proof from running, and any other non-zero exit means Docker execution, marker checks, artifact checks, Matrix room readback, or redaction validation failed. Missing live inputs are blockers, not successful proof; preflight blocker evidence means R003/R015 remain unvalidated.

`scripts/verify_live_proof.py` is the strict verifier. It reads `data/live-proof-result.json`, exits `0` only for strict success, exits non-zero on non-success statuses unless explicitly invoked for blocker reporting, and writes `data/live-proof-evidence.md`. The strict verifier exits non-zero when success is overclaimed without required markers, valid JPEG artifacts, Matrix room readback, or clean redaction results.

Strict success requires all of the following before R003/R015 can be validated: `LIVE_RTSP_CAPTURE_OK`, `LIVE_MATRIX_TEXT_OK`, and `LIVE_MATRIX_IMAGE_OK` are present; skip/failure markers are absent; `data/latest.jpg` is a valid raw camera JPEG; at least one `data/snapshots/live-proof-*.jpg` JPEG is retained; Matrix room readback verifies both visibly labelled `LIVE PROOF / TEST MESSAGE` text and `LIVE PROOF / TEST IMAGE` image evidence in the target room; and redaction scans find zero RTSP URLs, auth header markers, Matrix access-token strings, raw Matrix response bodies, tracebacks, or image bytes in logs/reports. Do not use `--skip-readback` for validation: skipped or unavailable readback leaves R003/R015 remain unvalidated because send responses alone do not prove room-visible Matrix delivery.

Skip markers identify missing live inputs (`LIVE_PROOF_SKIPPED_CONFIG_ABSENT`, `LIVE_PROOF_SKIPPED_RTSP_ENV_ABSENT`, or `LIVE_PROOF_SKIPPED_MATRIX_ENV_ABSENT`) and are blockers, not validation. Failure markers identify the failed phase (`LIVE_RTSP_CAPTURE_FAILED`, `LIVE_MATRIX_TEXT_FAILED`, or `LIVE_MATRIX_IMAGE_FAILED`) without logging RTSP URLs, Matrix tokens, Authorization headers, raw Matrix response bodies, tracebacks, or image bytes.

## Unattended Docker live alert soak

Use the unattended alert soak when you need bounded evidence that the tuned Docker service can run continuously and emit organic `occupancy-open-event` Matrix alerts without duplicate restart spam. This is a strict two-step flow: first run the bounded Docker soak producer, then run the strict verifier/report writer.

```sh
python scripts/run_docker_alert_soak.py
python scripts/verify_alert_soak.py
```

`scripts/run_docker_alert_soak.py` runs `docker compose run --rm ... parking-spot-monitor` for `--soak-seconds` (default `300`), treats the controlled timeout as normal soak completion, captures redacted Docker stdout/stderr logs, summarizes `health.json` and `state.json`, validates `latest.jpg` and `snapshots/occupancy-open-event-*.jpg`, records duplicate event/Matrix transaction diagnostics, performs Matrix room readback for observed organic open alerts, and writes `data/alert-soak-result.json` plus `data/alert-soak-evidence.md`. `scripts/verify_alert_soak.py` is the strict gate over that JSON. It exits `0` only for strict success unless explicitly invoked with `--allow-coverage-gap` or `--allow-preflight-blocker` to render honest non-validation handoff evidence.

Alert-soak statuses mean:

- `success` — strict success: Docker completed normally or reached the bounded soak timeout; at least one organic `occupancy-open-event` was observed; each alert has a matching valid raw occupancy-open-event snapshot; `latest.jpg` is a valid raw camera JPEG; `health.json` is present and diagnosable; `state.json` is parseable; Matrix readback is `verified` for every alert; duplicate event IDs and Matrix transaction IDs are empty; and redaction scans report zero secret or forbidden-pattern occurrences.
- `coverage_gap_no_alert` — no organic open event occurred during the bounded window. This is honest evidence about the soak window and may be rendered with `python scripts/verify_alert_soak.py --allow-coverage-gap`, but it is not full S08 strict live soak validation unless final closure explicitly accepts the residual risk.
- `preflight_failed` / preflight blocker — required live inputs such as `config.yaml`, `RTSP_URL`, Matrix routing, or Matrix token environment routing were missing before Docker ran. This can be rendered with `--allow-preflight-blocker`, but it is blocker evidence rather than validation.
- `docker_failed` — Docker startup/runtime exited unexpectedly or could not be launched. Inspect the safe Docker argv, phase, redacted Docker logs, and health/state summaries before rerunning.
- `readback_gap` — alerts were emitted, but Matrix room readback was skipped, unavailable, malformed, or did not show the expected per-spot text and raw snapshot messages. Send responses alone do not prove room-visible delivery.
- `validation_failed` with phase `duplicate_diagnostics` — duplicate organic event IDs or duplicate Matrix transaction IDs were detected, which is duplicate-spam failure evidence.
- `validation_failed` with phase `artifact_validation` — required raw snapshot artifacts are missing or invalid, including invalid `latest.jpg` or invalid `occupancy-open-event` JPEG snapshots.
- `validation_failed` with phase `redaction` — `data/alert-soak-result.json`, `data/alert-soak-evidence.md`, or the redacted Docker logs still contain secret or forbidden publication markers.
- Any verifier-level `verification_failed` report means the result JSON was missing, malformed, inconsistent, unsupported, or unsafe to publish.

Publication-safety rules are the same as the finite live proof and calibration workflows, but the alert-soak artifact boundary is specific. Keep `data/alert-soak-result.json`, `data/alert-soak-evidence.md`, `data/alert-soak-docker.stdout.log`, `data/alert-soak-docker.stderr.log`, raw snapshots under `data/snapshots/`, `data/latest.jpg`, `data/health.json`, and `data/state.json` local and ignored until reviewed. The JSON and Markdown evidence may summarize safe fields such as status, phase, safe Docker argv, per-spot alert/readback status, duplicate counts, artifact validity counts, health/state parse summaries, and redaction-scan counts. They must not include RTSP URLs, Matrix access tokens, auth header markers, raw Matrix response bodies, tracebacks, image payload bytes, or unredacted Docker output.

The soak workflow does not tune polygons/thresholds and does not add per-spot runtime schema. If live alert divergence suggests bad polygons, shared thresholds, or spot-specific behavior, feed new private labels through `scripts/compare_calibration_tuning.py` first and use its evidence gate before changing runtime tuning.

## Dockerized calibration bundle capture

Use `scripts/capture_calibration_bundle.py` when an operator needs one publication-safe calibration evidence bundle from the live Docker runtime. This workflow is for calibration and detector tuning evidence only: it does not prove Matrix room delivery like `scripts/run_docker_live_proof.py`, and it does not replace later replay or threshold-tuning slices.

```sh
python scripts/capture_calibration_bundle.py
```

By default the wrapper reads host `config.yaml`, uses host `./data`, runs Docker Compose with the existing application `--capture-once` path, and writes timestamped bundles under `data/calibration-bundles/`. Override paths only when you intentionally want a different operator workspace:

```sh
python scripts/capture_calibration_bundle.py \
  --config config.yaml \
  --data-dir ./data \
  --bundle-root ./data/calibration-bundles \
  --docker-timeout-seconds 180
```

Exit codes are part of the operator contract:

- `0` means Dockerized capture completed, required JPEG artifacts validated, detection summary evidence was found, and the redaction scan found no forbidden private text.
- `2` means names-only preflight blocked the run before Docker because required inputs such as the config file, RTSP environment variable name, or Matrix token environment routing were absent.
- `124` means Docker capture exceeded `--docker-timeout-seconds`; any partial stdout/stderr captured by Python is still redacted in the bundle when a bundle directory exists.
- Any other Docker non-zero exit is preserved as a `docker_failed` result so capture/config/runtime failures are not hidden by the wrapper.
- `1` means wrapper validation failed after Docker, including missing/invalid JPEG artifacts, missing detection evidence, malformed copied context, or redaction findings.

Each successful or failed post-preflight bundle is agent-readable without opening private raw logs first. Expect these files inside the timestamped bundle:

- `latest.jpg` — copied raw full-frame capture from `data/latest.jpg`.
- `debug_latest.jpg` — copied local tuning overlay from `data/debug_latest.jpg`.
- `docker.stdout.log` and `docker.stderr.log` — line-preserving redacted Docker output.
- `manifest.json` — status, phase, Docker exit code, safe Docker argv, timestamps, selected decode mode when emitted, capture/detection event summaries, artifact validation, context copy status, validation errors, and redaction scan results.
- `calibration-report.md` — short local README/report for humans and future agents.
- `context/health.json` and `context/state.json` when present — optional live runtime health/state context copied for inspection; missing or malformed context is reported as a gap rather than silently ignored.

The wrapper also writes `data/calibration-input-preflight.json` for preflight attempts. That file contains blocker names and presence booleans only; it intentionally omits raw camera URLs, token values, auth headers, YAML contents, image bytes, and private room responses.

Use this local preflight smoke when you need names-only blocker evidence without live secrets or a real config:

```sh
tmpdir=$(mktemp -d)
env -u RTSP_URL -u MATRIX_ACCESS_TOKEN \
  python scripts/capture_calibration_bundle.py --config "$tmpdir/missing.yaml"
rm -rf "$tmpdir"
```

That smoke should exit `2` and refresh `data/calibration-input-preflight.json` with missing input names only.

Publication-safety rules are strict. Keep `data/calibration-bundles/`, `data/health.json`, and `data/state.json` local and ignored. Do not commit raw frames, debug overlays, live health/state, camera URLs, Matrix tokens, Authorization headers, raw Matrix responses, unredacted Docker logs, tracebacks, or image bytes. Share only reviewed/redacted text summaries when calibration evidence needs to leave the operator machine.

## Labeling and replaying calibration cases

Use the replay workflow when you have one or more calibration bundle captures and need deterministic, publication-safe evidence about whether the current shared detection thresholds work for both configured spots. The replay path does not run YOLO or read image bytes. Operators label detector-neutral vehicle observations and expected spot presence, then the replay evaluator feeds those synthetic detections through the same spot filtering and occupancy primitives used by runtime.

Start from the tracked example label manifest:

```sh
cp examples/calibration-labels.example.yaml /tmp/calibration-labels.yaml
```

Each label manifest uses schema `parking-spot-monitor.replay.v1` and contains cases, scenarios, frames, expected spot states, detector-neutral detections, and optional semantic `tags` on cases or scenarios. Tags are normalized in reports so downstream verification can classify evidence without parsing names.

```yaml
schema_version: parking-spot-monitor.replay.v1
cases:
  - case_id: morning-left-occupied-right-empty
    tags: [real_capture, bottom_driveway, parked_empty_transition, threshold_decision]
    bundle_manifest: data/calibration-bundles/20260510T120000Z/manifest.json  # optional local metadata context
    scenarios:
      - scenario_id: shared-threshold-check
        tags: [passing_traffic, false_positive_probe, false_negative_probe]
        frames:
          - frame_id: frame-001
            snapshot_path: replay://operator/frame-001
            expected:
              left_spot: occupied
              right_spot: empty
            detections:
              - class_name: car
                confidence: 0.91
                bbox: [330, 190, 590, 335]
```

`bundle_manifest` is optional and should point only to a local calibration bundle `manifest.json` when you want evidence context. The CLI checks whether that JSON metadata file is present and readable; missing or malformed bundle manifests block coverage for that case instead of passing silently. Optional tags should be concise evidence classifiers such as `real_capture`, `bottom_driveway`, `passing_traffic`, `parked_empty_transition`, `false_positive_probe`, `false_negative_probe`, and `threshold_decision`; synthetic examples may show the vocabulary but do not satisfy R018 real-evidence coverage. Do not put `latest.jpg` or `debug_latest.jpg` paths in reports as evidence payloads: raw bundle images stay local/ignored and are never embedded by the replay CLI.

Run replay against the same config threshold values the runtime uses:

```sh
tmpdir=$(mktemp -d)
python scripts/replay_calibration_cases.py \
  --config config.yaml.example \
  --labels examples/calibration-labels.example.yaml \
  --output-dir "$tmpdir"
```

The command writes both:

- `replay-report.json` — agent-inspectable report with schema version, config thresholds, case IDs, normalized case/scenario tags, per-spot TP/TN/FP/FN metrics, blocked/not-covered reasons, event findings, redaction-scan outcome, and shared-threshold sufficiency evidence.
- `replay-report.md` — human-readable summary with the same safety boundaries, semantic tag coverage, and no embedded raw images.

Case/report statuses mean:

- `passed` — counted observations matched the expected occupied/empty labels for the case.
- `failed` — at least one counted observation produced a false positive or false negative.
- `blocked` — the case could not be assessed safely, such as missing detector data or a missing/malformed referenced bundle manifest.
- `not_covered` — the manifest intentionally left the case/frame unassessed or provided no counted coverage for the relevant spot.
- `inconclusive` — the shared-threshold sufficiency verdict cannot be claimed because coverage is sparse, blocked, missing for a configured spot, or errors do not isolate a spot-specific threshold problem.

Shared-threshold sufficiency is conservative: `sufficient` requires counted coverage for every configured spot with no false positives or false negatives; `insufficient` means spot-divergent FP/FN evidence shows the shared thresholds are not enough; `inconclusive` means more or safer evidence is needed before changing or defending thresholds.

Replay reports are designed to be publication-safe text artifacts. The JSON and Markdown report builders fail closed on RTSP URLs, Matrix tokens, auth header markers, raw Matrix response markers, tracebacks, and image-byte-looking content. If malformed labels, missing bundle metadata, or sparse evidence appear, treat the resulting blocked/not-covered/inconclusive report as a gap to fix rather than as validation success.

## Comparing calibration tuning proposals

Use the tuning comparison workflow when you have a baseline config, a proposed shared-threshold/polygon config, and the same replay label manifest for both runs. The workflow is evidence-gated: it compares both configs against identical labeled cases, writes deterministic publication-safe reports, and refuses to recommend production threshold/schema changes when replay evidence is blocked, sparse, unsafe, or non-improving.

The tracked proposed config example is intentionally synthetic:

- `config.yaml.example` is the baseline/current example.
- `examples/calibration-tuning-proposed.example.yaml` is a publication-safe smoke proposal derived from the baseline with a small shared confidence-threshold adjustment and minor polygon nudges.
- `examples/calibration-labels.example.yaml` contains synthetic labels only.

Run this safe operator smoke from the repository root; it writes reports into a temporary directory and does not need live RTSP or Matrix secrets:

```sh
tmpdir=$(mktemp -d)
python scripts/compare_calibration_tuning.py \
  --baseline-config config.yaml.example \
  --proposed-config examples/calibration-tuning-proposed.example.yaml \
  --labels examples/calibration-labels.example.yaml \
  --output-dir "$tmpdir"
ls "$tmpdir"/tuning-report.json "$tmpdir"/tuning-report.md
```

The CLI prints compact JSON status naming the phase, report paths, redaction result, decision, status counts, metric deltas, blocked reasons, and not-covered reasons. The output directory contains:

- `tuning-report.json` — agent-readable comparison with baseline/proposed thresholds, per-spot metric deltas, status counts, blocked/not-covered reasons, event deltas, redaction scan, and final decision.
- `tuning-report.md` — human-readable summary with the same safety boundary and no embedded raw images, RTSP URLs, Matrix tokens, Authorization headers, tracebacks, raw Matrix responses, or image bytes.

Tuning decisions mean:

- `keep_shared_thresholds` — the proposed shared thresholds did not reduce false-positive/false-negative evidence safely. Keep the existing shared runtime config.
- `apply_shared_tuning` — the proposed shared thresholds reduced false-positive/false-negative evidence with no new blocked evidence or safety regressions, and the proposed replay result still supports shared-threshold sufficiency. This justifies applying the shared config change, subject to operator review.
- `needs_per_spot_thresholds` — residual replay errors diverge by spot under shared proposed thresholds, which is evidence that shared thresholds may be insufficient. Treat this as an input to a future per-spot design, not as permission to quietly change runtime schema.
- `blocked` — the comparison cannot safely support a tuning decision because replay evidence was blocked/not-covered, redaction failed, label/config data was malformed or missing, or another safety condition prevented honest publication. Do not tune production thresholds from a blocked report.

D020 evidence gate: shared thresholds remain the preferred runtime shape. Do not add per-spot runtime threshold schema/config changes unless replay evidence proves shared thresholds insufficient across the labeled cases. A `needs_per_spot_thresholds` report is the artifact that can justify designing that follow-up; absent that evidence, keep the simpler shared-threshold config.

## M002 final closure validation package

Use the M002 final closure package only after replay, tuning, finite live-proof, and unattended alert-soak evidence has been generated locally. The assembler is fail-closed in the package payload: it combines the existing evidence artifacts into one publication-safe package and records a non-validated `final_status` unless the evidence is strict `validated` or explicitly accepted `residual_risk_accepted`. The CLI exits non-zero only when assembly or publication-safety checks fail; inspect the package status for the validation verdict.

Run it from the repository root with explicit replay and tuning report paths. The live-proof and alert-soak paths default to the local operator outputs shown below, and the final package output directory must remain `data/m002-validation` unless you are intentionally writing to another ignored local workspace:

```sh
python scripts/assemble_m002_validation_package.py \
  --replay-report data/replay-report.json \
  --tuning-report data/tuning-report.json \
  --live-proof-result data/live-proof-result.json \
  --alert-soak-result data/alert-soak-result.json \
  --output-dir data/m002-validation
```

If `--replay-report` is omitted, the assembler reads `data/s07-replay-evidence/replay/replay-report.json`. If `--tuning-report` is omitted, it reads `data/s07-replay-evidence/tuning/tuning-report.json`. If `--live-proof-result` is omitted, the assembler reads `data/live-proof-result.json`. If `--alert-soak-result` is omitted, it reads `data/alert-soak-result.json`. Pass paths explicitly when you need the closure record to name a different replay or tuning evidence set. Use `--allow-residual-risk "..."` only when a non-strict gap has been deliberately accepted and the acceptance text is safe to include in the local package.

The command produces two local artifacts:

- `data/m002-validation/m002-validation-package.json` — agent/automation input with per-evidence status, safe reasons, requirement implications, tuning/no-change decision, residual-risk text when supplied, and redaction/publication-safety counts.
- `data/m002-validation/m002-validation-package.md` — human review summary with the same status semantics and no raw Matrix bodies, tokens, RTSP URLs, tracebacks, image bytes, or private log content.

Final closure statuses mean:

- `validated` — replay, tuning, live-proof, and alert-soak evidence all passed their strict gates. This is the only status that validates M002 without a residual-risk note.
- `coverage_gap` — one or more evidence surfaces is honest but incomplete, such as `coverage_gap_no_alert` from an alert soak with no organic opening or S07 semantic replay evidence that is workflow-smoke only. This does not validate M002.
- `blocked` — required evidence is missing, malformed, unreadable, preflight-blocked, or requires follow-up before closure. Preflight blockers do not validate M002.
- `failed` — an evidence verifier, artifact check, Matrix readback check, duplicate-spam check, or publication-safety scan failed. Redaction hits fail closure until fixed.
- `residual_risk_accepted` — all strict surfaces passed except an explicitly accepted non-strict coverage gap. This is not the same as `validated`; it records a deliberate residual-risk acceptance for later review.

Requirement reconciliation is explicit in the package. R018, R019, and R028 are evidence-derived: they may be `coverage_gap`, `blocked`, or `failed` when current S07 artifacts are workflow-smoke evidence rather than strict real-world semantic evidence. Validating R018 real-traffic replay, R019 shared-threshold sufficiency, or R028 bottom-driveway exclusion requires strict S07 semantic evidence for all required tags: `real_capture`, `bottom_driveway`, `passing_traffic`, and `threshold_decision`. S07 workflow-smoke artifacts do not validate R018/R019/R028, and synthetic examples only prove the replay/report contract.

R020, R021, and R022 are deferred/out-of-scope for M002 rather than silently missing. R020 setup/Matrix/Docker/GPU/troubleshooting documentation is deferred to a later milestone; R021 encrypted Matrix room support remains out-of-scope for M002 and is not implemented by this milestone; R022 historical occupancy query/storage is out-of-scope for M002 beyond bounded local reports, retained snapshots, state, and logs.

Known non-validation evidence must stay non-validation in closure notes: `coverage_gap_no_alert`, preflight blockers, skipped readback, malformed inputs, verifier errors, redaction hits, Matrix send-only evidence, and send responses alone do not prove room-visible delivery and do not validate M002. S08 strict live soak validation requires strict alert-soak success: an organic alert, per-alert Matrix readback, valid snapshots, clean duplicate diagnostics, health/state summaries, and zero redaction hits. A Matrix send response without room readback is only delivery-attempt evidence. A no-alert soak documents the bounded observation window; it does not prove live alert behavior unless residual risk is explicitly accepted.

S05 does not tune polygons/thresholds and does not add per-spot threshold schema. A no-change/shared-threshold closure must cite evidence such as alert-soak strict success, tuning `keep_shared_thresholds` or `apply_shared_tuning`, clean redaction scans, or explicitly accepted residual risk. The tuning verdict needs_per_spot_thresholds blocks closure or creates a follow-up; do not quietly claim per-spot runtime threshold support from S05.

The publication boundary matches `.gitignore`: raw snapshots, logs, health/state/latest frames, live-proof/alert-soak results, replay/tuning reports, and final package outputs stay local/ignored until reviewed. Never add raw snapshots, raw logs, `data/latest.jpg`, `data/debug_latest.jpg`, `data/health.json`, `data/state.json`, `data/snapshots/`, live-proof/alert-soak JSON or logs, replay/tuning reports, or `data/m002-validation/m002-validation-package.json` / `data/m002-validation/m002-validation-package.md` to version control without deliberate publication review.

Honest limitation: the committed example labels and proposed config prove the workflow and report contract only. They do not prove real-world detector accuracy, live Matrix behavior, or production tuning quality. Private operator-labeled calibration cases from real captures are required before S08 or later work can claim tuned live alert behavior. Keep those private labels, bundle manifests, frames, overlays, and raw logs local unless they have been explicitly reviewed and redacted.

A successful capture writes two files in the data directory:

- `latest.jpg` is the raw full-frame camera evidence. Keep it unannotated; later Matrix alert slices must send raw snapshots rather than polygon overlays (D008).
- `debug_latest.jpg` is a local-only tuning artifact with the configured `left_spot` and `right_spot` street polygons drawn over the latest frame. Use it to verify the monitored street regions exclude the driveway car, but do not treat it as alert evidence.

After a successful capture and overlay attempt, the service runs local vehicle detection against the raw `latest.jpg` frame and emits one aggregate `detection-frame-processed` JSON-line event. The event includes the configured thresholds, spot IDs, total detections, accepted candidate summaries, and rejection reason counts; it does not overwrite raw frame artifacts or log image bytes. In `--capture-once`, detector/model failures emit `detection-frame-failed` and return non-zero. In the runtime loop, per-frame detector failures emit `detection-frame-failed` and the loop continues so inference failures are not misrepresented as empty candidate sets.

## Runtime occupancy state and street-sweeping events

The long-running runtime loop persists conservative per-spot occupancy markers to `/data/state.json` in Docker, which is `./data/state.json` on the host when using the default Compose volume. The JSON file is intentionally minimal restart state: spot status, hit/miss streaks, last confirmed bbox, duplicate-open suppression markers, active quiet-window IDs, and quiet-window notice IDs. Startup emits `state-loaded`; each successful detection-frame update attempts one atomic save and emits `state-saved`; corrupt JSON or invalid schema is quarantined beside the state file and reported as `state-corrupt-quarantined` before the service continues from unknown defaults. The loop also writes compact operator health to `/data/health.json` with current status, iteration, timestamps, selected decode mode, failure counters, Matrix delivery errors, retention failures, and state-save failures; inspect it with `python -m json.tool data/health.json`. Health write failures emit `health-write-failed` but do not stop capture, detection, state, or Matrix failure handling.

Occupancy events are decision-point JSON objects, not per-frame spam. Stable occupied/empty transitions emit `occupancy-state-changed`. A confirmed transition from occupied to empty emits `occupancy-open-event` unless a quiet window is active, in which case it emits `occupancy-open-suppressed` with the quiet-window reason. Capture failures and detector failures do not advance miss counters, so outages are not treated as empty parking spots.

Matrix alert timestamps are rendered in the configured operator timezone as a 12-hour clock with a timezone abbreviation, for example `2026-05-12 5:16:48 PM PDT`. Occupied-spot alerts always include the spot and observed time. Vehicle-history context is added only when it is useful to an operator: a human-readable vehicle label or a higher-signal history estimate. Low-confidence, profile-ID-only history such as generated `prof_sess-*` identifiers is intentionally omitted, so the alert stays concise instead of repeating implementation IDs and weak dwell estimates. Open-spot alerts and live-proof messages use the same 12-hour time display.

Owner vehicles can be listed in the private local vehicle-history registry at `data/vehicle-history/owner-vehicles.json`. The registry maps a recurring visual `profile_id` to an operator label and optional description. When an owner vehicle is actively parked in either monitored spot during an active quiet window, the runtime sends one deduped `owner-vehicle-quiet-window-alert` Matrix text message to the configured parking room. Owner-vehicle alerts intentionally omit dwell and history estimates; the actionable signal is that the operator's car is parked during street cleaning.

Authorized Matrix command senders configured in `matrix.command_authorized_senders` can correct and inspect the local vehicle-history archive from the parking room. Use `!parking help` to list available commands. Use `!parking who` to list active parking sessions by spot, known vehicle label, confidence, sample count, and session ID. Use `!parking owner <spot_id>` when the active vehicle in a spot is known to be the configured owner vehicle; for example, `!parking owner right_spot` assigns the active right-spot session to the owner profile with confidence `1.0` and adds that crop as a trusted owner-profile sample. Existing correction commands remain available: `!parking wrong <spot_id|session_id>` records a false match, and `!parking profile summary <profile_id>` returns a safe profile summary. Empty `matrix.command_authorized_senders` disables these mutation commands by default.

The configured street-sweeping quiet window is `street_sweeping`: the first and third Monday of each month in `America/Los_Angeles`, configured as `13:00` to `15:00` and shown to operators as `1:00 PM` to `3:00 PM`. Quiet-window boundary changes emit restart-safe `quiet-window-started` and `quiet-window-ended` event objects. S06 will send Matrix messages from these S05 event objects; it should consume `occupancy-open-event` for open-spot alerts and quiet-window notice objects for schedule notices rather than re-inferring transitions from logs.

The structured JSON-line output should include capture attempt/write events, selected decode mode, fallback behavior when hardware decode is unavailable, duration, output path, and byte size. Overlay writes add `debug-overlay-written` records with source/output paths, image dimensions, and spot IDs. Detection records add aggregate accepted/rejected spot-filtering diagnostics. Capture failures, corrupt frames, overlay write/decode failures, detector failures, state load/save failures, and corrupt-state quarantine should report safe structured diagnostics without printing stream URLs, tokens, tracebacks, YAML content, image bytes, or full FFmpeg argv.

## Vehicle-history retention, export, and prune

Vehicle-history sessions, profile metadata, correction events, and archive-owned occupied JPEG artifacts are retained indefinitely under the effective data directory at `vehicle-history/`. Matrix alert snapshot retention remains separate: `storage.snapshot_retention_count` bounds event-delivery snapshots in `snapshots/`, but it does not prune vehicle-history records or archive-owned occupied images.

Inspect `health.json` or `VehicleHistoryArchive.health_snapshot()` for operator-visible archive signals before opening files. The vehicle-history health fields include `retention_policy: "indefinite"`, `management_capabilities: ["export", "prune"]`, archive file/byte counts, oldest retained session timestamp, missing occupied-image reference count, and sanitized `last_maintenance_metadata` from the latest export/prune manifest.

Export a local operator-owned bundle from inside the installed package/container with the standard-library CLI:

```sh
python -m parking_spot_monitor.vehicle_history_cli \
  --data-dir ./data \
  export --output ./vehicle-history-export.tar.gz
```

The export command writes a `.tar.gz` bundle containing the archive files plus a metadata-only manifest, then persists the same safe maintenance summary under `vehicle-history/metadata/maintenance/`. The bundle may intentionally contain raw archive image files because it is an explicit local operator export; the CLI stdout, maintenance manifest, logs, and health text must remain metadata-only and must not serialize image bytes, RTSP URLs, Matrix tokens, Authorization headers, descriptors, raw Matrix bodies, or tracebacks.

Always dry-run prune before applying it:

```sh
python -m parking_spot_monitor.vehicle_history_cli \
  --data-dir ./data \
  prune --older-than-days 90 --dry-run

python -m parking_spot_monitor.vehicle_history_cli \
  --data-dir ./data \
  prune --older-than-days 90 --apply
```

Use `--older-than ISO_TIMESTAMP` when you need an explicit cutoff instead of a relative day count. Prune only considers closed sessions whose close timestamp is older than the cutoff. It never deletes active sessions, and it skips image files still referenced by active or retained sessions; missing image references are counted as safe metadata gaps. Invalid cutoffs, missing required arguments, and unwritable export outputs exit non-zero without applying prune.

## Local YOLO detection and Model storage policy

The runtime package includes `ultralytics>=8` so the local detector can load YOLO nano from the configured `detection.model` value, for example `yolov8n.pt`. `detection.model` accepts local model names and local POSIX paths only: use package/Ultralytics names such as `yolov8n.pt`, repo-relative operator paths such as `models/custom-detector.pt`, or mounted Docker paths such as `/models/yolov8n.pt`. Config validation rejects URL-like values (`https://...`, `s3://...`) and path traversal (`../...` or `/models/../...`) before runtime so model configuration failures are clear and do not leak secrets.

First-run Ultralytics downloads are allowed for local names such as `yolov8n.pt`, but they can block startup and require network access. For predictable Docker startup, pre-stage the model file on the host and set `detection.model: /models/yolov8n.pt`, then uncomment the optional read-only Compose mount:

```yaml
volumes:
  - ./models:/models:ro
```

A missing mounted model is treated as a detector/model-load runtime failure, not as a secret-bearing config error. Detector failure diagnostics are intentionally safe for JSON-line logs: they include the phase, model path, frame path when applicable, error type, and a sanitized message only. They must not include RTSP URLs, Matrix tokens, image payload bytes, full FFmpeg argv, or traceback spam.

The detector adapter imports Ultralytics lazily when the model object is constructed, reuses that single model for subsequent frame predictions, passes the configured `detection.inference_image_size` as Ultralytics `imgsz` when set, and normalizes model output into detector-neutral vehicle records before the spot filtering rules run. When `detection.spot_crop_inference` is enabled, the runtime also crops each configured spot with `detection.spot_crop_margin_px` padding, runs the same detector on those crops, translates crop-relative boxes back into full-frame coordinates, and sends the merged detections through the same spot geometry filter. Unit tests use fake YOLO result objects, so normal test runs do not download weights or run real inference. They prove deterministic class, confidence, area, centroid, overlap, adapter, and failure-path behavior without network access. Live camera accuracy proof remains operator evidence collected through the live-proof commands; the earlier detection.model allowlisting item is now implemented, while model-threshold tuning and non-root container hardening remain future hardening work after M001 (previously deferred to S07).

Build and inspect the Docker runtime contract. If your shell has real live secrets loaded, do not paste rendered Compose output into logs or tickets; use `docker compose config --no-interpolate` for structure-only inspection or scan the output for forbidden secret patterns.

```sh
docker build -t parking-spot-monitor:test .
docker compose config --no-interpolate
```

Run the Compose default as the real capture runtime against a mounted operator config and data directory. The service definition intentionally does not bake secret env values into `docker-compose.yml`; provide them from the shell or service manager at runtime.

```sh
mkdir -p data
docker compose up parking-spot-monitor
```

Run the same finite capture proof in Docker when you want a bounded smoke check that writes `/data/latest.jpg` inside the container and `./data/latest.jpg` on the host:

```sh
mkdir -p data
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor --config /config/config.yaml --data-dir /data --capture-once
```

Run the controlled end-to-end live proof in Docker when the host has a real `./config.yaml`, `RTSP_URL`, and Matrix access-token routing available to Compose. Use the repository wrapper so Docker exit status, marker checks, artifact checks, Matrix room readback, and redaction results are persisted before the strict verifier writes the evidence report:

```sh
mkdir -p data
python scripts/run_docker_live_proof.py
python scripts/verify_live_proof.py
```

`python scripts/run_docker_live_proof.py` produces `data/live-proof-result.json`, redacted `data/live-proof-docker.stdout.log`/`data/live-proof-docker.stderr.log`, `data/latest.jpg`, and retained `data/snapshots/live-proof-*.jpg` evidence when proof reaches the artifact phase. `python scripts/verify_live_proof.py` is the strict verifier/report writer for `data/live-proof-evidence.md`; it exits non-zero on non-success proof states, readback gaps, marker failures, missing/invalid JPEGs, or redaction hits.

Preflight exit `2` from the runner means required live inputs were unavailable. Treat missing `config.yaml`, `RTSP_URL`, Matrix access token/routing, `preflight_failed`, `docker_failed`, `validation_failed`, `readback_gap`, and `--skip-readback` as blockers or gaps, not as R003/R015 validation.

For R015 evidence, capture all of the following before marking the requirement validated:

- Docker logs showing safe structured `live-proof-started`, `live-proof-capture-ok`, `live-proof-matrix-text-ok`, and `live-proof-matrix-image-ok` events, or a concrete failure marker that explains why proof could not complete.
- The Matrix room contains one visibly labelled `LIVE PROOF / TEST MESSAGE` and one visibly labelled `LIVE PROOF / TEST IMAGE`; neither should look like an organic `Parking spot open` production alert.
- `./data/latest.jpg` exists on the host and is the raw full-frame camera capture.
- `./data/health.json` is inspectable with `python -m json.tool data/health.json` and includes current status/failure counters from the runtime surface.
- `./data/state.json` and `./data/snapshots/` are inspectable so operators can distinguish restart state, latest frame evidence, and retained Matrix event/live-proof snapshots. Use `find data/snapshots -maxdepth 1 -type f | sort` to inspect retained event snapshots after a live proof or alert run.
- No captured log or README/runbook artifact contains RTSP URLs, Matrix access tokens, Authorization headers, raw Matrix response bodies, tracebacks, or image bytes.

The compact health JSON shape is intentionally operator-readable and stable enough for smoke inspection. Expect keys such as `status`, `iteration`, `started_at`, `updated_at`, `last_frame_at`, `selected_decode_mode`, `"capture"`, `consecutive_capture_failures`, `consecutive_detection_failures`, `last_matrix_error`, `retention_failure_count`, `retention_error`, `state_save_error`, and `last_error`; `"capture"` includes `last_success_at` and `selected_decode_mode` so operators can quickly see the active decode path (`qsv`, `vaapi`, `drm`, or `software`) without scanning logs. Sanitized nested errors include phase/type/message metadata but not raw secrets or tracebacks.

M001 keeps the container running as root to avoid ambiguous host bind-mount ownership failures for `/data/latest.jpg`, `/data/state.json`, `/data/snapshots/`, and `/data/health.json`. Non-root container hardening is a future hardening item and must include docs/tests for host `./data` ownership before changing the Docker user.

Use this final local verification contract for Docker/operator changes:

```sh
python -m pytest tests/test_config.py tests/test_docker_contract.py -q
python -m pytest -q
docker build -t parking-spot-monitor:test .
docker compose config --no-interpolate
python - <<'PY'
import os
import subprocess
import sys

os.environ["RTSP_URL"] = "placeholder"
os.environ["MATRIX_ACCESS_TOKEN"] = "placeholder"
sys.exit(subprocess.call([
    sys.executable,
    "-m",
    "parking_spot_monitor",
    "--config",
    "config.yaml",
    "--validate-config",
]))
PY
python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once
python scripts/run_docker_live_proof.py
python scripts/verify_live_proof.py
python scripts/verify_hardware_decode.py --json
python -m json.tool data/health.json
find data/snapshots -maxdepth 1 -type f | sort
```

Exercise the safe startup failure path without mounting a config file:

```sh
docker run --rm parking-spot-monitor:test \
  python -m parking_spot_monitor --config /missing/config.yaml --validate-config
```

That failure command should exit non-zero and emit structured `startup-config-invalid` logs containing the config path and validation phase, not secret values.

## Hardware decode

`docker-compose.yml` mounts `/dev/dri:/dev/dri` so hardware decode can work inside the container. The image installs `intel-media-va-driver` and `vainfo`, and sets `LIBVA_DRIVER_NAME=iHD`; VAAPI should initialize on Intel Iris Xe hosts when `/dev/dri/renderD128` is passed through. The capture path tries VAAPI, DRM, then software by default. QSV may still fail on this host even though FFmpeg is built with `--enable-libvpl`, `libvpl2` is installed, and QSV codecs are listed; the observed failure is `Error creating a MFX session: -9`, while VAAPI succeeds and runtime health/logs should show `selected_mode=vaapi`. Without device passthrough, FFmpeg cannot create hardware devices and the capture path falls back to software decode.

Verify the active container hardware surface with `python scripts/verify_hardware_decode.py --json`. For a concise text check, run `python scripts/verify_hardware_decode.py`; expected output on this host is `hardware_decode_status=vaapi_supported_qsv_unavailable`, which means VAAPI is working and QSV is documented as unavailable. `qsv_required_but_unavailable` is only a failure when `--require-qsv` is intentionally used after changing the host/kernel/libvpl stack. `verifier_timeout` or `vaapi_unavailable` means the container hardware surface needs investigation before claiming hardware acceleration. The same compact summary is embedded in live-proof and alert-soak result/evidence artifacts as `hardware_decode_summary`. Hosts without `/dev/dri` should remove or override the `devices` mapping for software-only operation.
