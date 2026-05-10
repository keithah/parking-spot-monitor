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

## Finite validation and capture smoke checks

Use `--validate-config` for finite startup/configuration checks. Use `--capture-once` for the S02/S03 finite capture proof: it attempts one frame capture, writes `latest.jpg`, refreshes the local debug overlay at `debug_latest.jpg`, and exits instead of starting the continuous monitoring loop. Live R003 acceptance requires a real operator RTSP environment supplied through environment variables; do not commit those values or paste them into examples.

Local validation against the tracked example can use non-secret placeholder values because it does not connect to the camera or Matrix:

```sh
RTSP_URL=placeholder MATRIX_ACCESS_TOKEN=placeholder \
  python -m parking_spot_monitor --config config.yaml.example --validate-config
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

Strict success requires all of the following before R003/R015 can be validated: `LIVE_RTSP_CAPTURE_OK`, `LIVE_MATRIX_TEXT_OK`, and `LIVE_MATRIX_IMAGE_OK` are present; skip/failure markers are absent; `data/latest.jpg` is a valid raw camera JPEG; at least one `data/snapshots/live-proof-*.jpg` JPEG is retained; Matrix room readback verifies both visibly labelled `LIVE PROOF / TEST MESSAGE` text and `LIVE PROOF / TEST IMAGE` image evidence in the target room; and redaction scans find zero RTSP URLs, Authorization/Bearer headers, Matrix access-token strings, raw Matrix response bodies, tracebacks, or image bytes in logs/reports. Do not use `--skip-readback` for validation: skipped or unavailable readback leaves R003/R015 remain unvalidated because send responses alone do not prove room-visible Matrix delivery.

Skip markers identify missing live inputs (`LIVE_PROOF_SKIPPED_CONFIG_ABSENT`, `LIVE_PROOF_SKIPPED_RTSP_ENV_ABSENT`, or `LIVE_PROOF_SKIPPED_MATRIX_ENV_ABSENT`) and are blockers, not validation. Failure markers identify the failed phase (`LIVE_RTSP_CAPTURE_FAILED`, `LIVE_MATRIX_TEXT_FAILED`, or `LIVE_MATRIX_IMAGE_FAILED`) without logging RTSP URLs, Matrix tokens, Authorization headers, raw Matrix response bodies, tracebacks, or image bytes.

A successful capture writes two files in the data directory:

- `latest.jpg` is the raw full-frame camera evidence. Keep it unannotated; later Matrix alert slices must send raw snapshots rather than polygon overlays (D008).
- `debug_latest.jpg` is a local-only tuning artifact with the configured `left_spot` and `right_spot` street polygons drawn over the latest frame. Use it to verify the monitored street regions exclude the driveway car, but do not treat it as alert evidence.

After a successful capture and overlay attempt, the service runs local vehicle detection against the raw `latest.jpg` frame and emits one aggregate `detection-frame-processed` JSON-line event. The event includes the configured thresholds, spot IDs, total detections, accepted candidate summaries, and rejection reason counts; it does not overwrite raw frame artifacts or log image bytes. In `--capture-once`, detector/model failures emit `detection-frame-failed` and return non-zero. In the runtime loop, per-frame detector failures emit `detection-frame-failed` and the loop continues so inference failures are not misrepresented as empty candidate sets.

## Runtime occupancy state and street-sweeping events

The long-running runtime loop persists conservative per-spot occupancy markers to `/data/state.json` in Docker, which is `./data/state.json` on the host when using the default Compose volume. The JSON file is intentionally minimal restart state: spot status, hit/miss streaks, last confirmed bbox, duplicate-open suppression markers, active quiet-window IDs, and quiet-window notice IDs. Startup emits `state-loaded`; each successful detection-frame update attempts one atomic save and emits `state-saved`; corrupt JSON or invalid schema is quarantined beside the state file and reported as `state-corrupt-quarantined` before the service continues from unknown defaults. The loop also writes compact operator health to `/data/health.json` with current status, iteration, timestamps, selected decode mode, failure counters, Matrix delivery errors, retention failures, and state-save failures; inspect it with `python -m json.tool data/health.json`. Health write failures emit `health-write-failed` but do not stop capture, detection, state, or Matrix failure handling.

Occupancy events are decision-point JSON objects, not per-frame spam. Stable occupied/empty transitions emit `occupancy-state-changed`. A confirmed transition from occupied to empty emits `occupancy-open-event` unless a quiet window is active, in which case it emits `occupancy-open-suppressed` with the quiet-window reason. Capture failures and detector failures do not advance miss counters, so outages are not treated as empty parking spots.

The configured street-sweeping quiet window is `street_sweeping`: the first and third Monday of each month in `America/Los_Angeles`, from `13:00` to `15:00`. Quiet-window boundary changes emit restart-safe `quiet-window-started` and `quiet-window-ended` event objects. S06 will send Matrix messages from these S05 event objects; it should consume `occupancy-open-event` for open-spot alerts and quiet-window notice objects for schedule notices rather than re-inferring transitions from logs.

The structured JSON-line output should include capture attempt/write events, selected decode mode, fallback behavior when hardware decode is unavailable, duration, output path, and byte size. Overlay writes add `debug-overlay-written` records with source/output paths, image dimensions, and spot IDs. Detection records add aggregate accepted/rejected spot-filtering diagnostics. Capture failures, corrupt frames, overlay write/decode failures, detector failures, state load/save failures, and corrupt-state quarantine should report safe structured diagnostics without printing stream URLs, tokens, tracebacks, YAML content, image bytes, or full FFmpeg argv.

## Local YOLO detection and Model storage policy

The runtime package includes `ultralytics>=8` so the local detector can load YOLO nano from the configured `detection.model` value, for example `yolov8n.pt`. `detection.model` accepts local model names and local POSIX paths only: use package/Ultralytics names such as `yolov8n.pt`, repo-relative operator paths such as `models/custom-detector.pt`, or mounted Docker paths such as `/models/yolov8n.pt`. Config validation rejects URL-like values (`https://...`, `s3://...`) and path traversal (`../...` or `/models/../...`) before runtime so model configuration failures are clear and do not leak secrets.

First-run Ultralytics downloads are allowed for local names such as `yolov8n.pt`, but they can block startup and require network access. For predictable Docker startup, pre-stage the model file on the host and set `detection.model: /models/yolov8n.pt`, then uncomment the optional read-only Compose mount:

```yaml
volumes:
  - ./models:/models:ro
```

A missing mounted model is treated as a detector/model-load runtime failure, not as a secret-bearing config error. Detector failure diagnostics are intentionally safe for JSON-line logs: they include the phase, model path, frame path when applicable, error type, and a sanitized message only. They must not include RTSP URLs, Matrix tokens, raw image bytes, full FFmpeg argv, or traceback spam.

The detector adapter imports Ultralytics lazily when the model object is constructed, reuses that single model for subsequent frame predictions, and normalizes model output into detector-neutral vehicle records before the spot filtering rules run. Unit tests use fake YOLO result objects, so normal test runs do not download weights or run real inference. They prove deterministic class, confidence, area, centroid, overlap, adapter, and failure-path behavior without network access. Live camera accuracy proof remains operator evidence collected through the live-proof commands; the earlier detection.model allowlisting item is now implemented, while model-threshold tuning and non-root container hardening remain future hardening work after M001 (previously deferred to S07).

Build and inspect the Docker runtime contract. If your shell has real live secrets loaded, do not paste rendered Compose output into logs or tickets; use `docker compose config --no-interpolate` for structure-only inspection or scan the output for forbidden secret patterns.

```sh
docker build -t parking-spot-monitor:test .
docker compose config --no-interpolate
```

Run the Compose default as the real capture runtime against a mounted operator config and data directory. The service definition intentionally does not bake secret env values into `docker-compose.yml`; provide them from the shell or service manager at runtime.

```sh
mkdir -p data
RTSP_URL="$RTSP_URL" MATRIX_ACCESS_TOKEN="$MATRIX_ACCESS_TOKEN" docker compose up parking-spot-monitor
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

The compact health JSON shape is intentionally operator-readable and stable enough for smoke inspection. Expect keys such as `status`, `iteration`, `started_at`, `updated_at`, `last_frame_at`, `selected_decode_mode`, `consecutive_capture_failures`, `consecutive_detection_failures`, `last_matrix_error`, `retention_failure_count`, `retention_error`, `state_save_error`, and `last_error`; sanitized nested errors include phase/type/message metadata but not raw secrets or tracebacks.

M001 keeps the container running as root to avoid ambiguous host bind-mount ownership failures for `/data/latest.jpg`, `/data/state.json`, `/data/snapshots/`, and `/data/health.json`. Non-root container hardening is a future hardening item and must include docs/tests for host `./data` ownership before changing the Docker user.

Use this final local verification contract for Docker/operator changes:

```sh
python -m pytest tests/test_config.py tests/test_docker_contract.py -q
python -m pytest -q
docker build -t parking-spot-monitor:test .
docker compose config --no-interpolate
RTSP_URL=placeholder MATRIX_ACCESS_TOKEN=placeholder \
  python -m parking_spot_monitor --config config.yaml --validate-config
python -m parking_spot_monitor --config config.yaml --data-dir ./data --capture-once
python scripts/run_docker_live_proof.py
python scripts/verify_live_proof.py
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

`docker-compose.yml` mounts `/dev/dri:/dev/dri` so hardware decode can work inside the container. The capture path tries QSV, VAAPI, DRM, then software; on this host QSV/VAAPI device setup fails but DRM succeeds once `/dev/dri` is mounted. Without this device passthrough, FFmpeg cannot create hardware devices and the capture path falls back to software decode. Hosts without `/dev/dri` should remove or override the `devices` mapping for software-only operation.
