# Docker deployment and operations

This runbook is for an operator deploying the parking monitor on a Linux host. After following it, the operator can build the image, start the service, verify live health, measure resource use, upgrade safely, and roll back without losing runtime data.

## What runs in Docker

The default Docker build produces the complete live-monitor image. It contains Python 3.12, FFmpeg, the Intel media driver, the application, and the pinned CPU detector stack. The Compose service runs capture, YOLO inference, occupancy state, Matrix delivery, health reporting, timeline retention, and local operator intelligence in one container.

The image is disposable. Operator-owned state is deliberately outside it:

- `config.yaml` is mounted read-only at `/config/config.yaml`.
- The host model directory is mounted read-only at `/models`; the tracked default is `./models`.
- `data/` is mounted read-write at `/data` and contains health, state, images, snapshots, the Matrix outbox, vehicle history, and decision memory.
- Camera and Matrix credentials come from the host environment or the ignored `.env` file.

No inbound application port is published. The service makes outbound connections to the configured camera streams and Matrix homeserver.

The current image runs as root inside the container so it can use the configured bind mounts and render device. Treat the repository, `config.yaml`, `.env`, `/dev/dri`, and `data/` as trusted operator surfaces; this Compose setup is not a hardened multi-tenant isolation boundary.

Keep application-owned artifact directories under `data/` writable only by the service and trusted operators. Cleanup binds and rechecks inodes after moving them to exact `.<basename>.<16-lowercase-hex>.dispose` names. Before that move, it durably indexes the transition in a bounded `.owned-disposals.json` manifest. Startup recovers indexed work directly in the snapshot root, `.upload-derivatives`, `occupied-full`, and `occupied-crops`; Matrix retention and vehicle-image capture repeat the applicable bounded recovery before new cleanup or publication. A scan of at most 256 entries remains as the legacy fallback. Do not edit or delete these hidden manifests while the service is stopped. Linux does not provide an inode-conditional unlink: a noncooperating writer with simultaneous directory-write access can still race the final randomized-name check and unlink. The ownership checks minimize that window; they do not make a shared hostile directory safe.

An unlink can remove a filename before the following directory `fsync` reports an error. Retention treats that namespace deletion as complete, leaves its manifest record for reconciliation, and emits `snapshot-retention-durability-uncertain` instead of retrying the now-absent target as an ordinary deletion failure. A later startup or retention pass reconciles the record. Repeated `snapshot-retention-failed` events with `error_type` set to `RecoveryPending` indicate that the data mount cannot complete recovery and should be checked for write errors, permissions, or exhausted storage before manually touching hidden artifacts.

Manifest transition locking is process-local and keyed by the owned directory identity. The supported deployment model has exactly one `parking-spot-monitor` service process owning these artifact directories; do not run a second container, maintenance process, or cleanup command that mutates the same directories concurrently. Recovery and disposal for the same directory serialize across manifest record, rename, classification, and reconciliation, while unrelated owned directories remain concurrent.

After a disposal rename, only a conclusive missing path, nonregular artifact, or inode mismatch can select mismatch handling. Transient `open`, `stat`, or identity-observation failures such as EIO keep the indexed transition pending without restore or manifest deletion. Once the filesystem error clears, startup or the next applicable operation retries the exact manifest entry even when the directory contains more than the legacy scan bound.

## Host prerequisites

Provide:

- A Linux host with Docker Engine and the Docker Compose plugin.
- Enough storage for the Docker image and the operator-selected retention period.
- A detector weight file obtained from an approved, trusted artifact source, plus that source's published SHA-256 checksum.
- Network access to the RTSP camera streams and Matrix homeserver.
- Intel `/dev/dri` access for VAAPI/QSV hardware decoding, or a local Compose adjustment that removes the device mapping and uses software fallback.

Confirm the tooling and optional device:

```sh
docker version
docker compose version
ls -ld /dev/dri
```

The tracked service limits the container to 2 CPUs, 4 GiB of memory, and 512 processes. Docker JSON logs rotate at 10 MiB with three retained files.

The container is the complete runtime, not only a detector sidecar. It includes capture, inference, state transitions, Matrix delivery, command polling, health, decision memory, vehicle history, and artifact recovery. The image remains disposable; the bind-mounted configuration, model, and `/data` tree are the recovery authority.

## Dependency lock maintenance

All three generated manifests contain reviewed, exact hashes. `requirements-build.lock` authenticates the resolver toolchain, `requirements-runtime.lock` contains the application runtime, and `requirements-detector.lock` contains the detector stack. Every lock names PyPI explicitly; the detector lock also names the PyTorch CPU index. Maintainers continue to edit the broad dependency bounds in `requirements.txt`, `requirements-detector.txt`, and the matching dependency tables in `pyproject.toml`; do not hand-edit generated locks.

Runtime declarations in `requirements.txt` must match `project.dependencies`. Detector declarations in `requirements-detector.txt` must match `project.optional-dependencies.detector`. The generator and offline check reject drift between those pairs. The development extra is intentionally separate from container dependencies, so changes under `project.optional-dependencies.dev` do not invalidate these locks.

### Regenerate all three reviewed locks

After changing a dependency input, run this entire block from the repository root with Python 3.12. The first environment is authenticated by the current build lock and may only stage and validate the next build lock. The second environment is authenticated by that next lock, regenerates the complete set, verifies the staged build lock is byte-identical, and publishes all three locks together. The enclosing subshell runs the cleanup trap when the block finishes successfully, fails, or is interrupted:

```sh
(
  set -eu
  LOCK_TOOLS_DIR="$(mktemp -d)"
  NEXT_LOCK_TOOLS_DIR=""
  cleanup_lock_tools() {
    rm -rf -- "$LOCK_TOOLS_DIR"
    if [ -n "$NEXT_LOCK_TOOLS_DIR" ]; then
      rm -rf -- "$NEXT_LOCK_TOOLS_DIR"
    fi
  }
  trap cleanup_lock_tools EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM
  install_lock_tools() {
    lock_tools_dir="$1"
    lock_file="$2"
    python3.12 -m venv "$lock_tools_dir"
    env -u PIP_INDEX_URL -u PIP_EXTRA_INDEX_URL -u PIP_FIND_LINKS \
      -u PIP_NO_INDEX -u PIP_TRUSTED_HOST -u PIP_CONFIG_FILE \
      -u PIP_REQUIREMENT -u PIP_CONSTRAINT -u PIP_BUILD_CONSTRAINT \
      PIP_CONFIG_FILE=/dev/null \
      "$lock_tools_dir/bin/python" -I -m pip --disable-pip-version-check install \
      --index-url=https://pypi.org/simple --require-hashes -r "$lock_file"
  }

  install_lock_tools "$LOCK_TOOLS_DIR" requirements-build.lock
  "$LOCK_TOOLS_DIR/bin/python" -I scripts/lock_dependencies.py --stage-build-lock

  NEXT_LOCK_TOOLS_DIR="$(mktemp -d)"
  install_lock_tools "$NEXT_LOCK_TOOLS_DIR" requirements-build.next.lock
  "$NEXT_LOCK_TOOLS_DIR/bin/python" -I scripts/lock_dependencies.py

  git diff -- requirements-build.lock requirements-runtime.lock requirements-detector.lock
  python3.12 -I scripts/lock_dependencies.py --check
)
```

`requirements-build.txt` exactly pins the complete resolver toolchain. Review unexpected additions, removals, index changes, and version jumps in all three locks before committing. Generation uses the backtracking resolver, exact versions, SHA-256 hashes, and an isolated package-index configuration. If publication fails partway through, previously published files are restored to their prior contents or prior absent state. A shared application digest covers the runtime input manifests and matching project tables; the build lock has a separate digest of the exact tool input.

### Check freshness without package-index access

CI and pre-deployment validation only need the standard-library interpreter:

```sh
PIP_NO_INDEX=1 PYTHONNOUSERSITE=1 \
  python3 -I scripts/lock_dependencies.py --check
```

Check mode reads local inputs and lock files only. It verifies declaration synchronization, headers, approved indexes, exact pins, raw whitespace and continuation syntax, hashes, required direct dependencies, and that direct pins satisfy their input constraints. It neither imports `pip-tools`, contacts an index, nor rewrites a missing, stale, or malformed lock.

### Troubleshoot lock generation

- If generation reports the wrong Python version, missing `-I`, or a wrong build-tool version, let the cleanup trap discard that environment and repeat the documented bootstrap with Python 3.12 and `requirements-build.lock`.
- If generation or check reports an input mismatch, update both the requirements manifest and its matching `pyproject.toml` table before regenerating.
- If `--check` reports a stale lock, regenerate all three locks and review their diff; copying a digest between headers does not update resolved dependencies.
- If resolution cannot reach PyPI or the PyTorch CPU index, verify outbound HTTPS and proxy or certificate configuration, then rerun generation. Do not remove hashes or replace the CPU index with an unreviewed source.
- If generation is interrupted or compiler output is invalid, first run the offline check. It reports a missing, stale, malformed, or mixed generated set without rewriting it. Then rerun generation in a fresh tool environment. All compiler results are validated before publication, partial publication is rolled back when possible, cleanup failures are reported without hiding the original error, and the next offline check is the recovery authority.

## First deployment

### 1. Create local operator files

From the repository root:

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
cp config.yaml.example config.yaml
cp .env.example .env
mkdir -p data "$model_dir"
chmod 700 data
chmod 600 config.yaml .env
)
```

Edit `config.yaml` for the real Matrix endpoint, room, authorized senders, spot polygons, and supported thresholds. Edit `.env` with the real camera streams and Matrix access token. Do not put resolved credentials in `config.yaml`.

Compose reads `.env` automatically for variable interpolation even though the service has no `env_file` entry. Values already exported in the invoking shell take precedence according to normal Compose rules. Each workflow below runs in a subshell, independently resolves the same value from the invoking environment or Compose's `.env` environment, defaults it to `./models`, and exports it back to Compose. This keeps host file operations and the `/models` bind mount aligned without leaking the resolved value into the next block. For a custom location such as `/srv/models`, set `MODEL_DIR=/srv/models` in `.env` before running the next block.

### 2. Stage and authenticate the detector weights

Obtain `yolov8n.pt` through the operator-approved artifact channel and place it in the host model directory. Do not rely on an unreviewed first-run download for production. Check the local digest:

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
mkdir -p "$model_dir"
test -f "$model_dir/yolov8n.pt"
chmod 0644 "$model_dir/yolov8n.pt"
sha256sum "$model_dir/yolov8n.pt"
)
```

Compare the printed checksum with the SHA-256 checksum published by the trusted artifact source before continuing. This runbook intentionally does not embed a checksum because the artifact owner is the authority for the exact approved file. Keep `detection.model: /models/yolov8n.pt` in `config.yaml`. If the trusted directory is elsewhere, set `MODEL_DIR` in `.env`; Compose mounts that directory read-only at `/models`.

### 3. Build the complete detector image

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
docker compose config --quiet
docker compose build parking-spot-monitor
)
```

The resulting local image is `parking-spot-monitor:local`. The Dockerfile also has a smaller `runtime-app` target with the application and capture stack but without the YOLO detector packages; it is not the complete live camera-monitor image. The dependency-only `tooling` target omits both application source and capture packages and is intended for build-layer validation.

The model is not copied into the image. Container recreation reuses the authenticated host file through the read-only mount.

### 4. Validate configuration and the mounted model

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor \
  --config /config/config.yaml \
  --data-dir /data \
  --validate-config
)
```

Validation should name missing environment variables or invalid fields without printing resolved secrets. It also verifies that the explicit `/models/yolov8n.pt` path is a file inside the container. A missing or misnamed weight file fails here, before detector construction or the capture loop.

The tracked Compose service pins `YOLO_CONFIG_DIR=/data/ultralytics`; when launching the package directly, it can be left unset. Before importing Ultralytics, the runtime creates `<data-dir>/ultralytics` with mode `0750` and exports that exact path for the process, so Ultralytics does not write under the container user's home directory. A repeated in-process startup replaces only a fallback value that the runtime previously managed. Any explicitly configured `YOLO_CONFIG_DIR` must name the `ultralytics` directory directly under the selected `--data-dir`; a mismatch fails startup instead of silently redirecting writes.

### 5. Start and verify the service

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
docker compose up -d --build parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
)
```

The Compose status should become `healthy`. The startup log should include `startup-config-loaded`, `startup-ready`, and successful primary capture events. Confirm the container health command directly when needed:

```sh
docker compose exec -T parking-spot-monitor \
  python -m parking_spot_monitor.healthcheck \
  --health-file /data/health.json \
  --max-age-seconds 120
```

Confirm current host artifacts:

```sh
stat data/health.json data/state.json data/latest.jpg
python -m json.tool data/health.json
python -m json.tool data/state.json
```

Healthy runtime evidence includes a recent `last_frame_at`, zero or explained failure counters, and a compact `matrix_outbox` summary without record-level `items`. Detailed durable deliveries remain in `data/matrix-outbox.json`.

## Routine operations

Use these commands from the repository root:

```sh
# Status and recent logs
docker compose ps
docker compose logs --tail 100 parking-spot-monitor

# Follow structured logs
docker compose logs -f parking-spot-monitor

# Apply config or environment changes without rebuilding code
docker compose restart parking-spot-monitor

# Rebuild and recreate after source, dependency, or Dockerfile changes
docker compose up -d --build parking-spot-monitor

# Stop while preserving the host-mounted data directory
docker compose stop parking-spot-monitor

# Remove the container and Compose network while preserving host files/images
docker compose down
```

Do not use `docker compose down -v` as a routine operation. The current data is a host bind mount, but destructive volume cleanup is unnecessary and becomes risky if named volumes are added later.

## Resource and cadence verification

Take a point-in-time resource sample:

```sh
docker stats --no-stream "$(docker compose ps -q parking-spot-monitor)"
stat -c '%n %s bytes' data/health.json data/operator-decision-memory.json
```

For a short trend, repeat `docker stats --no-stream` at a fixed interval and compare block-write growth, memory, and CPU bursts. Use structured capture logs to confirm cadence and high-resolution use:

```sh
docker compose logs --since 10m parking-spot-monitor \
  | grep '"event":"capture-frame-written"'
```

### Low-latency production profile

This is an explicit production override, not a change to the compatible defaults in `config.yaml.example`. Use it when confirmed departure detection needs to meet a target of 30 seconds on a healthy host. The target covers the service's transition-confirmation telemetry; camera availability, Matrix network delivery, and an externally starved host are separate concerns.

Preflight the host before changing `config.yaml`:

```sh
free -h
df -h data
docker compose ps
container_id="$(docker compose ps -q parking-spot-monitor)"
if [ -n "$container_id" ]; then
  docker stats --no-stream "$container_id"
fi
```

Confirm that the host has usable available memory and disk space, that swap use is understood, and, for an existing service, that CPU and memory are not already saturated. Investigate an unhealthy service or sustained resource pressure before applying a faster capture cadence, because external host starvation invalidates the latency target. This profile authorizes changes only to the parking monitor: unrelated containers require separate operator authorization, so do not stop, restart, reconfigure, or resource-limit them as part of this procedure.

Before editing, record the seven current values in the deployment change record or create the protected backup described under [Backup and recovery](#backup-and-recovery). Do not make a second config copy inside the checkout: only `config.yaml` itself is guaranteed to be ignored by Git. Apply these overrides to the matching sections of the operator-owned `config.yaml`:

```yaml
stream:
  capture_timeout_seconds: 4

occupancy:
  confirm_frames: 2
  release_frames: 2

runtime:
  frame_interval_seconds: 8
  occupied_frame_interval_seconds: 8
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 12
```

Keep the other operator-specific keys, including stream environment names, polygons, Matrix routing, and detector thresholds. Validate before restart, and do not apply a configuration that fails validation:

```sh
docker compose config --quiet
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor \
  --config /config/config.yaml \
  --data-dir /data \
  --validate-config
docker compose restart parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
```

Every iteration begins with the primary low-resolution stream. When every spot is stably empty, the next primary capture uses the 12-second stable cadence; occupied, unknown, degraded, transitioning, partially confirmed, or weak-presence states use eight seconds. Thus occupied monitoring stays at eight seconds instead of slowing to the empty-stable cadence.

High-resolution verification remains conditional. Weak primary evidence that could enter occupied state triggers entry verification. For a currently occupied spot, missing primary evidence triggers release verification on the frame that would reach `release_frames`. Periodic verification still follows `stream.escalation_verification_seconds`. When an iteration escalates successfully, the high-resolution detection is the authoritative final result for that iteration's occupancy decision, state transition, and event snapshot; it is not combined with the primary result.

A confirmed occupied transition creates its base alert independently of history enrichment or profile matching. History or occupied-image preparation failures degrade that optional context without cancelling the confirmed alert. Matrix sends the base occupied text before snapshot preparation, so a snapshot preparation or upload failure leaves a durable text fallback when that text send succeeded. Treat the warning and degraded health as work to repair; do not infer that image delivery succeeded.

### Validate departure latency

After restart, confirm the service is healthy and observe representative real departures without moving the camera, modifying polygons, or disrupting other containers:

```sh
docker compose exec -T parking-spot-monitor \
  python -m parking_spot_monitor.healthcheck \
  --health-file /data/health.json \
  --max-age-seconds 120
docker compose logs --since 30m parking-spot-monitor \
  | grep '"event":"occupancy-transition-latency"' \
  | grep '"transition_direction":"occupied-to-empty"'
docker compose logs --since 30m parking-spot-monitor \
  | grep '"event":"capture-loop-cadence-changed"'
```

For each representative departure, check that `opposite_evidence_to_confirmation_seconds` is at most 30 seconds and retain the same event's `primary_capture_seconds`, optional `verification_capture_seconds`, `cadence_seconds`, and `cadence_reason`. The metric starts at the last contrary occupied evidence, so it is a conservative runtime proxy rather than a wall-clock timestamp supplied by a person watching the curb. Confirm that occupied/active decisions report an eight-second cadence and that only stable all-empty decisions report 12 seconds. Record host `free -h` and bounded service-only `docker stats --no-stream` samples alongside the transition events. A capture failure, decoder timeout, overloaded host, camera outage, or materially different workload invalidates that sample; repair the condition and repeat instead of claiming the target.

### Restore conservative settings

For an immediate fixed-cadence diagnostic rollback, set `adaptive_polling_enabled: false`; the loop then uses `frame_interval_seconds` regardless of occupied or stable state. To restore the tracked conservative transition behavior, set these values in the operator-owned `config.yaml`:

```yaml
stream:
  capture_timeout_seconds: 15

occupancy:
  confirm_frames: 3
  release_frames: 3

runtime:
  frame_interval_seconds: 30
  occupied_frame_interval_seconds: 30
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 60
```

You may instead omit `occupied_frame_interval_seconds`; compatibility semantics make it follow `frame_interval_seconds`, which is 30 seconds in this rollback. If a protected pre-change backup contains deliberately different conservative values, restore that configuration through the documented backup and recovery workflow instead. In either case, rerun the validation command above, restart only `parking-spot-monitor`, confirm fresh health, and retain the latency-profile logs with the rollback record.

The resource controls are:

- `runtime.frame_interval_seconds`: active or uncertain polling interval; compatible default is 30 seconds.
- `runtime.occupied_frame_interval_seconds`: occupied polling interval; when omitted it follows the active interval.
- `runtime.stable_frame_interval_seconds`: stable all-empty polling interval; compatible default is 60 seconds.
- `runtime.stable_settle_frames`: consecutive stable frames required before slower polling.
- `runtime.debug_overlay_interval_seconds`: periodic overlay interval; transitions still force an overlay.
- `stream.escalation_verification_seconds`: periodic high-resolution verification interval; transitions can still escalate sooner.

Capture validation rejects any primary or named profile above 7,680 pixels in either dimension or 33,177,600 pixels in total. A captured JPEG must be no larger than 32 MiB, decode as JPEG, and match its configured profile dimensions before publication. A rejected capture does not replace the previous known-good frame.

Detection-lab execution admits at most one active replay or tuning job. A concurrent request is persisted as blocked with `lab_busy`; retention does not remove the active job directory. The Matrix outbox similarly owns at most one delivery worker, and that worker drains no more than one durable record per pass.

Matrix timing and outage controls use these production defaults:

| Key | Default | Rollback or compatibility setting |
| --- | ---: | --- |
| `matrix.command_poll_interval_seconds` | `60` | `0` restores polling on every capture-loop iteration. |
| `matrix.command_failure_cooldown_seconds` | `60` | Must remain positive. Keep `60` for the documented compatibility baseline. |
| `matrix.command_failure_max_cooldown_seconds` | `900` | Set equal to the initial cooldown (`60`) to disable exponential cooldown growth while retaining failure pacing. |
| `matrix.command_request_timeout_seconds` | `2` | Increase only for a known slow homeserver; command lag can then increase by the same amount. |
| `matrix.command_retry_attempts` | `1` | Keep `1` to avoid a command sync monopolizing the worker; alert delivery keeps its separate policy. |
| `matrix.unauthorized_reply_cooldown_seconds` | `300` | `0` restores a rejection reply for every unauthorized command. |
| `matrix.retry_jitter_ratio` | `0.2` | `0` disables locally calculated retry jitter; server `Retry-After` remains authoritative. |
| `matrix.outbox_retry_interval_seconds` | `60` | Must remain positive. Use explicit drain tooling for immediate troubleshooting, or restore the rollback image for the prior delivery implementation. |
| `matrix.outbox_retry_max_seconds` | `900` | Set equal to the initial interval (`60`) to disable exponential growth while retaining due-time persistence. |

Capture and observability controls use these defaults:

| Key | Default | Operational effect |
| --- | ---: | --- |
| `stream.reconnect_seconds` | `5` | Initial capture reconnect delay. |
| `stream.reconnect_max_seconds` | `60` | Hard post-jitter reconnect ceiling; must cover the initial delay. |
| `stream.reconnect_jitter_ratio` | `0.2` | Local reconnect jitter; set `0` for deterministic delay. |
| `runtime.log_summary_interval_seconds` | `900` | Bounded aggregate INFO cadence. Routine frame detail remains DEBUG. |
| `runtime.decision_memory_checkpoint_interval_seconds` | `300` | Maximum routine-record checkpoint time under normal iteration progress. |
| `runtime.decision_memory_checkpoint_max_pending_records` | `50` | Count boundary that checkpoints routine records sooner. |

Successful command polls wait for the poll interval. Failed half-open probes double the cooldown up to the maximum, and a successful probe resets it. Capture continues while command polling is in cooldown; the loop does not sleep for the Matrix failure interval.

Command fetching is background-only. The single worker may hold one request and one result; a slow homeserver therefore delays operator commands without moving cursor persistence, archive mutations, replies, or detector work off the capture thread. The first successful result is applied at a later capture-loop boundary, so expected command visibility is the configured poll interval plus the bounded request and capture scheduling delay.

Outbox retries are selected from their persisted UTC due time. Pending work is considered before legacy retries without a due time, followed by scheduled retries in due order. A restart before the due time does not deliver early. The optional retry and upload-derivative fields remain in outbox schema version 1. One complete atomic JSON publication after each durable text, upload, image, retry, or terminal phase is intentionally retained; removing that bounded write amplification would require a separately reviewed storage migration.

Decision-memory durability has two classes. State transitions, alerts, command outcomes, feedback and corrections, and startup/shutdown lifecycle decisions request an immediate flush. Routine diagnostics request a checkpoint at the first configured time or pending-count boundary. At the defaults, 300 seconds and 50 pending records are checkpoint triggers under successful persistence, not a hard crash-loss ceiling: a failed publication remains dirty, and a multi-record append can cross the count boundary before the checkpoint attempt. A normal Compose stop calls close and attempts the pending flush. The persisted decision-memory JSON schema and retention bound are unchanged.

Vehicle-history health remains a streamed archive reconciliation behind the existing revision/TTL cache. In-process mutations invalidate it immediately; external filesystem replacement may remain stale only until the TTL expires. Profile summaries reuse the effective closed records already loaded for that request. Full archive reconciliation and on-demand analytics scans remain bounded and intentionally unchanged because there is no current recent-session hot-path consumer.

Canonical vehicle-history full JPEGs are published without re-encoding: copy-on-write reflink is preferred, with an exact bounded descriptor copy as fallback. Both yield an inode independently owned from the writable capture source; hardlinks are not used. Matrix retry evidence stores the exact validated derivative bytes selected for upload. Startup and the next applicable retention/capture operation recover indexed interrupted cleanup. Preserve `.owned-disposals.json` and `.upload-derivatives`, and keep these application-owned directories free of concurrent noncooperating writers.

### 2026-07-29 production rollout evidence

The resource-hardening image was built with `--pull`, validated against the production bind mounts, and recreated with the existing operator configuration, data, and authenticated model. Docker reported the service healthy, the explicit in-container healthcheck passed, and the restart count remained zero through the bounded observation. The previous immutable image remained tagged for rollback, and the pre-deployment configuration and model remained in a protected host backup.

The following evidence compares the tracked, redaction-safe [pre-change baseline](resource-hardening-prechange-baseline.md) with the first production observation. Neither side is a controlled benchmark: Docker statistics, thread count, outbox size, and health-snapshot duration are point samples under different live workloads.

| Measurement | Pre-change baseline | Post-deployment evidence |
| --- | ---: | ---: |
| Docker CPU | `63.07%` | `0.00%` in each of two instantaneous samples |
| Docker memory | `362.2 MiB` | `577.5 MiB`, then `624.4 MiB`, of a `4 GiB` limit |
| Docker PIDs / process threads | `13` / `13` | `14` / `14` |
| Matrix outbox file size | `1,565,331 bytes` | `1,614,450 bytes` |
| Vehicle-history health snapshot | `59.083 ms` | `63.968 ms` |
| Docker network I/O | `307 MB / 11.8 MB` after about 11 hours | `3.57 MB / 119 kB` after about 7 minutes |
| Docker block I/O | `14.2 GB / 5.7 GB` after about 11 hours | `319 MB / 5.68 MB` after about 7 minutes |

The bounded steady log window ran from `2026-07-30T06:11:40Z` through `2026-07-30T06:14:52Z` (`192` seconds). It contained `33` structured `INFO` records, no `WARNING` or `ERROR` records, and no unparsed records. Scaling that short count gives an explicit projection of `14,850 INFO records/day`. The baseline is an observed 24-hour count of `13,117 INFO`, `31 WARNING`, and `0 ERROR` records, not a projection. Startup was excluded because its `194.469`-second window projected `21,770 INFO records/day`; the 192-second result is still too short and workload-dependent to claim a log-volume reduction. Repeat the same aggregate-only measurement over a representative 24-hour period before drawing a trend conclusion.

A synthetic scheduler trace, without a Matrix network request, confirmed the documented failure pacing: a first failed poll retried after `60` seconds, a second consecutive failure doubled the wait to `120` seconds, and a success cleared the failure count and resumed the normal `60`-second interval. This demonstrates scheduler behavior only; it is not evidence of homeserver availability or end-to-end delivery.

These measurements establish a healthy, rollback-ready deployment, not a performance win. Network and block I/O are cumulative from container creation and reset on recreation, while memory and CPU can vary substantially with capture and inference timing. Keep the rollback image and backup until a representative observation period confirms acceptable behavior and resource use.

### 2026-07-30 initial final-audit deployment evidence

The final audited image `sha256:9a5e648eb77dff516c53041fd7c9e3c5d0297f2c5bff0e1f5bee3ca1cbfa542e` was recreated against the existing operator configuration, data, and read-only `.pt` model mounts in `10.465s`. Compose reported the service healthy immediately, the explicit in-container healthcheck passed, and the restart count remained zero. An explicit Compose stop completed in `1.189s` with exit code zero, no OOM condition, and no forced-kill evidence. A later quiesced-backup stop completed successfully in `5.260s`; the same image restarted healthy with zero restarts and final `StartedAt=2026-07-30T22:02:08.764320862Z`.

The first live archive attempt detected concurrent `/data` changes and was not accepted as the full backup. A later quiesced archive at `/home/keith/backups/parking-spot-monitor/task11-20260730-dUwuRu` remains a valid older recovery point, but it is not the backup for the final deployment. Its data timestamp and image metadata must not be retrofitted or represented as current.

| Measurement | Prior deployed warm sample | Final short warm evidence |
| --- | ---: | ---: |
| Docker CPU | `0.00%` point sample | `0.00%` in two initial samples and the post-backup sample |
| Docker memory | `246.3 MiB` | `373.1 MiB`, then `374.4 MiB`; post-backup restart `355.3 MiB` |
| Docker PIDs / Python threads | `14` / `14` | `16` / `15`, including one `docker-init` process |
| Docker block I/O | Container-lifetime value not comparable | `0 B / 3.56 MB`, then `0 B / 5.3 MB`; post-backup restart `71.8 MB / 2.63 MB` |
| INFO records | `346` over the preceding 30 minutes | `63` in an initial 10 minutes containing two startups and one stop; `6` in a later 45-second steady sample |
| Lifecycle event-name counts | Not collected | Across all final operations: `startup=3`, `shutdown-requested=2`, `outbox-enqueued=5` |
| Capture duration | Not measured | Six captures averaged `1.176850s`; maximum `1.194097s` |
| Outbox / decision / health bytes | `1,173,077` / `96,975` / `1,866` | Final post-backup `1,176,396` / `97,261` / `1,863` |
| Data files / upload derivatives | `4,124` / `0` | `4,126` / `0` |

The final `3,319`-byte outbox growth accompanies five durable lifecycle enqueues across three starts and two stops, including the quiesced backup cycle. No live Matrix outage was induced, so the final window does not measure outage retry latency; the controlled scheduler trace and serial tests remain the evidence for `60s -> 120s -> success/reset` backoff. No ONNX or TorchScript switch occurred; production continues to use `.pt`.

These measurements are not like-for-like. The initial final `373.1–374.4 MiB` points are roughly `11–12 MiB` above the original `362.2 MiB` point, while the post-backup restart point was `355.3 MiB`; all are below the earlier first-hardened `577.5–624.4 MiB` samples and above the immediately prior `246.3 MiB` warm point. None is peak RSS. The block-I/O values cover short restart/backup-adjacent windows, the 45-second INFO sample is too short to extrapolate strongly, capture duration was measured only after the final deployment, and transition/high-resolution/Matrix workloads were not matched. The deployment is healthy and rollback-ready; peak-RSS and production resource-improvement acceptance remain pending a representative equal-window observation.

### 2026-07-30 first residual re-audit deployment evidence

After the first residual re-audit findings were remediated, runtime source boundary `bed302f2317f467206db2de308012eb25ad0753b` passed `1,681` serial tests in `45.38s`, compileall, dependency-lock validation, Compose rendering, and production-mount config validation. The detector image rebuilt as `sha256:2e6624ada3f196372a863c7899bb688cad5034bf17d5c7a8ef29007b72e75227` and is retained as `parking-spot-monitor:release-final-fixes-bed302f-20260730`. Recreation took `5.61s`. The container became healthy and wrote a successful frame newer than its `StartedAt`; a later graceful stop took `1.05s`, exited zero without OOM or forced-kill evidence, and the same container restarted healthy with `StartedAt=2026-07-30T23:20:22.357413802Z`, restart count zero, and another post-start successful frame.

Five 10-second-spaced final warm samples reported `0.00%` CPU; RSS was `442.8`, `462.4`, `462.4`, `462.4`, and `467.4 MiB`. A point five seconds after the graceful restart was `0.01%` CPU and `355.8 MiB` RSS. Docker reported 16 PIDs; the monitor Python process had 15 threads plus `docker-init` (the sampling command briefly added a shell process). Final artifact metadata was outbox `1,181,748 B`, decision memory `96,283 B`, and health `1,863 B`.

The new samples again vary materially by capture/inference and restart phase. They are neither peak RSS nor a workload-matched comparison, and the block-I/O counters changed across restart. The final state is healthy, rollback-ready, and bounded by stronger persistence/recovery controls; production resource-improvement acceptance remains pending a representative equal-window observation.

### 2026-07-30 final adversarial acceptance evidence

Runtime source boundary `f07f02884444a50c75ae7b6afd8b1a65aa320d83` passed `1,691` serial tests in `44.31s`, compileall, dependency-lock validation, Compose rendering, production-mount validation, and the final adversarial persistence/cache/recovery tests. The detector image rebuilt as `sha256:780eb5e056276c53bda2004a89a8810a92cedfee4b1ce5822f12376bb78762a8` and is retained as `parking-spot-monitor:release-final-f07f028-20260730`. Recreation took `2.06s`. After an explicit graceful stop of `1.07s`, the same container restarted healthy at `2026-07-30T23:58:41.856629336Z`, exit zero, no OOM or forced-kill evidence, restart count zero, and health/frame evidence newer than `StartedAt`.

Immediately before recreation, the transactional helper created the quiesced bundle `/home/keith/backups/parking-spot-monitor/final-ac6c228-20260730T235642Z` at `2026-07-30T23:56:54.422705+00:00`. It records deployment-helper source `ac6c228d51a0939cd66dc2ddb8217c6687fbace8` and exact predecessor `sha256:2e6624ada3f196372a863c7899bb688cad5034bf17d5c7a8ef29007b72e75227` under `parking-spot-monitor:rollback-pre-final-ac6c228-20260730T235642Z`. The protected directory is mode `0700`; every file, including `.env`, is `0600`. Its complete `1,186,641,920`-byte data archive has SHA-256 `b08e17fef9d212daafad1e8ca8a5bbcf684ceaaeb85c9ea15b847045e40184dc`; archive safety, approved-model identity, every consistency manifest, and rollback tag/ID all verify.

Five 10-second-spaced final samples reported CPU `0.00%`, `9.31%`, `0.00%`, `0.00%`, and `0.00%`; RSS was `350.1`, `372.1`, `343.2`, `343.2`, and `343.4 MiB`. A point five seconds after restart was `0.00%` CPU and `355.4 MiB` RSS. Docker reported 16–17 PIDs during the samples; steady state has one 15-thread Python process plus `docker-init`. Final artifact sizes were outbox `1,187,100 B`, decision memory `94,721 B`, and health `1,869 B`. These remain short, workload-dependent health observations rather than peak or controlled performance evidence.

### 2026-07-31 final review-closure deployment evidence

Source boundary `503e2f53c0dd6386f61938405403f48e26efb2f6` passed `1,697` serial tests in `51.49s` with test-process peak RSS `205,704 KiB`, compileall, dependency-lock validation, Compose rendering, production-mount validation, and the final recovery/publication/cache/rollback regressions. The detector image rebuilt as `sha256:97b05f5fd098f7d3dd2d5cb9ace459e0b82a776f4cb798ad058605f2bc0613a5` and is retained as `parking-spot-monitor:release-final-503e2f5-20260731`. Recreation took `1.79s`.

Immediately before recreation, the transactional helper created `/home/keith/backups/parking-spot-monitor/final-503e2f5-20260731T002235Z` at `2026-07-31T00:23:03.669634+00:00`. The bundle records the exact predecessor `sha256:780eb5e056276c53bda2004a89a8810a92cedfee4b1ce5822f12376bb78762a8` under `parking-spot-monitor:rollback-pre-503e2f5-20260731T002235Z`. The directory is mode `0700`; every file is mode `0600`. Its complete `1,191,096,320`-byte data archive has SHA-256 `1d4456afee248bc6404e4868c771b2fd8db020d0244d5364b188346fad913251`; bundle manifests, archive safety, approved-model identity, and rollback image identity all passed.

Five 10-second-spaced samples reported CPU `194.57%`, `0.00%`, `0.00%`, `200.62%`, and `0.00%`; RSS was `387.8`, `356.1`, `356.1`, `388.4`, and `388.4 MiB`. The two high CPU points overlapped detector work on the configured two-CPU limit. Docker reported 16 PIDs: a 15-thread Python process plus `docker-init`. Block I/O moved from `7.23 MB / 3.07 MB` to `7.23 MB / 3.69 MB` over the short window. A graceful stop took `4.25s`, exited zero without OOM or runtime error, and restarted in `0.32s`. The same container returned healthy with zero restarts and post-start health/frame evidence; the post-restart point was `0.00%` CPU and `356.3 MiB` RSS. Final artifact sizes were outbox `1,191,114 B`, decision memory `93,980 B`, and health `1,869 B`. These are bounded health observations, not peak or workload-matched performance acceptance.

### 2026-07-31 liveness and durability closure deployment evidence

Source boundary `8950717f1ea25086a87132628c94392a558be7cd` passed `1,706` serial tests in `44.88s` with test-process peak RSS `212,492 KiB`, compileall, dependency locks, Compose rendering, production-mount validation, and the final conditional-repair, degraded-cache, conflict-recovery, backup-durability, and backup-health regressions. The image rebuilt as `sha256:b941c94a7a45e237bc02b21482b6068d3e1169d824467347bd748152ec24430c`, retained as `parking-spot-monitor:release-final-8950717-20260731`, and recreated in `1.26s`.

Immediately before recreation, the hardened helper durably published `/home/keith/backups/parking-spot-monitor/final-8950717-20260731T004635Z` at `2026-07-31T00:46:56.657999+00:00`, then waited for a healthy frame after restarting the predecessor. The mode-`0700` bundle contains only mode-`0600` files and records exact predecessor `sha256:97b05f5fd098f7d3dd2d5cb9ace459e0b82a776f4cb798ad058605f2bc0613a5` under `parking-spot-monitor:rollback-pre-8950717-20260731T004635Z`. Its complete `1,193,451,520`-byte archive has SHA-256 `064a4dd3fa5e02e1b2b7337bc0ded5faa02cd1c5239826bb229f85dcb520ca64`; bundle manifests, archive safety, approved model, exact image identity, archive fsync, staging-directory fsync, and published-parent fsync passed.

Five 10-second-spaced samples reported CPU `0.00%`, `0.01%`, `0.00%`, `0.00%`, and `4.24%`; RSS was `354.7`, `377.0`, `377.0`, `377.0`, and `391.4 MiB`. Docker reported 16 PIDs: a 15-thread Python process plus `docker-init`. Block I/O moved from `7.10 MB / 2.65 MB` to `7.24 MB / 3.67 MB`. A graceful stop took `1.45s`, exited zero without OOM or runtime error, and restarted in `0.19s`. The same container returned healthy with zero restarts and fresh post-start health/frame evidence; the post-restart point was `0.00%` CPU and `350.5 MiB` RSS. Final artifact sizes were outbox `1,195,128 B`, decision memory `94,925 B`, and health `1,869 B`. These remain short health observations, not peak or workload-matched resource acceptance.

### 2026-07-31 failure-window and cooperative-writer closure deployment evidence

Source boundary `b8959617ebe2f28dff62adfa977151a8fc16d126` passed `1,719` serial tests in `46.49s` (`47.13s` timed process elapsed, `206,252 KiB` peak RSS), compileall, dependency locks, Compose rendering, production-mount validation, and the final decision-publication, conflict-compaction, temporal-retention, backup-parent, secondary-recovery-note, and cooperative-outbox-writer regressions. The image rebuilt as `sha256:2f380efcdd1bce53d98a5b3f7f2f20f14806ca5f3e140c9026dc7142662471a5`, retained as `parking-spot-monitor:release-final-b895961-20260731`, and recreated in `1.25s`.

Immediately before recreation, the helper durably published `/home/keith/backups/parking-spot-monitor/final-b895961-20260731T011200Z` at `2026-07-31T01:12:20.337655+00:00`, then waited for a healthy frame after restarting the predecessor. The mode-`0700` bundle contains only mode-`0600` files and records exact predecessor `sha256:b941c94a7a45e237bc02b21482b6068d3e1169d824467347bd748152ec24430c` under `parking-spot-monitor:rollback-pre-b895961-20260731T011200Z`. Its complete `1,195,509,760`-byte archive has SHA-256 `70fb94951e2aee94e42b1c1b8afbbcfe2d17128df1f51103b1a5c9b8017a29a4`; bundle manifests, archive safety, approved model, exact image identity, and durable publication ordering passed.

Five 10-second-spaced samples reported CPU `0.00%`, `79.89%`, `0.01%`, `0.00%`, and `0.00%`; RSS was `350.5`, `350.5`, `350.5`, `350.5`, and `372.0 MiB`. Docker reported 16 PIDs: a 15-thread Python process plus `docker-init`. Block I/O moved from `6.83 MB / 3.93 MB` to `7.10 MB / 5.44 MB`. A graceful stop took `1.51s`, exited zero without OOM, and restarted in `0.20s`. The same container returned healthy with zero restarts and fresh post-start health/frame evidence; the post-restart point was `0.00%` CPU and `350.3 MiB` RSS. Post-restart artifact sizes were outbox `1,199,142 B`, decision memory `96,438 B`, and health `1,869 B`. These remain short health observations, not peak or workload-matched resource acceptance.

### 2026-07-31 final reconciliation and operations closure deployment evidence

Source boundary `2f26a8dd7db2db95d7d94269cf80f9e413265905` passed `1,727` serial tests in `52.73s` (`53.40s` timed process elapsed, `206,748 KiB` peak RSS), compileall, dependency locks, Compose rendering, production-mount validation, and the final legacy-publication, byte-bounded-compaction, cleanup-retry, protected-parent-identity, and failed-tag-cleanup regressions. Two independent final code reviews reported zero actionable findings. The image rebuilt as `sha256:a249a5ad7f0fedef234fda7e7f951b693377a8834d066876c8d0b8150fe6a17f`, retained as `parking-spot-monitor:release-final-2f26a8d-20260731`, and recreated in `1.59s`.

Immediately before recreation, the helper durably published `/home/keith/backups/parking-spot-monitor/final-2f26a8d-20260731T014731Z` at `2026-07-31T01:47:45.896619+00:00`, then waited for a healthy frame after restarting the predecessor. The mode-`0700` bundle contains only mode-`0600` files and records exact predecessor `sha256:2f380efcdd1bce53d98a5b3f7f2f20f14806ca5f3e140c9026dc7142662471a5` under `parking-spot-monitor:rollback-pre-2f26a8d-20260731T014731Z`. Its complete `1,194,854,400`-byte archive has SHA-256 `138efbfd3a73b8473a1979d36aca9d352886d34fce6d06af2026a4be090cb4d0`; bundle manifests, archive safety, approved model, exact image identity, held-directory publication, and fresh predecessor restart passed.

Five 10-second-spaced samples reported CPU `0.00%`, `121.94%`, `0.00%`, `0.01%`, and `0.00%`; RSS was `357.5`, `376.8`, `344.8`, `344.8`, and `344.8 MiB`. Docker reported 16 PIDs in each sample; a process snapshot coincided with the one-shot healthcheck and showed the 15-thread service, `docker-init`, and one transient Python process. Block I/O moved from `8.69 MB / 2.73 MB` to `8.84 MB / 3.53 MB`. A graceful stop took `1.92s`, exited zero without OOM, and restarted in `0.32s`. The same container returned healthy with zero restarts and fresh post-start health/frame evidence; the post-restart point was `0.01%` CPU and `358.0 MiB` RSS. Post-restart artifact sizes were outbox `1,203,156 B`, decision memory `96,227 B`, and health `1,869 B`. All `146` copied runtime/config files matched the container at aggregate path-and-byte SHA-256 `e93ef9aa7591bc7e40fd7aa8bc30faa41711d13b5e2718e57b848086618f92e9`. These remain short health observations, not peak or workload-matched resource acceptance.

### Comparable post-upgrade observation

Use equal healthy and Matrix-outage windows and record only aggregate, redaction-safe evidence. Do not print environment values, config bodies, health/outbox/decision payloads, raw log lines, camera images, or Matrix responses.

```sh
container_id="$(docker compose ps -q parking-spot-monitor)"
docker stats --no-stream "$container_id"
docker compose top parking-spot-monitor
docker compose logs --since 30m --no-log-prefix parking-spot-monitor \
  | awk -F'"level":"' 'NF>1 {split($2,a,"\""); count[a[1]]++} END {for (k in count) print k, count[k]}'
docker compose exec -T parking-spot-monitor sh -c \
  'wc -c /data/matrix-outbox.json /data/operator-decision-memory.json /data/health.json; find /data -xdev -type f | wc -l'
```

Sample CPU/RSS/threads repeatedly at the same points in the capture cadence. Compare Docker block-write deltas over equal wall-clock windows, not cumulative totals from differently aged containers. From structured events, retain aggregate counts and durations only: successful/failed captures, Matrix command fetches, outbox retry latency and publication counts, decision-memory publications, JPEG publication strategies and encode attempts, INFO/WARNING/ERROR counts, and shutdown seconds. State workload differences, including transitions, high-resolution escalation, Matrix availability, and outbox depth, before drawing conclusions. The checked audit matrix and measurement template are in `docs/final-audit-remediation-report.md`.

### Graceful-stop verification

Normal upgrades and backups must preserve `init: true`, `stop_signal: SIGTERM`, and `stop_grace_period: 2m`. Verify the stop path after the new image is healthy:

```sh
set -eu
start_seconds="$(date +%s)"
docker compose stop parking-spot-monitor
end_seconds="$(date +%s)"
echo "graceful_stop_seconds=$((end_seconds - start_seconds))"
docker compose up -d --no-build parking-spot-monitor
container_id="$(docker compose ps -q parking-spot-monitor)"
started_at="$(docker inspect "$container_id" --format '{{.State.StartedAt}}')"
docker compose exec -T parking-spot-monitor python - "$started_at" <<'PY'
import json
import sys
from datetime import datetime
from pathlib import Path

started_at = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
health_path = Path("/data/health.json")
payload = json.loads(health_path.read_text(encoding="utf-8"))
last_frame = payload.get("last_frame_at")
if health_path.stat().st_mtime_ns <= int(started_at.timestamp() * 1_000_000_000):
    raise SystemExit("health artifact predates this container start")
if not isinstance(last_frame, str):
    raise SystemExit("new container has not recorded a successful frame")
if datetime.fromisoformat(last_frame.replace("Z", "+00:00")) <= started_at:
    raise SystemExit("last successful frame predates this container start")
PY
docker compose exec -T parking-spot-monitor \
  python -m parking_spot_monitor.healthcheck \
  --health-file /data/health.json --max-age-seconds 120
```

The stop must complete without Docker reporting a forced kill. Retry the final freshness check until the configured startup/capture allowance expires; never accept the persistent file solely because it is younger than the generic healthcheck limit. After restart, the durable shutdown lifecycle record may drain once; it must not be duplicated. Inspect only aggregate event counts when preserving verification evidence.

## Offline detector backend benchmark (no automatic switch)

Production continues to use the configured `.pt` model. The benchmark below is an offline evidence tool only: it does not change `detection.model`, the detector factory, the Compose command, fallback behavior, or the running service. An ONNX or TorchScript production change requires a separate design, review, explicit operator approval, and deployment after the evidence gates pass.

Use a dedicated, approved export/benchmark environment with the repository's pinned Ultralytics version and all format-specific exporter/runtime dependencies already installed. Do not let an offline benchmark auto-install missing packages. Stage an authenticated copy of the baseline and export both alternatives into the ignored `data/` tree:

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
BENCH_ROOT=data/detector-benchmark
mkdir -p "$BENCH_ROOT/models" "$BENCH_ROOT/frames" "$BENCH_ROOT/evidence" "$BENCH_ROOT/ultralytics"
chmod 0750 "$BENCH_ROOT" "$BENCH_ROOT/models" "$BENCH_ROOT/frames" "$BENCH_ROOT/evidence" "$BENCH_ROOT/ultralytics"
cp -- "$model_dir/yolov8n.pt" "$BENCH_ROOT/models/baseline.pt"
sha256sum "$model_dir/yolov8n.pt" "$BENCH_ROOT/models/baseline.pt"
YOLO_CONFIG_DIR="$BENCH_ROOT/ultralytics" python - <<'PY'
from pathlib import Path
from ultralytics import YOLO

source = Path("data/detector-benchmark/models/baseline.pt")
for export_format, expected_name in (
    ("onnx", "baseline.onnx"),
    ("torchscript", "baseline.torchscript"),
):
    exported = Path(YOLO(str(source)).export(format=export_format))
    expected = source.with_name(expected_name)
    if exported.resolve() != expected.resolve():
        raise SystemExit(f"stage the exported {export_format} artifact as {expected}")
PY
)
```

Copy a fixed, representative set of JPEG frames into `data/detector-benchmark/frames/`. Preserve their serial order in a JSON manifest; paths are relative to the manifest:

```sh
python - <<'PY'
import json
from pathlib import Path

root = Path("data/detector-benchmark")
frames = sorted(root.joinpath("frames").glob("*.jpg"))
if not frames:
    raise SystemExit("stage at least one representative JPEG frame")
root.joinpath("manifest.json").write_text(
    json.dumps({"frames": [str(path.relative_to(root)) for path in frames]}, indent=2) + "\n",
    encoding="utf-8",
)
PY
```

Run the three heavy backends serially. The harness gives `.pt`, ONNX, and TorchScript separate spawned processes, performs three warmup passes and twenty measured passes by default, and writes aggregate evidence to the requested output. It caps the manifest at 1 MiB and 256 frames, each frame at 32 MiB, the complete corpus at 512 MiB, the combined readiness/warmup/measured workload across all three backends at 64 GiB, warmup at 20 passes, measured iterations at 100, and each model at 2 GiB. The report distinguishes the calculated per-backend workload from the enforced global workload. The per-worker deadline defaults to 1,800 seconds and is capped at 3,600 seconds. Each worker atomically writes bounded JSON into its private temporary spool; the parent never waits on a partially written pipe message. A timed-out worker is terminated, killed if necessary, and reaped before the command stops; its spool is removed and the next backend is not started. Do not run multiple copies concurrently, do not add `pytest-xdist`, and run the related tests without `-n`; minimizing peak host CPU and memory is more important than test throughput.

```sh
YOLO_CONFIG_DIR=data/detector-benchmark/ultralytics \
  python scripts/benchmark_detector_backends.py \
  --manifest data/detector-benchmark/manifest.json \
  --pt-model data/detector-benchmark/models/baseline.pt \
  --onnx-model data/detector-benchmark/models/baseline.onnx \
  --torchscript-model data/detector-benchmark/models/baseline.torchscript \
  --output data/detector-benchmark/evidence/backends.json \
  --warmup 3 \
  --iterations 20 \
  --worker-timeout-seconds 1800

python -m json.tool data/detector-benchmark/evidence/backends.json
python -m pytest tests/test_detector_backend_benchmark.py -q
```

A completed benchmark exits zero even when no alternative is eligible. Missing models or frames, a malformed manifest, a failed backend worker, a worker timeout, or malformed/non-finite evidence exits two. Preflight reads the manifest and every ordered frame from bounded `O_NOFOLLOW|O_NONBLOCK` descriptors, captures device/inode/size/mtime/ctime and SHA-256 identities, and creates one private read-only manifest snapshot plus ordered frame snapshots from those exact bytes before any worker starts. Every backend receives the same frame snapshot paths. FIFOs, sockets, devices, directories, symlinks, empty files, and oversized files are rejected as inputs without waiting for a writer. The report records equal original/snapshot manifest hashes, equal ordered original/snapshot frame hashes, the corpus digest, frame count, corpus size, and calculated workload. Immediately before and after every worker, the harness performs cheap device/inode/size/mtime/ctime checks and hashes only an input whose metadata identity changed. Immediately before publication it comprehensively rehashes every original and snapshot once. A worker changing its own snapshot or any sibling snapshot exits two before another backend starts and publishes no report.

Model preflight accepts only non-symlink regular files with the documented `.pt`, `.onnx`, and `.torchscript` suffixes; all three must have distinct resolved paths, inodes, and content. It streams each original from one bounded `O_NOFOLLOW|O_NONBLOCK` descriptor into a suffix-preserving private read-only snapshot, then captures and verifies the snapshot's device, inode, size, mtime, ctime, and SHA-256 before starting workers. Workers receive only those snapshots. Evidence records the original model's bounded size and SHA-256 plus the equal snapshot SHA-256. Original and snapshot revalidation includes ctime, so same-size mutate-and-restore attempts still invalidate the run; all snapshot directories are removed on every exit path.

Create the output parent directory before starting the benchmark, as shown above. The output must not be a model, manifest, frame, hardlink to any input, symlink, directory, or path through a symlinked parent. The harness opens every parent path component without following symlinks and holds the verified parent directory descriptor for the entire run. Publication creates a random private temporary through that descriptor and captures its device/inode identity immediately, before permission changes, writes, or syncs. It then writes and syncs the temporary, revalidates all inputs and the requested parent identity, replaces names through the held descriptor, syncs the directory, and verifies both the requested parent and committed inode again. A write, permission, sync, identity, input, or parent-binding failure removes only the harness-owned inode through the held descriptor. The harness never intentionally overwrites benchmark inputs.

`load_seconds` consistently means time from constructor start through the first completed, normalized prediction. That readiness prediction is excluded from warmup and measured inference timings. The harness compares normalized results from every measured iteration within each backend and records a bounded reference digest plus mismatch count/first mismatch; any intra-backend change makes the run ineligible even if the final iteration matches `.pt`. Eligibility also requires exact frame and ordered class/count parity with no added or omitted detections, minimum bbox IoU `0.99` using the runtime's canonical geometry function, maximum confidence delta `0.02`, and at least a 15% improvement in p95 inference time or isolated-process peak RSS. All alternatives must pass parity before the report can set `production_switch_eligible` to true. Treat that flag as permission to begin a separately approved production-switch review, never as authorization to edit the live backend automatically.

## Backup and recovery

The deployment helper is written for the operator performing a release or recovery from the repository root. Its four operations preserve the single Compose service and the existing host-owned config, environment, model, and data mounts. The helper fails closed on unmet preconditions and exits `2` on an operational failure.

Before using it:

- Configure Compose normally. Standard variables such as `COMPOSE_PROJECT_NAME`, `COMPOSE_FILE`, and `MODEL_DIR` are honored by the `docker compose` commands.
- Keep `config.yaml`, `.env`, the approved model, and `data/` on local protected storage. The helper never prints their contents.
- Obtain the detector SHA-256 from the approved artifact source. The operator-supplied digest establishes provenance; checksums generated inside the bundle establish only later bundle consistency.
- Run from a clean checkout for upgrade. Verification stays serial to minimize host CPU and RSS.

Check the command surface without changing deployment state:

```sh
python3 scripts/deployment_operations.py --help
python3 scripts/deployment_operations.py backup --help
python3 scripts/deployment_operations.py upgrade --help
python3 scripts/deployment_operations.py rollback --help
python3 scripts/deployment_operations.py restore-data --help
```

### Create the protected pre-upgrade bundle

The backup parent must already exist, resolve without any symlink component, be a directory, and have no group or other write bits. Create or harden it before invoking the helper; the helper validates it before stopping the service or creating staging data and fails closed rather than changing its permissions:

```sh
install -d -m 0700 /protected/parking-backups
```

Choose a new child destination and a collision-resistant rollback tag. Supply the trusted model digest explicitly:

```sh
python3 scripts/deployment_operations.py backup \
  --backup-dir /protected/parking-backups/2026-07-30T230000Z \
  --rollback-tag parking-spot-monitor:rollback-20260730T230000Z \
  --approved-model-sha256 "$APPROVED_MODEL_SHA256"
```

The helper resolves the running service container and tags its immutable image ID; it never assumes the mutable `parking-spot-monitor:local` tag still names the deployed image. It opens and retains a protected directory descriptor, revalidates its device/inode and path identity before tagging, stopping, and publishing, anchors staging through that descriptor, and publishes and fsyncs relative to the held directory. A path swap or permission change therefore fails instead of redirecting the bundle. It creates a mode-`0700` private staging directory before copying secrets, installs signal-aware cleanup immediately, stops the service, archives the complete quiesced data tree, writes a UTC recovery timestamp and exact source/image identities, verifies the archive and manifests, publishes the bundle atomically, and restarts the service on both success and failure. Every bundle file is mode `0600`.

If bundle creation fails before publication and the prior service recovers with fresh health, the helper removes the newly created rollback tag so the same operation can be retried without leaking an obsolete reference. If service recovery also fails, it retains the exact tag needed for operator recovery and prints only a sanitized retained-tag note; investigate service health before deciding whether to remove it.

The defaults are `config.yaml`, `.env`, and `data/` in the current checkout. Use `--config-file`, `--env-file`, or `--data-dir` when operator bind sources live elsewhere; rollback accepts the same three overrides. The selected environment file drives both `MODEL_DIR` resolution and every Compose command.

The bundle contains `config.yaml`, `.env`, `yolov8n.pt`, `approved-model.sha256`, the complete `data.tar`, exact image metadata, and separate consistency manifests. Retain the image tag or an exported image on protected media. Do not describe the co-created manifests as signatures or authentication; they detect later corruption only.

### Upgrade the exact reviewed revision

Create the bundle first, then deploy the full reviewed SHA:

```sh
python3 scripts/deployment_operations.py upgrade \
  --reviewed-revision "$REVIEWED_REVISION" \
  --rollback-tag parking-spot-monitor:rollback-20260730T230000Z
```

The helper requires a clean worktree before checkout, runs compileall, the complete serial test suite, dependency-lock validation, and Compose validation, and rechecks cleanliness after tests, after the image build, and immediately before recreation. It validates production mounts with the built image, recreates without rebuilding, then waits up to 180 seconds for a valid health status, `updated_at`, health-file mtime, and successful-frame timestamp all newer than the new container `StartedAt`. Only after that freshness gate does it run the one-shot in-container healthcheck.

Do not add `pytest-xdist` or `-n`. Do not edit the checkout while the operation runs. Keep the protected bundle and rollback tag through a representative observation window.

### Roll back image, config, environment, and model

Use the bundle created immediately before the failed upgrade:

```sh
python3 scripts/deployment_operations.py rollback \
  --rollback-dir /protected/parking-backups/2026-07-30T230000Z \
  --config-file /srv/parking/config.yaml \
  --env-file /srv/parking/monitor.env
```

Rollback verifies every bundle consistency manifest, the operator-approved model record, archive safety, bundle timestamp, and the exact rollback image tag/ID before downtime. Its private temporary directory and cleanup handler exist before environment or model bytes are copied. The rollback set is validated against the current data mount before the service stops. Config, the selected environment file, and the model in the bundle's resolved `MODEL_DIR` use same-directory atomic renames. If the active and bundled model directories differ, the active model remains intact and any pre-existing rollback-target model is preserved for failure recovery. Any failure after stop restores the prior files and image, recreates the prior service, and applies the same post-`StartedAt` freshness gate before returning failure.

An ordinary rollback deliberately keeps the current data tree. This preserves the latest durable Matrix outbox, decision memory, state, and vehicle history.

### Restore the complete data recovery point

Restore `data.tar` only when the current data tree itself is unusable:

```sh
python3 scripts/deployment_operations.py restore-data \
  --rollback-dir /protected/parking-backups/2026-07-30T230000Z
```

The helper validates archive paths and types before extraction, extracts into a private sibling directory, stops the service, preserves the current tree, activates the restored tree by rename, restarts, and applies the shared freshness gate. If restart or freshness fails, it moves the failed restored tree aside, restores the preserved tree, restarts the prior deployment, and verifies fresh health before surfacing the failure. On success it prints only the preserved directory path. Keep that directory until Matrix outbox continuity and application-owned recovery metadata have been checked.

Never merge two live data trees, extract over an active data directory, or delete a preserved tree until recovery acceptance is complete.

### Recorded final recovery point

The final release record names the actual quiesced bundle timestamp, runtime source SHA, immutable predecessor image, archive digest, and potential recovery window. A bundle copied or retagged after creation remains the older recovery point; changing image metadata does not make its data archive current.

## Troubleshooting deployment failures

### Container does not start

```sh
docker compose config --quiet
docker compose ps -a
docker compose logs --tail 200 parking-spot-monitor
```

Check missing environment variables, invalid YAML, config validation errors, unavailable `/dev/dri`, and host permissions on `config.yaml`, `models/`, or `data/`. If validation reports that the configured model file does not exist, confirm `MODEL_DIR`, the host filename, the `/models` read-only mount in `docker compose config`, and `detection.model` before retrying.

### Container is running but unhealthy

Inspect `data/health.json`, its modification time, and recent structured logs. A health timestamp older than 120 seconds fails the configured health check. Confirm the camera is reachable and the runtime can atomically replace files under `data/`.

### Code changed but behavior did not

`docker compose restart` does not rebuild an image. Use:

```sh
docker compose up -d --build --force-recreate parking-spot-monitor
```

### Hardware decoding is unavailable

Confirm the host exposes `/dev/dri` and the container can access it. The runtime attempts supported hardware modes and falls back to software decoding. On a host without the device, remove the local Compose device mapping before deployment.

### Disk use grows unexpectedly

Inspect Docker log rotation, `data/snapshots/`, `data/timeline/frames/`, vehicle history, and the Matrix outbox. Do not delete `data/state.json` or `data/matrix-outbox.json` merely to reclaim space; determine whether pending state or delivery evidence must be preserved first.
