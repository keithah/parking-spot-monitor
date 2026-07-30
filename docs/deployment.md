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

The resource controls are:

- `runtime.frame_interval_seconds`: active or uncertain polling interval; production example is 30 seconds.
- `runtime.stable_frame_interval_seconds`: stable polling interval; production example is 60 seconds.
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

To restore the former faster active polling, set `frame_interval_seconds` to 15 and restart. To disable adaptive cadence entirely, set `adaptive_polling_enabled: false`. Keep the stable interval greater than or equal to the active interval.

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

The first live archive attempt detected concurrent `/data` changes and was not accepted as the full backup. The service was then stopped and the complete quiesced data tree, including hidden recovery metadata, was archived at `/home/keith/backups/parking-spot-monitor/task11-20260730-dUwuRu/data.tar`. The archive is `1,166,182,400` bytes with mode `0600` and SHA-256 `6dce07d6d53ba1b5c89e6bf4ded1680c4821b1b02376ec41c0d76c2df58a10ca`; the protected directory remains mode `0700` and all files remain `0600`. The bundle includes the matched `.env`, config, model, full image metadata, `data.tar.sha256`, `yolov8n.pt.sha256`, and bundle manifest, and all checks pass. Its executable `rollback-image-*` fields now select the immediate predecessor of the final deployment: `parking-spot-monitor:rollback-pre-final-fixes-bed302f-20260730` at `sha256:d90baee2d3154c2262d55741ee2ca3d657efa1bcacb956c0c40800dc04f8910f`. The older fallback `parking-spot-monitor:rollback-pre-final-audit-b67f99c-20260730` remains at `sha256:9a5e648eb77dff516c53041fd7c9e3c5d0297f2c5bff0e1f5bee3ca1cbfa542e`. Keep both until a representative observation closes the resource acceptance gate.

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

### 2026-07-30 residual re-audit deployment evidence

After every independent re-audit finding was remediated, runtime source boundary `bed302f2317f467206db2de308012eb25ad0753b` passed `1,681` serial tests in `45.38s`, compileall, dependency-lock validation, Compose rendering, and production-mount config validation. The detector image rebuilt as `sha256:2e6624ada3f196372a863c7899bb688cad5034bf17d5c7a8ef29007b72e75227` and is retained as `parking-spot-monitor:release-final-fixes-bed302f-20260730`. Recreation took `5.61s`. The container became healthy and wrote a successful frame newer than its `StartedAt`; a later graceful stop took `1.05s`, exited zero without OOM or forced-kill evidence, and the same container restarted healthy with `StartedAt=2026-07-30T23:20:22.357413802Z`, restart count zero, and another post-start successful frame.

Five 10-second-spaced final warm samples reported `0.00%` CPU; RSS was `442.8`, `462.4`, `462.4`, `462.4`, and `467.4 MiB`. A point five seconds after the graceful restart was `0.01%` CPU and `355.8 MiB` RSS. Docker reported 16 PIDs; the monitor Python process had 15 threads plus `docker-init` (the sampling command briefly added a shell process). Final artifact metadata was outbox `1,181,748 B`, decision memory `96,283 B`, and health `1,863 B`.

The new samples again vary materially by capture/inference and restart phase. They are neither peak RSS nor a workload-matched comparison, and the block-I/O counters changed across restart. The final state is healthy, rollback-ready, and bounded by stronger persistence/recovery controls; production resource-improvement acceptance remains pending a representative equal-window observation.

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
start_seconds="$(date +%s)"
docker compose stop parking-spot-monitor
end_seconds="$(date +%s)"
echo "graceful_stop_seconds=$((end_seconds - start_seconds))"
docker compose up -d --no-build parking-spot-monitor
container_id="$(docker compose ps -q parking-spot-monitor)"
started_at="$(docker inspect "$container_id" --format '{{.State.StartedAt}}')"
docker compose exec -T parking-spot-monitor \
  python -m parking_spot_monitor.healthcheck \
  --health-file /data/health.json --max-age-seconds 120
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

Choose a new destination and a collision-resistant rollback tag. Supply the trusted model digest explicitly:

```sh
python3 scripts/deployment_operations.py backup \
  --backup-dir /protected/parking-backups/2026-07-30T230000Z \
  --rollback-tag parking-spot-monitor:rollback-20260730T230000Z \
  --approved-model-sha256 "$APPROVED_MODEL_SHA256"
```

The helper resolves the running service container and tags its immutable image ID; it never assumes the mutable `parking-spot-monitor:local` tag still names the deployed image. It creates a mode-`0700` private staging directory before copying secrets, installs signal-aware cleanup immediately, stops the service, archives the complete quiesced data tree, writes a UTC recovery timestamp and exact source/image identities, verifies the archive and manifests, publishes the bundle atomically, and restarts the service on both success and failure. Every bundle file is mode `0600`.

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
  --rollback-dir /protected/parking-backups/2026-07-30T230000Z
```

Rollback verifies every bundle consistency manifest, the operator-approved model record, archive safety, bundle timestamp, and the exact rollback image tag/ID before downtime. Its private temporary directory and cleanup handler exist before `.env` or model bytes are copied. The rollback set is validated against the current data mount before the service stops. Config, environment, and model replacements use same-directory atomic renames. Any failure after stop restores the prior files and image, recreates the prior service, and applies the same post-`StartedAt` freshness gate before returning failure.

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
