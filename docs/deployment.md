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

To restore the former faster active polling, set `frame_interval_seconds` to 15 and restart. To disable adaptive cadence entirely, set `adaptive_polling_enabled: false`. Keep the stable interval greater than or equal to the active interval.

## Backup and recovery

The minimum recovery set is:

- `config.yaml`, stored with restricted permissions.
- The approved detector weight file and its trusted-source checksum record.
- `.env`, stored in an approved secret backup rather than ordinary source archives.
- `data/state.json` and `data/matrix-outbox.json` for runtime continuity and durable pending delivery.
- `data/vehicle-history/`, snapshots, timeline frames, feedback labels, and decision memory according to retention and recovery needs.

Set `BACKUP_DIR` to a new protected destination, then run this block from the repository root. It stops the service for a consistent copy and restarts it on success, failure, or interruption:

```sh
(
  set -eu
  : "${BACKUP_DIR:?set BACKUP_DIR to a new protected backup directory}"
  if [ -z "${MODEL_DIR+x}" ]; then
    compose_environment="$(docker compose config --environment)"
    MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
  fi
  model_dir="${MODEL_DIR:-./models}"
  export MODEL_DIR="$model_dir"
  if [ -e "$BACKUP_DIR" ]; then
    echo "backup destination already exists" >&2
    exit 1
  fi
  mkdir -p "$BACKUP_DIR"
  docker compose stop parking-spot-monitor
  trap 'docker compose start parking-spot-monitor' EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM

  cp -- config.yaml "$BACKUP_DIR/config.yaml"
  test -f "$model_dir/yolov8n.pt"
  model_checksum="$(sha256sum "$model_dir/yolov8n.pt" | awk '{print $1}')"
  cp -- "$model_dir/yolov8n.pt" "$BACKUP_DIR/yolov8n.pt"
  (
    cd "$BACKUP_DIR"
    printf '%s  %s\n' "$model_checksum" yolov8n.pt > yolov8n.pt.sha256
    sha256sum -c yolov8n.pt.sha256
  )
  docker image inspect parking-spot-monitor:local \
    --format '{{.Id}}' > "$BACKUP_DIR/image-id.txt"
  cp -a -- data "$BACKUP_DIR/data"
)
```

Store the matching `.env` separately through the approved secret-management process and associate it with this backup. Keep the image tag or an image export in the operator's protected image registry; `image-id.txt` records which local image the filesystem backup expects. Before relying on a copied backup, inspect its contents and test its checksum with `(cd "$BACKUP_DIR" && sha256sum -c yolov8n.pt.sha256)`.

The runtime quarantines several malformed persisted JSON files rather than silently treating corrupt data as valid. Inspect logs and the corresponding `data/` artifacts before deleting any quarantine file.

## Safe upgrade

Run repository tests before publishing a change. Before changing the deployment checkout, complete the backup workflow above with a new protected `BACKUP_DIR`; retain that directory as `ROLLBACK_DIR` until the upgrade has passed its observation window. On the deployment host, tag the rollback image and then fast-forward the checkout:

```sh
(
set -eu
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
docker image tag parking-spot-monitor:local parking-spot-monitor:rollback
git status --short
git pull --ff-only
docker compose config --quiet
docker compose build parking-spot-monitor
docker compose up -d --no-build --force-recreate parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
)
```

Do not upgrade over uncommitted tracked changes. `config.yaml`, `.env`, `models/`, and `data/` are ignored operator files and remain on the host across a normal pull and container recreation. Authenticate replacement weights before recreating the service, and retain the previous approved weight file with the rollback image when an upgrade changes models.

After the new service becomes healthy, repeat the health, artifact, cadence, and resource checks above. Keep the rollback tag until the deployment has completed a representative observation window.

## Rollback

To return to the image, configuration, and model saved immediately before an upgrade, set `ROLLBACK_DIR` to that protected backup. Restore the matching `.env` through the approved secret-backup process before validation. Then run this complete workflow from the repository root:

```sh
(
set -eu
: "${ROLLBACK_DIR:?set ROLLBACK_DIR to the protected rollback backup}"
if [ -z "${MODEL_DIR+x}" ]; then
  compose_environment="$(docker compose config --environment)"
  MODEL_DIR="$(printf '%s\n' "$compose_environment" | sed -n 's/^MODEL_DIR=//p')"
fi
model_dir="${MODEL_DIR:-./models}"
export MODEL_DIR="$model_dir"
docker compose stop parking-spot-monitor
docker image tag parking-spot-monitor:rollback parking-spot-monitor:local
cp -- "$ROLLBACK_DIR/config.yaml" config.yaml
mkdir -p "$model_dir"
(cd "$ROLLBACK_DIR" && sha256sum -c yolov8n.pt.sha256)
cp -f -- "$ROLLBACK_DIR/yolov8n.pt" "$model_dir/yolov8n.pt"
chmod 0644 "$model_dir/yolov8n.pt"
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor \
  --config /config/config.yaml \
  --data-dir /data \
  --validate-config
docker compose up -d --no-build --force-recreate parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
)
```

The checksum check authenticates the backed-up model before it is restored. Within this explicit rollback workflow, `cp -f` intentionally replaces the active weight with the compatible backed-up file; it never copies the model directory itself. The one-shot command then validates that the rollback image can read the restored configuration and model through the same exported bind mount before Compose recreates the service. The persistent `data/` bind mount remains in place.

For a configuration-only rollback, restore the previous local `config.yaml` and `.env`, validate them with the one-shot Compose command, then restart the service. Never copy secrets into Git history or terminal output captured for tickets.

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
