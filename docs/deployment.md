# Docker deployment and operations

This runbook is for an operator deploying the parking monitor on a Linux host. After following it, the operator can build the image, start the service, verify live health, measure resource use, upgrade safely, and roll back without losing runtime data.

## What runs in Docker

The default Docker build produces the complete live-monitor image. It contains Python 3.12, FFmpeg, the Intel media driver, the application, and the pinned CPU detector stack. The Compose service runs capture, YOLO inference, occupancy state, Matrix delivery, health reporting, timeline retention, and local operator intelligence in one container.

The image is disposable. Operator-owned state is deliberately outside it:

- `config.yaml` is mounted read-only at `/config/config.yaml`.
- `data/` is mounted read-write at `/data` and contains health, state, images, snapshots, the Matrix outbox, vehicle history, and decision memory.
- Camera and Matrix credentials come from the host environment or the ignored `.env` file.

No inbound application port is published. The service makes outbound connections to the configured camera streams and Matrix homeserver.

The current image runs as root inside the container so it can use the configured bind mounts and render device. Treat the repository, `config.yaml`, `.env`, `/dev/dri`, and `data/` as trusted operator surfaces; this Compose setup is not a hardened multi-tenant isolation boundary.

## Host prerequisites

Provide:

- A Linux host with Docker Engine and the Docker Compose plugin.
- Enough storage for the Docker image and the operator-selected retention period.
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

Container builds install reviewed, exact hashes from two generated manifests. `requirements-runtime.lock` contains the application runtime, while `requirements-detector.lock` contains the detector stack and retains the PyTorch CPU package index. Maintainers continue to edit the broad dependency bounds in `requirements.txt`, `requirements-detector.txt`, and the dependency tables in `pyproject.toml`; do not hand-edit either lock.

### Prerequisites

Create a dedicated build-tool environment from the repository root. The project runtime environment does not need `pip-tools`:

```sh
python3 -m venv /tmp/parking-lock-tools
/tmp/parking-lock-tools/bin/python -m pip install -r requirements-build.txt
```

The generator verifies that the installed `pip-tools` version matches the exact pin in `requirements-build.txt` before resolving anything.

### Regenerate reviewed locks

After changing a dependency input, run:

```sh
/tmp/parking-lock-tools/bin/python scripts/lock_dependencies.py
git diff -- requirements-runtime.lock requirements-detector.lock
python3 scripts/lock_dependencies.py --check
```

Review unexpected additions, removals, index changes, and version jumps before committing both locks. Generation uses the backtracking resolver, exact versions, and SHA-256 hashes. It writes a shared source digest into both headers so a change to either input manifest or to a `pyproject.toml` dependency table invalidates both locks.

### Check freshness without package-index access

CI and pre-deployment validation only need the standard-library interpreter:

```sh
PIP_NO_INDEX=1 PYTHONNOUSERSITE=1 \
  python3 scripts/lock_dependencies.py --check
```

Check mode reads local inputs and lock headers only. It neither imports `pip-tools`, contacts an index, nor rewrites a missing or stale lock.

### Troubleshoot lock generation

- If generation reports a missing or wrong `pip-tools` version, create a fresh isolated environment and install `requirements-build.txt` in it.
- If `--check` reports a stale lock, regenerate both locks and review their diff; copying a digest between headers does not update resolved dependencies.
- If resolution cannot reach PyPI or the PyTorch CPU index, verify outbound HTTPS and proxy or certificate configuration, then rerun generation. Do not remove hashes or replace the CPU index with an unreviewed source.
- If generation is interrupted, rerun it. Each completed manifest is published from a temporary file, while check mode remains safe to run at any point.

## First deployment

### 1. Create local operator files

From the repository root:

```sh
cp config.yaml.example config.yaml
cp .env.example .env
mkdir -p data
chmod 700 data
chmod 600 config.yaml .env
```

Edit `config.yaml` for the real Matrix endpoint, room, authorized senders, spot polygons, and supported thresholds. Edit `.env` with the real camera streams and Matrix access token. Do not put resolved credentials in `config.yaml`.

Compose reads `.env` automatically for variable interpolation even though the service has no `env_file` entry. Values already exported in the invoking shell take precedence according to normal Compose rules.

### 2. Build the complete detector image

```sh
docker compose config --quiet
docker compose build parking-spot-monitor
```

The resulting local image is `parking-spot-monitor:local`. The Dockerfile also has a smaller `runtime-base` target for config and non-detector tooling, but it is not the live camera-monitor image.

The default model name may cause Ultralytics to download weights on the first live run. Allow outbound model-download access for that initial start, or pre-stage a model and enable the documented read-only model mount. Container recreation discards an unmounted container-local model cache.

### 3. Validate configuration inside the image

```sh
docker compose run --rm parking-spot-monitor \
  python -m parking_spot_monitor \
  --config /config/config.yaml \
  --data-dir /data \
  --validate-config
```

Validation should name missing environment variables or invalid fields without printing resolved secrets.

### 4. Start and verify the service

```sh
docker compose up -d --build parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
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

## Safe upgrade

Run repository tests before publishing a change. On the deployment host, keep a rollback image and then fast-forward the checkout:

```sh
docker image tag parking-spot-monitor:local parking-spot-monitor:rollback
git status --short
git pull --ff-only
docker compose config --quiet
docker compose build parking-spot-monitor
docker compose up -d --no-build --force-recreate parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
```

Do not upgrade over uncommitted tracked changes. `config.yaml`, `.env`, and `data/` are ignored operator files and remain on the host across a normal pull and container recreation.

After the new service becomes healthy, repeat the health, artifact, cadence, and resource checks above. Keep the rollback tag until the deployment has completed a representative observation window.

## Rollback

To return to the image saved immediately before an upgrade:

```sh
docker compose stop parking-spot-monitor
docker image tag parking-spot-monitor:rollback parking-spot-monitor:local
docker compose up -d --no-build --force-recreate parking-spot-monitor
docker compose ps
docker compose logs --tail 100 parking-spot-monitor
```

This replaces the container but reuses the same read-only `config.yaml` mount and persistent `data/` bind mount.

For a configuration-only rollback, restore the previous local `config.yaml` and `.env`, validate them with the one-shot Compose command, then restart the service. Never copy secrets into Git history or terminal output captured for tickets.

## Backup and recovery

The minimum recovery set is:

- `config.yaml`, stored with restricted permissions.
- `.env`, stored in an approved secret backup rather than ordinary source archives.
- `data/state.json` and `data/matrix-outbox.json` for runtime continuity and durable pending delivery.
- `data/vehicle-history/`, snapshots, timeline frames, feedback labels, and decision memory according to retention and recovery needs.

For a consistent filesystem backup, stop the service, copy `data/` and `config.yaml` to protected storage, then start it again. Back up `.env` separately through the operator's secret-management process.

The runtime quarantines several malformed persisted JSON files rather than silently treating corrupt data as valid. Inspect logs and the corresponding `data/` artifacts before deleting any quarantine file.

## Troubleshooting deployment failures

### Container does not start

```sh
docker compose config --quiet
docker compose ps -a
docker compose logs --tail 200 parking-spot-monitor
```

Check missing environment variables, invalid YAML, config validation errors, unavailable `/dev/dri`, and host permissions on `config.yaml` or `data/`.

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
