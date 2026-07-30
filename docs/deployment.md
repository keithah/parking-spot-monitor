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
| `matrix.unauthorized_reply_cooldown_seconds` | `300` | `0` restores a rejection reply for every unauthorized command. |
| `matrix.retry_jitter_ratio` | `0.2` | `0` disables locally calculated retry jitter; server `Retry-After` remains authoritative. |
| `matrix.outbox_retry_interval_seconds` | `60` | Must remain positive. Use explicit drain tooling for immediate troubleshooting, or restore the rollback image for the prior delivery implementation. |

Successful command polls wait for the poll interval. Failed half-open probes double the cooldown up to the maximum, and a successful probe resets it. Capture continues while command polling is in cooldown; the loop does not sleep for the Matrix failure interval.

To restore the former faster active polling, set `frame_interval_seconds` to 15 and restart. To disable adaptive cadence entirely, set `adaptive_polling_enabled: false`. Keep the stable interval greater than or equal to the active interval.

### 2026-07-29 production rollout evidence

The resource-hardening image was built with `--pull`, validated against the production bind mounts, and recreated with the existing operator configuration, data, and authenticated model. Docker reported the service healthy, the explicit in-container healthcheck passed, and the restart count remained zero through the bounded observation. The previous immutable image remained tagged for rollback, and the pre-deployment configuration and model remained in a protected host backup.

The following evidence compares the redaction-safe baseline in `data/resource-hardening-prechange-baseline.md` with the first production observation. Neither side is a controlled benchmark: Docker statistics, thread count, outbox size, and health-snapshot duration are point samples under different live workloads.

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

Run the three heavy backends serially. The harness gives `.pt`, ONNX, and TorchScript separate spawned processes, performs three warmup passes and twenty measured passes by default, and writes aggregate evidence to the requested output. It caps a manifest at 256 frames, warmup at 20 passes, measured iterations at 100, and each model at 2 GiB. The per-worker deadline defaults to 1,800 seconds and is capped at 3,600 seconds. A timed-out worker is terminated, killed if necessary, and reaped before the command stops; the next backend is not started. Do not run multiple copies concurrently, do not add `pytest-xdist`, and run the related tests without `-n`; minimizing peak host CPU and memory is more important than test throughput.

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

A completed benchmark exits zero even when no alternative is eligible. Missing models or frames, a malformed manifest, a failed backend worker, a worker timeout, or malformed/non-finite evidence exits two. Preflight accepts only non-symlink regular files with the documented `.pt`, `.onnx`, and `.torchscript` suffixes; all three must have distinct resolved paths, inodes, and content. The harness rechecks every model's stable identity, size, and SHA-256 digest after every worker and immediately before report publication, then records the bounded size and digest in evidence. If any backend changes any model, the run exits two and publishes no report.

Create the output parent directory before starting the benchmark, as shown above. The output must not be a model, manifest, frame, hardlink to any input, symlink, directory, or path through a symlinked parent. The harness validates those constraints before spawning a worker, pins the parent directory identity, and repeats its output and model checks immediately before an atomic report replacement. An output path that changes after preflight is rejected; the harness never intentionally overwrites benchmark inputs.

`load_seconds` consistently means time from constructor start through the first completed, normalized prediction. That readiness prediction is excluded from warmup and measured inference timings. The harness compares normalized results from every measured iteration within each backend and records a bounded reference digest plus mismatch count/first mismatch; any intra-backend change makes the run ineligible even if the final iteration matches `.pt`. Eligibility also requires exact frame and ordered class/count parity with no added or omitted detections, minimum bbox IoU `0.99` using the runtime's canonical geometry function, maximum confidence delta `0.02`, and at least a 15% improvement in p95 inference time or isolated-process peak RSS. All alternatives must pass parity before the report can set `production_switch_eligible` to true. Treat that flag as permission to begin a separately approved production-switch review, never as authorization to edit the live backend automatically.

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
