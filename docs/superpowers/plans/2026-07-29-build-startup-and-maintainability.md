# Build, Startup, Logging, and Maintainability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make container builds reproducible and cache-efficient, avoid optional startup work, reduce no-op logging, persist model weights explicitly, and delete confirmed duplication/dead code.

**Architecture:** Preserve the current Compose service and runtime entrypoint. Split Docker build stages by responsibility, install from hash-locked manifests, lazy-import optional command features, and reuse existing canonical script helpers.

**Tech Stack:** Docker BuildKit, Docker Compose, Python 3.12, pip-tools, pytest, Pillow

## Global Constraints

- The Docker Compose topology and runtime command remain unchanged.
- Base images stay digest-pinned.
- Runtime and detector locks are separate and contain hashes.
- Broad dependency bounds remain in `pyproject.toml` and input manifests.
- Build caches never remain in final image layers.
- Legacy bare model names remain valid; production examples use `/models/yolov8n.pt`.
- WARNING/ERROR behavior and structured redaction remain unchanged.
- No file is allowed to cross 1,000 lines because of this slice.
- Every task uses red-green-refactor and ends with a focused commit.

---

### Task 1: Lazy-Load Optional Operator Features and Suppress No-Op INFO Work

**Files:**
- Modify: `parking_spot_monitor/__main__.py:1-36,289-343`
- Modify: `parking_spot_monitor/logging.py:21-53`
- Modify: `parking_spot_monitor/runtime_commands.py:12-100`
- Modify: `src/parking_monitor/matrix_outbox_delivery.py:179-264`
- Modify: `parking_spot_monitor/runtime_detection.py:61-89`
- Modify: `tests/test_startup.py`
- Modify: `tests/test_matrix_outbox_delivery.py`
- Modify: `tests/test_detection.py`

**Interfaces:**
- Adds: `StructuredLogger.is_enabled_for(level: str) -> bool`
- Preserves: optional factory return types through string annotations/`TYPE_CHECKING`

- [ ] **Step 1: Write a failing optional-import startup test**

```python
def test_importing_main_does_not_import_operator_stack() -> None:
    blocked = {
        "parking_spot_monitor.matrix_cockpit",
        "parking_spot_monitor.matrix_commands",
        "parking_spot_monitor.operator_cockpit",
        "parking_spot_monitor.operator_feedback",
        "parking_spot_monitor.detection_lab",
    }
    script = (
        "import sys; import parking_spot_monitor.__main__; "
        f"blocked={blocked!r}; "
        "present=sorted(blocked.intersection(sys.modules)); "
        "assert not present, present"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
```

- [ ] **Step 2: Write failing log-level behavior tests**

```python
def test_logger_reports_enabled_levels_without_serializing() -> None:
    logger = StructuredLogger(level="INFO", stream=StringIO())
    assert logger.is_enabled_for("DEBUG") is False
    assert logger.is_enabled_for("INFO") is True
    assert logger.is_enabled_for("WARNING") is True

def test_empty_outbox_and_noop_command_success_are_not_info_records(tmp_path: Path) -> None:
    stream = StringIO()
    logger = StructuredLogger(level="INFO", stream=stream)
    class NoopCommandService:
        def poll_once(self):
            return SimpleNamespace(
                processed_count=0, ignored_count=0, error_count=0, bootstrapped=False
            )
    _poll_matrix_commands_once(NoopCommandService(), logger=logger, iteration=1)
    client = FakeMatrixClient()
    make_delivery(tmp_path, client, stream=stream).drain_outbox(max_records=1)
    events = [json.loads(line)["event"] for line in stream.getvalue().splitlines()]
    assert "matrix-outbox-drain-started" not in events
    assert "matrix-command-poll-succeeded" not in events
```

- [ ] **Step 3: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_startup.py tests/test_matrix_outbox_delivery.py tests/test_detection.py -k 'does_not_import_operator or enabled_levels or not_info' -q`

Expected: FAIL because optional imports are top-level and no level-query API exists.

- [ ] **Step 4: Move imports behind the authorized-sender gate**

Keep only lightweight type imports under `if TYPE_CHECKING:`. Inside `_default_matrix_command_service_factory`, return before importing `DetectionLabManager`, `MatrixOperatorCockpitContext`, `MatrixCommandService`, `build_who_snapshot_response`, and `OperatorFeedbackLabeler`.

- [ ] **Step 5: Implement level checks and conditional summaries**

```python
def is_enabled_for(self, level: str) -> bool:
    normalized = _normalize_level(level)
    return _LOG_LEVELS[normalized] >= _LOG_LEVELS[self.level]
```

Use DEBUG for empty drain attempts/results and zero-count command successes. Keep INFO for nonzero processed/delivered/retrying counts. In runtime detection, compute `candidate_summaries` only when INFO is enabled.

- [ ] **Step 6: Run startup/logging/runtime tests and commit**

Run: `python3 -m pytest tests/test_startup.py tests/test_matrix_outbox_delivery.py tests/test_detection.py tests/test_matrix.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/__main__.py parking_spot_monitor/logging.py parking_spot_monitor/runtime_commands.py src/parking_monitor/matrix_outbox_delivery.py parking_spot_monitor/runtime_detection.py tests/test_startup.py tests/test_matrix_outbox_delivery.py tests/test_detection.py
git commit -m "perf: trim optional startup and no-op logs"
```

### Task 2: Generate Hash-Locked Runtime and Detector Manifests

**Files:**
- Create: `requirements-build.txt`
- Create: `requirements-runtime.lock`
- Create: `requirements-detector.lock`
- Create: `scripts/lock_dependencies.py`
- Create: `tests/test_dependency_locks.py`
- Modify: `docs/deployment.md`

**Interfaces:**
- Produces: `python3 scripts/lock_dependencies.py --check`
- Inputs remain: `requirements.txt`, `requirements-detector.txt`, and `pyproject.toml`

- [ ] **Step 1: Write a failing lock-contract test**

```python
@pytest.mark.parametrize(
    "path",
    [Path("requirements-runtime.lock"), Path("requirements-detector.lock")],
)
def test_lock_files_pin_and_hash_every_requirement(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert "autogenerated" in text.lower()
    requirement_blocks = [block for block in text.split("\n\n") if "==" in block]
    assert requirement_blocks
    assert all("--hash=sha256:" in block for block in requirement_blocks)

def test_lock_check_reports_current_manifests() -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/lock_dependencies.py", "--check"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
```

- [ ] **Step 2: Run the lock test and verify RED**

Run: `python3 -m pytest tests/test_dependency_locks.py -q`

Expected: FAIL because lock files and the check script do not exist.

- [ ] **Step 3: Add the pinned lock tool and deterministic generation script**

```text
pip-tools==7.5.0
```

```python
COMMANDS = (
    (
        "requirements.txt",
        "requirements-runtime.lock",
    ),
    (
        "requirements-detector.txt",
        "requirements-detector.lock",
    ),
)

def compile_lock(source: str, destination: str) -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "piptools.scripts.compile",
            "--generate-hashes",
            "--resolver=backtracking",
            "--strip-extras",
            f"--output-file={destination}",
            source,
        ],
        check=True,
    )
```

Before generation, hash the normalized bytes of `requirements.txt`, `requirements-detector.txt`, and the dependency sections of `pyproject.toml`. Prefix each generated lock with `f"# source-sha256: {digest}\n"`. For `--check`, recompute that digest and compare it to both lock headers without contacting package indexes. Do not overwrite tracked files in check mode.

- [ ] **Step 4: Generate both locks**

Run: `python3 -m pip install --user -r requirements-build.txt && python3 scripts/lock_dependencies.py`

Expected: both lock files contain exact versions and SHA-256 hashes; the detector lock retains the PyTorch CPU index directive.

- [ ] **Step 5: Run lock checks and commit**

Run: `python3 -m pytest tests/test_dependency_locks.py -q && python3 scripts/lock_dependencies.py --check`

Expected: PASS.

```bash
git add requirements-build.txt requirements-runtime.lock requirements-detector.lock scripts/lock_dependencies.py tests/test_dependency_locks.py docs/deployment.md
git commit -m "build: lock container dependencies"
```

### Task 3: Split and Cache Docker Build Stages

**Files:**
- Modify: `Dockerfile:1-43`
- Modify: `.dockerignore:1-14`
- Modify: `tests/test_docker_contract.py:45-100`
- Modify: `tests/test_deployment_docs.py`

**Interfaces:**
- Produces stages: `python-base`, `tooling`, `capture-base`, `runtime-app`, and `runtime-detector`
- Docker install inputs: `requirements-runtime.lock` and `requirements-detector.lock`

- [ ] **Step 1: Extend failing Docker contract tests**

```python
def test_dockerfile_uses_buildkit_cache_hash_locks_and_compileall() -> None:
    text = Path("Dockerfile").read_text(encoding="utf-8")
    assert text.startswith("# syntax=docker/dockerfile:1.7")
    assert "--mount=type=cache,target=/root/.cache/pip" in text
    assert "pip install --require-hashes -r requirements-runtime.lock" in text
    assert "pip install --require-hashes -r requirements-detector.lock" in text
    assert "python -m compileall -q /app/parking_spot_monitor /app/src" in text

def test_tooling_stage_does_not_install_capture_packages() -> None:
    text = Path("Dockerfile").read_text(encoding="utf-8")
    tooling = text.split(" AS tooling", 1)[1].split(" AS capture-base", 1)[0]
    assert "ffmpeg" not in tooling
    assert "intel-media-va-driver" not in tooling
```

- [ ] **Step 2: Run Docker contract tests and verify RED**

Run: `python3 -m pytest tests/test_docker_contract.py -k 'buildkit or tooling_stage' -q`

Expected: FAIL against the current single `runtime-base`.

- [ ] **Step 3: Implement stage boundaries and cache mounts**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS python-base
WORKDIR /app

COPY requirements-runtime.lock ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --require-hashes -r requirements-runtime.lock

FROM python-base AS tooling

FROM python-base AS capture-base
RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg intel-media-va-driver tzdata \
    && rm -rf /var/lib/apt/lists/*
```

Build runtime stages from `capture-base`. Copy source once per final stage and run compileall after the copy. Keep the digest, healthcheck, command, timezone, and VAAPI environment.

- [ ] **Step 4: Shrink build context**

Add `tests/`, `docs/superpowers/`, `.worktrees/`, `.superpowers/`, coverage, caches, and local model/data artifacts to `.dockerignore`. Do not exclude runtime package directories, lock files, `main.py`, or `config.yaml.example`.

- [ ] **Step 5: Render and build both relevant targets**

Run: `docker compose config -q && docker build --target tooling -t parking-spot-monitor:tooling-test . && docker build --target runtime-detector -t parking-spot-monitor:detector-test .`

Expected: all commands exit 0; tooling history has no FFmpeg/Intel driver install; detector healthcheck command exists.

- [ ] **Step 6: Run Docker tests and commit**

Run: `python3 -m pytest tests/test_docker_contract.py tests/test_deployment_docs.py -q`

Expected: PASS.

```bash
git add Dockerfile .dockerignore tests/test_docker_contract.py tests/test_deployment_docs.py
git commit -m "build: cache and split container stages"
```

### Task 4: Make Model Persistence the Documented Production Default

**Files:**
- Modify: `docker-compose.yml:12-16`
- Modify: `config.yaml.example`
- Modify: `.env.example`
- Modify: `docs/deployment.md`
- Modify: `README.md`
- Modify: `parking_spot_monitor/__main__.py`
- Modify: `tests/test_deployment_docs.py`
- Modify: `tests/test_startup.py`

**Interfaces:**
- Compose mount: `${MODEL_DIR:-./models}:/models:ro`
- Example detector path: `/models/yolov8n.pt`
- Legacy bare names such as `yolov8n.pt` retain existing Ultralytics resolution

- [ ] **Step 1: Write failing model deployment tests**

```python
def test_compose_mounts_read_only_model_directory() -> None:
    text = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "${MODEL_DIR:-./models}:/models:ro" in text

def test_explicit_missing_model_path_fails_before_runtime_loop(tmp_path: Path) -> None:
    missing = tmp_path / "models" / "yolov8n.pt"
    with pytest.raises(ConfigError, match="configured model file does not exist"):
        validate_model_path(str(missing))

def test_legacy_bare_model_name_does_not_require_local_file() -> None:
    validate_model_path("yolov8n.pt")
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_deployment_docs.py tests/test_startup.py -k 'model_directory or missing_model_path or bare_model' -q`

Expected: FAIL because the mount is commented and explicit paths are not preflighted.

- [ ] **Step 3: Implement explicit-path preflight**

Add `validate_model_path(model: str) -> None`. Treat values containing a directory component or beginning with `/` as explicit paths. Require `Path(model).is_file()` before detector construction. Keep a bare filename untouched.

- [ ] **Step 4: Update deployment defaults and checksum workflow**

Enable the read-only Compose mount, add `MODEL_DIR=./models` to `.env.example`, point example YAML to `/models/yolov8n.pt`, and document:

```bash
mkdir -p models
sha256sum models/yolov8n.pt
docker compose run --rm parking-spot-monitor python -m parking_spot_monitor --config /config/config.yaml --data-dir /data --validate-config
docker compose up -d --build
```

The documentation must tell operators to compare the checksum against the trusted artifact source; it must not embed an unverified checksum.

- [ ] **Step 5: Run docs/startup/Compose tests and commit**

Run: `python3 -m pytest tests/test_deployment_docs.py tests/test_startup.py -k 'model or deployment or config' -q && docker compose config -q`

Expected: PASS.

```bash
git add docker-compose.yml config.yaml.example .env.example docs/deployment.md README.md parking_spot_monitor/__main__.py tests/test_deployment_docs.py tests/test_startup.py
git commit -m "docs: persist production model weights"
```

### Task 5: Consolidate Script Helpers and Remove Confirmed Dead Code

**Files:**
- Modify: `scripts/verification_helpers.py`
- Modify: `scripts/run_docker_live_proof.py:401-430`
- Modify: `scripts/run_docker_alert_soak.py:367-388`
- Modify: `scripts/verify_m007_matrix_outbox_closeout.py:626-675`
- Modify: `parking_spot_monitor/matrix_alerts.py:215-223`
- Modify: `scripts/verify_live_proof.py:43-72`
- Modify: `tests/test_script_helpers.py`
- Modify: `tests/test_live_proof_operator_contract.py`
- Modify: `tests/test_alert_soak_runner.py`
- Modify: `tests/test_m007_matrix_outbox_closeout.py`

**Interfaces:**
- Adds: `scripts.verification_helpers.jpeg_check(path: Path) -> dict[str, Any]`
- Reuses: `smoke_env`, `redact_text`, `bounded_text`, `safe_output`, and `assert_no_forbidden_markers`
- Removes: unreferenced `_occupied_snapshot_body` and `run_live_proof_command`

- [ ] **Step 1: Add shared JPEG-helper tests**

```python
def test_jpeg_check_reports_valid_missing_and_corrupt(tmp_path: Path) -> None:
    valid = tmp_path / "valid.jpg"
    Image.new("RGB", (8, 6)).save(valid, "JPEG")
    corrupt = tmp_path / "corrupt.jpg"
    corrupt.write_bytes(b"bad")
    assert jpeg_check(valid)["valid_jpeg"] is True
    assert jpeg_check(tmp_path / "missing.jpg")["error_type"] == "missing"
    assert jpeg_check(corrupt)["valid_jpeg"] is False
```

- [ ] **Step 2: Run helper tests and verify RED**

Run: `python3 -m pytest tests/test_script_helpers.py -k jpeg_check -q`

Expected: FAIL because the shared helper does not exist.

- [ ] **Step 3: Move the identical implementation into the canonical module**

Add `jpeg_check` to `scripts/verification_helpers.py` and import it in both Docker scripts. Remove both local copies and unused Pillow imports.

- [ ] **Step 4: Replace M007 closeout duplicates with canonical helpers**

Import the five functions from `scripts.closeout_helpers`. Bind constants at call sites:

```python
rendered = safe_output(
    completed.stdout,
    completed.stderr,
    patterns=SENSITIVE_PATTERNS,
    limit=OUTPUT_LIMIT,
)
assert_no_forbidden_markers(rendered, FORBIDDEN_OUTPUT_MARKERS)
```

Use `smoke_env(rtsp_placeholder=PLACEHOLDER_RTSP_URL, matrix_token_placeholder=PLACEHOLDER_MATRIX_TOKEN, base=base, pythonpath_prefix=str(ROOT / "src"))`. Delete the five duplicate helper bodies.

- [ ] **Step 5: Remove the two confirmed unreferenced functions**

Run: `rg -n '_occupied_snapshot_body|run_live_proof_command' . --glob '!docs/superpowers/**'`

Expected before deletion: definitions only. Delete both definitions and now-unused imports. Run the same command again; expected output is empty.

- [ ] **Step 6: Run script/closeout tests and commit**

Run: `python3 -m pytest tests/test_script_helpers.py tests/test_live_proof_operator_contract.py tests/test_alert_soak_runner.py tests/test_m007_matrix_outbox_closeout.py tests/test_matrix.py -q`

Expected: PASS.

```bash
git add scripts/verification_helpers.py scripts/run_docker_live_proof.py scripts/run_docker_alert_soak.py scripts/verify_m007_matrix_outbox_closeout.py parking_spot_monitor/matrix_alerts.py scripts/verify_live_proof.py tests/test_script_helpers.py tests/test_live_proof_operator_contract.py tests/test_alert_soak_runner.py tests/test_m007_matrix_outbox_closeout.py
git commit -m "refactor: consolidate verification helpers"
```

### Task 6: Verify Slice 4 and the Complete Program

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `docs/deployment.md`

**Interfaces:**
- Verifies all four implementation plans and documents final operator controls

- [ ] **Step 1: Document final configuration and rollback controls**

Add the exact Matrix timing keys, capture ceilings, one-job lab behavior, model mount, dependency-lock regeneration command, and rollback values to deployment docs and changelog. Do not claim measured improvement until Step 5 records it.

- [ ] **Step 2: Run the complete Python suite and compile checks**

Run: `python3 -m pytest -q && python3 -m compileall -q parking_spot_monitor src scripts tests && git diff --check`

Expected: all commands exit 0.

Run: `python3 -c 'from pathlib import Path; paths=[*Path("parking_spot_monitor").glob("*.py"),*Path("src/parking_monitor").glob("*.py")]; oversized=[(str(path),len(path.read_text(encoding="utf-8").splitlines())) for path in paths if len(path.read_text(encoding="utf-8").splitlines()) >= 1000]; assert not oversized, oversized'`

Expected: no production module reaches 1,000 lines.

- [ ] **Step 3: Validate and build the deployment image**

Run: `docker compose config -q && docker compose build --pull parking-spot-monitor && docker compose up -d --force-recreate && docker compose ps`

Expected: Compose renders, the detector image builds, and the service reaches healthy state.

- [ ] **Step 4: Run smoke and health checks**

Run: `docker compose exec -T parking-spot-monitor python -m parking_spot_monitor.healthcheck --health-file /data/health.json --max-age-seconds 120`

Expected: exit 0 with a safe health result.

- [ ] **Step 5: Capture before/after resource evidence**

Record `docker stats --no-stream`, container thread count, outbox size, health-snapshot duration, daily INFO log projection, and a Matrix failure/cooldown trace. Compare against the redaction-safe pre-implementation artifact captured in Slice 1 Task 1 Step 0; label unavailable measurements explicitly instead of estimating them. Summarize the measured comparison in `docs/deployment.md` without committing secrets or raw camera data.

- [ ] **Step 6: Commit final docs**

```bash
git add CHANGELOG.md docs/deployment.md
git commit -m "docs: document resource hardening rollout"
```
