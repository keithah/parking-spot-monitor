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

Strict success requires all of the following before R003/R015 can be validated: `LIVE_RTSP_CAPTURE_OK`, `LIVE_MATRIX_TEXT_OK`, and `LIVE_MATRIX_IMAGE_OK` are present; skip/failure markers are absent; `data/latest.jpg` is a valid raw camera JPEG; at least one `data/snapshots/live-proof-*.jpg` JPEG is retained; Matrix room readback verifies both visibly labelled `LIVE PROOF / TEST MESSAGE` text and `LIVE PROOF / TEST IMAGE` image evidence in the target room; and redaction scans find zero RTSP URLs, Authorization/Bearer headers, Matrix access-token strings, raw Matrix response bodies, tracebacks, or image bytes in logs/reports. Do not use `--skip-readback` for validation: skipped or unavailable readback leaves R003/R015 remain unvalidated because send responses alone do not prove room-visible Matrix delivery.

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

Publication-safety rules are the same as the finite live proof and calibration workflows, but the alert-soak artifact boundary is specific. Keep `data/alert-soak-result.json`, `data/alert-soak-evidence.md`, `data/alert-soak-docker.stdout.log`, `data/alert-soak-docker.stderr.log`, raw snapshots under `data/snapshots/`, `data/latest.jpg`, `data/health.json`, and `data/state.json` local and ignored until reviewed. The JSON and Markdown evidence may summarize safe fields such as status, phase, safe Docker argv, per-spot alert/readback status, duplicate counts, artifact validity counts, health/state parse summaries, and redaction-scan counts. They must not include RTSP URLs, Matrix access tokens, Authorization/Bearer headers, raw Matrix response bodies, tracebacks, raw image bytes, or unredacted Docker output.

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

Replay reports are designed to be publication-safe text artifacts. The JSON and Markdown report builders fail closed on RTSP URLs, Matrix tokens, Authorization/Bearer headers, raw Matrix response markers, tracebacks, and image-byte-looking content. If malformed labels, missing bundle metadata, or sparse evidence appear, treat the resulting blocked/not-covered/inconclusive report as a gap to fix rather than as validation success.

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

The configured street-sweeping quiet window is `street_sweeping`: the first and third Monday of each month in `America/Los_Angeles`, from `13:00` to `15:00`. Quiet-window boundary changes emit restart-safe `quiet-window-started` and `quiet-window-ended` event objects. S06 will send Matrix messages from these S05 event objects; it should consume `occupancy-open-event` for open-spot alerts and quiet-window notice objects for schedule notices rather than re-inferring transitions from logs.

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
