# Matrix Alert Outbox Contract

The local Matrix delivery outbox is wired into the default runtime open-spot Matrix delivery path and exposed through safe operator visibility surfaces. An open-spot alert is persisted before any Matrix network I/O, retryable failures survive process restart in `<data-dir>/matrix-outbox.json`, and later drains skip already delivered phases before retrying the remaining work.

The persistence boundary lives in `parking_monitor.outbox`; the runtime open-alert executor lives in `parking_monitor.matrix_outbox_delivery` and is created by the default runtime Matrix delivery factory. Runtime health JSON and Matrix cockpit replies consume the redacted `LocalOutbox.status_summary()` shape; operators should use those summaries before inspecting the raw outbox file.

## Runtime operator contract

### File location

At runtime the outbox file is resolved from the effective data directory:

- CLI default: `/data/matrix-outbox.json`
- CLI override: `--data-dir /path/to/data` writes `/path/to/data/matrix-outbox.json`
- Tests and embedded callers that inject their own Matrix delivery factory are responsible for their own outbox path, if any.

`parking_spot_monitor.paths.resolve_runtime_paths()` exposes this as `RuntimePaths.matrix_outbox_file`. The file is a single JSON document written by temp-file plus `os.replace`, so successful updates replace the previous durable payload atomically.

### Which Matrix events are outbox-backed in S02

Only `occupancy-open-event` alerts sent through the default runtime Matrix delivery factory are backed by the durable outbox in S02.

The following Matrix paths remain direct delivery paths for now:

- quiet-window notices;
- owner-vehicle quiet-window alerts;
- occupied-spot alerts;
- live-proof delivery;
- tests or integrations that inject a custom `matrix_delivery_factory` without a `drain_outbox()` method.

The runtime loop detects outbox-capable deliveries with duck-typed outbox hooks. A delivery object with `enqueue_open_spot_alert` lets frame dispatch persist an open alert without immediately draining Matrix network work. A delivery object with `drain_outbox` is drained at runtime startup and at the beginning of each capture-loop iteration. Legacy or test Matrix delivery fakes that do not expose those methods are ignored.

### Open-alert phase order and idempotency

Each open alert is represented by one outbox record with three phases:

1. `text` — sends the Matrix text event.
2. `upload` — prepares/copies the retained JPEG snapshot and uploads it to Matrix media.
3. `image` — sends the Matrix image event using the uploaded `content_uri`.

`enqueue_open_spot_alert(event)` enqueues the sanitized alert intent and predeclares all three phases without performing Matrix network I/O. The runtime frame dispatch path uses this enqueue-only method so frame processing does not block on Matrix text, upload, or image delivery.

`send_open_spot_alert(event)` remains available as a direct helper for callers that want enqueue-and-drain behavior in one call. That helper enqueues the same three-phase record and then drains phases in `text`, `upload`, `image` order.

Already delivered phases are skipped on later drains. This prevents a successful text phase from being sent again if upload fails, and prevents a successful upload from being repeated if image send fails after a restart. Upload success stores secret-safe phase result metadata needed by the image phase, including the Matrix `content_uri`, snapshot body, filename, and image info.

Open-alert phase transaction IDs are derived from the stable open event ID plus phase suffix:

- `<open-event-id>:text`
- `<open-event-id>:image`

The base `OutboxRecord.matrix_transaction_id` remains available for stable whole-record diagnostics. The runtime phase sender currently uses the phase-specific transaction IDs above for Matrix idempotency.

### Retry and drain semantics

Runtime drain points:

- once at capture-loop startup, before the first frame is captured;
- once at the beginning of every capture-loop iteration;
- direct `send_open_spot_alert(event)` calls outside the frame dispatch path.

`MatrixOutboxDelivery.drain_outbox()` selects records in `pending` or `retrying` state. Terminal `delivered`, `failed`, and `dead_lettered` records are not re-sent.

On a retryable phase exception, the executor:

1. normalizes the failure to a safe reason code such as `matrix_text_timeout`, `matrix_upload_timeout`, or `matrix_image_timeout`;
2. marks the whole record `retrying`;
3. leaves the failed phase pending;
4. returns without attempting later phases.

A later successful drain resumes from the first non-delivered phase and marks the record `delivered` only after all three phases are delivered.

Startup or iteration drains that still have retrying records are reflected as degraded Matrix health context with `error_type: retrying_records`; the durable outbox remains the source of truth for the pending work.

### Operator visibility surfaces

The primary inspection workflow is read-only and secret-safe:

1. Read runtime health JSON from the configured `settings.runtime.health_file` or the default `<data-dir>/health.json`.
2. Use the Matrix cockpit `status` reply for a compact operator summary.
3. Use the Matrix cockpit `confidence` reply when diagnosing Matrix delivery alongside decision-memory and timeline health.
4. Fall back to `LocalOutbox(path).status_summary()` only when health and cockpit surfaces are unavailable. Avoid asking operators to paste Matrix tokens, camera URLs, raw JSON payloads, or exception traces into chat.

#### Health JSON: `matrix_outbox`

Every runtime-loop health write includes `matrix_outbox` when the resolved outbox path is configured. The field is produced by `LocalOutbox.status_summary()` and then redacted again by `HealthStatus.to_json_dict()`. A healthy read adds `available: true` to the summary. If the outbox cannot be summarized, health still writes and reports:

```json
{
  "matrix_outbox": {
    "available": false,
    "phase": "matrix-outbox",
    "error": {
      "phase": "matrix-outbox",
      "action": "status-summary",
      "error_type": "...",
      "error_message": "matrix outbox status unavailable"
    }
  }
}
```

For available summaries, operators can rely on these safe fields:

- `total` and `counts_by_state` for `pending`, `retrying`, `delivered`, `failed`, and `dead_lettered` records;
- `retry_reason_counts` and `dead_letter_reason_counts` with normalized or `redacted` reason codes;
- bounded `items` with stable record IDs, Matrix transaction IDs, whole-record state, top-level phase, timestamps, retry/dead-letter reasons, and per-phase names/states/timestamps;
- safe phase result metadata, such as Matrix event IDs, media `content_uri`, MIME type, dimensions, filename, and snapshot body when those values passed persistence redaction;
- `recovery` metadata with recovered/quarantined counts, reason-code counts, and quarantine artifact names/paths that have passed redaction.

The health surface is diagnostic only. Reading `matrix_outbox` does not drain, retry, dead-letter, upload media, send Matrix messages, mutate runtime state, or touch camera/detector paths.

#### Matrix cockpit status and confidence replies

`format_operator_status_reply(...)` appends a `Matrix outbox:` section to the Matrix status response when the command context includes `matrix_outbox_path`. `format_operator_confidence_reply(...)` appends the same outbox summary lines under `Matrix delivery:`. The runtime command service passes the resolved `<data-dir>/matrix-outbox.json` path through `MatrixOperatorCockpitContext`, so these replies inspect the same durable file as runtime delivery.

The cockpit replies intentionally render aggregates instead of raw records:

- empty or missing file: `outbox empty`;
- populated file: `outbox total N: pending A, retrying B, delivered C, failed D, dead-lettered E`;
- phase aggregate: `phase states: text pending 1; upload delivered 1; image failed 1`;
- retry and dead-letter summaries: `retry reasons: redacted 1` and `dead-letter reasons: matrix_forbidden 1`;
- bounded record snippets: `record: state retrying; phase upload; retry redacted; phases text=delivered, upload=pending, image=pending`;
- truncated long lists: `records truncated: showing X of Y`;
- corrupt or recovered artifacts: `recovery: recovered 0; quarantined 1; reasons invalid_json 1`;
- unconfigured path: `outbox status unavailable (path not configured)`;
- unexpected summary failure: `outbox status unavailable (<error type>)`.

Status and confidence replies are bounded to the Matrix reply byte limit, omit file-system paths, omit alert bodies, and run without detector, camera, media upload, alert emission, or state mutation.

#### S04/S05 operator recovery and closeout evidence

Use this recovery order for a suspected Matrix outbox delivery, retention, dead-letter, or quarantine issue:

1. Inspect `<data-dir>/health.json` first, especially `matrix_outbox.available`, `matrix_outbox.counts_by_state`, `matrix_outbox.retry_reason_counts`, and the per-item `phases` list. This is the safest first-read surface because it is already redacted and does not retry delivery.
2. Ask the cockpit for `!parking status`, then `!parking confidence` when you need a compact Matrix-delivered operator view. Status shows the `Matrix outbox:` aggregate; confidence repeats Matrix delivery context alongside timeline and decision-memory health so an operator can separate delivery failures from detector/camera uncertainty.
3. Fall back to raw outbox inspection only when health and cockpit summaries are unavailable: read `<data-dir>/matrix-outbox.json` with `python -m json.tool` or inspect `LocalOutbox(path).status_summary()`. Do not paste raw JSON, Matrix tokens, camera URLs, room-private responses, or exception traces into chat or tickets.

Interpret recovery states conservatively:

- `pending` means delivery work is queued and has not completed all phases.
- `retrying` means a retryable Matrix phase failed; use `retry_reason_counts` and the first non-delivered phase to localize whether `text`, `upload`, or `image` is waiting.
- `failed` and `dead_lettered` are terminal inspection states and are not automatically re-sent by normal drains. A `dead_lettered` record means a permanent Matrix failure such as a forbidden upload was classified into bounded evidence; use `dead_letter_reason_counts` and per-item `dead_letter_reason` to understand the safe reason code, and do not expect restart or normal drains to retry it.
- Corrupt outbox JSON or malformed persisted records are quarantined beside the outbox in a hidden sibling quarantine directory such as `.<outbox-stem>-quarantine/`; health and cockpit summaries expose only recovery counts, safe reason codes such as `invalid_json`, and bounded quarantine artifact metadata, not the corrupt payload.
- Retained open-alert snapshots referenced by `pending` or `retrying` outbox records are protected from snapshot pruning, so delayed upload should use the original retained event evidence rather than a later `latest.jpg`.
- Phase state `text=delivered, upload=pending, image=pending` after a Matrix upload failure means the next drain should resume at upload, not resend text.
- Phase state `text=delivered, upload=delivered, image=pending` means the upload result metadata survived and the next drain should resume at image send.

A normal container restart is safe for retryable outbox records. The runtime drains the same durable `<data-dir>/matrix-outbox.json` at startup and every capture-loop iteration, so a fresh container should resume from the first non-delivered phase. Duplicate suppression depends on the persisted phase states and stable transaction IDs: after `text` succeeds, later drains skip text; after `upload` succeeds, later drains reuse the persisted upload result and skip upload before sending the image phase. Operators should treat repeated text sends for the same open event after a persisted text success as a duplicate-suppression regression.

The M007 Matrix outbox closeout smoke is the finite container-local proof for this behavior:

```sh
python scripts/verify_m007_matrix_outbox_closeout.py
```

Expected closeout markers are `M007_CLOSEOUT_START`, `M007_CLOSEOUT_PASS`, `M007_CLOSEOUT_FAIL`, `M007_CLOSEOUT_RESULT`, `M007_OUTBOX_FAILURE_OK`, `M007_OUTBOX_HEALTH_OK`, `M007_OUTBOX_RECOVERY_OK`, `M007_OUTBOX_DEAD_LETTER_OK`, `M007_OUTBOX_QUARANTINE_OK`, and `M007_OUTBOX_RETENTION_OK`. The failure phase proves a retrying outbox with `counts_by_state=retrying:1`, `retry_reason=matrix_upload_timeout`, and `phases=text:delivered,upload:pending,image:pending`. The health phase proves the retrying outbox is visible through the redacted `matrix_outbox` payload. The recovery phase uses a fresh container against the same mounted `/data` directory and proves `state=delivered` with `skipped=text called=upload,image`. The dead-letter phase proves `state=dead_lettered`, `dead_letter_reason=matrix_upload_http_403`, and `later_attempted=0` for a permanent upload failure. The quarantine phase proves corrupt persisted JSON reports `reason=invalid_json` and `quarantined_count=1` without leaking corrupt bytes. The retention phase proves delayed delivery uploads the retained original snapshot with `retained_original_upload=true`, prunes unrelated stale snapshots with `stale_pruned=true`, and finishes `state=delivered`.

The smoke is intentionally secret-safe and bounded. It injects placeholder `RTSP_URL` and `MATRIX_ACCESS_TOKEN` values for config validation, builds a local Docker image, runs short container snippets against test-only temp `/data`, writes test-only `matrix-outbox.json` and `health.json`, and verifies redacted diagnostics. It does not touch live RTSP, live Matrix sync or delivery, detector/model inference, cloud services, real operator credentials, or real retained runtime artifacts. Its fake Matrix clients are in-container test doubles only; success proves the outbox persistence/recovery contract, not a room-visible Matrix delivery or live camera path.

#### Safe diagnostics and logs

`status_summary()` remains the shared schema behind health and cockpit visibility. It includes only secret-safe diagnostic metadata: path, schema version, counts by state, retry/dead-letter reason counts, timestamps, item IDs, Matrix transaction IDs, per-phase states, safe phase result metadata, and recovery/quarantine summaries. It does not include raw access tokens, bearer headers, RTSP credentials, raw image bytes, tracebacks, cookies, passwords, or other secret-shaped values.

Structured outbox log events include:

- `matrix-outbox-enqueued`
- `matrix-outbox-runtime-drain-attempt`
- `matrix-outbox-runtime-drain-succeeded`
- `matrix-outbox-runtime-drain-failed`
- `matrix-outbox-drain-started`
- `matrix-outbox-drain-finished`
- `matrix-outbox-phase-attempt`
- `matrix-outbox-phase-succeeded`
- `matrix-outbox-phase-skip`
- `matrix-outbox-phase-retryable-failure`
- `matrix-outbox-snapshot-prepared`
- `matrix-outbox-record-delivered`

Failure log records use normalized reason codes and error types rather than raw exception text.

## Public API

Import through the configured `src` package layout:

```python
from parking_monitor.outbox import AlertIntent, LocalOutbox

outbox = LocalOutbox("var/matrix-outbox.json")
record = outbox.enqueue(
    AlertIntent(
        event_id="camera-1:1700000000:occupied",
        phase="text",
        room_id="!room:example.org",
        body="Parking spot occupied",
        metadata={"camera_id": "camera-1", "occupied": True},
    )
)
```

Key surfaces:

- `AlertIntent(event_id, phase, body, room_id=None, metadata={})` validates the caller-provided alert intent before persistence.
- `LocalOutbox(path, max_records=1000, max_terminal_age_seconds=None)` loads or creates the durable JSON file.
- `enqueue(intent)` stores a pending item atomically and returns the existing item when the same logical alert is enqueued again.
- `list_records(state=None)` and `list_pending()` return in-memory `OutboxRecord` snapshots for drainers.
- `mark_retrying(record_id, reason=...)`, `mark_delivered(record_id)`, `mark_failed(record_id, reason=...)`, and `mark_dead_lettered(record_id, reason=...)` persist whole-record state transitions.
- `ensure_phase_pending(record_id, phase)` idempotently adds a required phase to a retryable record.
- `mark_phase_delivered(record_id, phase, result=None)` and `mark_phase_failed(record_id, phase, reason=...)` persist phase-level delivery progress. Delivered phases may include optional secret-safe JSON result metadata such as a Matrix upload `content_uri`, image dimensions, MIME type, or Matrix event ID.
- `status_summary()` returns secret-safe diagnostic metadata for operators and future agents.

## File layout

The outbox file is a single JSON document:

```json
{
  "schema_version": 1,
  "items": [
    {
      "id": "outbox_<stable digest>",
      "matrix_transaction_id": "psm_<stable digest>",
      "intent": {"event_id": "...", "phase": "text", "body": "..."},
      "state": "pending",
      "created_at": "...Z",
      "updated_at": "...Z",
      "phases": [
        {"phase": "text", "state": "pending", "updated_at": "...Z"},
        {
          "phase": "upload",
          "state": "delivered",
          "updated_at": "...Z",
          "result": {"content_uri": "mxc://example.org/media", "width": 640, "height": 480}
        }
      ]
    }
  ]
}
```

Corrupt payloads are copied to a sibling quarantine directory named `.<outbox-stem>-quarantine/`, for example `.matrix-outbox-quarantine/`. Quarantine files are deterministic by reason and content digest and are bounded to the most recent 20 files.

## States and phases

Whole-record states:

- `pending`: newly enqueued and ready for delivery.
- `retrying`: delivery hit a retryable failure; `retry_reason` contains a normalized reason code.
- `delivered`: terminal success.
- `failed`: terminal non-retryable failure.
- `dead_lettered`: permanent failure retained for inspection; `dead_letter_reason` contains a normalized reason code.

Phase names are `text`, `upload`, and `image`. Phase states are `pending`, `delivered`, and `failed`. Marking the only pending phase delivered promotes the whole record to `delivered`; a predeclared multi-phase open-alert record is promoted only after all phases are delivered. Marking a phase failed dead-letters the whole record when using `mark_phase_failed`.

Delivered phases can carry an optional `result` object. This object is sanitized with the same secret filters as alert intent metadata and is intended only for safe retry inputs that must survive restart, for example Matrix event IDs, a Matrix media upload `content_uri`, image dimensions, MIME type, filename, and snapshot body. Empty result fields are dropped. Secret-shaped keys or values, raw image bytes, RTSP URLs, authorization/bearer strings, exception text, tracebacks, cookies, passwords, and token-like fields are rejected before persistence. Reloading older records where `result` is absent remains valid and exposes an empty `phase_results` mapping in memory.

Phase result updates are idempotent. Re-marking a delivered phase with no result or the same result returns the existing record without rewriting it. A delivered phase result cannot be overwritten with different metadata; callers should persist the upload result once and then reuse it when retrying later phases.

Terminal records cannot be retried, except a `failed` record may be moved to `dead_lettered` to attach explicit dead-letter metadata.

## Idempotency and Matrix transaction IDs

`enqueue()` is idempotent for the same sanitized alert intent. Both the outbox item ID and base Matrix transaction ID are deterministic SHA-256 digests of the sanitized intent JSON with sorted keys:

- Outbox item IDs use `outbox_` plus the first 32 hex characters.
- Base Matrix transaction IDs use `psm_` plus the first 48 hex characters.

Empty optional fields are removed before hashing, so omitting `room_id` is equivalent to passing an empty `room_id`. Runtime open-alert delivery also uses stable phase transaction IDs based on `open_spot_event_id(event)` and the phase name so Matrix retries are idempotent per sent event.

## Redaction and persistence guarantees

The outbox rejects secret-bearing data before writing JSON. It must not persist Matrix access tokens, authorization or bearer headers, raw RTSP URLs, image bytes, exception text, tracebacks, cookies, passwords, or fields whose names imply secrets.

Failure reasons are stored only as normalized reason codes. If a reason string looks secret-bearing, it is stored and summarized as `redacted`.

`status_summary()` is the safe inspection surface. It includes:

- counts by state;
- retry and dead-letter reason-code counts;
- oldest and newest timestamps;
- stable item IDs and Matrix transaction IDs;
- phase names, phase states, phase timestamps, and optional safe phase result metadata;
- recovery counts, reason codes, and quarantine paths.

It does not include raw corrupt payloads, access tokens, RTSP URLs, bearer headers, or image bytes.

## Failure modes surfaced to callers

- Corrupt JSON, unsupported schema versions, invalid top-level shapes, and malformed individual records do not crash startup. They are quarantined and surfaced through `LocalOutbox.recovery` and `status_summary()["recovery"]`.
- Write or replace failures raise `OutboxPersistenceError` with a generic secret-safe message. Prior durable files are preserved when a replacement write fails.
- Retryable runtime Matrix failures are recorded with `mark_retrying(...)` and retried on a later drain.
- Permanent delivery failures should be recorded with `mark_failed(...)` or `mark_dead_lettered(...)`; operators can inspect reason-code counts and per-item metadata through `status_summary()`.
- Over-retention is handled by `OutboxRetentionPolicy`: terminal records are pruned before retryable records when `max_records` is exceeded, and old terminal records can be pruned with `max_terminal_age_seconds`.
