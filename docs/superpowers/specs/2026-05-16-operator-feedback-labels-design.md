# Operator Feedback Labels and Live Who Snapshot Design

## Status

Approved for design by the operator on 2026-05-16. This document is design-only; implementation requires a separate plan.

## Problem

The monitor can send a Matrix notice that reports a spot as `occupied` or `open` when the operator can see the opposite. The most common correction the operator wants to make is: “this spot was reported as occupied/open, but the actual state was open/occupied.” Today that feedback is not captured as a first-class training or replay input.

The operator also wants `!parking who` to show a live current snapshot in addition to the active vehicle/session summary, so the command can be used as a quick visual check before or after reporting a correction.

## Goals

- Add a fast Matrix feedback command for authorized operators:
  - `!parking correct <spot_id> <open|occupied>`
- Persist corrections as structured local labels suitable for later training, replay, and tuning workflows.
- Link each correction to the most recent alert snapshot for that spot when the retained evidence exists.
- Keep corrections separate from live occupancy state; feedback must not mutate the monitor’s current state machine.
- Extend `!parking who` so it attempts one bounded fresh camera capture and replies with the active-session summary plus the newly captured image when available.
- Preserve the existing safety posture: no arbitrary paths from Matrix, no secrets in logs or labels, no image bytes in JSON/text, and no automatic threshold/model changes.

## Non-goals

- Do not auto-train a model from Matrix commands.
- Do not automatically change detector thresholds, polygons, occupancy state, quiet-window markers, or owner-vehicle profiles.
- Do not add per-spot runtime threshold schema.
- Do not expose arbitrary local files, bundle paths, labels paths, camera URLs, or shell commands through Matrix text.
- Do not rely on raw Matrix response bodies or Matrix client reply formatting for the primary correction flow.

## Existing system context

The project already has an authorized Matrix cockpit command surface behind `matrix.command_authorized_senders`. Existing commands include read-only operator inspection commands such as `!parking status`, `!parking config`, `!parking latest`, `!parking why <spot_id>`, and `!parking recent`, plus vehicle-history mutation commands such as `!parking who`, `!parking owner <spot_id>`, and `!parking wrong <spot_id|session_id>`.

The runtime already persists bounded operator decision memory in `data/operator-decision-memory.json`. That memory is useful for `why` and `recent`, but it is an explanatory timeline, not a durable training-label artifact. Detection-lab workflows already consume fixed local labels/configs under `data/detection-lab/`, so correction labels should be persisted in a deliberate local artifact that future lab/replay tooling can review or import.

## Approach

Use a simple correction command as the primary operator path:

```text
!parking correct left_spot open
!parking correct right_spot occupied
```

The command means: “the monitor’s most recent report for this spot was wrong; the actual state is the provided state.” The system finds the most recent alert/decision record for that spot, infers the reported state, and records a correction label.

This approach is preferred over reply-to-alert parsing because it is faster to type, works consistently across Matrix clients, and fits the current command parser. Reply-to-alert support can be added later if exact Matrix-event linkage becomes necessary.

## Operator UX

### Correction success with linked evidence

```text
Parking correction recorded
- spot: left_spot
- reported: occupied
- actual: open
- linked evidence: retained alert snapshot
- next: run !parking lab run replay after labels are reviewed
```

### Correction success without retained image evidence

```text
Parking correction recorded
- spot: left_spot
- reported: occupied
- actual: open
- linked evidence: unavailable; alert snapshot was not retained
- next: run !parking lab run replay after labels are reviewed
```

### No recent alert for the spot

The command should not invent a reported state. It should either reject the correction or record it as an unlinked operator label with clear wording. The safer default is to reject until a recent report exists:

```text
Parking correction not recorded
No recent alert was found for left_spot; use !parking latest or !parking who to inspect current evidence.
```

### Invalid input

Invalid spot IDs, invalid states, extra arguments, and unauthorized senders should follow existing command validation and authorization patterns. Invalid input records no feedback label.

## Feedback label artifact

Add a dedicated bounded local artifact:

```text
data/operator-feedback-labels.json
```

This artifact is separate from `operator-decision-memory.json`. Decision memory may receive a compact `command_outcome` entry for operator visibility, but training/replay data should come from the feedback labels artifact.

Suggested schema shape:

```json
{
  "schema_version": 1,
  "labels": [
    {
      "label_id": "feedback-20260516T174239Z-left_spot-abc12345",
      "spot_id": "left_spot",
      "reported_state": "occupied",
      "actual_state": "open",
      "source": "matrix_command",
      "operator_sender_hash": "sha256:...",
      "corrected_at": "2026-05-16T17:42:39Z",
      "reported_at": "2026-05-15T21:42:39Z",
      "alert_event_type": "occupancy-occupied-event",
      "alert_event_id": "...",
      "evidence": {
        "kind": "alert_snapshot",
        "path": "snapshots/occupancy-occupied-event-left_spot-...jpg",
        "available": true,
        "validated_jpeg": true,
        "width": 1920,
        "height": 1080,
        "byte_size": 123456
      }
    }
  ]
}
```

Implementation details may adjust field names, but the artifact must remain schema-versioned, bounded, redacted, and safe to inspect locally.

## Evidence linkage

When a correction is recorded, the service should:

1. Load bounded decision memory.
2. Find the most recent alert record for the requested spot.
3. Infer `reported_state` from the alert event type:
   - `occupancy-occupied-event` means `occupied`.
   - `occupancy-open-event` means `open`.
4. Copy only safe metadata from the alert record, such as observed time, event type, event ID, and retained snapshot path if present.
5. Validate the linked snapshot if it is still retained and local:
   - path must stay under the effective data directory or configured snapshots directory;
   - file must exist;
   - file must be a JPEG;
   - file must be under configured size bounds;
   - JSON/text must store metadata only, never image bytes.
6. Persist the label atomically.
7. Append a compact decision-memory `command_outcome` record so `!parking recent` shows that feedback was captured.

If the retained snapshot is unavailable, the correction can still be useful but must clearly record weaker evidence quality.

## `!parking who` fresh snapshot behavior

The existing `!parking who` command should keep its active-session summary and add an on-demand current image. The operator selected fresh capture on demand rather than reusing `latest.jpg`.

When an authorized sender runs:

```text
!parking who
```

the command should:

1. Build the existing active spot/session summary.
2. Attempt one bounded fresh camera capture.
3. Validate the captured JPEG.
4. Upload that image with the Matrix reply when available.
5. Include freshness and failure metadata in the text.

Example success reply:

```text
Parking monitor who
Snapshot: fresh capture at 2026-05-16 10:42:12 AM PDT

- left_spot: occupied; session sess-abc123; vehicle unknown; confidence 0.82
- right_spot: open; no active vehicle session
```

Example failure reply:

```text
Parking monitor who
Snapshot: fresh capture unavailable (capture_timeout); no live state was changed.

- left_spot: occupied; session sess-abc123; vehicle unknown; confidence 0.82
- right_spot: open; no active vehicle session
```

The fresh capture must be bounded and read-like: one attempt, short timeout, safe diagnostics, and no detector/model inference. It must not mutate occupancy state, quiet-window markers, owner-vehicle records, or threshold/config state. If existing health or command diagnostics already track command outcomes, those may record sanitized command success/failure metadata.

## Component boundaries

### Matrix parser and command service

- Parse `!parking correct <spot_id> <open|occupied>`.
- Keep the command authorized under `matrix.command_authorized_senders`.
- Route corrections through a feedback-label service rather than writing files directly in parser code.
- Extend `!parking who` response shape to allow optional image evidence.

### Feedback label store

- Own `operator-feedback-labels.json` schema, loading, validation, redaction, atomic append, and retention bounds.
- Reject unsafe fields and unsupported schema versions.
- Never store raw image bytes, raw Matrix bodies, secrets, tracebacks, RTSP URLs, access tokens, or Authorization headers.

### Alert evidence resolver

- Reads bounded decision memory.
- Locates the latest alert for a spot.
- Converts alert event type to reported state.
- Validates local snapshot metadata without exposing arbitrary filesystem paths.

### Fresh capture provider for `!parking who`

- Encapsulates one-shot capture so Matrix command code does not directly know camera details.
- Returns either validated image metadata/path or a sanitized failure reason.
- Does not run detection, update state, or trigger alert delivery.

### Detection lab integration

Initial implementation should only collect labels. A follow-up can add an explicit review/import path from `operator-feedback-labels.json` into the existing detection-lab labels workflow. The lab should continue to be evidence-gated and must not auto-apply tuning changes.

## Error handling and observability

- Correction write failures should reply with a safe failure reason such as `feedback_store_unavailable` and log a structured sanitized event.
- Missing decision memory should produce a clear “not recorded” response rather than guessing.
- Snapshot validation failures should still allow label persistence if a recent alert exists, but evidence quality must be `unavailable` or `invalid`.
- `!parking who` capture failures should not fail the whole command; the text summary should still be sent.
- All new logs must use existing structured logger redaction and avoid secrets, raw tracebacks, raw Matrix bodies, and image bytes.

## Testing strategy

- Parser tests:
  - accepts `!parking correct left_spot open` and `!parking correct right_spot occupied`;
  - rejects unsupported states, missing args, extra args, and invalid spot IDs.
- Feedback store tests:
  - appends valid labels atomically;
  - bounds retention;
  - rejects/quarantines malformed or oversized artifacts;
  - redacts unsafe text.
- Evidence resolver tests:
  - finds latest alert per spot;
  - infers reported state from open vs occupied event records;
  - handles missing/invalid/pruned snapshots safely.
- Matrix command service tests:
  - authorized correction records a label and sends confirmation;
  - unauthorized correction is denied and records nothing;
  - no recent alert returns a no-record response;
  - command outcome appears in decision memory when appropriate.
- `!parking who` tests:
  - success path includes existing active-session summary plus image response metadata;
  - capture failure sends text summary with safe unavailable reason;
  - capture provider is called once and does not run detection or mutate live occupancy state.
- Documentation tests:
  - README/operator docs include the new correction command and updated `!parking who` image behavior.
- Full suite:
  - run `python -m pytest` before completion.

## Open follow-up

After implementation, add an explicit reviewed import path from `operator-feedback-labels.json` to the detection-lab replay labels. That path should preserve operator review and evidence quality rather than silently treating every correction as production-ready training data.
