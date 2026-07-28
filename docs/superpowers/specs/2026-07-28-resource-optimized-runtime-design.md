# Resource-Optimized Runtime Design

## Purpose

Reduce steady-state CPU, filesystem traffic, storage growth, and flash wear without weakening occupancy decisions while a parking spot is changing. Every resource policy introduced by this design must be configurable so operators can restore the current 15-second behavior without a code change.

## Measured Baseline

The live container uses about 395 MiB of memory and bursts above one CPU core during inference. It reported 65.4 GB of block writes after four days. The runtime data directory is about 2.0 GB, including 1.1 GB of timeline frames and 707 MB of snapshots.

During a two-hour sample, 346 of 363 iterations escalated from the primary stream to the 4K stream. The primary captures produced about 106 MB while the escalated captures produced about 723 MB. The 1.56 MB outbox contains 706 delivered records, and every frame currently reparses that file and embeds all records in a 681 KB health document. The 92 KB decision-memory document is read, rewritten, and synced four times during a normal two-spot frame.

## Scope

This design covers two implementation slices that share the capture-loop hot path:

1. Compact and synchronized runtime persistence.
2. Adaptive capture, escalation, and image publication.

The following findings remain separate follow-up projects:

- Moving Matrix network operations to background workers.
- Bounding detection-lab execution with a worker queue.
- Migrating the JSON outbox or correction history to SQLite or an append-only journal.
- Changing inference engines or model formats.
- Container privilege and dependency-locking hardening.

## Configuration

The existing `runtime.frame_interval_seconds` remains the active/uncertain polling interval. The following settings are added with these defaults:

```yaml
runtime:
  frame_interval_seconds: 15
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 60
  stable_settle_frames: 3
  debug_overlay_interval_seconds: 60

stream:
  escalation_verification_seconds: 600
```

Setting `adaptive_polling_enabled` to `false` restores a fixed interval. Setting `stable_frame_interval_seconds` equal to `frame_interval_seconds` also removes the cadence difference. Setting `debug_overlay_interval_seconds` to `0` disables periodic overlays while retaining event evidence. Setting `escalation_verification_seconds` to `0` disables periodic stable-state verification, but transition-driven escalation remains enabled.

All intervals are positive finite numbers except the two explicitly disableable intervals, which may be zero. `stable_frame_interval_seconds` must be greater than or equal to `frame_interval_seconds`, and `stable_settle_frames` must be a positive integer.

## Runtime Resource Policy

A focused `runtime_resource_policy` module owns cadence and periodic-verification decisions. It is pure apart from an injected monotonic clock, so policy behavior is deterministic in tests.

The runtime starts in active cadence for safety. It continues using `frame_interval_seconds` when any of these conditions is true:

- A spot is `unknown`.
- A hit or miss streak has not yet reached its confirmation/release threshold.
- Weak or rejected presence evidence could suppress or produce a transition.
- A state transition occurred within the last `stable_settle_frames` successful iterations.
- Capture or detection is degraded.

After `stable_settle_frames` consecutive successful, unambiguous iterations with all spots confirmed, the runtime uses `stable_frame_interval_seconds`. Any uncertainty, transition, or processing failure immediately returns it to active cadence.

Sleeping is based on a monotonic deadline measured from the start of the iteration. Work time is subtracted from the selected interval, preventing capture, inference, and persistence latency from being added on top of the configured cadence. A non-positive remainder starts the next iteration immediately.

## Transition-Aware 4K Escalation

Primary-stream inference remains the first decision source. High-resolution escalation occurs when:

- A weak primary candidate belongs to a spot that is not already confirmed occupied.
- Missing primary evidence for an occupied spot is close enough to the release threshold that it could produce an open transition.
- Primary evidence is otherwise ambiguous in a way that could change occupancy or open-alert behavior.
- The configurable stable-state verification deadline is due.

A low-confidence accepted candidate for a stable occupied spot does not trigger 4K work on every frame. It is checked by periodic verification instead. Failed high-resolution verification degrades the iteration but does not destroy the valid primary frame or advance occupancy from incomplete evidence.

The existing confidence, confirmation, release, overlap, and presence-suppression thresholds are unchanged. This design changes when extra evidence is acquired, not what evidence is accepted.

## Atomic, Profile-Separated Images

Each capture is written to a temporary JPEG in the destination directory, validated, permissioned, and atomically published with `os.replace`.

- The primary profile publishes `/data/latest.jpg`.
- Named non-primary profiles publish `/data/latest-<sanitized-profile>.jpg`.
- A failed capture removes its temporary file and preserves the previous published image.

`FrameCaptureResult.latest_path` continues to identify the image used for that detection result. Transition evidence may therefore retain an escalated image, while the operator `latest` command and rolling timeline continue to use the smaller primary `latest.jpg`.

The debug overlay is generated from the primary frame at `debug_overlay_interval_seconds`, plus immediately when a transition needs evidence. It is not rewritten on every stable iteration. The rolling timeline remains one frame per minute but copies the primary frame, preventing routine 4K timeline growth.

## Compact Outbox Health

Health output contains only operational outbox information:

- Availability and schema version.
- Total and counts by state.
- Retry and dead-letter reason counts.
- Oldest and newest timestamps.
- Recovery counts and reason counts.

The `items` array and record-level phase results are excluded from health. Detailed records remain available through the existing outbox/operator surfaces.

The live `MatrixOutboxDelivery` exposes a compact summary from its existing in-memory `LocalOutbox`; the capture loop does not reconstruct the repository from disk on every frame. Tests and integrations without a live delivery object may use a file-backed compact fallback. Health remains atomically written, but its normal payload should be only a few kilobytes.

## Batched Decision Memory

The decision-memory module adds a batch append operation. One call:

1. Acquires a process-wide writer lock.
2. Loads and validates the current bounded document once.
3. Appends all sanitized records for the frame.
4. Applies the existing record-count bound once.
5. Atomically writes and syncs the result once.

Detection and runtime-state record builders return records without persisting them individually. The capture iteration combines them and performs one batch append after the frame plan has been computed. Alert, command, and lab writers use the same locked batch primitive, eliminating cross-thread lost updates inside the service process.

The current schema, record sanitization, maximum record count, maximum file size, and failure-is-nonfatal behavior remain unchanged. Cross-process writers are not introduced by the current deployment and are outside this slice.

## Error Handling

- Failure to write health or decision memory remains nonfatal and is reflected in structured diagnostics where currently supported.
- Capture validation failure never replaces the last known-good published JPEG.
- Adaptive policy state is in memory. Restarting begins in active cadence and schedules stable-state verification conservatively.
- A failed overlay does not alter occupancy evidence or the published raw frame.
- A failed periodic 4K verification returns the runtime to active cadence and records degradation; it does not discard successful primary evidence.

## Testing

Implementation follows test-driven development. Tests cover:

- Compact health omits outbox items and uses a live provider without reopening the file.
- File-backed compact health remains available to isolated callers.
- A two-spot frame produces one decision-memory durable write.
- Concurrent decision-memory appends preserve both writers' records.
- Successful capture atomically replaces the profile-specific destination.
- Failed and corrupt captures preserve the previous destination.
- Primary and high-resolution captures publish different paths.
- Stable states select the 60-second default; uncertainty and failures select 15 seconds.
- Transition-sensitive evidence escalates; stable weak occupied evidence does not escalate until verification is due.
- Timeline and periodic overlay use the primary image after an escalation.
- Setting adaptive intervals equal reproduces fixed-cadence behavior.

The existing capture, runtime-loop, escalation, health, decision-memory, configuration, and startup test modules are run after each focused change. The complete test suite is run before completion. The existing wall-clock-dependent analytics failure is tracked separately and must not be misreported as a regression from this design.

## Rollout and Observability

Structured logs add a bounded cadence decision reason, selected sleep interval, escalation reason, and periodic-verification status. Repetitive successful empty-outbox and no-op polling logs are not changed in this slice.

After deployment, compare a representative 24-hour period against the captured baseline:

- Escalation percentage.
- Primary and high-resolution capture counts.
- Average and peak container CPU and memory.
- Container block-write growth.
- Health, timeline, snapshot, and decision-memory sizes.
- Capture-to-confirmed-transition latency.
- Missed or false occupancy/open events from replay and operator feedback.

Rollback requires configuration only: disable adaptive polling, set overlay cadence to the active frame interval, and set periodic verification to the desired legacy-equivalent behavior. Atomic capture publication and compact/batched persistence remain safe to retain independently.
