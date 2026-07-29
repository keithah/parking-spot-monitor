# Whole-Project Resource Hardening Design

## Purpose

Finish the project-wide optimization pass by fixing every confirmed, applicable finding from the code-optimizer and thermo-nuclear maintainability audits. The implementation must lower peak resource use, remove avoidable work from the capture path, bound failure behavior, and simplify duplicated or weak boundaries without changing operator-visible behavior unnecessarily.

This work follows the operator preference established for this deployment: lower resource use is more important than immediate command responsiveness, slower background work is acceptable, and all new timing controls must remain adjustable.

## Compatibility Contract

The following remain backward compatible:

- Existing YAML keys and their meanings.
- Existing CLI commands and exit codes.
- Existing Matrix command text and alert content.
- Existing JSON schemas for runtime state, decision memory, vehicle history, corrections, and the Matrix outbox.
- Existing Docker Compose topology; no database, broker, or external service is added.
- Existing detector implementations and test doubles during the transition to in-memory crop inference.

New configuration keys receive safe defaults. Existing configuration files continue to load. Persisted JSON may be serialized more compactly, but its schema and values do not change.

## Audit Disposition

The audit found no database or browser-rendering layer. One reported resilience issue was rejected: the Matrix outbox already drains at startup and on each capture iteration. The design therefore does not add a redundant retry path.

Confirmed findings are grouped into four independently reviewable implementation slices:

1. Runtime safety and external-I/O isolation.
2. Archive and steady-state computation efficiency.
3. Image and serialization efficiency.
4. Build, startup, logging, and maintainability cleanup.

The slices share the compatibility contract but do not require an all-at-once merge. Each slice must leave the service runnable and testable.

## Slice 1: Runtime Safety and External-I/O Isolation

### Detection-Lab Concurrency

`DetectionLabManager` allows at most one replay or tuning job to run at a time. It does not need a general-purpose executor or an unbounded queue.

Starting a job while another job is active creates a normal persisted lab job with status `blocked`, phase `admission`, and safe error code `lab_busy`. This preserves the existing operator workflow while giving CPU, memory, GPU, disk, and thread use a fixed ceiling.

The manager tracks the active job ID under its existing lock. The worker releases admission in `finally`. Retention runs under the same synchronization boundary and excludes the active job directory. This removes the cleanup race without adding a second coordination mechanism.

### Capture Resource Ceilings

Stream profile geometry is validated at configuration load:

- Neither dimension may exceed 7,680 pixels.
- Total pixels may not exceed 33,177,600, equivalent to 8K UHD.

Before a temporary capture is published, validation enforces:

- Encoded size is at most 32 MiB.
- Pillow can identify and verify the file as JPEG.
- Decoded dimensions exactly match the selected stream profile.
- Pillow decompression-bomb warnings and errors are treated as capture failures.

Failure leaves the prior published frame untouched and removes the temporary file. These limits apply before overlay generation, crop inference, Matrix snapshot preparation, or YOLO inference.

### Matrix Outage Control

Matrix command polling remains synchronous to avoid concurrent mutations of vehicle-history state, but it no longer runs on every capture iteration.

`MatrixConfig` adds finite, non-negative timing controls:

```yaml
matrix:
  command_poll_interval_seconds: 60
  command_failure_cooldown_seconds: 60
  command_failure_max_cooldown_seconds: 900
  unauthorized_reply_cooldown_seconds: 300
  retry_jitter_ratio: 0.2
  outbox_retry_interval_seconds: 60
```

`command_poll_interval_seconds` and `unauthorized_reply_cooldown_seconds` may be zero to restore the corresponding legacy behavior. Failure cooldowns and `outbox_retry_interval_seconds` must be positive, the maximum cooldown must be at least the initial cooldown, and `retry_jitter_ratio` must be between 0 and 1 inclusive.

The capture loop stores command-poll state using the injected monotonic clock. Successful polls run no more often than the configured poll interval. A failed poll opens a cooldown after the first failure. Repeated failed half-open probes double the cooldown up to the configured maximum. A successful half-open probe resets it. Setting the poll interval to zero restores per-iteration polling for operators who prefer legacy responsiveness.

No failure sleeps are added to the capture loop: while the circuit is open, command polling is skipped and capture continues.

Unauthorized prefixed commands retain the existing rejection reply for the first event from a sender, then suppress repeated replies from that sender for `unauthorized_reply_cooldown_seconds`. The limiter holds at most 256 sender entries and evicts the least-recently-used entry, preventing the limiter itself from becoming an unbounded resource. The warning log follows the same aggregation window.

Locally calculated Matrix retry delays apply bounded jitter using `retry_jitter_ratio`. A server-provided `Retry-After` remains the minimum delay. Randomness is injected for deterministic tests.

### Outbox Delivery Worker

Alert outbox delivery moves off the capture-critical path into one bounded in-process worker owned by `MatrixOutboxDelivery`:

- Startup starts at most one worker.
- Enqueue wakes the worker.
- The worker drains at most one record per pass, preserving the current resource ceiling.
- When no retryable work exists, it waits without polling the filesystem.
- Retryable failures use the existing record state and wait `outbox_retry_interval_seconds` before another network attempt.
- Shutdown signals the worker, waits for a bounded join, then closes the client.

The durable outbox remains the queue; no second in-memory alert queue is introduced. Public explicit `drain_outbox()` remains available for tests and operator tooling.

All Matrix events produced by frame updates use durable text-only or snapshot outbox records. Lifecycle notices emitted before the capture loop starts or while it shuts down may remain immediate. This prevents quiet-window and occupancy event delivery from adding HTTP latency to frame processing without inventing another persistence model.

`LocalOutbox` adds an `RLock` around reads and mutations so capture-thread enqueue and worker-thread delivery cannot race. A record-ID index replaces repeated linear lookups. Persistence continues to use atomic replacement and directory sync. JSON is written with compact separators instead of indentation, reducing full-file write volume without changing schema.

The compact runtime health view adds worker state, last-attempt time, and a redacted last error type. These are health fields, not persisted outbox fields.

Routine runtime drain calls are removed after the worker owns delivery. Startup still verifies outbox recovery and reports its compact health summary.

### Runtime Type Boundaries

Small protocols define only the Matrix delivery and command methods actually consumed by the runtime. The outbox drain compatibility branch that catches `TypeError` and retries without `max_records` is removed; internal `TypeError` exceptions must never cause a second side-effecting drain.

The capture loop remains explicit. Only cohesive helpers for command scheduling, outbox lifecycle, and capture-failure pacing are extracted. No generic event bus, service container, or state-machine framework is introduced.

## Slice 2: Archive and Steady-State Efficiency

### Streaming Vehicle-History Health

Health calculation scans session iterators and updates counters incrementally. It does not build a list of all indefinitely retained closed sessions.

The streaming accumulator produces the current health schema, including counts, oldest timestamps, missing image references, and unknown-profile counts. Active-session callers that need records may continue receiving lists; the unbounded closed archive is never materialized solely for health.

### Revision-Aware Caches

Caches are bounded to one current value per data source and always have explicit invalidation:

- Correction replay state is keyed by the archive correction revision plus `(mtime_ns, size)` signatures for the correction and quarantine files.
- The owner-vehicle registry is keyed by its `(mtime_ns, size)` signature.
- Active-session snapshots are keyed by `VehicleHistoryArchive.mutation_revision()`.
- Detector signature capability is cached per detector instance using weak references, so detectors can be collected normally.

Archive mutation methods bump the relevant revision. External file replacement is detected by signatures. Cache corruption or stat failure falls back to a fresh read; it never returns silently stale state after a known mutation.

### Merge and Lookup Complexity

Profile corrections construct one canonical profile-ID map with path compression per replay state. Session processing performs constant-time canonical lookups rather than walking the same merge chain for every record.

Wrong-match lookup no longer sorts archive filenames when order is irrelevant. It streams records and retains only the latest candidate. A durable secondary index is not added because that would introduce another persisted format and recovery path.

### Bounded Formatting

Diagnostic and operator formatting consumes at most `limit + 1` items with `itertools.islice`. It never materializes an arbitrary iterable merely to display 6–24 values. Mapping order and truncation markers remain unchanged.

The tuning renderer stops using JSON serialization as a deep-copy mechanism only after mutation tests prove the renderer is read-only. If the copy is enforcing an ownership boundary, it is replaced with a targeted copy of the mutable fields instead of removed blindly.

## Slice 3: Image and Serialization Efficiency

### Direct Overlay Drawing

Debug overlays draw RGBA fills and outlines directly onto the working RGB image using Pillow's RGBA drawing mode. The implementation does not allocate separate full-frame canvas, overlay, alpha-composite, and reconverted images.

Colors, alpha values, line widths, labels, output mode, and JPEG output remain pixel-compatible within normal codec variation. Golden tests compare dimensions, representative pixels, and configured spot coverage rather than raw JPEG bytes.

### Shared Bounded-JPEG Encoder

A focused image helper owns “largest permitted dimensions, then highest permitted JPEG quality under a byte budget.” Matrix alert snapshots and operator `who` snapshots use the same helper.

For each candidate dimension, the helper first encodes the minimum allowed quality. If that cannot meet the byte budget, it immediately tries the next smaller dimension. Once a dimension is viable, it binary-searches the configured quality list for the highest valid quality. A single `BytesIO` buffer is rewound and truncated between attempts.

This preserves selection semantics while reducing the worst case from every dimension-quality combination to approximately one attempt per dimension plus a logarithmic quality search.

The helper accepts explicit byte budget, initial/minimum dimensions, scale factor, quality sequence, and resampling mode. It returns immutable bytes plus width, height, quality, and attempt count for logging and tests.

### Crop Inference

The detector contract gains an explicit in-memory image method implemented by the Ultralytics adapter. Spot-crop inference uses a context-managed Pillow crop and calls that method, eliminating temporary JPEG encode, write, reopen, and decode work.

The existing path-based `detect()` method remains supported. A runtime-checkable `InMemoryDetector` protocol selects the optimized path; detectors that do not implement it retain the bounded temporary-file fallback. Runtime code does not inspect arbitrary signatures or catch `TypeError` as capability detection.

### Descriptor Stability

Vehicle descriptor computation is not changed to derive its 8×8 hash from the 32×32 histogram thumbnail because that would alter matching output. This audit suggestion is deliberately rejected under the compatibility contract.

## Slice 4: Build, Startup, Logging, and Maintainability

### Reproducible and Cached Builds

Broad dependency bounds remain in `pyproject.toml` for library metadata. Docker installs from reviewed, exact lock/constraints files generated for the Linux container target. The runtime and detector dependency sets remain separate so the base image does not resolve Torch.

The lock files include hashes for every downloaded artifact accepted by the Linux container build. Lock regeneration is an explicit maintenance command, and a repository verification test checks that manifests and locks agree.

The Dockerfile opts into current BuildKit syntax and uses pip cache mounts. The cache is available to subsequent builds but is absent from final image layers. A minimal Python/tooling base is separated from the capture runtime; FFmpeg and Intel media drivers are installed only in stages that perform live capture. Source is compiled with `python -m compileall` after the final copy, allowing `PYTHONDONTWRITEBYTECODE=1` without repeated parsing on fresh containers.

`.dockerignore` excludes tests, development specifications, local worktrees, and validation artifacts not copied into the image. Required runtime scripts and documentation remain included only if a Docker command consumes them.

### Model Persistence

The Compose file supports an explicit read-only host model directory, defaulting to `./models`, mounted at `/models`. The example configuration points to `/models/yolov8n.pt`. Deployment documentation provides a checksum-verifiable pre-stage command. Startup fails clearly when an explicitly pathed model such as `/models/yolov8n.pt` is absent; a legacy bare model name retains Ultralytics' existing resolution behavior.

Existing configurations that use `yolov8n.pt` continue to load, but the documented production path no longer relies on a container-local first-run download.

### Startup Imports

Matrix command, operator cockpit, feedback, and detection-lab modules are imported inside the command-service factory after authorized senders are confirmed. Core capture startup no longer imports the optional operator stack when commands are disabled. Compatibility facade exports remain intact for external callers.

### Logging Volume

Logging stays synchronous and structured; a new logging thread is not justified after no-op volume is removed.

- Empty outbox work and successful no-op command polls are DEBUG.
- INFO is retained for state transitions, non-empty delivery/poll results, startup configuration, and periodic aggregate summaries.
- WARNING and ERROR behavior remains unchanged.
- Expensive candidate summaries are constructed only when the target level is enabled.

This avoids a new queue/drop policy while removing most steady-state serialization and Docker log traffic.

### Deduplication and Dead Code

Security-sensitive closeout redaction/bounding helpers use the canonical `scripts/closeout_helpers.py` implementation. Docker live-proof and alert-soak JPEG validation share one verification helper. Confirmed uncalled private helpers are removed after repository-wide reference and focused test checks.

The two snapshot-resize implementations disappear into the shared encoder. Private runtime helpers used across modules are promoted to narrow public helpers or moved to their canonical owner. Thin aliases remain only where tests or documented imports establish a compatibility requirement.

## Error Handling

- Resource-limit rejection is explicit, redacted, and nonfatal to the monitor.
- A detection-lab admission rejection never starts a worker.
- Capture validation failure preserves the last known-good frame.
- Matrix command cooldown changes health status only when an actual poll fails; skipped cooldown intervals do not repeatedly append failure memory.
- Outbox worker exceptions are contained, recorded in outbox/health state, and do not terminate capture.
- Cache failures cause bounded recomputation, not stale-value trust.
- Image budget failure returns the existing safe Matrix/operator error shape.
- Build and model checks fail before the long-running loop starts.

## Testing Strategy

Every behavior change follows red-green-refactor. Focused tests must cover:

- One active detection-lab job, deterministic `lab_busy`, and active-directory-safe retention.
- JPEG byte, format, dimension, and decompression-bomb rejection while preserving the prior frame.
- Matrix poll intervals, first-failure cooldown, exponential cap, half-open recovery, and legacy zero interval.
- Bounded unauthorized-reply suppression and jittered retries that respect `Retry-After`.
- One outbox worker, wake-on-enqueue, bounded drain, clean shutdown, concurrent enqueue/drain, and unchanged JSON schema.
- Streaming history health on a large synthetic archive with bounded peak allocations.
- Cache hits and every revision/signature invalidation path.
- Merge-chain path compression and unchanged canonical results.
- Bounded formatter consumption from generators that fail if over-read.
- Direct overlay behavior and reduced full-frame allocation count.
- Shared JPEG selection semantics, attempt ceilings, and both callers.
- In-memory crop inference without temporary files and unchanged translated detections.
- Lazy import behavior when Matrix commands are disabled.
- Docker lock usage, model mount documentation, Compose validation, build context, and bytecode compilation.
- Log levels and absence of steady-state no-op INFO records.
- Removed duplicate/dead helpers with all closeout and Docker validation tests passing.

After each slice, run its focused test modules and a structural review. Before completion, run the complete Python suite, static/compile checks, Docker Compose rendering, image build, container health check, and an observed resource comparison against the current deployed baseline.

## Rollout and Rollback

Deploy slices in order. Runtime safety lands first because later performance work assumes bounded jobs and protected capture artifacts. Archive and image changes follow independently. Build and cleanup land last so dependency/image changes do not obscure runtime regressions.

All new timings are configurable. The Matrix command interval can be set to zero to restore per-iteration polling. Explicit manual drain tooling remains available for troubleshooting the worker. Existing JSON data remains readable by the prior release, so rollback does not require data migration.

Post-deployment observation records:

- Container CPU, RSS, thread count, and block-write growth.
- Capture iteration duration during healthy and failed Matrix access.
- Detection-lab concurrent job count.
- Outbox size, retry counts, and delivery latency.
- Vehicle-health snapshot duration and peak allocation on the live archive.
- Overlay and Matrix JPEG encode attempts, time, and peak allocation.
- Startup time with commands enabled and disabled.
- Daily INFO/WARNING/ERROR log counts.

The optimization is accepted only if functional tests stay green, persisted schemas remain compatible, resource ceilings are enforced, and measured steady-state or outage behavior improves without missed occupancy transitions.
