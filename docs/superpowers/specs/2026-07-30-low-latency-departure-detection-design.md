# Low-Latency Occupancy Transition Detection Design

## Purpose and Success Criteria

Reduce the time between a vehicle entering or leaving a monitored spot and the corresponding Matrix alert. Under healthy host and camera conditions, the target is an alert within 30 seconds of either physical transition.

The service cannot observe a transition while the host cannot schedule captures or the camera stream is unavailable. The latency target therefore applies when capture deadlines are honored, the primary stream remains reachable, inference completes within the selected cadence, and Matrix accepts the notification normally. Camera and host outages remain visible as degraded health rather than being misreported as occupancy evidence.

## Incident Finding

The reported departure occurred around 22:33, during a host-wide memory-pressure event. Capture attempts timed out across VAAPI, DRM, and software modes, Docker and containerd operations also timed out, and the next successful frame did not arrive until about 22:36. Three configured release frames then delayed the confirmed open transition until 22:38:44. Matrix delivery completed about four seconds later.

A later arrival exposed an independent healthy-path problem. The open event was delivered at 23:17:46. A car began entering around 23:21, weak evidence triggered high-resolution verification at 23:22:10, and occupancy was not confirmed until 23:23:40 because stable empty polling and three-frame confirmation were too slow. The confirmed transition still produced no Matrix notice: vehicle-history image recovery failed, the enriched occupied alert was never built, and the generic state-change event was skipped by the open-events-only Matrix dispatch policy. Occupied notification must therefore be driven by the confirmed transition, not by optional vehicle-history enrichment.

The monitor currently uses about 16 MiB of memory. During follow-up inspection, an unrelated container used about 12 GiB and more than four CPU cores while the host had only about 759 MiB available and 13 GiB of swap in use. This design improves the monitor's healthy-path latency and bounds its capture retries, but host resource containment is an operational prerequisite for reliable latency.

## Approaches Considered

### State-aware low-resolution polling with high-resolution verification

Poll the existing primary stream every eight seconds while a spot is uncertain, confirming, occupied, or transitioning. Poll every 12 seconds when all spots are stably empty so a new arrival is still observed inside the latency budget. Use the existing high-resolution stream when entry evidence is uncertain and whenever a release would reach its threshold. This is the selected approach because it meets both transition targets without routine 4K work.

### Fixed fast polling

Polling every eight seconds is simpler, but it uses the same capture and inference resources when all spots are empty. It remains available as a configuration rollback or diagnostic mode, but the selected 8/12-second policy saves some empty-state work.

### Continuous video or camera motion events

A continuous decoder or camera-side motion signal could react faster, but adds connection state, buffering, motion filtering, and recovery complexity. It is unnecessary for the 30-second target and is outside this slice.

## Configuration

Add these adjustable settings:

```yaml
stream:
  capture_timeout_seconds: 4

occupancy:
  confirm_frames: 2
  release_frames: 2

runtime:
  frame_interval_seconds: 8
  occupied_frame_interval_seconds: 8
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 12
```

`runtime.frame_interval_seconds` controls unknown, entry-confirmation, weak-evidence, degraded, and transition-settle polling. `runtime.occupied_frame_interval_seconds` controls primary-stream polling whenever at least one spot was confirmed occupied, including while that spot accumulates a partial release streak. `runtime.stable_frame_interval_seconds` controls polling only after every spot is confirmed empty and stable. The occupied value resolves to `frame_interval_seconds` when omitted for backward compatibility. Every cadence must be positive and finite; the stable interval must remain greater than or equal to the active interval.

`stream.capture_timeout_seconds` is the maximum time allowed for each decoder-mode attempt. It remains 15 seconds when omitted for backward compatibility. Production uses four seconds, bounding three sequential decoder attempts to about 12 seconds when the host honors subprocess deadlines. Existing decoder fallback order remains unchanged.

The model defaults for `occupancy.confirm_frames` and `occupancy.release_frames` remain compatible with existing configurations. Production uses two for each direction. Restoring either value to three increases false-transition resistance at the cost of one active-cadence interval.

## Cadence Policy

The resource policy selects the next iteration deadline in this order:

1. A degraded, unknown, weak-presence, entry-confirmation, or post-transition state uses `frame_interval_seconds`.
2. If any spot is confirmed occupied, or a confirmed occupied spot has a partial miss streak, use `occupied_frame_interval_seconds`.
3. Once every spot is confirmed empty and the existing settle requirement is satisfied, use `stable_frame_interval_seconds`.

The occupied rule is evaluated before the generic partial-streak rule so a first missing observation remains on the occupied cadence. Disabling adaptive polling continues to select `frame_interval_seconds` everywhere. Setting all intervals equal provides fixed-cadence operation.

At a 12-second stable-empty cadence followed by an eight-second active cadence with two-frame entry confirmation, an arrival just after a successful empty frame should normally confirm within 20 seconds. At an eight-second occupied cadence with two-frame release confirmation, a departure just after a successful frame should normally confirm within 16 seconds. Allowing several seconds for high-resolution verification, state persistence, outbox enqueue, and Matrix delivery keeps both normal paths below 30 seconds.

## Resolution Escalation

Routine polling continues to use the configured 1458×806 primary stream. Entry evidence below the authoritative confidence threshold immediately receives one high-resolution check. High-confidence entry evidence may confirm through two primary frames without 4K work. The existing release-transition escalation is retained:

1. A primary frame supplies the next missing observation for a previously occupied spot.
2. When that observation would reach `release_frames`, the runtime immediately captures the configured 3840×2160 escalation profile.
3. Only the high-resolution detection is allowed to drive that iteration's final occupancy update.
4. A confirmed high-resolution absence produces the open event and Matrix notification.
5. High-resolution vehicle evidence prevents the false release and preserves occupied state.

No routine 4K polling is introduced. A failed high-resolution capture or detection degrades the iteration and does not advance occupancy from incomplete evidence.

The primary resolution is not reduced further in this slice. Its current geometry and spot calibration are known to work, while a smaller frame could lose distant-vehicle evidence. A lower-resolution primary profile remains a future replay-calibrated optimization.

## Occupied Notification Independence

A confirmed transition from a non-occupied state to `OCCUPIED` is the sole trigger for one occupied notification event. This preserves the existing startup behavior for a confirmed vehicle discovered from `UNKNOWN` while covering normal `EMPTY -> OCCUPIED` arrivals. The transition event ID provides restart-safe idempotency. The alert uses the authoritative frame from the confirming iteration and contains a generic spot/vehicle message that does not require a recognized vehicle profile.

Vehicle-history session creation, image attachment, profile matching, and owner estimates remain best-effort enrichments. When available, their bounded fields may enrich the occupied alert. A failure in any enrichment phase is logged and recorded in health, but cannot suppress, delay, or duplicate the base notification.

Snapshot preparation is also best effort for occupied alerts. If a valid event snapshot can be prepared, the outbox persists the existing text/upload/image phases. If snapshot preparation fails before enqueue, the outbox durably enqueues a text-only occupied alert with the same event ID and a bounded degradation reason. Outbox persistence failure remains a real delivery failure and is not masked by an in-memory send.

## Capture Failure Behavior

The configured timeout is wired through every runtime-owned call to `capture_latest`, including primary and high-resolution captures. Each decoder mode receives the same bounded timeout. Existing typed, redacted failures and decoder fallback remain intact.

Capture failure does not count as an empty observation. The loop retains the previous occupancy state, records degraded health, and uses the existing bounded reconnect backoff. Reducing the timeout shortens recovery attempts but cannot force progress when the entire host is starved.

## Latency Observability

Structured transition and delivery records expose bounded timing fields without secrets or unbounded payloads:

- Time between the last opposite-state evidence and the confirmed transition evidence.
- Primary and escalated capture durations used for the transition.
- Time from confirmed transition to outbox enqueue.
- Time from enqueue to successful Matrix delivery.
- Selected cadence and cadence reason leading into the transition.

These values distinguish camera-observation delay, confirmation delay, and notification delay during future incidents. Existing stale-frame health remains the source of truth when no recent frame exists.

## Host Resource Boundary

The parking monitor must not silently mutate unrelated services. Deployment verification records host available memory, swap use, and top container resource consumers. If another workload can consume nearly all host memory or CPU, its Compose project should receive an appropriate memory/CPU limit or be moved to a separate host by that workload's operator.

The monitor's own container limit does not reserve memory or CPU and therefore cannot protect it from a competing container. A hard 30-second guarantee requires host-level headroom or scheduling controls outside this repository.

## Testing

Implementation follows red-green-refactor development. Tests cover:

- Configuration defaults preserve the previous cadence and 15-second capture timeout.
- Explicit occupied cadence and capture timeout are validated and included in sanitized summaries.
- A stable occupied spot selects the occupied cadence.
- A partial release streak retains the occupied cadence.
- Stable all-empty state selects the configurable 12-second production cadence.
- Disabling adaptive polling retains fixed active cadence.
- Runtime-owned primary and escalated captures receive the configured timeout.
- The second missing frame with `release_frames: 2` triggers high-resolution verification before the transition.
- The second accepted entry frame with `confirm_frames: 2` confirms occupancy inside the cadence budget.
- Failed capture or high-resolution verification never counts as vacancy evidence.
- A confirmed occupied transition queues exactly one alert even when vehicle-history image recovery or enrichment fails.
- Occupied snapshot-preparation failure queues one durable text-only fallback with the same event ID.
- Transition timing fields are bounded and correctly partition capture, confirmation, enqueue, and delivery time.

Focused configuration, capture, resource-policy, escalation, runtime-loop, and Matrix delivery tests run after each change. The complete suite runs before deployment.

## Rollout and Rollback

Before deployment, preserve the current image, Compose configuration, and operator configuration. Deploy the code with the production values shown above, validate configuration, run a capture and detector smoke test, recreate only the parking-monitor service, and verify health and Matrix connectivity.

Measure at least one controlled or naturally occurring transition in each direction. Success requires the corresponding alert within 30 seconds under healthy host conditions, with no duplicate event. Departure must receive high-resolution confirmation; uncertain arrival must receive high-resolution confirmation. Record container CPU/memory, capture counts, escalation count, and timing fields.

Rollback is configuration-first: restore `confirm_frames: 3` and `release_frames: 3`, restore the previous active/stable cadence, omit or raise `occupied_frame_interval_seconds`, and restore `capture_timeout_seconds: 15`. If code rollback is necessary, redeploy the protected prior image and configuration bundle.
