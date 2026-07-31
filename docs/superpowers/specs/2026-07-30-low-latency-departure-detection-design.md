# Low-Latency Departure Detection Design

## Purpose and Success Criteria

Reduce the time between a vehicle leaving a monitored spot and the Matrix open-spot alert. Under healthy host and camera conditions, the target is an alert within 30 seconds of departure for a previously confirmed occupied spot.

The service cannot observe a departure while the host cannot schedule captures or the camera stream is unavailable. The latency target therefore applies when capture deadlines are honored, the primary stream remains reachable, inference completes within the selected cadence, and Matrix accepts the notification normally. Camera and host outages remain visible as degraded health rather than being misreported as departure detections.

## Incident Finding

The reported departure occurred around 22:33, during a host-wide memory-pressure event. Capture attempts timed out across VAAPI, DRM, and software modes, Docker and containerd operations also timed out, and the next successful frame did not arrive until about 22:36. Three configured release frames then delayed the confirmed open transition until 22:38:44. Matrix delivery completed about four seconds later.

The monitor currently uses about 16 MiB of memory. During follow-up inspection, an unrelated container used about 12 GiB and more than four CPU cores while the host had only about 759 MiB available and 13 GiB of swap in use. This design improves the monitor's healthy-path latency and bounds its capture retries, but host resource containment is an operational prerequisite for reliable latency.

## Approaches Considered

### State-aware low-resolution polling with high-resolution verification

Poll the existing primary stream quickly only while a spot is occupied, uncertain, or accumulating release evidence. Confirm a likely release once using the existing high-resolution stream. Back off when all spots are stably empty. This is the selected approach because it meets the latency target without paying the fast-polling cost continuously.

### Fixed fast polling

Polling every 8–10 seconds is simpler, but it uses the same capture and inference resources when all spots are empty. It remains available as a configuration rollback or diagnostic mode, but is not the production recommendation.

### Continuous video or camera motion events

A continuous decoder or camera-side motion signal could react faster, but adds connection state, buffering, motion filtering, and recovery complexity. It is unnecessary for the 30-second target and is outside this slice.

## Configuration

Add these adjustable settings:

```yaml
stream:
  capture_timeout_seconds: 4

occupancy:
  release_frames: 2

runtime:
  frame_interval_seconds: 30
  occupied_frame_interval_seconds: 8
  adaptive_polling_enabled: true
  stable_frame_interval_seconds: 60
```

`runtime.occupied_frame_interval_seconds` controls primary-stream polling whenever at least one spot was confirmed occupied, including while that spot accumulates a partial release streak. When omitted, it resolves to `frame_interval_seconds` for backward compatibility. It must be a positive finite number. It may be lower than `frame_interval_seconds` because it is a deliberate state-specific fast path.

`stream.capture_timeout_seconds` is the maximum time allowed for each decoder-mode attempt. It remains 15 seconds when omitted for backward compatibility. Production uses four seconds, bounding three sequential decoder attempts to about 12 seconds when the host honors subprocess deadlines. Existing decoder fallback order remains unchanged.

The model default for `occupancy.release_frames` remains three for compatibility. Production uses two. Restoring three increases false-release resistance at the cost of one occupied-cadence interval. Operators can also raise the occupied interval without a code change.

## Cadence Policy

The resource policy selects the next iteration deadline in this order:

1. A degraded, unknown, weak-presence, entry-confirmation, or post-transition state uses `frame_interval_seconds` under the existing policy.
2. If any spot is confirmed occupied, or a confirmed occupied spot has a partial miss streak, use `occupied_frame_interval_seconds`.
3. Once every spot is confirmed empty and the existing settle requirement is satisfied, use `stable_frame_interval_seconds`.

The occupied rule is evaluated before the generic partial-streak rule so a first missing observation does not accidentally return the loop to the slower 30-second active cadence. Disabling adaptive polling continues to select `frame_interval_seconds` everywhere. Setting all intervals equal provides fixed-cadence operation.

At an eight-second occupied cadence with two-frame release confirmation, a departure just after a successful frame should normally be seen on the next two primary captures within 16 seconds. The second missing observation immediately invokes high-resolution verification. Allowing several seconds for verification, state persistence, and Matrix delivery keeps the normal path below 30 seconds.

## Resolution Escalation

Routine polling continues to use the configured 1458×806 primary stream. The existing release-transition escalation is retained:

1. A primary frame supplies the next missing observation for a previously occupied spot.
2. When that observation would reach `release_frames`, the runtime immediately captures the configured 3840×2160 escalation profile.
3. Only the high-resolution detection is allowed to drive that iteration's final occupancy update.
4. A confirmed high-resolution absence produces the open event and Matrix notification.
5. High-resolution vehicle evidence prevents the false release and preserves occupied state.

No routine 4K polling is introduced. A failed high-resolution capture or detection degrades the iteration and does not advance occupancy from incomplete evidence.

## Capture Failure Behavior

The configured timeout is wired through every runtime-owned call to `capture_latest`, including primary and high-resolution captures. Each decoder mode receives the same bounded timeout. Existing typed, redacted failures and decoder fallback remain intact.

Capture failure does not count as an empty observation. The loop retains the previous occupancy state, records degraded health, and uses the existing bounded reconnect backoff. Reducing the timeout shortens recovery attempts but cannot force progress when the entire host is starved.

## Latency Observability

Structured transition and delivery records expose bounded timing fields without secrets or unbounded payloads:

- Time between the last occupied evidence and the confirmed empty evidence.
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
- Stable all-empty state retains the slower stable cadence.
- Disabling adaptive polling retains fixed active cadence.
- Runtime-owned primary and escalated captures receive the configured timeout.
- The second missing frame with `release_frames: 2` triggers high-resolution verification before the transition.
- Failed capture or high-resolution verification never counts as vacancy evidence.
- Transition timing fields are bounded and correctly partition capture, confirmation, enqueue, and delivery time.

Focused configuration, capture, resource-policy, escalation, runtime-loop, and Matrix delivery tests run after each change. The complete suite runs before deployment.

## Rollout and Rollback

Before deployment, preserve the current image, Compose configuration, and operator configuration. Deploy the code with the production values shown above, validate configuration, run a capture and detector smoke test, recreate only the parking-monitor service, and verify health and Matrix connectivity.

Measure at least one controlled occupied-to-empty test. Success requires a correct high-resolution-confirmed alert within 30 seconds under healthy host conditions, with no duplicate event. Record container CPU/memory, capture counts, escalation count, and timing fields.

Rollback is configuration-first: restore `release_frames: 3`, omit or raise `occupied_frame_interval_seconds`, and restore `capture_timeout_seconds: 15`. If code rollback is necessary, redeploy the protected prior image and configuration bundle.
