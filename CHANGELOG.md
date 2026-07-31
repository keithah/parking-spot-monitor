# Changelog

## 2026-07-30

### Changed

- Split structural hot spots behind compatibility facades, removed confirmed dead pass-throughs, and replaced per-frame detector capability inspection with one shared, retry-safe lazy detector adapter.
- Bounded owner-registry reads and diagnostics, preserved the last known-good registry across invalid replacement files, and made runtime and Matrix snapshot providers explicit.
- Made cadence and reconnect waits interruptible, queued lifecycle notices durably, added cooperative Matrix cancellation, isolated cleanup failures, and configured Compose for `SIGTERM` with a two-minute grace period.
- Moved Matrix command fetches to one capacity-one background worker, added capped/jittered reconnect and retry controls, and replaced routine per-frame INFO records with bounded 15-minute aggregate summaries.
- Persisted per-record Matrix outbox retry due times within schema version 1 and cached compact summaries. Complete atomic outbox publication after each durable delivery phase remains intentionally unchanged to preserve restart semantics.
- Added a service-scoped decision-memory store: transition, alert, command, correction, feedback, and lifecycle decisions request an immediate flush; routine records trigger a checkpoint after 300 seconds or at 50 pending records by default. Failed publications remain dirty and retry.
- Reused already-loaded vehicle-history records for profile summaries and retained the existing streamed revision/TTL health cache without adding an archive index or secondary durable format.
- Reused one canonical full JPEG through reflink or bounded copy, shared bounded JPEG decoding, persisted exact Matrix upload derivatives for retry/restart reuse, and added indexed recovery for interrupted owned-artifact cleanup.
- Routed Ultralytics settings to `/data/ultralytics` and added a serial, isolated `.pt`/ONNX/TorchScript benchmark harness. Production remains on `.pt`; the harness cannot switch the live backend.
- Added a final remediation report and expanded Docker backup, upgrade, observation, graceful-stop, rollback, and aggregate-log guidance.
- Bound correction replay to 10,000 valid events, 200 invalid lines, and 16 MiB with compaction beginning at 12 MiB and safe rejection at the hard bounds; made legacy owned-file recovery advance across bounded scans; reduced benchmark rehashing and canonical JPEG validation reads; hardened file-signature caches and decision-memory reconciliation; and added indexed outbox lookups.
- Split eight oversized test monoliths into 46 focused modules plus shared support, preserved 797-case collection parity, and enforced a 999-line ceiling for these suites.
- Deployed the final review-closure detector image `sha256:97b05f5fd098f7d3dd2d5cb9ace459e0b82a776f4cb798ad058605f2bc0613a5` with the existing operator bind mounts. Compose recreation took `1.79s`; fresh post-start frame evidence passed, the service stopped gracefully in `4.25s`, and it restarted healthy with zero restarts and no OOM. Five short samples ranged `356.1–388.4 MiB`, while a post-restart point was `356.3 MiB`; unequal windows and workloads make these health observations, not a peak-memory or resource-improvement claim.
- Added a checked-in transactional deployment helper for exact-image quiesced backup, exact-revision upgrade, rollback, and complete-data restore. It centralizes the post-start freshness gate, supports external bind sources and root-owned data, makes the selected environment file authoritative, transactionally handles different active and bundled model directories, uses operator-approved model provenance, and has injected failure-path tests.
- Closed the final adversarial persistence and cache findings: selected cleanup now waits for bounded recovery scans, malformed outbox records are removed from canonical storage after quarantine, stable invalid owner registries reuse the last-good snapshot for the configured polling window, and decision-memory rollback remains conditional until concurrent writer churn stabilizes.

### Compatibility

- Existing YAML, CLI, Matrix command text, occupancy thresholds, service topology, and durable schema versions are retained. New timing fields have validated defaults, and new outbox fields are optional schema-version-1 metadata.
- Verification is intentionally serial; `pytest-xdist` is not used because minimizing peak host CPU and memory takes priority over test throughput.

## 2026-07-28

### Changed

- Added adaptive runtime pacing with a 30-second active/uncertain cadence, a 60-second stable cadence after three stable frames, and explicit fixed-cadence rollback controls.
- High-resolution capture is now transition-aware with periodic verification, while primary frames retain ownership of `latest.jpg`, routine timeline frames, and debug overlays.
- Runtime health now reports compact Matrix outbox counts instead of record-level items and reuses cached vehicle-history health until the archive changes or the cache expires.
- Runtime decision-memory writes are locked and batched once per frame, and primary and named-profile captures publish atomically to separate paths so failed captures preserve the last known-good JPEG.
- Capture inputs are bounded to 7,680 pixels per dimension, 33,177,600 pixels total, and 32 MiB encoded JPEGs; detection-lab work admits one active job at a time.
- Matrix command polling defaults to 60 seconds with a 60-to-900-second failure cooldown, unauthorized replies use a 300-second sender cooldown, local retries use 0.2 jitter, and outbox retries use a 60-second interval. The deployment runbook documents zero/equal-bound compatibility settings and full-image rollback.
- Production detector weights are mounted read-only at `/models`, and dependency maintenance uses authenticated, hash-locked build, runtime, and detector manifests with an offline freshness check.
- The first production rollout remained healthy with zero restarts through a fixed 192-second steady observation. That window projected 14,850 INFO records/day versus the pre-change observed 13,117 over 24 hours, while two instantaneous samples reported 0.00% CPU and 577.5–624.4 MiB memory. The unequal windows and live workloads do not establish a performance improvement; the runbook records the full evidence and limitations.
- Added a Docker deployment and operations runbook plus a secret-safe environment template covering first deployment, validation, upgrades, rollback, backup, health checks, and resource measurement.

These changes are intended to reduce steady-state CPU and durable filesystem traffic without changing occupancy thresholds or conservative transition behavior. Resource savings require post-deployment measurement and are not claimed by this release note.

## 2026-06-02

### Changed

- Runtime frame-loop wiring is documented as focused startup, capture, detection, health, state, vehicle-history, command, and Matrix dispatch modules instead of a monolithic entrypoint.
- Docker documentation now describes the `runtime-base` and `runtime-detector` image targets, the split `requirements.txt` / `requirements-detector.txt` dependency contract, and rebuild-focused Compose deployment.
- Matrix outbox documentation now distinguishes runtime enqueue-only open-alert dispatch from direct enqueue-and-drain helper calls.
- Publication notes now document the disabled local push remote pattern and the local Compose deploy smoke contract.

### Fixed

- Documentation no longer describes the stale flat `src/main.py` repository layout or an unsplit detector dependency image as the current deliverable.

## 2026-05-13

### Changed

- Matrix alert timestamps now use a 12-hour Los Angeles display with a timezone abbreviation.
- Occupied-spot Matrix alerts omit low-signal vehicle-history context when the only available details are generated profile IDs and low-confidence estimates.
- Documentation now describes the concise occupied-alert behavior and the updated timestamp format.
- Added private owner-vehicle registry support for deduped street-cleaning alerts when the operator's car is parked in a monitored spot.
