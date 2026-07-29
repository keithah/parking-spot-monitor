# Changelog

## 2026-07-28

### Changed

- Added adaptive runtime pacing with a 30-second active/uncertain cadence, a 60-second stable cadence after three stable frames, and explicit fixed-cadence rollback controls.
- High-resolution capture is now transition-aware with periodic verification, while primary frames retain ownership of `latest.jpg`, routine timeline frames, and debug overlays.
- Runtime health now reports compact Matrix outbox counts instead of record-level items and reuses cached vehicle-history health until the archive changes or the cache expires.
- Runtime decision-memory writes are locked and batched once per frame, and primary and named-profile captures publish atomically to separate paths so failed captures preserve the last known-good JPEG.
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
