# Changelog

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
