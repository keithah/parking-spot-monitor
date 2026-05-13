# Parking Spot Monitor Script Spec

## Goal

Build a small local Docker service that watches a UniFi Protect RTSP stream, monitors two specific street-parking spots, and posts to Matrix when either spot changes from occupied to empty.

Source stream:

```text
<RTSP_URL>
```

Source image dimensions: `1458 x 806`.

The monitored spots are:

- **Left street spot**: the parked car on the left side of the image, near the tree.
- **Right street spot**: the parked car on the right side of the image.

Do **not** monitor the bottom driveway car.
Do **not** trigger from cars merely driving on the road.

---

## High-Level Design

```text
RTSP stream
  -> hardware-accelerated frame grabber when available
  -> periodic frame sampling
  -> crop/polygon mask for left and right parking spots only
  -> vehicle detection
  -> stationary/occupancy state machine
  -> Matrix notification on occupied -> empty transition
```

The core idea is to avoid analyzing the entire scene. The script should only evaluate tight parking-spot regions and should require parked-vehicle persistence before considering a spot occupied.

---

## Proposed Parking Spot Regions

These are initial approximate polygons based on the provided screenshot. They should be configurable in `config.yaml` and adjustable after testing.

Coordinate system: image origin is top-left, using source resolution `1458 x 806`.

### Left Spot Polygon

Covers the parked black car near the tree, excluding most of the roadway.

```yaml
left_spot:
  polygon:
    - [300, 180]
    - [610, 160]
    - [690, 285]
    - [420, 360]
    - [260, 300]
```

### Right Spot Polygon

Covers the parked car on the right curb, excluding the travel lane.

```yaml
right_spot:
  polygon:
    - [1010, 155]
    - [1395, 170]
    - [1395, 355]
    - [1040, 370]
    - [960, 250]
```

### Explicitly Ignored Area

The bottom driveway car should be ignored by design because it is outside both polygons.

---

## Detection Rules

### Vehicle Classes

Detect these object classes only:

- `car`
- `truck`
- `bus`
- optionally `motorcycle`, if desired later

### A Detection Counts for a Spot Only If

A vehicle detection is considered present in a parking spot when all are true:

1. Detection confidence is above threshold, default `0.45`.
2. Detection bounding box overlaps the spot polygon by enough area.
3. Detection centroid is inside the spot polygon.
4. Detection remains spatially stable across multiple samples.
5. Detection persists long enough to be considered parked.

Recommended thresholds:

```yaml
detection:
  confidence_threshold: 0.45
  min_bbox_area_px: 4000
  min_polygon_overlap_ratio: 0.25
  sample_interval_seconds: 5
```

---

## Anti-False-Positive Logic for Active Road

Passing cars are the biggest failure mode. The script must treat a car as parked only after persistence and stability checks.

### Parking Confirmation

A spot becomes `occupied` only after vehicle presence is detected for a sustained period.

Recommended default:

```yaml
occupancy:
  occupied_after_consecutive_hits: 12   # 12 samples * 5 sec = 60 sec
```

This prevents a passing car from becoming an occupied spot.

### Stationary Check

For a candidate vehicle to count as parked, its bounding box center should not move much over the confirmation window.

Recommended default:

```yaml
occupancy:
  max_centroid_drift_px: 45
  max_bbox_iou_change: 0.45
```

### Empty Confirmation

A spot becomes `empty` only after no parked vehicle is detected for a sustained period.

Recommended default:

```yaml
occupancy:
  empty_after_consecutive_misses: 18    # 18 samples * 5 sec = 90 sec
```

This avoids alerts from momentary occlusion, lighting changes, pedestrians, or detector misses.

### Alert Rule

Send Matrix notification only on:

```text
occupied -> empty
```

Each open-spot alert must include a full-frame camera snapshot captured when the spot is confirmed empty. The image should show the empty street space in context, not only a cropped ROI, so the Matrix room can verify the opening without opening the camera app.

Do not alert on:

- unknown -> empty
- empty -> empty
- passing car enters ROI briefly
- vehicle detected in road lane but not stable in spot
- normal spot openings during configured street-sweeping windows

### Street-Sweeping Quiet Window

Normal open-spot alerts are suppressed during street sweeping:

```text
1:00 PM–3:00 PM on the 1st and 3rd Monday of each month
```

The service should continue capturing frames, running detection, and updating occupancy state during the quiet window. Only normal `occupied -> empty` Matrix alerts are suppressed. The service should post a Matrix notice when the sweeping window begins and another when it ends, so the room has explicit context for the muted period.

---

## Local Inference Options

### MVP Detector

Use Ultralytics YOLO nano model, CPU-capable:

```text
yolo11n.pt or yolov8n.pt
```

Sampling every 5-10 seconds should be enough for this use case.

### Optional Optimization

Run detection only on each spot crop instead of the full frame. This reduces compute and makes false positives from road traffic less likely.

---

## RTSP Frame Capture

Use FFmpeg or OpenCV. Prefer FFmpeg because hardware decode and reconnect behavior are easier to control.

### Intel Hardware Decode Requirement

Use Intel hardware decoding if available, then fall back to software decoding.

Supported decode paths, in preference order:

1. Intel QSV if available
2. VAAPI if available
3. Software decode fallback

The container should mount Intel GPU devices when available:

```bash
docker run \
  --device /dev/dri:/dev/dri \
  --env RTSP_URL='<RTSP_URL>' \
  parking-spot-monitor:latest
```

The app should detect `/dev/dri` at startup and choose the best available FFmpeg args.

Example FFmpeg strategies:

```text
QSV:   -hwaccel qsv -c:v h264_qsv
VAAPI: -hwaccel vaapi -hwaccel_device /dev/dri/renderD128 -hwaccel_output_format vaapi
CPU:   no hwaccel flags
```

The implementation should tolerate unsupported hardware flags and retry with the next strategy.

---

## Matrix Notification

Use Matrix Client-Server API with a bot access token.

Required config:

```yaml
matrix:
  homeserver: "https://matrix.example.com"
  room_id: "!roomid:example.com"
  access_token_env: "MATRIX_ACCESS_TOKEN"
  send_snapshot: true
  snapshot_mode: "full_frame"
```

Open-spot message format:

```text
Parking spot open: left_spot at 2026-05-12 5:16:48 PM PDT
Parking spot open: right_spot at 2026-05-12 5:16:48 PM PDT
```

Occupied-spot status messages use the same timestamp style and stay concise when vehicle-history evidence is weak:

```text
Parking spot occupied: right_spot at 2026-05-12 5:16:48 PM PDT
```

Only add vehicle-history lines when they give an operator real information, such as a human-readable vehicle label or a higher-signal history estimate. Do not include generated profile IDs, low-confidence dwell ranges, or other implementation noise as if they were meaningful context.

Each open-spot message must include a JPEG snapshot attachment. The snapshot should be the full camera frame captured at the confirmed-empty moment, retained locally under `/data/events/` for debugging.

Street-sweeping message formats:

```text
Street sweeping starts in 1 hour: street_sweeping:2026-05-18:13:00-15:00
Street sweeping started: street_sweeping:2026-05-18:13:00-15:00
Street sweeping ended: street_sweeping:2026-05-18:13:00-15:00
```

---

## Configuration File

Create `config.yaml`:

```yaml
stream:
  url_env: RTSP_URL
  width: 1458
  height: 806
  sample_interval_seconds: 5
  reconnect_backoff_seconds: 10
  prefer_hw_decode: true

spots:
  left_spot:
    label: "Left street spot"
    polygon:
      - [300, 180]
      - [610, 160]
      - [690, 285]
      - [420, 360]
      - [260, 300]

  right_spot:
    label: "Right street spot"
    polygon:
      - [1010, 155]
      - [1395, 170]
      - [1395, 355]
      - [1040, 370]
      - [960, 250]

detection:
  model: "yolo11n.pt"
  classes: ["car", "truck", "bus"]
  confidence_threshold: 0.45
  min_bbox_area_px: 4000
  min_polygon_overlap_ratio: 0.25

occupancy:
  occupied_after_consecutive_hits: 12
  empty_after_consecutive_misses: 18
  max_centroid_drift_px: 45
  alert_on_transition: "occupied_to_empty"

matrix:
  homeserver: "https://matrix.example.com"
  room_id: "!roomid:example.com"
  access_token_env: "MATRIX_ACCESS_TOKEN"
  send_snapshot: true
  snapshot_mode: "full_frame"

quiet_windows:
  street_sweeping:
    enabled: true
    timezone: "America/Los_Angeles"
    ordinal_weekdays: [1, 3]
    weekday: "monday"
    start: "13:00"
    end: "15:00"
    suppress_open_spot_alerts: true
    post_start_notice: true
    post_end_notice: true
```

---

## Docker Deliverables

Repository structure:

```text
parking-spot-monitor/
  Dockerfile
  docker-compose.yml
  requirements.txt
  config.yaml.example
  src/
    main.py
    capture.py
    detector.py
    occupancy.py
    scheduler.py
    matrix_client.py
    geometry.py
  tests/
    test_geometry.py
    test_occupancy.py
    test_scheduler.py
  README.md
```

### Dockerfile Requirements

- Python 3.11 or 3.12 base image
- FFmpeg installed
- OpenCV headless
- Ultralytics or ONNX Runtime
- Non-root user if practical
- `/config/config.yaml` as mounted config

### docker-compose.yml Requirements

```yaml
services:
  parking-spot-monitor:
    build: .
    container_name: parking-spot-monitor
    restart: unless-stopped
    devices:
      - /dev/dri:/dev/dri
    environment:
      RTSP_URL: "<RTSP_URL>"
      MATRIX_ACCESS_TOKEN: "${MATRIX_ACCESS_TOKEN}"
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./data:/data
```

---

# Milestones, Slices, and Tasks

The project should be planned as multiple milestones. M001 delivers the first complete local service with conservative defaults and enough observability to trust what it is doing. Later milestones tune it against real traffic and improve operator documentation.

## Milestone 1: Working Local Parking Monitor

Goal: ship a complete Dockerized service that watches the RTSP stream, evaluates the two configured street-parking spots, applies conservative occupancy rules, respects the street-sweeping quiet window, and posts Matrix messages with full-frame snapshots when a spot opens.

### Slice 1.1: Runtime Skeleton and Config

Tasks:

- Create repository structure.
- Add `src/main.py` entrypoint.
- Add `config.yaml.example` with stream, spots, detection, occupancy, Matrix, and quiet-window settings.
- Add basic structured logging.
- Load config from `/config/config.yaml`.
- Validate required env vars: `RTSP_URL`, `MATRIX_ACCESS_TOKEN`.

Acceptance criteria:

- `docker compose up` starts the service.
- App logs config summary without exposing secrets.
- App exits clearly if required config or env vars are missing.
- Street-sweeping quiet-window settings are loaded and validated.

### Slice 1.2: Frame Capture and Debug Frames

Tasks:

- Implement FFmpeg-based snapshot capture.
- Sample one frame every `sample_interval_seconds`.
- Prefer Intel hardware decode when available: QSV, then VAAPI, then software fallback.
- Save latest frame to `/data/latest.jpg`.
- Reconnect on stream failure.

Acceptance criteria:

- Service can pull frames from the RTSP URL.
- Frame resolution matches expected `1458 x 806`, or logs actual resolution if different.
- Unsupported hardware decode falls back to software without crashing.
- Stream reconnect attempts are visible in logs.

### Slice 1.3: Spot Geometry and Debug Overlay

Tasks:

- Load left and right spot polygons from config.
- Validate polygon coordinates are inside frame bounds.
- Implement point-in-polygon and bbox/polygon overlap checks.
- Draw spot polygons on a debug frame.
- Save `/data/debug_latest.jpg`.

Acceptance criteria:

- Tests cover centroid inside/outside polygon.
- Tests cover bbox overlap with spot polygon.
- Bottom driveway car is outside all configured spots.
- Visual output clearly shows left and right monitored spots.

### Slice 1.4: Vehicle Detection and Spot Filtering

Tasks:

- Load configured YOLO nano model.
- Run detector at sample interval.
- Filter to vehicle classes only: `car`, `truck`, `bus`.
- Apply confidence, minimum-area, centroid-inside-polygon, and overlap thresholds.
- Emit per-spot detection candidates.

Acceptance criteria:

- Vehicles in the left and right street spots are detected on the provided image.
- The bottom driveway car does not count for either monitored spot.
- Passing-road vehicles are rejected unless they satisfy the configured spot geometry rules.
- Logs explain accepted/rejected detection counts without being noisy.

### Slice 1.5: Occupancy State and Quiet-Window Policy

Tasks:

- Track per-spot consecutive hits and misses.
- Track candidate bbox centroid drift.
- Mark a spot `occupied` only after configured stable consecutive hits.
- Mark a spot `empty` only after configured consecutive misses.
- Keep `unknown` state at startup until occupied or empty is confirmed.
- Implement the 1st/3rd Monday, 1:00 PM–3:00 PM street-sweeping quiet window.
- Continue state updates during the quiet window while suppressing normal open-spot alerts.
- Emit schedule events when the quiet window begins and ends.

Acceptance criteria:

- A passing car does not mark a spot occupied.
- A parked car present for around 60 seconds marks the spot occupied.
- Brief occlusions do not mark a spot empty.
- Detector misses for around 90 seconds mark an occupied spot empty.
- Startup does not send false empty alerts.
- A confirmed opening during street sweeping updates internal state but does not send a normal open-spot alert.
- Sweep start/end events are emitted exactly once per window.

### Slice 1.6: Matrix Text, Snapshot Upload, and End-to-End Wiring

Tasks:

- Implement Matrix message sender using a bot access token from env.
- Send text alert to configured room.
- Save event snapshots to `/data/events/`.
- Attach a full-frame JPEG snapshot to open-spot alerts.
- Send street-sweeping start/end notices.
- Retry transient Matrix failures with backoff.
- Wire capture, detection, occupancy, scheduler, and Matrix together in the service loop.

Acceptance criteria:

- One Matrix alert is sent when the left spot empties outside the quiet window.
- One Matrix alert is sent when the right spot empties outside the quiet window.
- Each open-spot alert includes a full-frame snapshot showing the empty spot in context.
- No normal open-spot alert is sent during the street-sweeping quiet window.
- Matrix receives a start notice when sweeping begins and an end notice when normal alerts resume.
- Failed Matrix posts are logged and retried without crashing the service.

### Slice 1.7: Docker Runtime and Operator Signals

Tasks:

- Build Python image with FFmpeg installed.
- Add OpenCV headless, Ultralytics, Matrix client, and scheduling dependencies.
- Add `/dev/dri` mount in compose for Intel GPU access.
- Add `restart: unless-stopped`.
- Add periodic heartbeat log.
- Add optional health file or healthcheck endpoint.

Acceptance criteria:

- Image builds locally.
- Container starts on a host without `/dev/dri` and falls back cleanly.
- Container starts on Intel host with `/dev/dri` mounted when available.
- It is easy to tell whether the script is running, seeing frames, and posting alerts.

## Milestone 2: Calibration and Real-Traffic Hardening

Goal: run the service against real street traffic long enough to tune false positives/false negatives and make the monitored polygons and thresholds trustworthy.

### Slice 2.1: Real-World Observation Run

Tasks:

- Run for at least one full day.
- Review false positives and false negatives.
- Inspect saved event snapshots and debug overlays.
- Compare logs against expected street behavior.

Acceptance criteria:

- No alerts from ordinary passing traffic during the observation period.
- Spot-empty alerts occur within roughly 1-2 minutes after a parked car leaves.
- Any missed alerts or false alerts have enough logged context to explain why.

### Slice 2.2: Threshold and Polygon Tuning

Tasks:

- Adjust spot polygons if needed.
- Tune confidence, overlap, hit/miss, drift, and sample interval thresholds.
- Decide whether each spot needs separate thresholds.
- Update `config.yaml.example` with tuned defaults.

Acceptance criteria:

- User can tune left and right spots without touching Python code.
- Tuned defaults reflect real camera behavior rather than only the initial screenshot.
- Tuning changes are documented with rationale.

## Milestone 3: Documentation and Maintenance Polish

Goal: make the service easy to install, operate, calibrate, and debug later.

### Slice 3.1: README and Setup Guide

Tasks:

- Document setup.
- Document config fields.
- Document Docker Compose usage.
- Document Matrix bot setup.
- Document Intel GPU passthrough.
- Document required env vars without exposing secret values.

Acceptance criteria:

- A clean machine can run the service from README instructions.
- A future operator can tell how to configure Matrix, RTSP, GPU passthrough, and the street-sweeping schedule.

### Slice 3.2: Calibration and Troubleshooting Guide

Tasks:

- Explain how to edit polygons.
- Explain how to inspect debug overlays.
- Explain recommended thresholds.
- Explain why road traffic should be excluded by ROI and persistence logic.
- Explain how to diagnose Matrix post failures, RTSP reconnects, and model detection misses.

Acceptance criteria:

- User can tune left and right spots without touching Python code.
- Troubleshooting starts from durable logs and saved images, not guesswork.

---

## Non-Goals

- Do not build a full NVR.
- Do not store continuous video.
- Do not identify license plates.
- Do not use cloud AI APIs.
- Do not alert on every vehicle detection.
- Do not monitor the driveway/bottom car.
- Do not suppress detection or state updates during street sweeping; only suppress normal open-spot alerts.

---

## Initial Implementation Choice

Start with:

- Python
- FFmpeg frame capture
- Ultralytics YOLO nano
- OpenCV geometry/debug overlay
- Matrix Client-Server API
- Docker Compose

Only consider Frigate, Home Assistant, or a heavier tracker if this script cannot meet the false-positive requirements after tuning.
