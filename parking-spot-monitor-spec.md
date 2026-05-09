# Parking Spot Monitor Script Spec

## Goal

Build a small local Docker service that watches a UniFi Protect RTSP stream, monitors two specific street-parking spots, and posts to Matrix when either spot changes from occupied to empty.

Source stream:

```text
rtsps://192.168.42.73:7441/WMV14zD0lCh90yFb?enableSrtp
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

Do not alert on:

- unknown -> empty
- empty -> empty
- passing car enters ROI briefly
- vehicle detected in road lane but not stable in spot

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
  --env RTSP_URL='rtsps://192.168.42.73:7441/WMV14zD0lCh90yFb?enableSrtp' \
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
```

Message format:

```text
Parking spot open: left_spot
Parking spot open: right_spot
```

Optional: include a JPEG snapshot with the alert.

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
    matrix_client.py
    geometry.py
  tests/
    test_geometry.py
    test_occupancy.py
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
      RTSP_URL: "rtsps://192.168.42.73:7441/WMV14zD0lCh90yFb?enableSrtp"
      MATRIX_ACCESS_TOKEN: "${MATRIX_ACCESS_TOKEN}"
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./data:/data
```

---

# Milestones, Slices, and Tasks

## Milestone 1: Project Skeleton and Docker Runtime

### Slice 1.1: Create App Skeleton

Tasks:

- Create repository structure.
- Add `src/main.py` entrypoint.
- Add `config.yaml.example`.
- Add basic structured logging.
- Load config from `/config/config.yaml`.
- Validate required env vars: `RTSP_URL`, `MATRIX_ACCESS_TOKEN`.

Acceptance criteria:

- `docker compose up` starts the service.
- App logs config summary without exposing secrets.
- App exits clearly if required config is missing.

### Slice 1.2: Dockerfile and Compose

Tasks:

- Build Python image with FFmpeg installed.
- Add dependencies in `requirements.txt`.
- Add `/dev/dri` mount in compose for Intel GPU access.
- Add `restart: unless-stopped`.

Acceptance criteria:

- Image builds locally.
- Container starts on a host without `/dev/dri` and falls back cleanly.
- Container starts on Intel host with `/dev/dri` mounted.

---

## Milestone 2: RTSP Capture with Intel HW Decode Fallback

### Slice 2.1: Software RTSP Snapshot Capture

Tasks:

- Implement FFmpeg-based snapshot capture.
- Sample one frame every `sample_interval_seconds`.
- Save latest debug frame to `/data/latest.jpg`.
- Reconnect on stream failure.

Acceptance criteria:

- Service can pull frames from the RTSP URL.
- Frame resolution matches expected `1458 x 806`, or logs actual resolution if different.
- Stream reconnects after temporary failure.

### Slice 2.2: Intel Hardware Decode Detection

Tasks:

- Detect whether `/dev/dri/renderD128` exists.
- Attempt QSV decode first.
- Attempt VAAPI decode second.
- Fall back to software decode.
- Log selected decode mode.

Acceptance criteria:

- On unsupported hosts, app falls back to software without crashing.
- On Intel GPU hosts, app uses hardware decode if FFmpeg supports it.
- Failed hardware attempts are logged once, not spammed.

---

## Milestone 3: Geometry and Spot Masking

### Slice 3.1: Polygon Configuration

Tasks:

- Load left and right spot polygons from config.
- Validate polygon coordinates are inside frame bounds.
- Implement point-in-polygon test.
- Implement polygon overlap ratio for detection boxes.

Acceptance criteria:

- Tests cover centroid inside/outside polygon.
- Tests cover bbox overlap with spot polygon.
- Bottom driveway car is outside all configured spots.

### Slice 3.2: Debug Overlay

Tasks:

- Draw spot polygons on debug frame.
- Draw accepted detections in each spot.
- Draw rejected detections in a different style or omit them.
- Save `/data/debug_latest.jpg`.

Acceptance criteria:

- Visual output clearly shows left and right monitored spots.
- Passing-road areas are not included in monitored polygons.

---

## Milestone 4: Vehicle Detection

### Slice 4.1: YOLO Detector Integration

Tasks:

- Load configured YOLO model.
- Run detector at sample interval.
- Filter to vehicle classes only.
- Apply confidence and minimum-area thresholds.

Acceptance criteria:

- Vehicles in the left and right spot are detected on the provided image.
- The bottom driveway car does not count for either monitored spot.

### Slice 4.2: Spot-Level Detection Filtering

Tasks:

- Assign detections to spots only if centroid is inside polygon.
- Require minimum overlap with spot polygon.
- Ignore detections outside spot polygons.

Acceptance criteria:

- Passing road vehicles are rejected unless they remain inside a spot polygon.
- A parked car partly overlapping a spot still counts if above overlap threshold.

---

## Milestone 5: Occupancy State Machine

### Slice 5.1: Occupied State Confirmation

Tasks:

- Track per-spot consecutive hits.
- Track candidate bbox centroid drift.
- Mark spot `occupied` only after configured consecutive stable hits.

Acceptance criteria:

- A passing car does not mark a spot occupied.
- A parked car present for around 60 seconds marks the spot occupied.

### Slice 5.2: Empty State Confirmation

Tasks:

- Track per-spot consecutive misses.
- Mark spot `empty` only after configured misses.
- Keep `unknown` state at startup until either occupied or empty is confirmed.

Acceptance criteria:

- Brief occlusions do not mark a spot empty.
- Detector misses for 90 seconds mark an occupied spot empty.
- Startup does not send false `empty` alerts.

### Slice 5.3: Transition Events

Tasks:

- Emit event only for `occupied -> empty`.
- Suppress duplicate alerts until spot becomes occupied again.
- Include spot ID, label, timestamp, and optional snapshot path.

Acceptance criteria:

- One notification is sent when the left spot empties.
- One notification is sent when the right spot empties.
- No notification is sent for road traffic.

---

## Milestone 6: Matrix Integration

### Slice 6.1: Text Notifications

Tasks:

- Implement Matrix message sender.
- Read access token from env var.
- Send text alert to configured room.
- Retry transient failures with backoff.

Acceptance criteria:

- Test message posts to Matrix room.
- Failed Matrix post is logged and retried.

### Slice 6.2: Snapshot Upload

Tasks:

- Save event snapshot to `/data/events/`.
- Upload image to Matrix media endpoint.
- Send message with attached image.

Acceptance criteria:

- Alert includes a snapshot showing the open spot.
- Snapshot file is retained locally for debugging.

---

## Milestone 7: Hardening and Tuning

### Slice 7.1: Runtime Observability

Tasks:

- Log per-spot state changes.
- Log detection counts and accepted/rejected counts.
- Add periodic heartbeat log.
- Add optional healthcheck endpoint or health file.

Acceptance criteria:

- It is easy to tell whether the script is running and seeing frames.
- Logs explain why a detection did or did not count.

### Slice 7.2: Tuning Pass with Real Street Traffic

Tasks:

- Run for at least one full day.
- Review false positives and false negatives.
- Adjust spot polygons.
- Tune confidence, overlap, hit/miss thresholds.
- Decide whether each spot needs separate thresholds.

Acceptance criteria:

- No alerts from ordinary passing traffic during test period.
- Spot-empty alerts occur within roughly 1-2 minutes after a parked car leaves.

---

## Milestone 8: Documentation

### Slice 8.1: README

Tasks:

- Document setup.
- Document config fields.
- Document Docker Compose usage.
- Document Matrix bot setup.
- Document Intel GPU passthrough.

Acceptance criteria:

- A clean machine can run the service from README instructions.

### Slice 8.2: Calibration Guide

Tasks:

- Explain how to edit polygons.
- Explain how to inspect debug overlays.
- Explain recommended thresholds.
- Explain why road traffic should be excluded by ROI and persistence logic.

Acceptance criteria:

- User can tune left and right spots without touching Python code.

---

## Non-Goals

- Do not build a full NVR.
- Do not store continuous video.
- Do not identify license plates.
- Do not use cloud AI APIs.
- Do not alert on every vehicle detection.
- Do not monitor the driveway/bottom car.

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
