# Image and Serialization Efficiency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce full-frame copies, JPEG encode attempts, and crop disk round trips while preserving image selection and detection semantics.

**Architecture:** Introduce one pure bounded-JPEG primitive, then migrate the two callers. Draw overlays directly and add an explicit optional in-memory detector protocol with a bounded path fallback.

**Tech Stack:** Python 3.12, Pillow, Ultralytics adapter, BytesIO, runtime-checkable protocols, pytest

## Global Constraints

- Matrix byte budgets, image dimensions, quality preferences, alert bodies, and metadata remain unchanged.
- The encoder selects the largest viable dimensions, then the highest viable configured quality.
- One reusable `BytesIO` buffer is used per resize operation.
- Existing path-based detectors continue to work.
- Vehicle descriptor/hash behavior remains unchanged.
- No raw JPEG byte equality is required after re-encoding; dimensions, budgets, representative pixels, and selection semantics are required.
- Every task uses red-green-refactor and ends with a focused commit.

---

### Task 1: Build the Shared Bounded-JPEG Encoder

**Files:**
- Create: `parking_spot_monitor/image_budget.py`
- Create: `tests/test_image_budget.py`

**Interfaces:**
- Produces: `JpegBudgetResult(data: bytes, width: int, height: int, quality: int, attempts: int)`
- Produces: `encode_jpeg_under_budget(image, *, max_bytes, initial_max_dimension, min_dimension, dimension_scale, qualities, resampling) -> JpegBudgetResult`

- [ ] **Step 1: Write failing selection and attempt-bound tests**

```python
def test_encoder_selects_largest_dimension_then_highest_quality(monkeypatch) -> None:
    attempts: list[tuple[tuple[int, int], int]] = []

    def fake_encode(image, buffer, quality):
        attempts.append((image.size, quality))
        sizes = {
            ((100, 50), 40): 120,
            ((80, 40), 40): 80,
            ((80, 40), 70): 95,
            ((80, 40), 85): 110,
        }
        buffer.write(b"x" * sizes[(image.size, quality)])

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    result = encode_jpeg_under_budget(
        Image.new("RGB", (100, 50)),
        max_bytes=100,
        initial_max_dimension=100,
        min_dimension=40,
        dimension_scale=0.8,
        qualities=(85, 70, 40),
        resampling=Image.Resampling.LANCZOS,
    )
    assert (result.width, result.height, result.quality) == (80, 40, 70)
    assert result.attempts <= 4


def test_encoder_reuses_one_buffer(monkeypatch) -> None:
    buffer_ids: set[int] = set()
    monkeypatch.setattr(
        image_budget,
        "_encode_jpeg",
        lambda image, buffer, quality: (buffer_ids.add(id(buffer)), buffer.write(b"x" * 101)),
    )
    with pytest.raises(ImageBudgetError):
        encode_jpeg_under_budget(
            Image.new("RGB", (100, 50)),
            max_bytes=100,
            initial_max_dimension=100,
            min_dimension=40,
            dimension_scale=0.8,
            qualities=(85, 70, 40),
            resampling=Image.Resampling.LANCZOS,
        )
    assert len(buffer_ids) == 1
```

- [ ] **Step 2: Run the new tests and verify RED**

Run: `python3 -m pytest tests/test_image_budget.py -q`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement minimum-quality probing and quality-index binary search**

```python
@dataclass(frozen=True, slots=True)
class JpegBudgetResult:
    data: bytes
    width: int
    height: int
    quality: int
    attempts: int

def _attempt(image, buffer: BytesIO, quality: int) -> bytes:
    buffer.seek(0)
    buffer.truncate(0)
    _encode_jpeg(image, buffer, quality)
    return buffer.getvalue()
```

Normalize qualities to unique descending integers. For each dimension, encode the lowest quality once. Move smaller immediately when it exceeds the budget. At the first viable dimension, binary-search indices for the earliest/highest quality that fits and return immutable bytes. Close every resized candidate promptly after its final encode attempt; never close the caller-owned source image. Raise `ImageBudgetError` after the minimum dimension fails.

- [ ] **Step 4: Add real-Pillow budget and aspect-ratio tests**

```python
def test_real_encoder_stays_under_budget_and_preserves_aspect_ratio() -> None:
    image = Image.effect_noise((1280, 720), 80).convert("RGB")
    result = encode_jpeg_under_budget(
        image,
        max_bytes=300_000,
        initial_max_dimension=960,
        min_dimension=320,
        dimension_scale=0.85,
        qualities=(85, 75, 65, 55, 45, 35),
        resampling=Image.Resampling.LANCZOS,
    )
    assert len(result.data) <= 300_000
    assert result.width / result.height == pytest.approx(16 / 9, rel=0.02)
    assert result.quality in {85, 75, 65, 55, 45, 35}
```

- [ ] **Step 5: Run and commit the helper**

Run: `python3 -m pytest tests/test_image_budget.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/image_budget.py tests/test_image_budget.py
git commit -m "perf: add bounded JPEG encoder"
```

### Task 2: Migrate Matrix Alert Snapshot Resizing

**Files:**
- Modify: `parking_spot_monitor/matrix_snapshots.py:45-97`
- Modify: `tests/test_matrix.py:511-620`
- Modify: `tests/test_matrix_outbox_delivery.py`

**Interfaces:**
- Consumes: `encode_jpeg_under_budget`
- Preserves: `_matrix_snapshot_upload(snapshot: MatrixSnapshot, *, logger: StructuredLogger | None) -> dict[str, Any]`

- [ ] **Step 1: Add a failing attempt-ceiling integration test**

```python
def test_matrix_snapshot_resize_uses_shared_encoder(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "large.jpg"
    Image.effect_noise((1280, 720), 80).convert("RGB").save(source, "JPEG", quality=95)
    seen: dict[str, object] = {}

    def fake_encoder(image, **kwargs):
        seen.update(kwargs)
        return JpegBudgetResult(b"jpeg", 640, 360, 65, 6)

    monkeypatch.setattr(matrix_snapshots, "encode_jpeg_under_budget", fake_encoder)
    data, info = matrix_snapshots._resize_jpeg_for_matrix_upload(source)
    assert data == b"jpeg"
    assert info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert seen["max_bytes"] == MAX_MATRIX_UPLOAD_IMAGE_BYTES
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_matrix.py tests/test_matrix_outbox_delivery.py -k 'resize or oversized' -q`

Expected: FAIL because the caller still owns nested loops.

- [ ] **Step 3: Replace local loops and duplicate dimension helper**

Open the source with Pillow, call `draft("RGB", bounded_size)` before loading JPEG data, convert once only when required, and delegate selection. Include `attempts` and `quality` in safe resize logs. Delete the local `_bounded_dimensions` when no caller remains.

- [ ] **Step 4: Run Matrix image regressions and commit**

Run: `python3 -m pytest tests/test_matrix.py tests/test_matrix_outbox_delivery.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/matrix_snapshots.py tests/test_matrix.py tests/test_matrix_outbox_delivery.py
git commit -m "perf: share Matrix JPEG budgeting"
```

### Task 3: Migrate Operator Who Snapshot Resizing

**Files:**
- Modify: `parking_spot_monitor/operator_cockpit_snapshots.py:242-308`
- Modify: `tests/test_matrix_operator_cockpit.py:998-1075`

**Interfaces:**
- Consumes: `encode_jpeg_under_budget`
- Preserves: `LatestSnapshotValidation` and `who_latest.jpg`

- [ ] **Step 1: Add a failing shared-encoder contract test**

```python
def test_who_resize_uses_shared_budget_encoder(monkeypatch, tmp_path: Path) -> None:
    source = _write_test_jpeg(tmp_path / "latest.jpg", size=(1280, 720))
    monkeypatch.setattr(
        operator_cockpit_snapshots,
        "encode_jpeg_under_budget",
        lambda image, **kwargs: JpegBudgetResult(b"jpeg", 640, 360, 65, 5),
    )
    result = operator_cockpit_snapshots._resize_who_snapshot_for_matrix(
        source,
        destination=tmp_path / "who_latest.jpg",
        now=datetime.now(timezone.utc),
        logger=None,
    )
    assert result.info == {"mimetype": "image/jpeg", "size": 4, "w": 640, "h": 360}
    assert result.path.read_bytes() == b"jpeg"
```

- [ ] **Step 2: Run focused operator tests and verify RED**

Run: `python3 -m pytest tests/test_matrix_operator_cockpit.py -k 'who.*resize or shared_budget' -q`

Expected: FAIL because the operator module still owns its encoding loop.

- [ ] **Step 3: Delegate to the shared helper and atomically publish**

Use the operator constants as explicit helper arguments. Write result bytes through a temporary sibling and `os.replace`, preserving the prior valid `who_latest.jpg` on failure. Delete the duplicate local dimension helper.

- [ ] **Step 4: Run cockpit tests and commit**

Run: `python3 -m pytest tests/test_matrix_operator_cockpit.py tests/test_matrix.py -k 'who or snapshot' -q`

Expected: PASS.

```bash
git add parking_spot_monitor/operator_cockpit_snapshots.py tests/test_matrix_operator_cockpit.py
git commit -m "perf: share operator JPEG budgeting"
```

### Task 4: Draw Debug Overlays Without Full-Frame Composites

**Files:**
- Modify: `parking_spot_monitor/debug_overlay.py:95-122`
- Modify: `tests/test_debug_overlay.py:16-92`

**Interfaces:**
- Preserves: `_draw_spot_overlay(image, spot_items) -> None`
- Removes: separate RGBA canvas/overlay/composite allocations

- [ ] **Step 1: Add a failing allocation-boundary test**

```python
def test_spot_overlay_does_not_allocate_full_frame_overlay(monkeypatch) -> None:
    image = Image.new("RGB", (1458, 806), (20, 30, 40))
    original_new = Image.new

    def guarded_new(mode, size, *args, **kwargs):
        if mode == "RGBA" and size == image.size:
            raise AssertionError("full-frame overlay allocation")
        return original_new(mode, size, *args, **kwargs)

    monkeypatch.setattr(Image, "new", guarded_new)
    _draw_spot_overlay(image, _configured_spots(load_example_settings()))
```

- [ ] **Step 2: Run the test and verify RED**

Run: `python3 -m pytest tests/test_debug_overlay.py -k does_not_allocate -q`

Expected: FAIL at the current full-frame `Image.new("RGBA", canvas.size, (0, 0, 0, 0))`.

- [ ] **Step 3: Draw directly with RGBA blending**

```python
def _draw_spot_overlay(image: Image.Image, spot_items) -> None:
    draw = ImageDraw.Draw(image, "RGBA")
    for spot_id, spot in spot_items:
        points = [(point.x, point.y) for point in spot.polygon]
        style = styles.get(spot_id, fallback)
        draw.polygon(points, fill=style["fill"], outline=style["outline"])
        draw.line(points + [points[0]], fill=style["outline"], width=5, joint="curve")
        draw.text((points[0][0] + 6, points[0][1] + 6), spot_id, fill=style["outline"])
```

Ensure the caller converts the loaded source to one RGB working image before drawing.

- [ ] **Step 4: Run overlay/startup tests and commit**

Run: `python3 -m pytest tests/test_debug_overlay.py tests/test_startup.py -k 'overlay' -q`

Expected: PASS.

```bash
git add parking_spot_monitor/debug_overlay.py tests/test_debug_overlay.py
git commit -m "perf: draw overlays in place"
```

### Task 5: Add Explicit In-Memory Crop Inference

**Files:**
- Modify: `parking_spot_monitor/detection.py:1-200`
- Modify: `parking_spot_monitor/runtime_detection.py:1-169`
- Modify: `tests/test_detection.py:300-430`
- Modify: `tests/test_startup.py:330-430`

**Interfaces:**
- Produces: runtime-checkable `InMemoryDetector.detect_image(image: object, *, confidence_threshold: float | None, inference_image_size: int | None) -> list[VehicleDetection]`
- Preserves: `UltralyticsVehicleDetector.detect(frame_path, *, confidence_threshold=None, inference_image_size=None)`
- Preserves: bounded temporary-JPEG fallback for detectors without `InMemoryDetector`

- [ ] **Step 1: Write failing adapter and runtime crop tests**

```python
def test_ultralytics_detector_accepts_in_memory_image() -> None:
    detector = UltralyticsVehicleDetector("model.pt", yolo_class=FakeYOLO)
    image = Image.new("RGB", (32, 24))
    detector.detect_image(image, confidence_threshold=0.2, inference_image_size=320)
    assert detector._model.predict_calls == [  # type: ignore[attr-defined]
        {"source": image, "verbose": False, "conf": 0.2, "imgsz": 320}
    ]

def test_spot_crop_inference_uses_no_temporary_files_for_in_memory_detector(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        Path("config.yaml.example").read_text(encoding="utf-8").replace(
            "spot_crop_inference: false", "spot_crop_inference: true"
        ),
        encoding="utf-8",
    )
    frame = tmp_path / "latest.jpg"
    Image.new("RGB", (1458, 806), (20, 30, 40)).save(frame, "JPEG")
    class RecordingInMemoryDetector:
        def __init__(self):
            self.image_sizes: list[tuple[int, int]] = []
        def detect(self, frame_path, **kwargs):
            return []
        def detect_image(self, image, **kwargs):
            self.image_sizes.append(image.size)
            if image.size == (531, 296):
                return [VehicleDetection("car", 0.88, (98, 93, 483, 233))]
            return []
    detector = RecordingInMemoryDetector()
    monkeypatch.setattr(tempfile, "TemporaryDirectory", lambda *a, **k: pytest.fail("temp dir"))
    result = _process_detection_for_capture(
        load_settings(config_path, environ=fake_environ()),
        detector,
        frame,
        logger=StructuredLogger(),
        mode="test",
        frame_geometry=FrameGeometry(stream_profile="primary", expected_size=(1458, 806)),
    )
    assert detector.image_sizes == [(526, 276), (531, 296)]
    assert result.by_spot["right_spot"].accepted.bbox == pytest.approx((1010, 215, 1395, 355))
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python3 -m pytest tests/test_detection.py tests/test_startup.py -k 'in_memory or spot_crop' -q`

Expected: FAIL because `detect_image` and the protocol do not exist.

- [ ] **Step 3: Share prediction normalization inside the adapter**

Extract `_predict(source, confidence_threshold, inference_image_size, frame_label)`. Path detection passes the safe string and includes it in diagnostics. Image detection passes the Pillow object and uses `frame_label="<in-memory-image>"`, never serializing image contents.

- [ ] **Step 4: Use protocol-based optimized path with explicit fallback**

```python
@runtime_checkable
class InMemoryDetector(Protocol):
    def detect_image(
        self,
        image: object,
        *,
        confidence_threshold: float | None = None,
        inference_image_size: int | None = None,
    ) -> list[VehicleDetection]:
        raise NotImplementedError
```

Open the frame once. For each spot, use `with image.crop(box) as crop`. If `isinstance(detector, InMemoryDetector)`, call `detect_image`; otherwise retain the existing temporary directory and path call. Replace repeated `inspect.signature` with a weak-key one-entry-per-detector capability cache for the path method. If a detector cannot be weak-referenced, catch `TypeError` and recompute capability without caching rather than retaining it strongly.

- [ ] **Step 5: Run detector/runtime tests and commit**

Run: `python3 -m pytest tests/test_detection.py tests/test_startup.py tests/test_runtime_stream_escalation.py -q`

Expected: PASS.

```bash
git add parking_spot_monitor/detection.py parking_spot_monitor/runtime_detection.py tests/test_detection.py tests/test_startup.py
git commit -m "perf: infer spot crops in memory"
```

### Task 6: Verify Slice 3 as an Independent Deliverable

**Files:**
- No planned modifications

**Interfaces:**
- Verifies all interfaces produced by Tasks 1-5

- [ ] **Step 1: Run image and detector regression suites**

Run: `python3 -m pytest tests/test_image_budget.py tests/test_debug_overlay.py tests/test_detection.py tests/test_matrix.py tests/test_matrix_outbox_delivery.py tests/test_matrix_operator_cockpit.py tests/test_startup.py -q`

Expected: PASS.

- [ ] **Step 2: Run full and structural verification**

Run: `python3 -m pytest -q && python3 -m compileall -q parking_spot_monitor src scripts tests && git diff --check`

Expected: all commands exit 0.

- [ ] **Step 3: Record resource evidence**

Record the shared encoder's maximum attempt count on the noisy 1280×720 fixture, verify no full-frame RGBA allocation, and verify zero crop temporary files for `UltralyticsVehicleDetector`. Do not create an empty verification commit.
