from __future__ import annotations

from dataclasses import FrozenInstanceError
from io import BytesIO
from typing import Any

import pytest
from PIL import Image

from parking_spot_monitor import image_budget
from parking_spot_monitor.image_budget import ImageBudgetError, encode_jpeg_under_budget


RESAMPLING = Image.Resampling.LANCZOS


def _encode(
    image: object,
    *,
    max_bytes: int = 100,
    initial_max_dimension: int = 100,
    min_dimension: int = 40,
    dimension_scale: float = 0.8,
    qualities: object = (85, 70, 40),
) -> image_budget.JpegBudgetResult:
    return encode_jpeg_under_budget(
        image,
        max_bytes=max_bytes,
        initial_max_dimension=initial_max_dimension,
        min_dimension=min_dimension,
        dimension_scale=dimension_scale,
        qualities=qualities,
        resampling=RESAMPLING,
    )


def test_encoder_selects_largest_dimension_then_highest_quality(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[tuple[tuple[int, int], int]] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempts.append((image.size, quality))
        sizes = {
            ((100, 50), 40): 120,
            ((80, 40), 40): 80,
            ((80, 40), 70): 95,
            ((80, 40), 85): 110,
        }
        buffer.write(b"x" * sizes[(image.size, quality)])

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (100, 50)) as source:
        result = _encode(source)

    assert (result.width, result.height, result.quality) == (80, 40, 70)
    assert result.attempts == 4
    assert attempts == [
        ((100, 50), 40),
        ((80, 40), 40),
        ((80, 40), 70),
        ((80, 40), 85),
    ]
    assert len(result.data) == 95


def test_encoder_copies_only_viable_payloads_from_promptly_released_buffer_views(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[int] = []
    copied_sizes: list[int] = []
    exported_views: list[memoryview] = []

    class StrictBytesIO(BytesIO):
        def getvalue(self) -> bytes:
            raise AssertionError("getvalue may share reusable buffer storage")

        def getbuffer(self) -> memoryview:
            view = super().getbuffer()
            copied_sizes.append(view.nbytes)
            exported_views.append(view)
            return view

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempts.append(quality)
        payloads = {
            40: b"l" * 80,
            70: b"m" * 95,
            85: b"h" * 110,
        }
        buffer.write(payloads[quality])

    monkeypatch.setattr(image_budget, "BytesIO", StrictBytesIO)
    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (80, 40)) as source:
        result = _encode(source, initial_max_dimension=80)

    assert attempts == [40, 70, 85]
    assert result.attempts == 3
    assert result.quality == 70
    assert result.data == b"m" * 95
    assert isinstance(result.data, bytes)
    assert copied_sizes == [80, 95]
    for view in exported_views:
        with pytest.raises(ValueError, match="released memoryview"):
            _ = view.nbytes


def test_encoder_uses_exact_initial_dominant_dimension_when_float_scaling_would_round_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted_sizes: list[tuple[int, int]] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempted_sizes.append(image.size)
        buffer.write(b"fits")

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (581, 337)) as source:
        result = _encode(
            source,
            max_bytes=10,
            initial_max_dimension=320,
            min_dimension=160,
            qualities=(40,),
        )

    assert (result.width, result.height) == (320, 185)
    assert attempted_sizes == [(320, 185)]


def test_encoder_uses_exact_terminal_minimum_dominant_dimension_when_float_scaling_would_round_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted_sizes: list[tuple[int, int]] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempted_sizes.append(image.size)
        buffer.write(b"x" * (101 if max(image.size) > 320 else 80))

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (337, 581)) as source:
        result = _encode(
            source,
            initial_max_dimension=400,
            min_dimension=320,
            dimension_scale=0.8,
            qualities=(40,),
        )

    assert (result.width, result.height) == (185, 320)
    assert attempted_sizes == [(232, 400), (185, 320)]


def test_encoder_does_not_materialize_immutable_payloads_for_oversize_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copied_sizes: list[int] = []

    class RecordingBytesIO(BytesIO):
        def getvalue(self) -> bytes:
            raise AssertionError("getvalue may share reusable buffer storage")

        def getbuffer(self) -> memoryview:
            view = super().getbuffer()
            copied_sizes.append(view.nbytes)
            return view

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        buffer.write(b"x" * 101)

    monkeypatch.setattr(image_budget, "BytesIO", RecordingBytesIO)
    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (100, 50)) as source:
        with pytest.raises(ImageBudgetError):
            _encode(source, qualities=(40,))

    assert copied_sizes == []


def test_encoder_reuses_one_reset_buffer_for_every_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    buffers: list[Any] = []
    starting_offsets: list[int] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        buffers.append(buffer)
        starting_offsets.append(buffer.tell())
        assert not buffer.closed
        buffer.write(b"x" * 101)

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (100, 50)) as source:
        with pytest.raises(ImageBudgetError, match="could not be encoded under byte budget"):
            _encode(source)

    assert all(buffer is buffers[0] for buffer in buffers)
    assert starting_offsets == [0, 0, 0, 0, 0]


def test_encoder_normalizes_qualities_to_unique_descending_values(monkeypatch: pytest.MonkeyPatch) -> None:
    attempted_qualities: list[int] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempted_qualities.append(quality)
        buffer.write(b"x" * {40: 80, 70: 95, 85: 110}[quality])

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (80, 40)) as source:
        result = _encode(source, initial_max_dimension=80, qualities=(40, 85, 70, 85, 40))

    assert result.quality == 70
    assert result.attempts == 3
    assert attempted_qualities == [40, 70, 85]


def test_encoder_does_not_upscale_source_smaller_than_initial_maximum(monkeypatch: pytest.MonkeyPatch) -> None:
    attempted_images: list[Image.Image] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempted_images.append(image)
        buffer.write(b"small")

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (64, 32)) as source:
        result = _encode(
            source,
            max_bytes=10,
            initial_max_dimension=960,
            min_dimension=320,
            qualities=(75,),
        )
        assert attempted_images == [source]
        assert source.getpixel((0, 0)) == (0, 0, 0)

    assert (result.width, result.height) == (64, 32)


def test_encoder_closes_resized_candidates_but_not_caller_image(monkeypatch: pytest.MonkeyPatch) -> None:
    source = Image.new("RGB", (100, 50))
    resized_candidates: list[Image.Image] = []
    original_resize = Image.Image.resize

    def recording_resize(self: Image.Image, *args: object, **kwargs: object) -> Image.Image:
        candidate = original_resize(self, *args, **kwargs)
        resized_candidates.append(candidate)
        return candidate

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        buffer.write(b"x" * (120 if image.size == (100, 50) else 80))

    monkeypatch.setattr(Image.Image, "resize", recording_resize)
    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)

    result = _encode(source, qualities=(40,))

    assert (result.width, result.height) == (80, 40)
    assert source.getpixel((0, 0)) == (0, 0, 0)
    assert len(resized_candidates) == 1
    with pytest.raises(ValueError, match="closed image"):
        resized_candidates[0].getpixel((0, 0))
    source.close()


def test_encoder_attempts_minimum_dimension_once_before_safe_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    attempted_sizes: list[tuple[int, int]] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempted_sizes.append(image.size)
        buffer.write(b"x" * 101)

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (5, 3)) as source:
        with pytest.raises(ImageBudgetError, match="^image could not be encoded under byte budget$"):
            _encode(
                source,
                initial_max_dimension=5,
                min_dimension=3,
                dimension_scale=0.1,
                qualities=(40,),
            )

    assert attempted_sizes == [(5, 3), (3, 1)]


def test_encoder_forces_dimension_progress_when_scaling_rounds_to_same_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted_sizes: list[tuple[int, int]] = []

    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        attempted_sizes.append(image.size)
        buffer.write(b"x" * 101)

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (3, 2)) as source:
        with pytest.raises(ImageBudgetError):
            _encode(
                source,
                initial_max_dimension=3,
                min_dimension=1,
                dimension_scale=0.999,
                qualities=(40,),
            )

    assert attempted_sizes == [(3, 2), (2, 1), (1, 1)]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"max_bytes": 0}, "max_bytes must be a positive integer"),
        ({"max_bytes": True}, "max_bytes must be a positive integer"),
        ({"initial_max_dimension": 0}, "initial_max_dimension must be a positive integer"),
        ({"min_dimension": 0}, "min_dimension must be a positive integer"),
        (
            {"initial_max_dimension": 39, "min_dimension": 40},
            "initial_max_dimension must be greater than or equal to min_dimension",
        ),
        ({"dimension_scale": 0}, "dimension_scale must be greater than 0 and less than 1"),
        ({"dimension_scale": 1}, "dimension_scale must be greater than 0 and less than 1"),
        ({"dimension_scale": float("nan")}, "dimension_scale must be greater than 0 and less than 1"),
        ({"qualities": ()}, "qualities must contain at least one quality"),
        ({"qualities": (85, True)}, "qualities must contain integers from 1 through 100"),
        ({"qualities": (85, 0)}, "qualities must contain integers from 1 through 100"),
        ({"qualities": (85, 101)}, "qualities must contain integers from 1 through 100"),
    ],
)
def test_encoder_rejects_invalid_parameters(overrides: dict[str, object], message: str) -> None:
    defaults: dict[str, object] = {
        "max_bytes": 100,
        "initial_max_dimension": 100,
        "min_dimension": 40,
        "dimension_scale": 0.8,
        "qualities": (85, 70, 40),
    }
    defaults.update(overrides)
    with Image.new("RGB", (100, 50)) as source:
        with pytest.raises(ImageBudgetError, match=f"^{message}$"):
            _encode(source, **defaults)


def test_encoder_rejects_invalid_source_dimensions() -> None:
    class InvalidImage:
        size = (0, 50)

    with pytest.raises(ImageBudgetError, match="^image dimensions must be positive integers$"):
        _encode(InvalidImage())


def test_result_is_frozen_slotted_and_owns_immutable_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_encode(image: Image.Image, buffer: Any, quality: int) -> None:
        buffer.write(bytearray(b"jpeg"))

    monkeypatch.setattr(image_budget, "_encode_jpeg", fake_encode)
    with Image.new("RGB", (4, 2)) as source:
        result = _encode(
            source,
            max_bytes=4,
            initial_max_dimension=4,
            min_dimension=2,
            qualities=(75,),
        )

    assert result.data == b"jpeg"
    assert isinstance(result.data, bytes)
    assert not hasattr(result, "__dict__")
    with pytest.raises(FrozenInstanceError):
        result.quality = 50  # type: ignore[misc]


def test_real_encoder_stays_under_budget_and_preserves_aspect_ratio() -> None:
    with Image.effect_noise((1280, 720), 80).convert("RGB") as source:
        result = encode_jpeg_under_budget(
            source,
            max_bytes=300_000,
            initial_max_dimension=960,
            min_dimension=320,
            dimension_scale=0.85,
            qualities=(85, 75, 65, 55, 45, 35),
            resampling=RESAMPLING,
        )
        assert source.getpixel((0, 0)) is not None

    assert len(result.data) <= 300_000
    assert result.width / result.height == pytest.approx(16 / 9, rel=0.02)
    assert result.quality in {85, 75, 65, 55, 45, 35}
