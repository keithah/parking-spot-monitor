from __future__ import annotations

import math
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from parking_spot_monitor.vehicle_profiles import (
    MatchStatus,
    VehicleDescriptor,
    VehicleProfileDescriptorError,
    VehicleProfileRecord,
    descriptor_distance,
    extract_vehicle_descriptor,
    match_confidence,
    match_vehicle_profile,
)


def write_vehicle_jpeg(path: Path, color: tuple[int, int, int], *, size: tuple[int, int] = (96, 48)) -> None:
    image = Image.new("RGB", size, color)
    draw = ImageDraw.Draw(image)
    draw.rectangle((8, 10, size[0] - 8, size[1] - 10), outline=(20, 20, 20), width=2)
    draw.ellipse((18, size[1] - 16, 32, size[1] - 2), fill=(12, 12, 12))
    draw.ellipse((size[0] - 32, size[1] - 16, size[0] - 18, size[1] - 2), fill=(12, 12, 12))
    image.save(path, format="JPEG", quality=95)


def test_descriptor_extraction_is_deterministic_and_normalized_for_jpeg(tmp_path: Path) -> None:
    crop_path = tmp_path / "occupied-crop.jpg"
    write_vehicle_jpeg(crop_path, (120, 40, 40))

    first = extract_vehicle_descriptor(crop_path)
    second = extract_vehicle_descriptor(crop_path)

    assert first == second
    assert first.width == 96
    assert first.height == 48
    assert first.aspect_ratio == pytest.approx(2.0)
    assert len(first.rgb_histogram) == 64
    assert sum(first.rgb_histogram) == pytest.approx(1.0)
    assert 0 <= first.average_hash < 2**first.hash_bits
    assert descriptor_distance(first, second) == pytest.approx(0.0)
    assert match_confidence(descriptor_distance(first, second)) == pytest.approx(1.0)


def test_similar_crop_matches_separated_existing_profile(tmp_path: Path) -> None:
    query_path = tmp_path / "query.jpg"
    similar_path = tmp_path / "same-car.jpg"
    distinct_path = tmp_path / "other-car.jpg"
    write_vehicle_jpeg(query_path, (120, 40, 40))
    write_vehicle_jpeg(similar_path, (122, 42, 42))
    write_vehicle_jpeg(distinct_path, (40, 130, 210), size=(64, 80))
    query = extract_vehicle_descriptor(query_path)
    similar = extract_vehicle_descriptor(similar_path)
    distinct = extract_vehicle_descriptor(distinct_path)

    result = match_vehicle_profile(
        query,
        [
            VehicleProfileRecord("profile-same", similar, sample_count=3),
            VehicleProfileRecord("profile-other", distinct),
        ],
    )

    assert result.status == MatchStatus.MATCHED
    assert result.profile_id == "profile-same"
    assert result.confidence > 0.9
    assert result.best_candidate is not None
    assert result.best_candidate.profile_id == "profile-same"
    assert result.second_candidate is not None
    assert result.second_candidate.profile_id == "profile-other"


def test_empty_candidate_list_requests_new_profile(tmp_path: Path) -> None:
    crop_path = tmp_path / "query.jpg"
    write_vehicle_jpeg(crop_path, (120, 40, 40))
    descriptor = extract_vehicle_descriptor(crop_path)

    result = match_vehicle_profile(descriptor, [])

    assert result.status == MatchStatus.NEW_PROFILE
    assert result.profile_id is None
    assert result.reason == "no-candidates"
    assert result.distance is None
    assert result.confidence == 0.0


def test_distinct_crop_requests_new_profile_without_selecting_candidate(tmp_path: Path) -> None:
    query_path = tmp_path / "red-car.jpg"
    existing_path = tmp_path / "blue-tall-car.jpg"
    write_vehicle_jpeg(query_path, (140, 30, 30), size=(120, 50))
    write_vehicle_jpeg(existing_path, (20, 80, 210), size=(45, 95))
    query = extract_vehicle_descriptor(query_path)
    existing = extract_vehicle_descriptor(existing_path)

    result = match_vehicle_profile(query, [VehicleProfileRecord("profile-blue", existing)])

    assert result.status == MatchStatus.NEW_PROFILE
    assert result.profile_id is None
    assert result.reason == "best-candidate-distant"
    assert result.distance is not None
    assert result.distance >= 0.45
    assert 0.0 <= result.confidence <= 1.0


def test_ambiguous_near_tie_returns_unknown_profile_without_poisoning_history(tmp_path: Path) -> None:
    query_path = tmp_path / "query.jpg"
    candidate_path = tmp_path / "candidate.jpg"
    write_vehicle_jpeg(query_path, (90, 90, 90))
    write_vehicle_jpeg(candidate_path, (90, 90, 90))
    query = extract_vehicle_descriptor(query_path)
    candidate = extract_vehicle_descriptor(candidate_path)

    result = match_vehicle_profile(
        query,
        [
            VehicleProfileRecord("profile-a", candidate),
            VehicleProfileRecord("profile-b", candidate),
        ],
    )

    assert result.status == MatchStatus.AMBIGUOUS
    assert result.profile_id is None
    assert result.reason == "best-candidate-not-separated"
    assert result.best_candidate is not None
    assert result.second_candidate is not None
    assert result.best_candidate.distance == pytest.approx(result.second_candidate.distance)


def test_threshold_uncertainty_returns_unknown_without_candidate_selection(tmp_path: Path) -> None:
    query_path = tmp_path / "query.jpg"
    candidate_path = tmp_path / "candidate.jpg"
    write_vehicle_jpeg(query_path, (120, 40, 40))
    write_vehicle_jpeg(candidate_path, (40, 120, 40))
    query = extract_vehicle_descriptor(query_path)
    candidate = extract_vehicle_descriptor(candidate_path)

    result = match_vehicle_profile(
        query,
        [VehicleProfileRecord("profile-green", candidate)],
        match_distance_threshold=0.01,
        new_profile_distance_threshold=0.99,
    )

    assert result.status == MatchStatus.UNKNOWN
    assert result.profile_id is None
    assert result.reason == "best-candidate-threshold-uncertain"
    assert result.distance is not None
    assert math.isfinite(result.distance)
    assert 0.0 <= result.confidence <= 1.0


@pytest.mark.parametrize(
    ("name", "contents", "expected_reason"),
    [
        ("not-a-jpeg.txt", b"not image bytes rtsp://camera.local access_token=supersecret", "file is unreadable"),
        ("corrupt.jpg", b"not really a jpeg token=supersecret raw_image_bytes", "file is unreadable"),
    ],
)
def test_invalid_or_unreadable_inputs_raise_safe_descriptor_error(
    tmp_path: Path, name: str, contents: bytes, expected_reason: str
) -> None:
    bad_path = tmp_path / "parent-secret-token-abc123" / name
    bad_path.parent.mkdir()
    bad_path.write_bytes(contents)

    with pytest.raises(VehicleProfileDescriptorError) as exc_info:
        extract_vehicle_descriptor(bad_path)

    message = str(exc_info.value)
    assert expected_reason in message
    assert name.split(".")[0] in message
    assert str(bad_path.parent) not in message
    assert "rtsp://" not in message
    assert "supersecret" not in message
    assert "raw_image_bytes" not in message


def test_missing_file_error_uses_sanitized_basename_only(tmp_path: Path) -> None:
    missing_path = tmp_path / "secret-parent" / "access_token=supersecret-camera-crop.jpg"

    with pytest.raises(VehicleProfileDescriptorError) as exc_info:
        extract_vehicle_descriptor(missing_path)

    message = str(exc_info.value)
    assert "file is missing" in message
    assert "access_token=<redacted>" in message
    assert "supersecret" not in message
    assert str(missing_path.parent) not in message


def test_non_jpeg_image_is_rejected_even_when_pillow_can_read_it(tmp_path: Path) -> None:
    png_path = tmp_path / "crop.png"
    Image.new("RGB", (20, 20), (1, 2, 3)).save(png_path, format="PNG")

    with pytest.raises(VehicleProfileDescriptorError) as exc_info:
        extract_vehicle_descriptor(png_path)

    assert "input must be a JPEG" in str(exc_info.value)
    assert str(tmp_path) not in str(exc_info.value)


def test_confidence_and_distance_helpers_remain_finite_and_clamped() -> None:
    descriptor = VehicleDescriptor(
        width=10,
        height=10,
        aspect_ratio=1.0,
        rgb_histogram=(1.0,) + (0.0,) * 63,
        average_hash=0,
    )
    opposite = VehicleDescriptor(
        width=10,
        height=30,
        aspect_ratio=1 / 3,
        rgb_histogram=(0.0,) * 63 + (1.0,),
        average_hash=(2**64) - 1,
    )

    distance = descriptor_distance(descriptor, opposite)

    assert math.isfinite(distance)
    assert 0.0 <= distance <= 1.0
    assert match_confidence(distance) == pytest.approx(1.0 - distance)
    assert match_confidence(math.nan) == 0.0
    assert match_confidence(math.inf) == 0.0
    assert match_confidence(-math.inf) == 0.0


def test_descriptor_rejects_non_finite_histogram_values() -> None:
    descriptor = VehicleDescriptor(
        width=10,
        height=10,
        aspect_ratio=1.0,
        rgb_histogram=(math.nan,) + (0.0,) * 63,
        average_hash=0,
    )

    with pytest.raises(ValueError, match="finite"):
        descriptor_distance(descriptor, descriptor)


def test_vehicle_profiles_module_avoids_heavy_or_remote_dependencies() -> None:
    source = Path("parking_spot_monitor/vehicle_profiles.py").read_text()

    forbidden = ["numpy", "cv2", "ultralytics", "matrix", "rtsp", "httpx", "clip", "sklearn"]
    lowered = source.lower()
    for dependency in forbidden:
        assert f"import {dependency}" not in lowered
        assert f"from {dependency}" not in lowered
