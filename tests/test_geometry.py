from __future__ import annotations

from pathlib import Path

import pytest

from parking_spot_monitor.config import Point, load_settings
from parking_spot_monitor.geometry import (
    bbox_area,
    bbox_centroid,
    bbox_polygon_intersection_area,
    bbox_polygon_overlap_ratio,
    point_in_polygon,
    polygon_area,
)
from tests.test_config import fake_environ


LEFT_SPOT = [(300, 180), (610, 160), (690, 285), (420, 360), (260, 300)]
RIGHT_SPOT = [(1010, 155), (1395, 170), (1395, 355), (1040, 370), (960, 250)]
DRIVEWAY_BBOX = (260, 330, 940, 806)
DRIVEWAY_CENTROID = (600, 568)


def test_example_config_uses_m001_camera_dimensions_and_street_polygons() -> None:
    settings = load_settings("config.yaml.example", environ=fake_environ())

    assert settings.stream.frame_width == 1458
    assert settings.stream.frame_height == 806
    assert [(point.x, point.y) for point in settings.spots.left_spot.polygon] == LEFT_SPOT
    assert [(point.x, point.y) for point in settings.spots.right_spot.polygon] == RIGHT_SPOT


def test_synthetic_camera_fixture_matches_config_dimensions(tmp_path: Path) -> None:
    import struct

    from PIL import Image

    settings = load_settings("config.yaml.example", environ=fake_environ())
    fixture_path = tmp_path / "camera.png"
    Image.new("RGB", (settings.stream.frame_width, settings.stream.frame_height), (20, 30, 40)).save(fixture_path, format="PNG")
    png_header = fixture_path.read_bytes()[:24]
    width, height = struct.unpack(">II", png_header[16:24])

    assert png_header.startswith(b"\x89PNG\r\n\x1a\n")
    assert (width, height) == (settings.stream.frame_width, settings.stream.frame_height)


@pytest.mark.parametrize("point", [(5, 5), (0, 0), (10, 5), (5, 0), Point(x=0, y=5)])
def test_point_in_polygon_treats_interior_edges_and_vertices_as_inside(point: tuple[int, int] | Point) -> None:
    square = [(0, 0), (10, 0), (10, 10), (0, 10)]

    assert point_in_polygon(point, square) is True


@pytest.mark.parametrize("point", [(-1, 5), (5, -1), (11, 5), (5, 11)])
def test_point_in_polygon_rejects_points_outside_polygon_even_inside_frame(point: tuple[int, int]) -> None:
    square = [(0, 0), (10, 0), (10, 10), (0, 10)]

    assert point_in_polygon(point, square) is False


def test_bbox_centroid_and_area_use_x_min_y_min_x_max_y_max_order() -> None:
    bbox = (2, 4, 12, 10)

    assert bbox_centroid(bbox) == (7.0, 7.0)
    assert bbox_area(bbox) == 60.0


@pytest.mark.parametrize("bbox", [(0, 0, 0, 10), (0, 0, 10, 0), (10, 0, 0, 10), (0, 10, 10, 0), (0, 0, 1)])
def test_invalid_or_degenerate_bboxes_raise_value_error(bbox: tuple[int, ...]) -> None:
    with pytest.raises(ValueError, match="bbox"):
        bbox_area(bbox)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="bbox"):
        bbox_centroid(bbox)  # type: ignore[arg-type]


@pytest.mark.parametrize("polygon", [[], [(0, 0)], [(0, 0), (1, 1)]])
def test_invalid_polygons_raise_value_error(polygon: list[tuple[int, int]]) -> None:
    with pytest.raises(ValueError, match="polygon"):
        point_in_polygon((0, 0), polygon)

    with pytest.raises(ValueError, match="polygon"):
        polygon_area(polygon)


def test_polygon_area_uses_absolute_shoelace_area() -> None:
    clockwise = [(0, 0), (0, 10), (10, 10), (10, 0)]
    counter_clockwise = list(reversed(clockwise))

    assert polygon_area(clockwise) == 100.0
    assert polygon_area(counter_clockwise) == 100.0


def test_bbox_polygon_intersection_area_handles_no_partial_and_full_overlap() -> None:
    square = [(0, 0), (10, 0), (10, 10), (0, 10)]

    assert bbox_polygon_intersection_area((20, 20, 30, 30), square) == 0.0
    assert bbox_polygon_intersection_area((5, 5, 15, 15), square) == 25.0
    assert bbox_polygon_intersection_area((0, 0, 10, 10), square) == 100.0


def test_bbox_polygon_overlap_ratio_is_intersection_area_divided_by_bbox_area() -> None:
    square = [(0, 0), (10, 0), (10, 10), (0, 10)]
    bbox = (5, 5, 15, 15)

    assert bbox_polygon_overlap_ratio(bbox, square) == pytest.approx(25.0 / 100.0)


def test_driveway_car_centroid_is_not_inside_configured_street_spots() -> None:
    assert point_in_polygon(DRIVEWAY_CENTROID, LEFT_SPOT) is False
    assert point_in_polygon(DRIVEWAY_CENTROID, RIGHT_SPOT) is False


def test_driveway_car_bbox_has_no_meaningful_overlap_with_configured_street_spots() -> None:
    assert bbox_polygon_overlap_ratio(DRIVEWAY_BBOX, LEFT_SPOT) < 0.01
    assert bbox_polygon_overlap_ratio(DRIVEWAY_BBOX, RIGHT_SPOT) == 0.0
