from __future__ import annotations

from typing import Protocol, Sequence, TypeAlias, runtime_checkable


Coordinate: TypeAlias = tuple[float, float]
BBox: TypeAlias = tuple[float, float, float, float]


@runtime_checkable
class PointLike(Protocol):
    x: float
    y: float


PointInput: TypeAlias = PointLike | Sequence[float]
PolygonInput: TypeAlias = Sequence[PointInput]


def point_in_polygon(point: PointInput, polygon: PolygonInput) -> bool:
    """Return True when point is inside polygon, counting edges and vertices as inside."""
    x, y = _coerce_point(point, label="point")
    vertices = _coerce_polygon(polygon)

    inside = False
    previous = vertices[-1]
    for current in vertices:
        x1, y1 = previous
        x2, y2 = current

        if _point_on_segment((x, y), previous, current):
            return True

        crosses = (y1 > y) != (y2 > y)
        if crosses:
            x_intersection = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x_intersection == x:
                return True
            if x_intersection > x:
                inside = not inside
        previous = current

    return inside


def bbox_centroid(bbox: Sequence[float]) -> Coordinate:
    x_min, y_min, x_max, y_max = _coerce_bbox(bbox)
    return ((x_min + x_max) / 2.0, (y_min + y_max) / 2.0)


def bbox_area(bbox: Sequence[float]) -> float:
    x_min, y_min, x_max, y_max = _coerce_bbox(bbox)
    return (x_max - x_min) * (y_max - y_min)


def bbox_iou(first: Sequence[float], second: Sequence[float]) -> float:
    """Return intersection-over-union for two valid xyxy bounding boxes."""
    first_x_min, first_y_min, first_x_max, first_y_max = _coerce_bbox(first)
    second_x_min, second_y_min, second_x_max, second_y_max = _coerce_bbox(second)

    intersection_width = max(0.0, min(first_x_max, second_x_max) - max(first_x_min, second_x_min))
    intersection_height = max(0.0, min(first_y_max, second_y_max) - max(first_y_min, second_y_min))
    intersection_area = intersection_width * intersection_height
    if intersection_area == 0:
        return 0.0

    union_area = bbox_area((first_x_min, first_y_min, first_x_max, first_y_max)) + bbox_area(
        (second_x_min, second_y_min, second_x_max, second_y_max)
    ) - intersection_area
    return intersection_area / union_area


def polygon_area(polygon: PolygonInput) -> float:
    vertices = _coerce_polygon(polygon)
    return _polygon_area(vertices)


def bbox_polygon_intersection_area(bbox: Sequence[float], polygon: PolygonInput) -> float:
    x_min, y_min, x_max, y_max = _coerce_bbox(bbox)
    vertices = _coerce_polygon(polygon)

    clipped = _clip_polygon(vertices, axis="x", boundary=x_min, keep_greater=True)
    clipped = _clip_polygon(clipped, axis="x", boundary=x_max, keep_greater=False)
    clipped = _clip_polygon(clipped, axis="y", boundary=y_min, keep_greater=True)
    clipped = _clip_polygon(clipped, axis="y", boundary=y_max, keep_greater=False)

    if len(clipped) < 3:
        return 0.0
    return _polygon_area(clipped)


def bbox_polygon_overlap_ratio(bbox: Sequence[float], polygon: PolygonInput) -> float:
    area = bbox_area(bbox)
    return bbox_polygon_intersection_area(bbox, polygon) / area


def _coerce_point(point: PointInput, *, label: str) -> Coordinate:
    if isinstance(point, PointLike):
        return (float(point.x), float(point.y))
    if isinstance(point, Sequence) and not isinstance(point, (str, bytes)) and len(point) == 2:
        return (float(point[0]), float(point[1]))
    raise ValueError(f"{label} must be a Point-like object or an (x, y) pair")


def _coerce_polygon(polygon: PolygonInput) -> list[Coordinate]:
    if not isinstance(polygon, Sequence) or isinstance(polygon, (str, bytes)):
        raise ValueError("polygon must be a sequence of at least 3 points")
    if len(polygon) < 3:
        raise ValueError("polygon must contain at least 3 points")
    return [_coerce_point(point, label="polygon point") for point in polygon]


def _coerce_bbox(bbox: Sequence[float]) -> BBox:
    if not isinstance(bbox, Sequence) or isinstance(bbox, (str, bytes)) or len(bbox) != 4:
        raise ValueError("bbox must be a 4-item (x_min, y_min, x_max, y_max) sequence")
    x_min, y_min, x_max, y_max = (float(value) for value in bbox)
    if x_max <= x_min or y_max <= y_min:
        raise ValueError("bbox must satisfy x_min < x_max and y_min < y_max")
    return (x_min, y_min, x_max, y_max)


def _polygon_area(vertices: Sequence[Coordinate]) -> float:
    area = 0.0
    previous_x, previous_y = vertices[-1]
    for current_x, current_y in vertices:
        area += previous_x * current_y - current_x * previous_y
        previous_x, previous_y = current_x, current_y
    return abs(area) / 2.0


def _point_on_segment(point: Coordinate, start: Coordinate, end: Coordinate) -> bool:
    px, py = point
    x1, y1 = start
    x2, y2 = end
    cross = (py - y1) * (x2 - x1) - (px - x1) * (y2 - y1)
    if abs(cross) > 1e-9:
        return False
    return min(x1, x2) - 1e-9 <= px <= max(x1, x2) + 1e-9 and min(y1, y2) - 1e-9 <= py <= max(y1, y2) + 1e-9


def _clip_polygon(vertices: Sequence[Coordinate], *, axis: str, boundary: float, keep_greater: bool) -> list[Coordinate]:
    if not vertices:
        return []

    clipped: list[Coordinate] = []
    previous = vertices[-1]
    previous_inside = _inside_clip(previous, axis=axis, boundary=boundary, keep_greater=keep_greater)

    for current in vertices:
        current_inside = _inside_clip(current, axis=axis, boundary=boundary, keep_greater=keep_greater)
        if current_inside:
            if not previous_inside:
                clipped.append(_clip_intersection(previous, current, axis=axis, boundary=boundary))
            clipped.append(current)
        elif previous_inside:
            clipped.append(_clip_intersection(previous, current, axis=axis, boundary=boundary))
        previous = current
        previous_inside = current_inside

    return clipped


def _inside_clip(point: Coordinate, *, axis: str, boundary: float, keep_greater: bool) -> bool:
    value = point[0] if axis == "x" else point[1]
    return value >= boundary if keep_greater else value <= boundary


def _clip_intersection(start: Coordinate, end: Coordinate, *, axis: str, boundary: float) -> Coordinate:
    x1, y1 = start
    x2, y2 = end
    if axis == "x":
        if x2 == x1:
            return (boundary, y1)
        ratio = (boundary - x1) / (x2 - x1)
        return (boundary, y1 + ratio * (y2 - y1))
    if y2 == y1:
        return (x1, boundary)
    ratio = (boundary - y1) / (y2 - y1)
    return (x1 + ratio * (x2 - x1), boundary)
