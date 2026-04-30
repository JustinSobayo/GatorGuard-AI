"""
Generate fixed PostGIS grid cells for Gainesville.

Run:
    python -m backend.jobs.generate_grid_cells
"""

from __future__ import annotations

import argparse
import math
import os
from typing import Any

try:
    from backend.postgis_ops import ensure_prediction_schema, upsert_grid_cells
except ImportError:  # pragma: no cover - supports direct execution from backend/
    from postgis_ops import ensure_prediction_schema, upsert_grid_cells


DEFAULT_BBOX = (
    float(os.getenv("GAINESVILLE_BBOX_SOUTH", "29.5900")),
    float(os.getenv("GAINESVILLE_BBOX_WEST", "-82.4200")),
    float(os.getenv("GAINESVILLE_BBOX_NORTH", "29.7200")),
    float(os.getenv("GAINESVILLE_BBOX_EAST", "-82.2700")),
)
DEFAULT_CELL_SIZE_METERS = int(os.getenv("GRID_CELL_SIZE_METERS", "500"))


def _meters_to_lat_degrees(meters: int) -> float:
    return meters / 111_320.0


def _meters_to_lon_degrees(meters: int, latitude: float) -> float:
    return meters / (111_320.0 * math.cos(math.radians(latitude)))


def _polygon_wkt(west: float, south: float, east: float, north: float) -> str:
    return (
        "POLYGON(("
        f"{west:.7f} {south:.7f}, "
        f"{east:.7f} {south:.7f}, "
        f"{east:.7f} {north:.7f}, "
        f"{west:.7f} {north:.7f}, "
        f"{west:.7f} {south:.7f}"
        "))"
    )


def generate_grid_cells(
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX,
    cell_size_meters: int = DEFAULT_CELL_SIZE_METERS,
) -> list[dict[str, Any]]:
    """
    Split a bounding box into stable rectangular grid cells.

    BBox format is (south, west, north, east).
    """
    south, west, north, east = bbox
    if south >= north:
        raise ValueError("bbox south must be less than north")
    if west >= east:
        raise ValueError("bbox west must be less than east")
    if cell_size_meters <= 0:
        raise ValueError("cell_size_meters must be positive")

    avg_lat = (south + north) / 2.0
    lat_step = _meters_to_lat_degrees(cell_size_meters)
    lon_step = _meters_to_lon_degrees(cell_size_meters, avg_lat)

    cells: list[dict[str, Any]] = []
    row = 0
    current_south = south
    while current_south < north:
        current_north = min(current_south + lat_step, north)
        col = 0
        current_west = west
        while current_west < east:
            current_east = min(current_west + lon_step, east)
            center_lat = round((current_south + current_north) / 2.0, 5)
            center_lon = round((current_west + current_east) / 2.0, 5)
            grid_id = f"grid_{center_lat:.5f}_{center_lon:.5f}"

            cells.append(
                {
                    "grid_id": grid_id,
                    "center_lat": center_lat,
                    "center_lon": center_lon,
                    "cell_size_meters": cell_size_meters,
                    "row": row,
                    "col": col,
                    "polygon_wkt": _polygon_wkt(
                        current_west,
                        current_south,
                        current_east,
                        current_north,
                    ),
                }
            )

            current_west = current_east
            col += 1

        current_south = current_north
        row += 1

    return cells


def run(
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX,
    cell_size_meters: int = DEFAULT_CELL_SIZE_METERS,
) -> int:
    """Generate and upsert the Gainesville grid cells."""
    ensure_prediction_schema()
    cells = generate_grid_cells(bbox=bbox, cell_size_meters=cell_size_meters)
    return upsert_grid_cells(cells)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Gainesville grid_cells rows.")
    parser.add_argument("--south", type=float, default=DEFAULT_BBOX[0])
    parser.add_argument("--west", type=float, default=DEFAULT_BBOX[1])
    parser.add_argument("--north", type=float, default=DEFAULT_BBOX[2])
    parser.add_argument("--east", type=float, default=DEFAULT_BBOX[3])
    parser.add_argument("--cell-size-meters", type=int, default=DEFAULT_CELL_SIZE_METERS)
    args = parser.parse_args()

    count = run(
        bbox=(args.south, args.west, args.north, args.east),
        cell_size_meters=args.cell_size_meters,
    )
    print(f"Generated/upserted {count} grid cells.")


if __name__ == "__main__":
    main()
