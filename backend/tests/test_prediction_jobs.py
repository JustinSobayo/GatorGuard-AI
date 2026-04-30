from datetime import date

from backend.jobs.generate_daily_predictions import calculate_prediction_rows
from backend.jobs.generate_grid_cells import generate_grid_cells


def test_generate_grid_cells_creates_stable_parseable_ids():
    cells = generate_grid_cells(
        bbox=(29.6500, -82.3300, 29.6550, -82.3250),
        cell_size_meters=500,
    )

    assert cells
    assert cells[0]["grid_id"].startswith("grid_")
    assert "_" in cells[0]["grid_id"].replace("grid_", "")
    assert cells[0]["polygon_wkt"].startswith("POLYGON((")


def test_calculate_prediction_rows_normalizes_counts_and_levels():
    aggregates = [
        {
            "grid_id": "grid_29.65000_-82.32000",
            "same_day_count": 10,
            "recent_30_day_count": 5,
            "nearby_poi_score": 0.0,
            "dominant_crime_type": "theft",
        },
        {
            "grid_id": "grid_29.66000_-82.33000",
            "same_day_count": 0,
            "recent_30_day_count": 0,
            "nearby_poi_score": 0.0,
            "dominant_crime_type": None,
        },
    ]

    rows = calculate_prediction_rows(
        aggregates=aggregates,
        prediction_date=date(2026, 4, 30),
        day_of_week="Thursday",
        data_snapshot_version="snapshot-1",
    )

    assert len(rows) == 2
    assert rows[0]["risk_score"] == 0.8
    assert rows[0]["risk_level"] == "high"
    assert rows[0]["prediction_window"] == "Next 24 hours"
    assert rows[1]["risk_score"] == 0.0
    assert rows[1]["risk_level"] == "low"
    assert rows[1]["dominant_crime_type"] == "unknown"
