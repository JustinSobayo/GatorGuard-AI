"""
Generate cached daily grid risk predictions from PostGIS crime facts.

Run after crime ingestion and grid generation:
    python -m backend.jobs.generate_daily_predictions
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Any

from psycopg2.extras import RealDictCursor

try:
    from backend.postgis_ops import (
        DEFAULT_MODEL_VERSION,
        ensure_prediction_schema,
        get_connection,
        upsert_daily_predictions,
    )
except ImportError:  # pragma: no cover - supports direct execution from backend/
    from postgis_ops import (  # type: ignore
        DEFAULT_MODEL_VERSION,
        ensure_prediction_schema,
        get_connection,
        upsert_daily_predictions,
    )

PREDICTION_WINDOW = "Next 24 hours"


def risk_level_for_score(score: float) -> str:
    """Map a normalized score into the frontend risk bands."""
    if score >= 0.67:
        return "high"
    if score >= 0.34:
        return "medium"
    return "low"


def _normalize(value: int, maximum: int) -> float:
    if maximum <= 0:
        return 0.0
    return min(value / maximum, 1.0)


def calculate_prediction_rows(
    aggregates: list[dict[str, Any]],
    prediction_date: date,
    day_of_week: str,
    data_snapshot_version: str,
    model_version: str = DEFAULT_MODEL_VERSION,
) -> list[dict[str, Any]]:
    """
    Convert PostGIS aggregate counts into prediction table rows.

    Formula:
        0.5 * same-day historical count
      + 0.3 * recent 30-day count
      + 0.2 * nearby POI score

    The counts are normalized across the generated grid for the selected date.
    """
    max_same_day = max((row.get("same_day_count") or 0 for row in aggregates), default=0)
    max_recent = max((row.get("recent_30_day_count") or 0 for row in aggregates), default=0)

    predictions = []
    for row in aggregates:
        same_day_count = int(row.get("same_day_count") or 0)
        recent_count = int(row.get("recent_30_day_count") or 0)
        poi_score = float(row.get("nearby_poi_score") or 0.0)

        score = (
            0.5 * _normalize(same_day_count, max_same_day)
            + 0.3 * _normalize(recent_count, max_recent)
            + 0.2 * min(max(poi_score, 0.0), 1.0)
        )
        score = round(min(max(score, 0.0), 1.0), 4)

        predictions.append(
            {
                "grid_id": row["grid_id"],
                "prediction_date": prediction_date,
                "prediction_window": PREDICTION_WINDOW,
                "day_of_week": day_of_week,
                "time_window_start": None,
                "time_window_end": None,
                "risk_score": score,
                "risk_level": risk_level_for_score(score),
                "dominant_crime_type": row.get("dominant_crime_type") or "unknown",
                "historical_count": same_day_count,
                "recent_30_day_count": recent_count,
                "nearby_poi_score": poi_score,
                "model_version": model_version,
                "data_snapshot_version": data_snapshot_version,
            }
        )

    return predictions


def fetch_grid_aggregates(target_date: date, conn: Any) -> list[dict[str, Any]]:
    """Ask PostGIS for crime counts by grid cell."""
    day_of_week = target_date.strftime("%A")
    query = """
        WITH snapshot AS (
            SELECT COALESCE(MAX(offense_date)::date, %s::date) AS reference_date
            FROM historical_crimes
        )
        SELECT
            g.grid_id,
            COUNT(h.id)::integer AS total_historical_count,
            COUNT(h.id) FILTER (
                WHERE h.offense_day_of_week = %s
            )::integer AS same_day_count,
            COUNT(h.id) FILTER (
                WHERE h.offense_date >= (s.reference_date - INTERVAL '30 days')
                  AND h.offense_date < (s.reference_date + INTERVAL '1 day')
            )::integer AS recent_30_day_count,
            COALESCE((
                SELECT h2.incident_type
                FROM historical_crimes h2
                WHERE h2.geometry IS NOT NULL
                  AND ST_Contains(g.geom, h2.geometry)
                  AND h2.incident_type IS NOT NULL
                GROUP BY h2.incident_type
                ORDER BY COUNT(*) DESC, h2.incident_type
                LIMIT 1
            ), 'unknown') AS dominant_crime_type,
            0.0::double precision AS nearby_poi_score
        FROM grid_cells g
        CROSS JOIN snapshot s
        LEFT JOIN historical_crimes h
          ON h.geometry IS NOT NULL
         AND ST_Contains(g.geom, h.geometry)
        GROUP BY g.grid_id, g.geom
        ORDER BY g.grid_id
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (target_date, day_of_week))
        return [dict(row) for row in cur.fetchall()]


def get_data_snapshot_version(conn: Any) -> str:
    """Return a lightweight input-data version string for cache keys."""
    query = """
        SELECT COALESCE(
            MAX(updated_at)::text,
            MAX(offense_date)::text,
            NOW()::text
        ) AS snapshot_version
        FROM historical_crimes
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        row = cur.fetchone()
        return row["snapshot_version"] if row and row["snapshot_version"] else datetime.utcnow().isoformat()


def run(
    prediction_date: date | None = None,
    model_version: str = DEFAULT_MODEL_VERSION,
) -> int:
    """Generate and upsert daily prediction rows."""
    target_date = prediction_date or date.today()
    day_of_week = target_date.strftime("%A")

    ensure_prediction_schema()
    with get_connection() as conn:
        aggregates = fetch_grid_aggregates(target_date, conn)
        if not aggregates:
            return 0

        snapshot_version = get_data_snapshot_version(conn)
        rows = calculate_prediction_rows(
            aggregates=aggregates,
            prediction_date=target_date,
            day_of_week=day_of_week,
            data_snapshot_version=snapshot_version,
            model_version=model_version,
        )
        return upsert_daily_predictions(rows, conn=conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily grid predictions.")
    parser.add_argument("--date", dest="prediction_date", help="Prediction date in YYYY-MM-DD format.")
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    args = parser.parse_args()

    target_date = (
        datetime.strptime(args.prediction_date, "%Y-%m-%d").date()
        if args.prediction_date
        else date.today()
    )
    count = run(prediction_date=target_date, model_version=args.model_version)
    print(f"Generated/upserted {count} daily prediction rows for {target_date.isoformat()}.")


if __name__ == "__main__":
    main()
