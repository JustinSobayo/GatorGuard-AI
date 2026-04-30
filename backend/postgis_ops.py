"""
PostGIS query helpers for crime facts, grid cells, and prediction metadata.

This module is intentionally database-focused. It does not score predictions
and it does not call the LLM; it only owns Postgres/PostGIS reads, writes, and
schema setup needed by the API and scheduled jobs.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import date, time
from decimal import Decimal
from typing import Any, Iterator, Sequence

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "gainesville_crime")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

DEFAULT_MODEL_VERSION = os.getenv("PREDICTION_MODEL_VERSION", "mvp-weighted-v1")


@contextmanager
def get_connection() -> Iterator[Any]:
    """Yield a Postgres connection using project environment variables."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        yield conn
    finally:
        conn.close()


def _coerce_json_value(value: Any) -> Any:
    """Convert DB driver values into JSON-friendly Python values."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, time)):
        return value.isoformat()
    return value


def _coerce_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _coerce_json_value(value) for key, value in row.items()}


def ensure_prediction_schema(conn: Any | None = None) -> None:
    """
    Run the idempotent PostGIS schema from init_db.sql.

    The Spark processor calls this on startup so streaming ingestion and the
    prediction jobs agree on the same tables and indexes.
    """
    sql_path = os.path.join(os.path.dirname(__file__), "init_db.sql")

    def _execute(target_conn: Any) -> None:
        with open(sql_path, "r", encoding="utf-8") as sql_file:
            sql_content = sql_file.read()
        with target_conn.cursor() as cur:
            cur.execute(sql_content)
        target_conn.commit()

    if conn is not None:
        _execute(conn)
        return

    with get_connection() as managed_conn:
        _execute(managed_conn)


def get_historical_crimes(
    offense_date: date | str,
    limit: int = 1000,
    conn: Any | None = None,
) -> list[dict[str, Any]]:
    """Return cleaned crime records for one offense date."""
    query = """
        SELECT
            id,
            incident_type,
            description,
            offense_date,
            report_date,
            latitude,
            longitude,
            address,
            city,
            state,
            offense_hour,
            offense_day_of_week
        FROM historical_crimes
        WHERE offense_date::date = %s::date
        ORDER BY offense_date DESC NULLS LAST
        LIMIT %s
    """

    def _fetch(target_conn: Any) -> list[dict[str, Any]]:
        with target_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (offense_date, limit))
            return [_coerce_row(dict(row)) for row in cur.fetchall()]

    if conn is not None:
        return _fetch(conn)

    with get_connection() as managed_conn:
        return _fetch(managed_conn)


def find_grid_by_point(lat: float, lon: float, conn: Any | None = None) -> dict[str, Any] | None:
    """Find the grid cell containing a user-selected lat/lon point."""
    query = """
        SELECT grid_id, center_lat, center_lon, cell_size_meters
        FROM grid_cells
        WHERE ST_Contains(geom, ST_SetSRID(ST_Point(%s, %s), 4326))
        ORDER BY grid_id
        LIMIT 1
    """

    def _fetch(target_conn: Any) -> dict[str, Any] | None:
        with target_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (lon, lat))
            row = cur.fetchone()
            return _coerce_row(dict(row)) if row else None

    if conn is not None:
        return _fetch(conn)

    with get_connection() as managed_conn:
        return _fetch(managed_conn)


def get_grid_prediction(
    grid_id: str,
    prediction_date: date | str | None = None,
    prediction_window: str | None = None,
    model_version: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    """Return the cached prediction metadata for a grid cell."""
    prediction_date = prediction_date or date.today()
    model_version = model_version or DEFAULT_MODEL_VERSION

    filters = [
        "p.grid_id = %s",
        "p.prediction_date = %s::date",
        "p.model_version = %s",
    ]
    params: list[Any] = [grid_id, prediction_date, model_version]

    if prediction_window:
        filters.append("p.prediction_window = %s")
        params.append(prediction_window)

    query = f"""
        SELECT
            p.prediction_id,
            p.grid_id,
            p.prediction_date,
            p.prediction_window,
            p.day_of_week,
            p.time_window_start,
            p.time_window_end,
            p.risk_score,
            p.risk_level,
            p.dominant_crime_type,
            p.historical_count,
            p.recent_30_day_count,
            p.nearby_poi_score,
            p.model_version,
            p.data_snapshot_version,
            p.generated_at,
            g.center_lat,
            g.center_lon,
            g.cell_size_meters
        FROM daily_grid_predictions p
        JOIN grid_cells g ON g.grid_id = p.grid_id
        WHERE {" AND ".join(filters)}
        ORDER BY p.risk_score DESC, p.generated_at DESC
        LIMIT 1
    """

    def _fetch(target_conn: Any) -> dict[str, Any] | None:
        with target_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return _coerce_row(dict(row)) if row else None

    if conn is not None:
        return _fetch(conn)

    with get_connection() as managed_conn:
        return _fetch(managed_conn)


def get_predictions_geojson(
    prediction_date: date | str | None = None,
    prediction_window: str | None = None,
    min_risk_level: str | None = None,
    model_version: str | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Return cached prediction rows joined to grid polygons as GeoJSON."""
    prediction_date = prediction_date or date.today()
    model_version = model_version or DEFAULT_MODEL_VERSION

    risk_rank = {"low": 1, "medium": 2, "high": 3}
    filters = ["p.prediction_date = %s::date", "p.model_version = %s"]
    params: list[Any] = [prediction_date, model_version]

    if prediction_window:
        filters.append("p.prediction_window = %s")
        params.append(prediction_window)

    if min_risk_level:
        minimum_rank = risk_rank.get(min_risk_level.lower())
        if minimum_rank is not None:
            allowed_levels = [level for level, rank in risk_rank.items() if rank >= minimum_rank]
            filters.append("p.risk_level = ANY(%s)")
            params.append(allowed_levels)

    query = f"""
        SELECT
            p.grid_id,
            p.prediction_date,
            p.prediction_window,
            p.day_of_week,
            p.risk_score,
            p.risk_level,
            p.dominant_crime_type,
            p.historical_count,
            p.recent_30_day_count,
            p.nearby_poi_score,
            p.model_version,
            p.data_snapshot_version,
            p.generated_at,
            g.center_lat,
            g.center_lon,
            ST_AsGeoJSON(g.geom) AS geometry
        FROM daily_grid_predictions p
        JOIN grid_cells g ON g.grid_id = p.grid_id
        WHERE {" AND ".join(filters)}
        ORDER BY p.risk_score DESC, p.grid_id
    """

    def _fetch(target_conn: Any) -> dict[str, Any]:
        with target_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        features = []
        for row in rows:
            row_dict = _coerce_row(dict(row))
            geometry = row_dict.pop("geometry")
            features.append(
                {
                    "type": "Feature",
                    "geometry": json.loads(geometry) if isinstance(geometry, str) else geometry,
                    "properties": row_dict,
                }
            )

        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "prediction_date": prediction_date.isoformat()
                if isinstance(prediction_date, date)
                else str(prediction_date),
                "prediction_window": prediction_window,
                "model_version": model_version,
                "feature_count": len(features),
            },
        }

    if conn is not None:
        return _fetch(conn)

    with get_connection() as managed_conn:
        return _fetch(managed_conn)


def upsert_grid_cells(cells: Sequence[dict[str, Any]], conn: Any | None = None) -> int:
    """Insert or update generated grid cells."""
    if not cells:
        return 0

    values = [
        (
            cell["grid_id"],
            cell["polygon_wkt"],
            cell["center_lat"],
            cell["center_lon"],
            cell["cell_size_meters"],
        )
        for cell in cells
    ]

    query = """
        INSERT INTO grid_cells (
            grid_id, geom, center_lat, center_lon, cell_size_meters
        ) VALUES %s
        ON CONFLICT (grid_id) DO UPDATE SET
            geom = EXCLUDED.geom,
            center_lat = EXCLUDED.center_lat,
            center_lon = EXCLUDED.center_lon,
            cell_size_meters = EXCLUDED.cell_size_meters,
            updated_at = NOW()
    """
    template = "(%s, ST_GeomFromText(%s, 4326), %s, %s, %s)"

    def _write(target_conn: Any) -> int:
        with target_conn.cursor() as cur:
            execute_values(cur, query, values, template=template)
        target_conn.commit()
        return len(values)

    if conn is not None:
        return _write(conn)

    with get_connection() as managed_conn:
        return _write(managed_conn)


def upsert_daily_predictions(rows: Sequence[dict[str, Any]], conn: Any | None = None) -> int:
    """Insert or update daily prediction rows."""
    if not rows:
        return 0

    values = [
        (
            row["grid_id"],
            row["prediction_date"],
            row["prediction_window"],
            row.get("day_of_week"),
            row.get("time_window_start"),
            row.get("time_window_end"),
            row["risk_score"],
            row["risk_level"],
            row.get("dominant_crime_type"),
            row.get("historical_count", 0),
            row.get("recent_30_day_count", 0),
            row.get("nearby_poi_score", 0.0),
            row["model_version"],
            row.get("data_snapshot_version"),
        )
        for row in rows
    ]

    query = """
        INSERT INTO daily_grid_predictions (
            grid_id,
            prediction_date,
            prediction_window,
            day_of_week,
            time_window_start,
            time_window_end,
            risk_score,
            risk_level,
            dominant_crime_type,
            historical_count,
            recent_30_day_count,
            nearby_poi_score,
            model_version,
            data_snapshot_version
        ) VALUES %s
        ON CONFLICT (grid_id, prediction_date, prediction_window, model_version)
        DO UPDATE SET
            day_of_week = EXCLUDED.day_of_week,
            time_window_start = EXCLUDED.time_window_start,
            time_window_end = EXCLUDED.time_window_end,
            risk_score = EXCLUDED.risk_score,
            risk_level = EXCLUDED.risk_level,
            dominant_crime_type = EXCLUDED.dominant_crime_type,
            historical_count = EXCLUDED.historical_count,
            recent_30_day_count = EXCLUDED.recent_30_day_count,
            nearby_poi_score = EXCLUDED.nearby_poi_score,
            data_snapshot_version = EXCLUDED.data_snapshot_version,
            generated_at = NOW()
    """

    def _write(target_conn: Any) -> int:
        with target_conn.cursor() as cur:
            execute_values(cur, query, values)
        target_conn.commit()
        return len(values)

    if conn is not None:
        return _write(conn)

    with get_connection() as managed_conn:
        return _write(managed_conn)


def make_advice_cache_key(prediction: dict[str, Any]) -> str:
    """Build a stable cache key for generated safety advice."""
    data_version = prediction.get("data_snapshot_version") or "unknown-data"
    return "|".join(
        [
            str(prediction["grid_id"]),
            str(prediction["prediction_window"]),
            str(prediction.get("model_version") or DEFAULT_MODEL_VERSION),
            str(data_version),
        ]
    )


def get_cached_advice(cache_key: str, conn: Any | None = None) -> dict[str, Any] | None:
    """Return cached advice JSON if it exists."""
    query = "SELECT response_json FROM predictive_advice_cache WHERE cache_key = %s"

    def _fetch(target_conn: Any) -> dict[str, Any] | None:
        with target_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (cache_key,))
            row = cur.fetchone()
            return row["response_json"] if row else None

    if conn is not None:
        return _fetch(conn)

    with get_connection() as managed_conn:
        return _fetch(managed_conn)


def upsert_cached_advice(
    cache_key: str,
    prediction: dict[str, Any],
    response: dict[str, Any],
    conn: Any | None = None,
) -> None:
    """Store generated advice so repeated clicks avoid an LLM call."""
    query = """
        INSERT INTO predictive_advice_cache (
            cache_key,
            grid_id,
            prediction_window,
            model_version,
            data_snapshot_version,
            response_json
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (cache_key) DO UPDATE SET
            response_json = EXCLUDED.response_json,
            updated_at = NOW()
    """
    params = (
        cache_key,
        prediction["grid_id"],
        prediction["prediction_window"],
        prediction.get("model_version") or DEFAULT_MODEL_VERSION,
        prediction.get("data_snapshot_version"),
        json.dumps(response),
    )

    def _write(target_conn: Any) -> None:
        with target_conn.cursor() as cur:
            cur.execute(query, params)
        target_conn.commit()

    if conn is not None:
        _write(conn)
        return

    with get_connection() as managed_conn:
        _write(managed_conn)
