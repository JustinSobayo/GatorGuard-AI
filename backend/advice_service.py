"""
Application service that composes PostGIS, Neo4j, and LangChain/Gemini.
"""

from __future__ import annotations

from typing import Any, Callable

try:
    from backend.explanation_chain import generate_advice_narrative
    from backend.neo4j_ops import get_crime_explanation
    from backend.postgis_ops import (
        find_grid_by_point,
        get_cached_advice,
        get_grid_prediction,
        make_advice_cache_key,
        upsert_cached_advice,
    )
except ImportError:  # pragma: no cover - supports direct execution from backend/
    from explanation_chain import generate_advice_narrative  # type: ignore
    from neo4j_ops import get_crime_explanation  # type: ignore
    from postgis_ops import (  # type: ignore
        find_grid_by_point,
        get_cached_advice,
        get_grid_prediction,
        make_advice_cache_key,
        upsert_cached_advice,
    )


class PredictionNotFoundError(LookupError):
    """Raised when no cached prediction exists for a selected grid."""


class GridNotFoundError(LookupError):
    """Raised when a clicked lat/lon does not fall inside a grid cell."""


def _prediction_public_fields(prediction: dict[str, Any]) -> dict[str, Any]:
    return {
        "grid_id": prediction.get("grid_id"),
        "risk_level": prediction.get("risk_level"),
        "prediction_window": prediction.get("prediction_window"),
        "risk_score": prediction.get("risk_score"),
        "dominant_crime_type": prediction.get("dominant_crime_type"),
        "historical_count": prediction.get("historical_count"),
        "recent_30_day_count": prediction.get("recent_30_day_count"),
        "nearby_poi_score": prediction.get("nearby_poi_score"),
        "model_version": prediction.get("model_version"),
        "data_snapshot_version": prediction.get("data_snapshot_version"),
        "center": {
            "lat": prediction.get("center_lat"),
            "lon": prediction.get("center_lon"),
        },
    }


def _build_response(
    prediction: dict[str, Any],
    facts: dict[str, Any],
    narrative: dict[str, Any],
) -> dict[str, Any]:
    response = {
        **_prediction_public_fields(prediction),
        "explanation": narrative.get("summary"),
        "why_risky": narrative.get("why_risky", []),
        "safety_advice": narrative.get("safety_advice", []),
        "disclaimer": narrative.get("disclaimer"),
        "facts": facts,
    }
    if narrative.get("llm_error"):
        response["llm_error"] = narrative["llm_error"]
    return response


def resolve_grid_id(grid_id: str | None, lat: float | None, lon: float | None) -> str:
    """Use a supplied grid_id or resolve one from clicked coordinates."""
    if grid_id:
        return grid_id

    if lat is None or lon is None:
        raise GridNotFoundError("Provide grid_id or both lat and lon.")

    grid = find_grid_by_point(lat=lat, lon=lon)
    if not grid:
        raise GridNotFoundError("No grid cell contains the selected point.")
    return str(grid["grid_id"])


def generate_predictive_safety_advice(
    grid_id: str | None = None,
    radius_meters: int = 500,
    prediction_date: str | None = None,
    prediction_window: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    use_cache: bool = True,
    use_llm: bool | None = None,
    prediction_lookup: Callable[..., dict[str, Any] | None] = get_grid_prediction,
    facts_lookup: Callable[[str, int], dict[str, Any]] = get_crime_explanation,
    narrative_generator: Callable[..., dict[str, Any]] = generate_advice_narrative,
) -> dict[str, Any]:
    """
    Return the full frontend response contract for a selected predicted grid.

    The optional callables make this function easy to unit test without live
    Postgres, Neo4j, or Gemini connections.
    """
    resolved_grid_id = resolve_grid_id(grid_id, lat, lon)
    prediction = prediction_lookup(
        resolved_grid_id,
        prediction_date=prediction_date,
        prediction_window=prediction_window,
    )
    if not prediction:
        raise PredictionNotFoundError(
            f"No prediction found for grid '{resolved_grid_id}' on the selected date/window."
        )

    cache_key = make_advice_cache_key(prediction)
    if use_cache:
        try:
            cached = get_cached_advice(cache_key)
            if cached and not cached.get("llm_error"):
                return cached
        except Exception:
            pass

    facts = facts_lookup(resolved_grid_id, radius_meters)
    narrative = narrative_generator(
        prediction=prediction,
        facts=facts,
        radius_meters=radius_meters,
        use_llm=use_llm,
    )
    response = _build_response(prediction, facts, narrative)

    if use_cache and not response.get("llm_error"):
        try:
            upsert_cached_advice(cache_key, prediction, response)
        except Exception:
            pass

    return response
