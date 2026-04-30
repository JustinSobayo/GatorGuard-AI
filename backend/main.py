from datetime import date as date_type

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

try:
    from backend.advice_service import (
        GridNotFoundError,
        PredictionNotFoundError,
        generate_predictive_safety_advice,
    )
    from backend.neo4j_ops import get_crime_explanation
    from backend.postgis_ops import get_historical_crimes as fetch_historical_crimes
    from backend.postgis_ops import get_predictions_geojson
except ImportError:  # pragma: no cover - supports uvicorn main:app from backend/
    from advice_service import (  # type: ignore
        GridNotFoundError,
        PredictionNotFoundError,
        generate_predictive_safety_advice,
    )
    from neo4j_ops import get_crime_explanation  # type: ignore
    from postgis_ops import get_historical_crimes as fetch_historical_crimes  # type: ignore
    from postgis_ops import get_predictions_geojson  # type: ignore

app = FastAPI(title="GainesvilleGuard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["null"],
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health_check():
    return {"status": "ok", "service": "GainesvilleGuard-AI"}


@app.get("/crimes/history")
def get_historical_crimes(
    date: str = Query(..., description="Offense date in YYYY-MM-DD format."),
    limit: int = Query(1000, ge=1, le=10000),
):
    """
    Query PostGIS for cleaned historical crimes on a specific date.
    """
    return {"date": date, "crimes": fetch_historical_crimes(date, limit=limit)}


@app.get("/crimes/predict")
def get_crime_predictions(
    prediction_date: str | None = Query(
        None,
        alias="date",
        description="Prediction date in YYYY-MM-DD format. Defaults to today.",
    ),
    prediction_window: str | None = None,
    min_risk_level: str | None = Query(
        None,
        description="Optional risk filter: low, medium, or high.",
    ),
):
    """
    Return cached prediction polygons as GeoJSON for the frontend heatmap.
    """
    target_date = date_type.today() if prediction_date in (None, "today") else prediction_date
    return get_predictions_geojson(
        prediction_date=target_date,
        prediction_window=prediction_window,
        min_risk_level=min_risk_level,
    )


@app.get("/predict/explain")
def explain_prediction(grid_id: str, radius: int = 500):
    """
    Get crime explanation context for a location.
    
    Args:
        grid_id: Location identifier (e.g., "29.65,-82.32" or "grid_29.65_-82.32")
        radius: Search radius in meters (default 500m)
    
    Returns:
        Dict with crime breakdown, temporal patterns, nearby places, and seasonal data.
        This data can be passed to an LLM for natural language explanation.
    """
    return get_crime_explanation(grid_id, radius_meters=radius)


@app.get("/predict/advice")
def predictive_safety_advice(
    grid_id: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius: int = Query(500, ge=1, le=5000),
    prediction_date: str | None = Query(None, alias="date"),
    prediction_window: str | None = None,
    use_cache: bool = True,
    use_llm: bool | None = Query(
        None,
        description="Override LLM use. Defaults to true only when GOOGLE_API_KEY exists.",
    ),
):
    """
    Return prediction metadata, Neo4j facts, and user-facing safety advice.

    The client may send a grid_id from /crimes/predict or a clicked lat/lon.
    """
    try:
        return generate_predictive_safety_advice(
            grid_id=grid_id,
            lat=lat,
            lon=lon,
            radius_meters=radius,
            prediction_date=prediction_date,
            prediction_window=prediction_window,
            use_cache=use_cache,
            use_llm=use_llm,
        )
    except GridNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PredictionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
