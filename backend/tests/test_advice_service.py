import pytest

from backend.advice_service import (
    PredictionNotFoundError,
    generate_predictive_safety_advice,
)
from backend.explanation_chain import build_fallback_advice, validate_advice_response


def _prediction():
    return {
        "grid_id": "grid_29.65000_-82.32000",
        "risk_level": "high",
        "prediction_window": "Next 24 hours",
        "risk_score": 0.82,
        "dominant_crime_type": "theft",
        "historical_count": 12,
        "recent_30_day_count": 4,
        "nearby_poi_score": 0.0,
        "model_version": "mvp-weighted-v1",
        "data_snapshot_version": "snapshot-1",
        "center_lat": 29.65,
        "center_lon": -82.32,
    }


def test_generate_predictive_safety_advice_composes_dependencies(mocker):
    prediction_lookup = mocker.Mock(return_value=_prediction())
    facts_lookup = mocker.Mock(
        return_value={
            "crime_breakdown": [{"type": "theft", "count": 12}],
            "temporal_patterns": {},
            "nearby_places": [],
        }
    )
    narrative_generator = mocker.Mock(
        return_value={
            "summary": "Elevated theft risk is shown for this window.",
            "prediction_window": "Next 24 hours",
            "why_risky": ["Historical theft counts are elevated."],
            "safety_advice": ["Use well-lit routes."],
            "disclaimer": "Risk estimate only.",
        }
    )

    result = generate_predictive_safety_advice(
        grid_id="grid_29.65000_-82.32000",
        use_cache=False,
        prediction_lookup=prediction_lookup,
        facts_lookup=facts_lookup,
        narrative_generator=narrative_generator,
    )

    assert result["grid_id"] == "grid_29.65000_-82.32000"
    assert result["risk_level"] == "high"
    assert result["explanation"] == "Elevated theft risk is shown for this window."
    assert result["safety_advice"] == ["Use well-lit routes."]
    prediction_lookup.assert_called_once()
    facts_lookup.assert_called_once_with("grid_29.65000_-82.32000", 500)


def test_generate_predictive_safety_advice_raises_when_prediction_missing(mocker):
    with pytest.raises(PredictionNotFoundError):
        generate_predictive_safety_advice(
            grid_id="missing",
            use_cache=False,
            prediction_lookup=mocker.Mock(return_value=None),
        )


def test_fallback_advice_has_frontend_contract():
    result = build_fallback_advice(_prediction(), facts={"nearby_places": []})

    assert result["summary"]
    assert result["prediction_window"] == "Next 24 hours"
    assert len(result["safety_advice"]) >= 3
    assert "guarantee" in result["disclaimer"]


def test_validate_advice_response_fills_missing_fields():
    result = validate_advice_response({"summary": "Short."}, _prediction(), facts={})

    assert result["summary"] == "Short."
    assert result["prediction_window"] == "Next 24 hours"
    assert result["why_risky"]
    assert result["safety_advice"]
