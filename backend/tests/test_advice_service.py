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
    narrative_generator.assert_called_once_with(
        prediction=_prediction(), facts=None, radius_meters=500, use_llm=None,
        grid_id="grid_29.65000_-82.32000", prediction_date=None, prediction_window=None,
    )
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


def test_service_uses_agent_facts_without_second_lookup(mocker):
    facts = {"nearby_places": [{"name": "Library"}]}
    narrative = {**build_fallback_advice(_prediction(), facts), "_tool_facts": facts}
    facts_lookup = mocker.Mock()
    result = generate_predictive_safety_advice(
        grid_id=_prediction()["grid_id"], use_cache=False,
        prediction_lookup=mocker.Mock(return_value=_prediction()),
        facts_lookup=facts_lookup, narrative_generator=mocker.Mock(return_value=narrative),
    )
    assert result["facts"] == facts
    assert "_tool_facts" not in result
    assert "_tool_facts" in narrative  # Do not mutate injected results.
    facts_lookup.assert_not_called()


def test_service_survives_facts_fill_in_failure(mocker):
    result = generate_predictive_safety_advice(
        grid_id=_prediction()["grid_id"], use_cache=False,
        prediction_lookup=mocker.Mock(return_value=_prediction()),
        facts_lookup=mocker.Mock(side_effect=RuntimeError("Neo4j unavailable")),
        narrative_generator=mocker.Mock(return_value=build_fallback_advice(_prediction())),
    )
    assert result["facts"] == {}
    assert result["explanation"]


def test_service_cache_hit_skips_agent_and_facts(mocker):
    cached = {"grid_id": _prediction()["grid_id"], "facts": {}}
    mocker.patch("backend.advice_service.get_cached_advice", return_value=cached)
    facts_lookup = mocker.Mock()
    narrative_generator = mocker.Mock()
    assert generate_predictive_safety_advice(
        grid_id=_prediction()["grid_id"],
        prediction_lookup=mocker.Mock(return_value=_prediction()),
        facts_lookup=facts_lookup, narrative_generator=narrative_generator,
    ) == cached
    facts_lookup.assert_not_called()
    narrative_generator.assert_not_called()
