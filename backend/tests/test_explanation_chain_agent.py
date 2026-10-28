import json
from datetime import date

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from backend import explanation_chain as chain
from backend.tests.test_advice_service import _prediction


@pytest.fixture
def agent(mocker, monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "offline-test-key")
    constructor = mocker.patch("langchain_google_genai.ChatGoogleGenerativeAI")
    model = constructor.return_value.bind_tools.return_value
    prediction_lookup = mocker.patch.object(chain, "get_grid_prediction", return_value=_prediction())
    facts_lookup = mocker.patch.object(chain, "get_crime_explanation", return_value={"nearby_places": []})
    return constructor, model, prediction_lookup, facts_lookup


def _calls(*names):
    return AIMessage(content="", tool_calls=[
        {"name": name, "args": {"grid_id": _prediction()["grid_id"]}, "id": str(index)}
        for index, name in enumerate(names)
    ])


def _answer():
    return AIMessage(content=json.dumps(chain.build_fallback_advice(_prediction())))


def test_agent_executes_both_tools_and_reuses_facts(agent):
    constructor, model, prediction_lookup, facts_lookup = agent
    model.invoke.side_effect = [
        _calls("get_grid_prediction_tool", "get_crime_explanation_tool"), _answer()
    ]
    result = chain.generate_advice_narrative(
        prediction=_prediction(), radius_meters=300, prediction_date="2026-10-27"
    )
    assert set(chain.ADVICE_RESPONSE_SCHEMA["required"]) <= result.keys()
    assert result["_tool_facts"] == {"nearby_places": []}
    assert "llm_error" not in result
    prediction_lookup.assert_called_once_with(
        _prediction()["grid_id"], prediction_date="2026-10-27", prediction_window="Next 24 hours"
    )
    facts_lookup.assert_called_once_with(_prediction()["grid_id"], 300)
    messages = model.invoke.call_args.args[0]
    tool_messages = [message for message in messages if isinstance(message, ToolMessage)]
    assert [message.tool_call_id for message in tool_messages] == ["0", "1"]
    assert len(constructor.return_value.bind_tools.call_args.args[0]) == 2


def test_agent_reserves_third_invocation_for_structured_output(agent):
    constructor, model, _, _ = agent
    model.invoke.side_effect = [
        _calls("get_grid_prediction_tool"), _calls("get_crime_explanation_tool")
    ]
    constructor.return_value.invoke.return_value = _answer()
    result = chain.generate_advice_narrative(prediction=_prediction())
    assert "llm_error" not in result
    assert model.invoke.call_count == 2
    constructor.return_value.invoke.assert_called_once()
    assert constructor.call_args.kwargs["response_schema"] == chain.ADVICE_RESPONSE_SCHEMA
    assert constructor.call_args.kwargs["response_mime_type"] == "application/json"


def test_agent_is_bounded_even_if_final_model_calls_tools(agent):
    constructor, model, _, _ = agent
    model.invoke.return_value = _calls("get_crime_explanation_tool")
    constructor.return_value.invoke.return_value = _calls("get_crime_explanation_tool")
    result = chain.generate_advice_narrative(prediction=_prediction())
    assert "step limit" in result["llm_error"]
    assert model.invoke.call_count + constructor.return_value.invoke.call_count == 3


@pytest.mark.parametrize("failure", [RuntimeError("unavailable"), AIMessage(content="bad JSON")])
def test_agent_failure_uses_service_prediction_and_collected_facts(agent, failure):
    _, model, prediction_lookup, _ = agent
    prediction_lookup.return_value = {**_prediction(), "historical_count": 999}
    model.invoke.side_effect = [
        _calls("get_grid_prediction_tool", "get_crime_explanation_tool"), failure
    ]
    result = chain.generate_advice_narrative(prediction=_prediction())
    assert result["llm_error"]
    assert "12 historical" in result["why_risky"][1]
    assert "_tool_facts" in result


@pytest.mark.parametrize("name,args", [
    ("unknown_tool", {"grid_id": _prediction()["grid_id"]}),
    ("get_grid_prediction_tool", {"grid_id": "different"}),
])
def test_agent_rejects_unknown_tools_or_other_grids(agent, name, args):
    _, model, prediction_lookup, facts_lookup = agent
    model.invoke.return_value = AIMessage(content="", tool_calls=[
        {"name": name, "args": args, "id": "bad"}
    ])
    result = chain.generate_advice_narrative(prediction=_prediction())
    assert result["llm_error"]
    prediction_lookup.assert_not_called()
    facts_lookup.assert_not_called()


@pytest.mark.parametrize("use_llm,key", [(False, "present"), (True, ""), (None, "")])
def test_fallback_does_not_invoke_model(mocker, monkeypatch, use_llm, key):
    monkeypatch.setenv("GOOGLE_API_KEY", key)
    invoke = mocker.patch.object(chain, "_invoke_gemini")
    result = chain.generate_advice_narrative(_prediction(), {}, use_llm=use_llm)
    assert result == chain.build_fallback_advice(_prediction(), {})
    invoke.assert_not_called()


def test_agent_can_resolve_prediction_from_tool_result(agent):
    _, model, _, _ = agent
    model.invoke.side_effect = [_calls("get_grid_prediction_tool"), _answer()]
    result = chain.generate_advice_narrative(grid_id=_prediction()["grid_id"])
    assert result["prediction_window"] == "Next 24 hours"
    assert "llm_error" not in result


def test_prediction_tool_serializes_dates_and_not_found(mocker):
    lookup = mocker.patch.object(chain, "get_grid_prediction", return_value={"prediction_date": date(2026, 10, 27)})
    assert json.loads(chain.get_grid_prediction_tool.invoke({"grid_id": "grid"})) == {
        "prediction_date": "2026-10-27"
    }
    lookup.return_value = None
    assert json.loads(chain.get_grid_prediction_tool.invoke({"grid_id": "grid"})) == {
        "error": "prediction_not_found"
    }
