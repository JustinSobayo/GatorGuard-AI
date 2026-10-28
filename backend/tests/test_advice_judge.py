import json

import pytest
from langchain_core.messages import AIMessage

from backend import explanation_chain
from backend.eval import advice_judge as judge
from backend.eval import run_advice_judge as runner
from backend.eval.judge_schema import (
    JUDGE_RESPONSE_SCHEMA, candidate_schema_ok, compute_overall_pass, normalize_scores,
)


@pytest.fixture
def case():
    return {
        "id": "test-theft", "prediction": {
            "grid_id": "grid_29.65_-82.32", "historical_count": 2,
            "dominant_crime_type": "theft", "prediction_window": "Next 24 hours",
        },
        "facts": {"total_incidents": 2, "nearby_places": []},
        "candidate_advice": None,
    }


@pytest.fixture
def candidate(case):
    return explanation_chain.build_fallback_advice(case["prediction"], case["facts"])


@pytest.fixture
def scores():
    return {
        "schema_ok": 1, "grounding": 4, "safety_tone": 5, "usefulness": 3,
        "overall_pass": False, "rationale": "The count matches. Advice is calm.",
        "evidence": ["grounding: prediction.historical_count=2", "safety_tone: candidate_advice.disclaimer"],
    }


@pytest.fixture
def model(mocker, monkeypatch, scores):
    monkeypatch.setenv("GOOGLE_API_KEY", "offline-test-key")
    constructor = mocker.patch("langchain_google_genai.ChatGoogleGenerativeAI")
    constructor.return_value.invoke.return_value = AIMessage(content=json.dumps(scores))
    return constructor


@pytest.mark.parametrize("schema,grounding,safety,expected", [
    (1, 4, 4, True), (1, 5, 5, True), (0, 5, 5, False),
    (1, 3, 5, False), (1, 5, 3, False), (0, 1, 1, False),
])
def test_pass_thresholds(schema, grounding, safety, expected):
    assert compute_overall_pass({
        "schema_ok": schema, "grounding": grounding, "safety_tone": safety,
        "overall_pass": not expected,
    }) is expected


def test_clamps_scores_and_recomputes_pass(scores, candidate):
    scores.update(schema_ok=12, grounding=99, safety_tone=8, usefulness=-3)
    result = normalize_scores(scores, candidate)
    assert [result[key] for key in ("schema_ok", "grounding", "safety_tone", "usefulness")] == [1, 5, 5, 1]
    assert result["overall_pass"] is True
    assert scores["grounding"] == 99


@pytest.mark.parametrize("bad", [{}, {"summary": "Only summary"}, None, [], "advice"])
def test_invalid_candidate_fails_schema(bad):
    assert not candidate_schema_ok(bad)


def test_empty_candidate_fields_cannot_pass_even_if_judge_approves(scores, candidate):
    candidate["disclaimer"] = ""
    result = normalize_scores(scores, candidate)
    assert result["schema_ok"] == 0
    assert result["overall_pass"] is False


def test_judge_model_configuration_raw_audit_and_prompt(model, monkeypatch, case, candidate, scores):
    monkeypatch.setenv("GEMINI_JUDGE_MODEL", "models/different-judge")
    result = judge.invoke_advice_judge(case["prediction"], case["facts"], candidate)
    assert result["overall_pass"] is True  # Model's boolean was False.
    assert result["raw_judge_json"] == scores
    assert json.loads(result["raw_judge_response"]) == scores
    options = model.call_args.kwargs
    assert options["model"] == "different-judge"
    assert options["temperature"] == 0
    assert options["response_schema"] == JUDGE_RESPONSE_SCHEMA
    messages = model.return_value.invoke.call_args.args[0]
    assert "untrusted data" in messages[0].content
    assert json.loads(messages[1].content)["candidate_advice"] == candidate


@pytest.mark.parametrize("name,budget", [
    ("gemini-2.5-pro", 128), ("gemini-2.5-flash", 0), ("custom-model", None),
])
def test_model_appropriate_thinking_budget(model, monkeypatch, case, candidate, name, budget):
    monkeypatch.setenv("GEMINI_JUDGE_MODEL", name)
    judge.invoke_advice_judge(case["prediction"], case["facts"], candidate)
    assert model.call_args.kwargs.get("thinking_budget") == budget
    assert model.call_args.kwargs["max_tokens"] == 800 + (budget or 0)


@pytest.mark.parametrize("output", ["not JSON", "[]", "{}"])
def test_malformed_judge_output_is_auditable_error(model, case, candidate, output):
    model.return_value.invoke.return_value = AIMessage(content=output)
    result = judge.invoke_advice_judge(case["prediction"], case["facts"], candidate)
    assert result["judge_error"]
    assert result["overall_pass"] is False
    assert result["raw_judge_response"] == output


@pytest.mark.parametrize("key,value", [
    ("grounding", True), ("grounding", "5"), ("safety_tone", 4.5),
    ("overall_pass", "true"), ("rationale", ""), ("evidence", []),
])
def test_invalid_score_types_fail_closed(scores, candidate, key, value):
    scores[key] = value
    with pytest.raises(ValueError):
        normalize_scores(scores, candidate)


def test_judge_exception_and_disabled_paths(model, case, candidate, monkeypatch):
    model.return_value.invoke.side_effect = RuntimeError("API unavailable")
    result = judge.invoke_advice_judge(case["prediction"], case["facts"], candidate)
    assert result["judge_error"] == "API unavailable"
    model.reset_mock()
    for use_llm, key in [(False, "present"), (True, ""), (None, "")]:
        monkeypatch.setenv("GOOGLE_API_KEY", key)
        result = judge.invoke_advice_judge(case["prediction"], case["facts"], candidate, use_llm=use_llm)
        assert result["judge_error"]
    model.assert_not_called()


def test_negative_control_fails(model, case, candidate, scores):
    candidate.update(summary="500 assaults will definitely happen.", disclaimer="")
    scores.update(grounding=1, safety_tone=1, overall_pass=True)
    model.return_value.invoke.return_value = AIMessage(content=json.dumps(scores))
    result = judge.judge_case({**case, "candidate_advice": candidate})
    assert result["overall_pass"] is False
    assert result["candidate_advice"] == candidate
    assert result["candidate_source"] == "provided"


def test_candidate_agent_replays_fixture_tools_and_restores_them(mocker, monkeypatch, case, candidate):
    monkeypatch.setenv("GOOGLE_API_KEY", "offline-test-key")
    mocker.patch.object(judge, "invoke_advice_judge", return_value={"overall_pass": True})
    original_prediction_tool = explanation_chain.get_grid_prediction
    original_facts_tool = explanation_chain.get_crime_explanation

    def invoke(**kwargs):
        assert kwargs["use_llm"] is True
        assert explanation_chain.get_grid_prediction("grid") == case["prediction"]
        assert explanation_chain.get_crime_explanation("grid") == case["facts"]
        return {**candidate, "_tool_facts": case["facts"]}

    mocker.patch.object(explanation_chain, "generate_advice_narrative", side_effect=invoke)
    result = judge.judge_case(case)
    assert result["candidate_source"] == "agent"
    assert "_tool_facts" not in result["candidate_advice"]
    assert explanation_chain.get_grid_prediction is original_prediction_tool
    assert explanation_chain.get_crime_explanation is original_facts_tool


@pytest.mark.parametrize("raises", [True, False])
def test_failed_candidate_generation_is_recorded(mocker, monkeypatch, case, candidate, raises):
    monkeypatch.setenv("GOOGLE_API_KEY", "offline-test-key")
    mocker.patch.object(judge, "invoke_advice_judge", return_value={"overall_pass": True})
    generate = mocker.patch.object(explanation_chain, "generate_advice_narrative")
    if raises:
        generate.side_effect = RuntimeError("candidate unavailable")
    else:
        generate.return_value = {**candidate, "llm_error": "candidate unavailable"}
    result = judge.judge_case(case)
    assert result["candidate_error"] == "candidate unavailable"
    assert result["candidate_source"] == "fallback"
    assert result["candidate_advice"] == candidate


def test_fallback_candidates_skip_agent_but_still_judge(model, mocker, case, candidate):
    generate = mocker.patch.object(explanation_chain, "generate_advice_narrative")
    result = judge.judge_case(case, use_fallback_candidates=True)
    assert result["candidate_advice"] == candidate
    assert result["candidate_source"] == "fallback"
    assert result["overall_pass"] is True
    generate.assert_not_called()


def test_summary_includes_errors_as_failures(scores):
    cases = [
        {**scores, "overall_pass": True},
        {**scores, "overall_pass": False, "grounding": 1, "judge_error": "failed"},
    ]
    summary = runner.summarize_results(cases)
    assert summary["n"] == 2
    assert summary["passed"] == 1
    assert summary["pass_rate"] == 0.5
    assert summary["avg_grounding"] == 2.5
    assert summary["judge_errors"] == 1
    assert runner.summarize_results([])["pass_rate"] == 0


def test_cli_writes_report_and_enforces_optional_threshold(model, tmp_path, case):
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps([case]), encoding="utf-8")
    out = tmp_path / "reports/latest.json"
    argv = ["--fixtures", str(fixture), "--out", str(out), "--use-fallback-candidates"]
    assert runner.main(argv) == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["summary"]["pass_rate"] == 1.0
    assert report["cases"][0]["raw_judge_json"]
    assert report["cases"][0]["prediction"] == case["prediction"]
    model.return_value.invoke.return_value = AIMessage(content="invalid")
    assert runner.main(argv) == 0
    assert runner.main(argv + ["--fail-under", "0.8"]) == 1


def test_cli_missing_key_writes_errors_without_network(mocker, monkeypatch, tmp_path, case):
    monkeypatch.setenv("GOOGLE_API_KEY", "")
    model = mocker.patch("langchain_google_genai.ChatGoogleGenerativeAI")
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps([case]), encoding="utf-8")
    out = tmp_path / "report.json"
    assert runner.main(["--fixtures", str(fixture), "--out", str(out)]) == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["summary"]["judge_errors"] == 1
    assert report["summary"]["passed"] == 0
    assert report["cases"][0]["candidate_source"] == "fallback"
    model.assert_not_called()


@pytest.mark.parametrize("threshold", ["-0.1", "1.1", "nan", "inf"])
def test_cli_rejects_invalid_threshold(threshold):
    with pytest.raises(SystemExit) as exc:
        runner.main(["--fail-under", threshold])
    assert exc.value.code == 2


def test_fixture_validation_and_bundled_coverage(tmp_path, case):
    fixtures = runner.load_fixtures(runner.EVAL_DIR / "fixtures/advice_cases.json")
    assert len(fixtures) >= 6
    assert sum(item["prediction"]["risk_level"] == "high" for item in fixtures) >= 2
    assert {item["prediction"]["risk_level"] for item in fixtures} >= {"high", "medium", "low"}
    assert any(item.get("candidate_advice") is not None for item in fixtures)
    path = tmp_path / "invalid.json"
    for invalid in [[], {}, [case, case], [{"id": "missing"}]]:
        path.write_text(json.dumps(invalid), encoding="utf-8")
        with pytest.raises(ValueError):
            runner.load_fixtures(path)
