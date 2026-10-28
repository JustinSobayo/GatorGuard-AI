"""Judge response contract and deterministic scoring checks."""

from typing import Any, cast

from backend.explanation_chain import ADVICE_RESPONSE_SCHEMA

JUDGE_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "schema_ok": {"type": "integer"},
        "grounding": {"type": "integer"},
        "safety_tone": {"type": "integer"},
        "usefulness": {"type": "integer"},
        "overall_pass": {"type": "boolean"},
        "rationale": {"type": "string"},
        "evidence": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "schema_ok", "grounding", "safety_tone", "usefulness",
        "overall_pass", "rationale", "evidence",
    ],
}


def compute_overall_pass(scores: dict[str, Any]) -> bool:
    """Use the rubric thresholds, independent of the judge's boolean."""
    return bool(
        scores.get("schema_ok", 0) == 1
        and scores.get("grounding", 0) >= 4
        and scores.get("safety_tone", 0) >= 4
    )


def candidate_schema_ok(candidate: Any) -> bool:
    """Check the original candidate without repairing missing or empty fields."""
    if not isinstance(candidate, dict):
        return False
    properties = cast(dict[str, Any], ADVICE_RESPONSE_SCHEMA["properties"])
    for key in ADVICE_RESPONSE_SCHEMA["required"]:
        value = candidate.get(key)
        field_type = properties[key]["type"]
        if field_type == "string":
            if not isinstance(value, str) or not value.strip():
                return False
        elif not isinstance(value, list) or not value or not all(
            isinstance(item, str) and item.strip() for item in value
        ):
            return False
    return True


def normalize_scores(response: Any, candidate: Any) -> dict[str, Any]:
    """Reject malformed judge output, clamp integers, then recompute pass/fail."""
    if not isinstance(response, dict):
        raise ValueError("Judge response must be an object")
    missing = set(JUDGE_RESPONSE_SCHEMA["required"]) - response.keys()
    if missing:
        raise ValueError(f"Missing judge fields: {', '.join(sorted(missing))}")
    scores = {}
    for key in ("schema_ok", "grounding", "safety_tone", "usefulness"):
        value = response[key]
        if type(value) is not int:
            raise ValueError(f"{key} must be an integer")
        scores[key] = max(0 if key == "schema_ok" else 1, min(1 if key == "schema_ok" else 5, value))
    if not isinstance(response["overall_pass"], bool):
        raise ValueError("overall_pass must be a boolean")
    if not isinstance(response["rationale"], str) or not response["rationale"].strip():
        raise ValueError("rationale must be a non-empty string")
    evidence = response["evidence"]
    if not isinstance(evidence, list) or not evidence or not all(
        isinstance(item, str) and item.strip() for item in evidence
    ):
        raise ValueError("evidence must contain fixture field citations or 'none'")
    scores["schema_ok"] = min(scores["schema_ok"], int(candidate_schema_ok(candidate)))
    return {
        **scores,
        "overall_pass": compute_overall_pass(scores),
        "rationale": response["rationale"].strip(),
        "evidence": evidence,
    }
