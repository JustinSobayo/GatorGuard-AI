"""
LangChain/Gemini layer for user-facing predictive safety advice.

The chain receives already-computed prediction metadata and Neo4j facts. It
does not query databases and it does not decide the risk score.
"""

from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv

load_dotenv()

DEFAULT_DISCLAIMER = (
    "This is a risk estimate based on historical and recent incident patterns, "
    "not a guarantee that crime will occur."
)

SAFETY_RULES = [
    "Use only the supplied facts.",
    "Describe elevated risk without claiming that crime will happen.",
    "Avoid profiling people or groups.",
    "Keep advice calm, practical, and non-alarming.",
    "Include the prediction time window and a short uncertainty disclaimer.",
]

ADVICE_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "prediction_window": {"type": "string"},
        "why_risky": {"type": "array", "items": {"type": "string"}},
        "safety_advice": {"type": "array", "items": {"type": "string"}},
        "disclaimer": {"type": "string"},
    },
    "required": [
        "summary",
        "prediction_window",
        "why_risky",
        "safety_advice",
        "disclaimer",
    ],
}


def build_fallback_advice(prediction: dict[str, Any], facts: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return deterministic advice when Gemini is unavailable or malformed."""
    facts = facts or {}
    risk_level = prediction.get("risk_level", "elevated")
    crime_type = prediction.get("dominant_crime_type") or "reported incidents"
    prediction_window = prediction.get("prediction_window") or "the selected prediction window"
    recent_count = prediction.get("recent_30_day_count", 0)
    historical_count = prediction.get("historical_count", 0)

    why_risky = [
        (
            f"The selected grid is currently marked {risk_level} risk for "
            f"{prediction_window}."
        ),
        (
            f"The score is driven most by {crime_type}, with {historical_count} "
            "historical incidents in comparable day patterns."
        ),
    ]
    if recent_count:
        why_risky.append(f"There were {recent_count} incidents in the recent 30-day window.")

    nearby_places = facts.get("nearby_places") or []
    if nearby_places:
        place = nearby_places[0]
        place_name = place.get("name") or place.get("place_name") or "nearby places"
        place_type = place.get("type") or place.get("place_type") or "POIs"
        why_risky.append(f"Neo4j context shows incident relationships near {place_name} ({place_type}).")

    return {
        "summary": (
            f"This area shows elevated {crime_type} risk during {prediction_window} "
            "based on the available spatial and temporal data."
        ),
        "prediction_window": prediction_window,
        "why_risky": why_risky,
        "safety_advice": [
            "Prefer well-lit, higher-traffic routes when moving through the area.",
            "Stay aware around parking lots, ATMs, and isolated streets.",
            "Consider traveling with others during late evening or overnight periods.",
        ],
        "disclaimer": DEFAULT_DISCLAIMER,
    }


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _extract_json_object(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM response did not contain a JSON object")
    return json.loads(text[start : end + 1])


def _as_list(value: Any, fallback: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return fallback


def validate_advice_response(
    response: dict[str, Any],
    prediction: dict[str, Any],
    facts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fill missing fields and enforce the frontend response shape."""
    fallback = build_fallback_advice(prediction, facts)

    summary = str(response.get("summary") or fallback["summary"]).strip()
    prediction_window = str(
        response.get("prediction_window")
        or prediction.get("prediction_window")
        or fallback["prediction_window"]
    ).strip()
    disclaimer = str(response.get("disclaimer") or DEFAULT_DISCLAIMER).strip()

    return {
        "summary": summary,
        "prediction_window": prediction_window,
        "why_risky": _as_list(response.get("why_risky"), fallback["why_risky"]),
        "safety_advice": _as_list(response.get("safety_advice"), fallback["safety_advice"]),
        "disclaimer": disclaimer,
    }


def _invoke_gemini(prediction: dict[str, Any], facts: dict[str, Any], radius_meters: int) -> dict[str, Any]:
    try:
        from langchain_core.prompts import ChatPromptTemplate
    except ImportError:  # pragma: no cover - older langchain layout
        from langchain.prompts import ChatPromptTemplate  # type: ignore

    from langchain_google_genai import ChatGoogleGenerativeAI

    model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").replace("models/", "")
    model = ChatGoogleGenerativeAI(
        model=model_name,
        temperature=0.2,
        max_tokens=700,
        request_timeout=30,
        response_mime_type="application/json",
        response_schema=ADVICE_RESPONSE_SCHEMA,
        thinking_budget=0,
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You generate concise public safety advice for a map modal. "
                "Return only valid JSON. Never add markdown fences.",
            ),
            (
                "human",
                """
Prediction metadata:
{prediction_json}

Neo4j explanation facts:
{facts_json}

Radius searched: {radius_meters} meters

Rules:
{rules_json}

Return this exact JSON shape:
{{
  "summary": "one calm sentence",
  "prediction_window": "the supplied prediction window",
  "why_risky": ["fact-based reason", "fact-based reason"],
  "safety_advice": ["practical advice", "practical advice", "practical advice"],
  "disclaimer": "short uncertainty disclaimer"
}}
""",
            ),
        ]
    )

    chain = prompt | model
    result = chain.invoke(
        {
            "prediction_json": json.dumps(prediction, default=_json_default),
            "facts_json": json.dumps(facts, default=_json_default),
            "radius_meters": radius_meters,
            "rules_json": json.dumps(SAFETY_RULES),
        }
    )
    content = getattr(result, "content", result)
    if isinstance(content, list):
        content = "".join(str(item) for item in content)
    return _extract_json_object(str(content))


def generate_advice_narrative(
    prediction: dict[str, Any],
    facts: dict[str, Any] | None,
    radius_meters: int = 500,
    use_llm: bool | None = None,
) -> dict[str, Any]:
    """
    Generate structured narrative advice.

    When use_llm is None, the chain uses Gemini only if GOOGLE_API_KEY exists.
    Any import/API/format failure falls back to deterministic advice.
    """
    facts = facts or {}
    should_use_llm = bool(os.getenv("GOOGLE_API_KEY")) if use_llm is None else use_llm

    if not should_use_llm:
        return build_fallback_advice(prediction, facts)

    try:
        llm_response = _invoke_gemini(prediction, facts, radius_meters)
        return validate_advice_response(llm_response, prediction, facts)
    except Exception as exc:
        fallback = build_fallback_advice(prediction, facts)
        fallback["llm_error"] = str(exc)
        return fallback
