"""Manual CLI for fixture-based advice evaluation; Gemini calls incur cost."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any

from backend.eval.advice_judge import judge_case, judge_model_name

EVAL_DIR = Path(__file__).resolve().parent


def summarize_results(cases: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(cases)
    passed = sum(case["overall_pass"] is True for case in cases)
    return {
        "n": n, "passed": passed, "pass_rate": passed / n if n else 0.0,
        **{f"avg_{key}": sum(case[key] for case in cases) / n if n else 0.0
           for key in ("grounding", "safety_tone", "usefulness")},
        "judge_errors": sum(bool(case.get("judge_error")) for case in cases),
        "candidate_errors": sum(bool(case.get("candidate_error")) for case in cases),
    }


def load_fixtures(path: Path) -> list[dict[str, Any]]:
    fixtures = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(fixtures, list) or not fixtures:
        raise ValueError("Fixtures must be a non-empty array")
    seen = set()
    for case in fixtures:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str) or not case["id"].strip():
            raise ValueError("Each fixture must have a non-empty string id")
        if case["id"] in seen:
            raise ValueError(f"Duplicate fixture id: {case['id']}")
        seen.add(case["id"])
        if not isinstance(case.get("prediction"), dict) or not case["prediction"].get("grid_id"):
            raise ValueError(f"Fixture {case['id']} needs a prediction with grid_id")
        if not isinstance(case.get("facts"), dict):
            raise ValueError(f"Fixture {case['id']} needs a facts object")
    return fixtures


def _threshold(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= 1:
        raise argparse.ArgumentTypeError("Threshold must be between 0 and 1")
    return number


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, default=EVAL_DIR / "fixtures/advice_cases.json")
    parser.add_argument("--out", type=Path, default=EVAL_DIR / "results/latest.json")
    parser.add_argument("--use-fallback-candidates", action="store_true")
    parser.add_argument("--fail-under", type=_threshold)
    args = parser.parse_args(argv)
    try:
        fixtures = load_fixtures(args.fixtures)
        cases = []
        for index, case in enumerate(fixtures, 1):
            print(f"[{index}/{len(fixtures)}] Evaluating {case['id']}", flush=True)
            cases.append(judge_case(case, use_fallback_candidates=args.use_fallback_candidates))
        summary = summarize_results(cases)
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "advice_model": os.getenv("GEMINI_MODEL", "gemini-2.5-flash").replace("models/", ""),
            "judge_model": judge_model_name(), "cases": cases, "summary": summary,
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(summary, indent=2))
    print(f"Report: {args.out}")
    return int(args.fail_under is not None and summary["pass_rate"] < args.fail_under)


if __name__ == "__main__":
    raise SystemExit(main())
