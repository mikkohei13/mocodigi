"""Compare three Darwin Core call_data methods by pairwise agreement."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from comparison_utils import values_equal

RUN_ID = "run_h1"
IMAGES_DIR = Path(__file__).resolve().parent.parent / "images_lajifi"

# Fields that contain semicolon-separated values and should be set-compared.
SEMICOLON_LIST_FIELDS = {"identifiedBy", "catalogNumber", "recordedBy"}


def load_call_data(specimen_dir: Path) -> list[dict] | None:
    """Load call_data from specimen run darwin_core.json, or None when unavailable."""
    dc_path = specimen_dir / RUN_ID / "darwin_core.json"
    if not dc_path.is_file():
        return None

    try:
        with dc_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    call_data = payload.get("call_data")
    if not isinstance(call_data, list) or len(call_data) != 3:
        return None
    if not all(isinstance(item, dict) for item in call_data):
        return None
    return call_data


def score_specimen(call_data: list[dict]) -> dict:
    """
    Score 3 methods by field-level pairwise agreement.

    For each field and each method:
    - +1 when method agrees with method A
    - +1 when method agrees with method B
    Max per field per method = 2.
    """
    method_count = 3
    all_fields = set()
    for method_output in call_data:
        all_fields.update(method_output.keys())

    if not all_fields:
        return {
            "fields_compared": 0,
            "methods": [{"method_index": i, "agreements": 0} for i in range(method_count)],
            "winners": [0, 1, 2],
        }

    agreements = [0, 0, 0]

    for field in all_fields:
        is_semicolon_list = field in SEMICOLON_LIST_FIELDS
        values = [method_output.get(field) for method_output in call_data]
        for i in range(method_count):
            for j in range(method_count):
                if i == j:
                    continue
                if values_equal(values[i], values[j], semicolon_list=is_semicolon_list):
                    agreements[i] += 1

    methods = []
    for i in range(method_count):
        methods.append(
            {
                "method_index": i,
                "agreements": agreements[i],
            }
        )

    best_agreements = max(m["agreements"] for m in methods)
    winners = [m["method_index"] for m in methods if m["agreements"] == best_agreements]

    return {
        "fields_compared": len(all_fields),
        "methods": methods,
        "winners": winners,
    }


def aggregate_global(specimen_scores: list[dict]) -> dict:
    """Aggregate method-level agreement stats across all specimens."""
    method_count = 3
    totals = [{"agreement_total": 0, "wins": 0, "ties": 0, "specimens": 0} for _ in range(method_count)]

    for specimen in specimen_scores:
        winners = specimen["winners"]
        for method in specimen["methods"]:
            idx = method["method_index"]
            totals[idx]["agreement_total"] += method["agreements"]
            totals[idx]["specimens"] += 1
            if idx in winners:
                if len(winners) == 1:
                    totals[idx]["wins"] += 1
                else:
                    totals[idx]["ties"] += 1

    methods = []
    for i, t in enumerate(totals):
        methods.append(
            {
                "method_index": i,
                "agreement_total": t["agreement_total"],
                "wins": t["wins"],
                "ties": t["ties"],
                "specimens_scored": t["specimens"],
            }
        )

    return {"specimens_scored": len(specimen_scores), "methods": methods}


def write_csv(specimen_scores: list[dict], output_path: Path) -> None:
    """Write per-specimen agreement breakdown to CSV."""
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "specimen_id",
                "fields_compared",
                "method0_agreements",
                "method1_agreements",
                "method2_agreements",
                "winner_methods",
            ]
        )
        for specimen in specimen_scores:
            methods = specimen["methods"]
            writer.writerow(
                [
                    specimen["specimen_id"],
                    specimen["fields_compared"],
                    methods[0]["agreements"],
                    methods[1]["agreements"],
                    methods[2]["agreements"],
                    ";".join(str(i) for i in specimen["winners"]),
                ]
            )


def main() -> None:
    specimen_scores = []

    for specimen_dir in sorted([p for p in IMAGES_DIR.iterdir() if p.is_dir()]):
        call_data = load_call_data(specimen_dir)
        if call_data is None:
            continue

        specimen_score = score_specimen(call_data)
        specimen_score["specimen_id"] = specimen_dir.name
        specimen_scores.append(specimen_score)

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    json_path = IMAGES_DIR / f"method_agreement_{timestamp}.json"
    csv_path = IMAGES_DIR / f"method_agreement_{timestamp}.csv"

    global_stats = aggregate_global(specimen_scores)
    payload = {
        "format_version": "0.1",
        "type": "darwin_core_method_agreement",
        "datetime": datetime.now().isoformat(),
        "settings": {
            "run_id": RUN_ID,
            "method_count": 3,
            "scoring": "pairwise_agreement_count_only",
            "semicolon_list_fields": sorted(SEMICOLON_LIST_FIELDS),
        },
        "global": global_stats,
        "specimens": specimen_scores,
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    write_csv(specimen_scores, csv_path)

    print(f"Scored specimens: {global_stats['specimens_scored']}")
    for method in global_stats["methods"]:
        print(
            f"Method {method['method_index']}: "
            f"agreement_total={method['agreement_total']}, "
            f"wins={method['wins']}, ties={method['ties']}"
        )
    print(f"JSON output: {json_path}")
    print(f"CSV output: {csv_path}")


if __name__ == "__main__":
    main()
