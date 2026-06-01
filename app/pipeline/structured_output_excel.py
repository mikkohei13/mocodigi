"""Export a random sample of structured-output batch rows to Excel.

Step 7B alternative:
- Same discovery as structured_output_report (step-7 summary → predictions.jsonl).
- Parses Gemini JSON from response.candidates[].content.parts[].text.
- Writes one row per sampled record: document_long_id, one column per structured field,
  then processed_time.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from openpyxl import Workbook

from utils.files import load_json, load_jsonl_rows, resolve_path as resolve_path_from_root
from utils.pipeline_config import archive_pipeline_settings, load_step_settings
from utils.runtime import log

# Fraction of rows to include after loading (0 < value <= 1).
SAMPLE_FRACTION = 0.10

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "structured_output_excel_settings.json"


def extract_response_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("response", {}).get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        return ""
    first_candidate = candidates[0]
    if not isinstance(first_candidate, dict):
        return ""
    content = first_candidate.get("content", {})
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts", [])
    if not isinstance(parts, list):
        return ""
    for part in parts:
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str):
            return text
    return ""


def parse_structured_object(raw_text: str) -> dict[str, Any] | None:
    stripped = raw_text.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def excel_cell_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    return json.dumps(value, ensure_ascii=False)


def sample_count(total: int, fraction: float) -> int:
    if total <= 0:
        return 0
    return min(total, int(total * fraction))


def main() -> None:
    merged_settings, _ = load_step_settings("structured_output_excel", SETTINGS_PATH)

    target_run_id = str(merged_settings["target_run_id"]).strip()
    source_run_id = str(merged_settings["source_run_id"]).strip()

    if not (0 < SAMPLE_FRACTION <= 1):
        raise ValueError("SAMPLE_FRACTION must be between 0 and 1 (exclusive of 0).")

    source_summary_file = resolve_path_from_root(
        PROJECT_ROOT,
        f"app/output/pipeline_runs/{source_run_id}/structured_output_batch_monitor.json",
    )
    if not source_summary_file.exists():
        raise FileNotFoundError(f"Step-7 summary file not found: {source_summary_file}")

    step7_summary = load_json(source_summary_file)
    responses_folder = str(step7_summary.get("data", {}).get("responses_folder", "")).strip()
    if not responses_folder:
        raise ValueError("Missing responses folder in step-7 summary data.responses_folder")

    responses_root = resolve_path_from_root(PROJECT_ROOT, responses_folder)
    if not responses_root.exists() or not responses_root.is_dir():
        raise FileNotFoundError(f"Responses folder not found: {responses_root}")

    prediction_files = sorted(responses_root.rglob("predictions.jsonl"))
    if not prediction_files:
        raise FileNotFoundError(f"No predictions.jsonl files under {responses_root}")

    records: list[tuple[str, str, dict[str, Any] | None]] = []
    for prediction_file in prediction_files:
        for payload in load_jsonl_rows(prediction_file):
            if not isinstance(payload, dict):
                continue
            doc_id = str(payload.get("document_long_id", "")).strip()
            proc_time = str(payload.get("processed_time", "")).strip()
            structured = parse_structured_object(extract_response_text(payload))
            records.append((doc_id, proc_time, structured))

    n = len(records)
    k = sample_count(n, SAMPLE_FRACTION)
    sampled = random.sample(records, k) if k else []

    field_names: set[str] = set()
    for _doc, _pt, structured in sampled:
        if structured:
            field_names.update(structured.keys())
    ordered_fields = sorted(field_names)

    wb = Workbook()
    ws = wb.active
    ws.title = "structured_output"
    header = ["document_long_id", *ordered_fields, "processed_time"]
    ws.append(header)

    for doc_id, proc_time, structured in sampled:
        row: list[Any] = [doc_id]
        for key in ordered_fields:
            if structured and key in structured:
                row.append(excel_cell_value(structured.get(key)))
            else:
                row.append("")
        row.append(proc_time)
        ws.append(row)

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{target_run_id}")
    run_output_dir.mkdir(parents=True, exist_ok=True)
    archive_pipeline_settings(run_output_dir)

    out_path = run_output_dir / "structured_output_sample.xlsx"
    wb.save(out_path)

    log(f"Source summary file: {source_summary_file}")
    log(f"Responses folder: {responses_root}")
    log(f"Prediction files: {len(prediction_files)}")
    log(f"Total rows: {n}, sample fraction: {SAMPLE_FRACTION}, sampled rows: {len(sampled)}")
    log(f"Wrote Excel: {out_path}")


if __name__ == "__main__":
    main()
