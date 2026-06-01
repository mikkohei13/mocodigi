"""Export a random sample of structured-output batch rows to Excel.

Step 7B alternative:
- Same discovery as structured_output_report (step-7 summary → predictions.jsonl).
- Parses Gemini JSON from response.candidates[].content.parts[].text.
- Writes one row per sampled record: empty reviewed column, document_long_id (hyperlink),
  structured fields (schema order, then any extra keys), processed_time.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.worksheet.hyperlink import Hyperlink
from openpyxl.worksheet.worksheet import Worksheet

from utils.files import load_json, load_jsonl_rows, resolve_path as resolve_path_from_root, save_json
from utils.pipeline_config import archive_pipeline_settings, load_step_settings
from utils.runtime import RUN_STATUS_FINISHED, log, now_iso

# Fraction of rows to include after loading (0 < value <= 1).
SAMPLE_FRACTION = 0.10

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "structured_output_excel_settings.json"

# Top-level property order for herbarium specimen structured output (matches pipeline schema).
# Keys present in model output but not listed here are appended after these, sorted.
SCHEMA_PROPERTY_ORDER: tuple[str, ...] = (
    "collectionName",
    "specimenIdentifier",
    "collectorFieldNumber",
    "scientificName",
    "scientificNameAuthorship",
    "dateIdentified",
    "identifiedBy",
    "family",
    "typeStatus",
    "typeName",
    "eventDate",
    "eventDateInterpretation",
    "localityDescription",
    "quadratum",
    "country",
    "countryInterpretation",
    "stateProvince",
    "municipality",
    "coordinates",
    "coordinateSystemInterpretation",
    "latitude",
    "longitude",
    "elevation",
    "habitat",
    "recordedBy",
    "occurrenceRemarks",
    "nonWildInterpretation",
    "chemicalNotes"
)
_SCHEMA_KEYS = frozenset(SCHEMA_PROPERTY_ORDER)


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


def _cell_has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def drop_columns_empty_on_all_data_rows(ws: Worksheet, *, keep_first_column: bool = True) -> None:
    """Remove columns where every data row (row 2+) is empty. Column 1 (reviewed) is never removed."""
    if ws.max_row < 2:
        # No data rows: drop every column except reviewed.
        for col in range(ws.max_column, 1, -1):
            ws.delete_cols(col)
        return
    first_data_row = 2
    last_data_row = ws.max_row
    # Delete right-to-left so indices stay valid.
    for col in range(ws.max_column, 0, -1):
        if keep_first_column and col == 1:
            continue
        has_content = False
        for row in range(first_data_row, last_data_row + 1):
            if _cell_has_content(ws.cell(row=row, column=col).value):
                has_content = True
                break
        if not has_content:
            ws.delete_cols(col)


def build_field_columns(keys_from_data: set[str]) -> list[str]:
    extras = sorted(k for k in keys_from_data if k not in _SCHEMA_KEYS)
    return [*SCHEMA_PROPERTY_ORDER, *extras]


def main() -> None:
    merged_settings, _ = load_step_settings("structured_output_excel", SETTINGS_PATH)

    target_run_id = str(merged_settings["target_run_id"]).strip()
    source_run_id = str(merged_settings["source_run_id"]).strip()

    if not (0 < SAMPLE_FRACTION <= 1):
        raise ValueError("SAMPLE_FRACTION must be between 0 and 1 (exclusive of 0).")

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{target_run_id}")
    summary_file = run_output_dir / "structured_output_excel.json"
    if summary_file.exists():
        existing = load_json(summary_file)
        if existing.get("run_status") == RUN_STATUS_FINISHED:
            log(
                f"Run output already exists with status '{RUN_STATUS_FINISHED}'. "
                f"Not starting run: {summary_file}"
            )
            return

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

    keys_from_data: set[str] = set()
    for _doc, _pt, structured in sampled:
        if structured:
            keys_from_data.update(structured.keys())
    ordered_fields = build_field_columns(keys_from_data)

    wb = Workbook()
    ws = wb.active
    ws.title = "structured_output"
    header = ["reviewed", "document_long_id", *ordered_fields, "processed_time"]
    ws.append(header)

    link_font = Font(color="0563C1", underline="single")
    doc_col = 2  # 1-based column index for document_long_id

    for row_idx, (doc_id, proc_time, structured) in enumerate(sampled, start=2):
        row: list[Any] = ["", doc_id]
        for key in ordered_fields:
            if structured and key in structured:
                row.append(excel_cell_value(structured.get(key)))
            else:
                row.append("")
        row.append(proc_time)
        ws.append(row)
        if doc_id:
            cell = ws.cell(row=row_idx, column=doc_col)
            cell.hyperlink = Hyperlink(ref=cell.coordinate, target=doc_id)
            cell.font = link_font

    drop_columns_empty_on_all_data_rows(ws)

    run_output_dir.mkdir(parents=True, exist_ok=True)
    archive_pipeline_settings(run_output_dir)

    out_path = run_output_dir / "structured_output_sample.xlsx"
    wb.save(out_path)

    final_headers = [ws.cell(row=1, column=c).value for c in range(1, ws.max_column + 1)]

    finished_at = now_iso()
    save_json(
        summary_file,
        {
            "format_version": "0.1",
            "type": "pipeline_output",
            "run_status": RUN_STATUS_FINISHED,
            "started_at": finished_at,
            "finished_at": finished_at,
            "last_updated_at": finished_at,
            "error": None,
            "settings": {
                "target_run_id": target_run_id,
                "source_run_id": source_run_id,
                "sample_fraction": SAMPLE_FRACTION,
            },
            "data": {
                "excel_file": out_path.name,
                "total_rows": n,
                "sampled_rows": len(sampled),
                "excel_columns": final_headers,
            },
        },
    )

    log(f"Source summary file: {source_summary_file}")
    log(f"Responses folder: {responses_root}")
    log(f"Prediction files: {len(prediction_files)}")
    log(f"Total rows: {n}, sample fraction: {SAMPLE_FRACTION}, sampled rows: {len(sampled)}")
    log(f"Wrote Excel: {out_path}")
    log(f"Wrote run summary: {summary_file}")


if __name__ == "__main__":
    main()
