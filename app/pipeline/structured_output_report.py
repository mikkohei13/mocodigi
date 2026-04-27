"""Generate an HTML report from structured-output batch prediction responses.

Step 7B:
- Read step-7 monitor summary to locate downloaded batch responses.
- Parse all predictions.jsonl rows.
- Build an HTML table with specimen id (qname), source images, and formatted JSON.
"""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from utils.files import load_json, load_jsonl_rows, resolve_path as resolve_path_from_root
from utils.pipeline_config import archive_pipeline_settings, load_step_settings
from utils.runtime import log


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "structured_output_report_settings.json"


def validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
    required = ["run_id", "source_run_id"]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def extract_qname(payload: dict[str, Any]) -> str:
    direct_qname = str(payload.get("qname", "")).strip()
    if direct_qname:
        return direct_qname
    return ""


def extract_source_images_display(payload: dict[str, Any]) -> str:
    raw = payload.get("source_images")
    if isinstance(raw, list):
        return ", ".join(str(x).strip() for x in raw if str(x).strip())
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return ""


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


def format_structured_text(raw_text: str) -> str:
    stripped = raw_text.strip()
    if not stripped:
        return ""
    try:
        parsed = json.loads(stripped)
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    except json.JSONDecodeError:
        return raw_text


def build_html_report(*, run_id: str, source_run_id: str, rows: list[dict[str, str]]) -> str:
    table_rows = []
    for item in rows:
        qname = item.get("qname", "")
        qname_text = html.escape(qname)
        if qname:
            qname_html = (
                f"<a href=\"https://id.luomus.fi/{qname_text}\" "
                f"target=\"_blank\" rel=\"noopener noreferrer\">{qname_text}</a>"
            )
        else:
            qname_html = ""
        images_html = html.escape(item.get("source_images", ""))
        structured_html = html.escape(item.get("structured_json", ""))
        table_rows.append(
            "<tr>"
            f"<td>{qname_html}</td>"
            f"<td class=\"images\">{images_html}</td>"
            f"<td class=\"structured\"><pre>{structured_html}</pre></td>"
            "</tr>"
        )

    rows_html = "\n".join(table_rows)
    title = html.escape(f"Structured Output Report - {run_id}")
    subtitle = html.escape(
        f"Source run: {source_run_id} | Rows: {len(rows)}"
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      margin: 20px;
    }}
    h1 {{
      margin-bottom: 8px;
    }}
    p.meta {{
      margin-top: 0;
      color: #444;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    th, td {{
      border: 1px solid #d0d0d0;
      padding: 8px;
      vertical-align: top;
      text-align: left;
    }}
    th {{
      background: #f5f5f5;
      position: sticky;
      top: 0;
    }}
    td:first-child, th:first-child {{
      width: 180px;
      white-space: nowrap;
    }}
    td.images, th:nth-child(2) {{
      width: 260px;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    td.structured pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <p class="meta">{subtitle}</p>
  <table>
    <thead>
      <tr>
        <th>Specimen ID</th>
        <th>Source images</th>
        <th>Structured output</th>
      </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
</body>
</html>
"""


def main() -> None:
    merged_settings, _ = load_step_settings("structured_output_report", SETTINGS_PATH)
    settings = validate_settings(merged_settings)

    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()

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
        raise FileNotFoundError(f"No predictions.jsonl files found under {responses_root}")

    rows: list[dict[str, str]] = []
    for prediction_file in prediction_files:
        for payload in load_jsonl_rows(prediction_file):
            raw_text = extract_response_text(payload)
            rows.append(
                {
                    "qname": extract_qname(payload),
                    "source_images": extract_source_images_display(payload),
                    "structured_json": format_structured_text(raw_text),
                }
            )

    rows.sort(key=lambda item: (item.get("qname", "").lower(), item.get("qname", "")))

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    report_file = run_output_dir / "structured_output_report.html"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    archive_pipeline_settings(run_output_dir)

    report_html = build_html_report(run_id=run_id, source_run_id=source_run_id, rows=rows)
    report_file.write_text(report_html, encoding="utf-8")

    log(f"Source summary file: {source_summary_file}")
    log(f"Responses folder: {responses_root}")
    log(f"Prediction files: {len(prediction_files)}")
    log(f"Report rows: {len(rows)}")
    log(f"Wrote structured output report: {report_file}")


if __name__ == "__main__":
    main()
