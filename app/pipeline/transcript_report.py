"""Generate an HTML transcript report from batch prediction responses.

Step 4:
- Read step-3 monitor summary to locate downloaded batch responses.
- Parse all predictions.jsonl rows.
- Build an HTML table with specimen id (qname) and transcript text.
"""

from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from utils.files import load_json, load_jsonl_rows, resolve_path as resolve_path_from_root, validate_json_file
from utils.runtime import log


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "transcript_report_settings.json"


def validate_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = raw.get("settings", {})
    required = ["run_id", "source_run_id"]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def extract_qname(payload: dict[str, Any]) -> str:
    contents = payload.get("request", {}).get("contents", [])
    if not isinstance(contents, list):
        return ""
    for content in contents:
        if not isinstance(content, dict):
            continue
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            file_data = part.get("fileData")
            if not isinstance(file_data, dict):
                continue
            file_uri = str(file_data.get("fileUri", "")).strip()
            if not file_uri.startswith("gs://"):
                continue
            uri_parts = [segment for segment in file_uri.split("/") if segment]
            # gs://bucket/qname/file.jpg => qname is penultimate segment
            if len(uri_parts) >= 4:
                return uri_parts[-2]
    return ""


def extract_transcript(payload: dict[str, Any]) -> str:
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


def build_html_report(*, run_id: str, source_run_id: str, rows: list[dict[str, str]]) -> str:
    table_rows = []
    for item in rows:
        qname_html = html.escape(item.get("qname", ""))
        transcript_html = html.escape(item.get("transcript", ""))
        table_rows.append(
            "<tr>"
            f"<td>{qname_html}</td>"
            f"<td class=\"transcript\">{transcript_html}</td>"
            "</tr>"
        )

    rows_html = "\n".join(table_rows)
    title = html.escape(f"Transcript Report - {run_id}")
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
    td.transcript {{
      white-space: pre-wrap;
      word-break: break-word;
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
        <th>Transcript</th>
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
    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    validate_json_file(SETTINGS_PATH)
    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)

    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()

    source_summary_file = resolve_path_from_root(
        PROJECT_ROOT,
        f"app/output/pipeline_runs/{source_run_id}/transcript_batch_monitor.json",
    )
    if not source_summary_file.exists():
        raise FileNotFoundError(f"Step-3 summary file not found: {source_summary_file}")

    step3_summary = load_json(source_summary_file)
    responses_folder = str(step3_summary.get("data", {}).get("responses_folder", "")).strip()
    if not responses_folder:
        raise ValueError("Missing responses folder in step-3 summary data.responses_folder")

    responses_root = resolve_path_from_root(PROJECT_ROOT, responses_folder)
    if not responses_root.exists() or not responses_root.is_dir():
        raise FileNotFoundError(f"Responses folder not found: {responses_root}")

    prediction_files = sorted(responses_root.rglob("predictions.jsonl"))
    if not prediction_files:
        raise FileNotFoundError(f"No predictions.jsonl files found under {responses_root}")

    rows: list[dict[str, str]] = []
    for prediction_file in prediction_files:
        for payload in load_jsonl_rows(prediction_file):
            row = {
                "qname": extract_qname(payload),
                "transcript": extract_transcript(payload),
            }
            rows.append(row)

    rows.sort(key=lambda item: (item.get("qname", "").lower(), item.get("qname", "")))

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    report_file = run_output_dir / "transcript_report.html"
    report_file.parent.mkdir(parents=True, exist_ok=True)

    report_html = build_html_report(run_id=run_id, source_run_id=source_run_id, rows=rows)
    report_file.write_text(report_html, encoding="utf-8")

    log(f"Source summary file: {source_summary_file}")
    log(f"Responses folder: {responses_root}")
    log(f"Prediction files: {len(prediction_files)}")
    log(f"Report rows: {len(rows)}")
    log(f"Wrote transcript report: {report_file}")


if __name__ == "__main__":
    main()
