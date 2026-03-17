"""Generate an HTML table report from batch prediction responses."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


pipeline_run_id = "solanaceae-0-id-pre4"


def read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as infile:
        for line_number, line in enumerate(infile, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} in {path}") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"Expected object JSON on line {line_number} in {path}")
            rows.append(payload)
    return rows


def get_prompt_text(row: dict[str, Any]) -> str:
    request = row.get("request", {})
    if not isinstance(request, dict):
        return ""
    contents = request.get("contents", [])
    if not isinstance(contents, list):
        return ""

    text_parts: list[str] = []
    for content in contents:
        if not isinstance(content, dict):
            continue
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text:
                text_parts.append(text)

    return "\n\n".join(text_parts)


def get_response_pretty_json(row: dict[str, Any]) -> str:
    response = row.get("response", {})
    if not isinstance(response, dict):
        return ""
    candidates = response.get("candidates", [])
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

    response_text = ""
    for part in parts:
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str) and text:
            response_text = text
            break

    if not response_text:
        return ""

    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        return response_text

    return json.dumps(response_json, ensure_ascii=False, indent=2)


def make_html(rows: list[dict[str, Any]], pipeline_run_id: str) -> str:
    table_rows: list[str] = []

    for row in rows:
        document_long_id = str(row.get("document_long_id", ""))
        prompt_text = get_prompt_text(row)
        response_pretty_json = get_response_pretty_json(row)

        link_html = ""
        if document_long_id:
            safe_href = html.escape(document_long_id, quote=True)
            safe_text = html.escape(document_long_id)
            link_html = f'<a href="{safe_href}" target="_blank" rel="noopener noreferrer">{safe_text}</a>'

        table_rows.append(
            "<tr>"
            f"<td>{link_html}</td>"
            f"<td><pre>{html.escape(prompt_text)}</pre></td>"
            f"<td><pre>{html.escape(response_pretty_json)}</pre></td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Structured output report: {html.escape(pipeline_run_id)}</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 16px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      table-layout: fixed;
    }}
    th, td {{
      border: 1px solid #ccc;
      padding: 8px;
      vertical-align: top;
      text-align: left;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #f5f5f5;
    }}
    td pre {{
      margin: 0;
      white-space: pre-wrap;
      word-wrap: break-word;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      line-height: 1.4;
    }}
  </style>
</head>
<body>
  <h1>Structured output report: {html.escape(pipeline_run_id)}</h1>
  <p>Records: {len(rows)}</p>
  <table>
    <thead>
      <tr>
        <th>document_long_id</th>
        <th>prompt</th>
        <th>response</th>
      </tr>
    </thead>
    <tbody>
      {''.join(table_rows)}
    </tbody>
  </table>
</body>
</html>
"""


def main() -> None:
    app_dir = Path(__file__).resolve().parent.parent
    run_dir = app_dir / "output" / "pipeline_runs" / pipeline_run_id
    responses_dir = run_dir / "structured_output_batch_responses"
    output_file = run_dir / "structured_report.html"

    if not run_dir.exists() or not run_dir.is_dir():
        raise FileNotFoundError(f"Pipeline run directory not found: {run_dir}")
    if not responses_dir.exists() or not responses_dir.is_dir():
        raise FileNotFoundError(f"Responses directory not found: {responses_dir}")

    input_files = sorted(responses_dir.rglob("predictions.jsonl"))
    if not input_files:
        raise FileNotFoundError(f"No predictions.jsonl files found under: {responses_dir}")

    rows: list[dict[str, Any]] = []
    for input_file in input_files:
        rows.extend(read_jsonl_rows(input_file))

    output_file.write_text(make_html(rows, pipeline_run_id), encoding="utf-8")
    print(f"Wrote HTML report: {output_file}")


if __name__ == "__main__":
    main()
