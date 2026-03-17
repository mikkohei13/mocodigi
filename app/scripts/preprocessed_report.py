"""Generate an HTML table report from preprocess_structure.jsonl."""

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


def make_html(rows: list[dict[str, Any]], pipeline_run_id: str) -> str:
    table_rows: list[str] = []

    for row in rows:
        document_long_id = str(row.get("document_long_id", ""))
        data = row.get("data", {})
        if not isinstance(data, dict):
            data = {}

        preprocessed_transcript = str(data.get("preprocessed_transcript", ""))
        preprocess_details = data.get("preprocess_details", {})
        if preprocess_details is None:
            preprocess_details = {}

        pretty_details = json.dumps(preprocess_details, ensure_ascii=False, indent=2)

        link_html = ""
        if document_long_id:
            safe_href = html.escape(document_long_id, quote=True)
            safe_text = html.escape(document_long_id)
            link_html = f'<a href="{safe_href}" target="_blank" rel="noopener noreferrer">{safe_text}</a>'

        table_rows.append(
            "<tr>"
            f"<td>{link_html}</td>"
            f"<td><pre>{html.escape(preprocessed_transcript)}</pre></td>"
            f"<td><pre>{html.escape(pretty_details)}</pre></td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Preprocessed report: {html.escape(pipeline_run_id)}</title>
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
  <h1>Preprocessed report: {html.escape(pipeline_run_id)}</h1>
  <p>Records: {len(rows)}</p>
  <table>
    <thead>
      <tr>
        <th>document_long_id</th>
        <th>preprocessed_transcript</th>
        <th>preprocess_details</th>
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
    input_file = run_dir / "preprocess_structure.jsonl"
    output_file = run_dir / "preprocessed_report.html"

    if not run_dir.exists() or not run_dir.is_dir():
        raise FileNotFoundError(f"Pipeline run directory not found: {run_dir}")
    if not input_file.exists():
        raise FileNotFoundError(f"Input JSONL not found: {input_file}")

    rows = read_jsonl_rows(input_file)
    output_file.write_text(make_html(rows, pipeline_run_id), encoding="utf-8")
    print(f"Wrote HTML report: {output_file}")


if __name__ == "__main__":
    main()
