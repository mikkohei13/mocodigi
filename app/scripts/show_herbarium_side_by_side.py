"""Generate an HTML view of herbarium scans, transcripts, and structured output.

The script reads specimen folders under `images-solanaceae-trial` and renders one
table row per specimen with these columns:
- specimen scan image
- transcript text from run_solanaceae1
- structured output JSON from run_solanaceae1
"""

from __future__ import annotations

import html
import json
import os
from pathlib import Path

DATASET_DIR = "../images-solanaceae-trial"
RUN_FOLDER = "run_solanaceae2"
TRANSCRIPT_SUFFIX = "_transcript.json"
STRUCTURED_OUTPUT_FILE = "structured_output.json"
OUTPUT_FILE = "herbarium_side_by_side.html"
OUTPUT_DIR = "output"


def read_transcript_text(run_dir: Path) -> str:
    transcript_files = sorted(run_dir.glob(f"*{TRANSCRIPT_SUFFIX}"))
    if not transcript_files:
        return "[missing transcript file]"

    transcript_path = transcript_files[0]
    try:
        with transcript_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return f"[error reading {transcript_path.name}: {exc}]"

    data = payload.get("data")
    if not isinstance(data, dict):
        return "[missing data object]"

    transcript = data.get("transcript")
    if transcript is None:
        return "[missing data.transcript]"
    return str(transcript)


def read_structured_output(run_dir: Path) -> str:
    output_path = run_dir / STRUCTURED_OUTPUT_FILE
    if not output_path.exists():
        return "[missing structured_output.json]"

    try:
        with output_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return f"[error reading {output_path.name}: {exc}]"

    data = payload.get("data")
    if not isinstance(data, dict):
        return json.dumps(payload, indent=2, ensure_ascii=False)
    return json.dumps(data, indent=2, ensure_ascii=False)


def find_scan_image(specimen_dir: Path) -> Path | None:
    candidates = sorted(specimen_dir.glob("*.jpg")) + sorted(specimen_dir.glob("*.jpeg"))
    if candidates:
        return candidates[0]
    return None


def to_pre(text: str) -> str:
    return f"<pre>{html.escape(text or '')}</pre>"


def build_html(rows: list[str], specimen_count: int) -> str:
    body_rows = "\n".join(rows) if rows else "<tr><td colspan='4'>No data found.</td></tr>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Herbarium Side-by-Side View</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 24px;
      color: #1f2328;
      background: #ffffff;
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 24px;
    }}
    .meta {{
      margin-bottom: 18px;
      color: #57606a;
      font-size: 14px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      table-layout: fixed;
    }}
    th, td {{
      border: 1px solid #d0d7de;
      vertical-align: top;
      padding: 10px;
      text-align: left;
    }}
    th {{
      background: #f6f8fa;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    th:nth-child(1), td:nth-child(1) {{
      width: 130px;
    }}
    th:nth-child(2), td:nth-child(2) {{
      width: 350px;
    }}
    th:nth-child(3), td:nth-child(3),
    th:nth-child(4), td:nth-child(4) {{
      width: calc((100% - 480px) / 2);
    }}
    .scan img {{
      max-width: 100%;
      height: auto;
      display: block;
      border: 1px solid #d8dee4;
      border-radius: 4px;
    }}
    .scan .missing {{
      color: #57606a;
      font-style: italic;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      line-height: 1.45;
      max-height: 620px;
      overflow: auto;
    }}
  </style>
</head>
<body>
  <h1>Herbarium Specimen Side-by-Side View</h1>
  <div class="meta">
    Dataset: {html.escape(DATASET_DIR)} |
    Run folder: {html.escape(RUN_FOLDER)} |
    Specimens: {specimen_count}
  </div>
  <table>
    <thead>
      <tr>
        <th>Specimen</th>
        <th>Scan</th>
        <th>Transcript</th>
        <th>Structured output</th>
      </tr>
    </thead>
    <tbody>
      {body_rows}
    </tbody>
  </table>
</body>
</html>
"""


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    dataset_root = script_dir / DATASET_DIR
    output_dir = script_dir / OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILE

    rows: list[str] = []
    specimen_count = 0

    for specimen_dir in sorted(p for p in dataset_root.iterdir() if p.is_dir()):
        run_dir = specimen_dir / RUN_FOLDER
        if not run_dir.exists() or not run_dir.is_dir():
            continue

        specimen_count += 1
        transcript_text = read_transcript_text(run_dir)
        structured_text = read_structured_output(run_dir)

        image_path = find_scan_image(specimen_dir)
        if image_path is None:
            scan_html = '<div class="missing">[missing image]</div>'
        else:
            relative = os.path.relpath(image_path, output_dir).replace("\\", "/")
            scan_html = (
                f'<a href="{html.escape(relative)}" target="_blank" rel="noopener noreferrer">'
                f'<img src="{html.escape(relative)}" alt="{html.escape(specimen_dir.name)} scan"></a>'
            )

        specimen_id = html.escape(specimen_dir.name)
        specimen_url = f"http://id.luomus.fi/{specimen_id}"
        row_html = (
            "<tr>"
            f"<td><a href=\"{specimen_url}\" target=\"_blank\" rel=\"noopener noreferrer\">{specimen_id}</a></td>"
            f"<td class='scan'>{scan_html}</td>"
            f"<td>{to_pre(transcript_text)}</td>"
            f"<td>{to_pre(structured_text)}</td>"
            "</tr>"
        )
        rows.append(row_html)

    html_doc = build_html(rows, specimen_count)
    output_path.write_text(html_doc, encoding="utf-8")

    print(f"Saved side-by-side HTML: {output_path}")
    print(f"Specimens included: {specimen_count}")


if __name__ == "__main__":
    main()
