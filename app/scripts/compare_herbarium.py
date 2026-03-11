"""Generate an HTML comparison table for herbarium transcripts.

The script compares transcript JSON files under:
- images-solanaceae-trial
- images-solanaceae-trial-cropped

Rows are matched by specimen folder name (for example, C.319050) and display
`data.transcript` from both sources side by side.
"""

from __future__ import annotations

import html
import json
from pathlib import Path

RUN_FOLDER = "run_solanaceae1"
TRANSCRIPT_SUFFIX = "_transcript.json"
LEFT_LABEL = "../images-solanaceae-trial"
RIGHT_LABEL = "../images-solanaceae-trial-cropped"


def read_transcript_text(json_path: Path) -> str:
    """Read transcript text from a transcript JSON file."""
    try:
        with json_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return f"[error reading {json_path.name}: {exc}]"

    data = payload.get("data")
    if not isinstance(data, dict):
        return "[missing data object]"

    transcript = data.get("transcript")
    if transcript is None:
        return "[missing data.transcript]"

    return str(transcript)


def collect_transcripts(root: Path) -> dict[str, str]:
    """Collect one transcript per specimen folder from the given root."""
    transcripts: dict[str, str] = {}
    if not root.exists():
        return transcripts

    for specimen_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        run_dir = specimen_dir / RUN_FOLDER
        if not run_dir.exists() or not run_dir.is_dir():
            continue

        transcript_files = sorted(run_dir.glob(f"*{TRANSCRIPT_SUFFIX}"))
        if not transcript_files:
            continue

        transcripts[specimen_dir.name] = read_transcript_text(transcript_files[0])

    return transcripts


def to_html_pre(text: str) -> str:
    escaped = html.escape(text or "")
    return f"<pre>{escaped}</pre>"


def char_count_without_linebreaks(text: str) -> int:
    normalized = text.replace("\r", "").replace("\n", "")
    return len(normalized)


def build_html_table(
    left_transcripts: dict[str, str],
    right_transcripts: dict[str, str],
) -> str:
    """Build a complete HTML document with comparison table."""
    specimen_ids = sorted(set(left_transcripts) | set(right_transcripts))
    rows: list[str] = []

    for specimen_id in specimen_ids:
        left_text = left_transcripts.get(specimen_id, "[missing]")
        right_text = right_transcripts.get(specimen_id, "[missing]")
        left_char_count = char_count_without_linebreaks(left_text)
        right_char_count = char_count_without_linebreaks(right_text)
        char_difference = abs(left_char_count - right_char_count)
        row_html = (
            "<tr>"
            f"<td>{html.escape(specimen_id)}</td>"
            f"<td>{char_difference}</td>"
            f"<td>{to_html_pre(left_text)}</td>"
            f"<td>{to_html_pre(right_text)}</td>"
            "</tr>"
        )
        rows.append(row_html)

    table_rows = "\n".join(rows) if rows else (
        "<tr><td colspan='4'>No transcript files found.</td></tr>"
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Herbarium Transcript Comparison</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 24px;
      color: #1f2328;
      background: #ffffff;
    }}
    h1 {{
      margin-top: 0;
      margin-bottom: 8px;
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
      width: 140px;
    }}
    th:nth-child(2), td:nth-child(2) {{
      width: 80px;
      text-align: right;
    }}
    th:nth-child(3), td:nth-child(3),
    th:nth-child(4), td:nth-child(4) {{
      width: calc((100% - 220px) / 2);
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      line-height: 1.45;
    }}
  </style>
</head>
<body>
  <h1>Herbarium Transcript Comparison</h1>
  <div class="meta">
    Rows: {len(specimen_ids)} |
    Left: {html.escape(LEFT_LABEL)} |
    Right: {html.escape(RIGHT_LABEL)}
  </div>
  <table>
    <thead>
      <tr>
        <th>Specimen</th>
        <th>Char diff</th>
        <th>{html.escape(LEFT_LABEL)}</th>
        <th>{html.escape(RIGHT_LABEL)}</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>
</body>
</html>
"""


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    left_root = script_dir / LEFT_LABEL
    right_root = script_dir / RIGHT_LABEL
    output_path = script_dir / "herbarium_comparison.html"

    left_transcripts = collect_transcripts(left_root)
    right_transcripts = collect_transcripts(right_root)
    html_content = build_html_table(left_transcripts, right_transcripts)
    output_path.write_text(html_content, encoding="utf-8")

    print(f"Saved comparison HTML: {output_path}")
    print(f"Specimens compared: {len(set(left_transcripts) | set(right_transcripts))}")


if __name__ == "__main__":
    main()