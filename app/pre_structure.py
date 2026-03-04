"""Preprocess specimen transcripts before structuring with an LLM."""

from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path

from preprocess_utils import preprocess

# Configuration
SOURCE_ROOT = "images_lajifi" # images-solanaceae-trial
RUN_VERSION = "h1" # solanaceae2

SUBRUN_VERSION = "A"

TRANSCRIPT_SUFFIX = "_transcript.json"
REPORT_FILENAME = "herbarium_preprocess_comparison.html"


def read_transcript_text(json_path: Path) -> tuple[str | None, str | None]:
    """Read `data.transcript` from JSON, returning (transcript, error)."""
    try:
        with json_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"error reading {json_path.name}: {exc}"

    data = payload.get("data")
    if not isinstance(data, dict):
        return None, "missing data object"

    transcript = data.get("transcript")
    if transcript is None:
        return None, "missing data.transcript"

    return str(transcript), None


def output_filename_for_transcript(transcript_path: Path) -> str:
    """Build `*_subrun-{SUBRUN_VERSION}_preprocessed.json` output filename."""
    stem = transcript_path.stem
    if stem.endswith("_transcript"):
        stem = stem[: -len("_transcript")]
    return f"{stem}_subrun-{SUBRUN_VERSION}_preprocessed.json"


def write_preprocessed_json(
    output_path: Path,
    source_path: Path,
    raw_transcript: str,
    preprocessed_transcript: str,
    preprocess_details: dict[str, object],
) -> None:
    """Write preprocessed transcript payload to JSON."""
    payload = {
        "format_version": "0.1",
        "type": "preprocessed_transcript",
        "datetime": datetime.now().isoformat(),
        "settings": {
            "run_version": RUN_VERSION,
            "subrun_version": SUBRUN_VERSION,
            "source_file": source_path.name,
        },
        "data": {
            "raw_transcript": raw_transcript,
            "preprocessed_transcript": preprocessed_transcript,
            "preprocess_details": preprocess_details,
        },
    }
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def to_html_pre(text: str) -> str:
    return f"<pre>{html.escape(text or '')}</pre>"


def build_html_report(rows: list[dict[str, str]]) -> str:
    """Build a comparison HTML page for all specimens."""
    body_rows: list[str] = []
    for row in rows:
        body_rows.append(
            "<tr>"
            f"<td>{html.escape(row['specimen_id'])}</td>"
            f"<td>{html.escape(row['source'])}</td>"
            f"<td>{html.escape(row['status'])}</td>"
            f"<td>{to_html_pre(row['raw'])}</td>"
            f"<td>{to_html_pre(row['preprocessed'])}</td>"
            f"<td>{to_html_pre(row['details'])}</td>"
            "</tr>"
        )

    table_rows = "\n".join(body_rows) if body_rows else (
        "<tr><td colspan='6'>No specimen folders found.</td></tr>"
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Herbarium Transcript Preprocess Comparison</title>
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
      width: 120px;
    }}
    th:nth-child(2), td:nth-child(2) {{
      width: 170px;
    }}
    th:nth-child(3), td:nth-child(3) {{
      width: 140px;
    }}
    th:nth-child(4), td:nth-child(4),
    th:nth-child(5), td:nth-child(5),
    th:nth-child(6), td:nth-child(6) {{
      width: calc((100% - 430px) / 3);
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
  <h1>Herbarium Transcript Preprocess Comparison</h1>
  <div class="meta">
    Source root: {html.escape(SOURCE_ROOT)} |
    Run: {html.escape(RUN_VERSION)} |
    Subrun: {html.escape(SUBRUN_VERSION)} |
    Rows: {len(rows)}
  </div>
  <table>
    <thead>
      <tr>
        <th>Specimen</th>
        <th>Source transcript file</th>
        <th>Status</th>
        <th>Raw transcript</th>
        <th>Preprocessed transcript</th>
        <th>Preprocess details</th>
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
    source_root_path = script_dir / SOURCE_ROOT
    folder_names = [p for p in source_root_path.iterdir() if p.is_dir()] if source_root_path.exists() else []
    report_path = script_dir / "output" / REPORT_FILENAME

    rows: list[dict[str, str]] = []
    processed_count = 0
    missing_run_count = 0
    missing_transcript_count = 0
    error_count = 0

    for specimen_dir in sorted(folder_names, key=lambda p: p.name):
        run_dir = specimen_dir / f"run_{RUN_VERSION}"
        row = {
            "specimen_id": specimen_dir.name,
            "source": "[missing]",
            "status": "",
            "raw": "",
            "preprocessed": "",
            "details": "",
        }

        if not run_dir.exists() or not run_dir.is_dir():
            row["status"] = f"missing run folder: {run_dir.name}"
            missing_run_count += 1
            rows.append(row)
            continue

        transcript_files = sorted(run_dir.glob(f"*{TRANSCRIPT_SUFFIX}"))
        if not transcript_files:
            row["status"] = "missing transcript file"
            missing_transcript_count += 1
            rows.append(row)
            continue

        transcript_path = transcript_files[0]
        row["source"] = transcript_path.name
        transcript_text, error = read_transcript_text(transcript_path)
        if error or transcript_text is None:
            row["status"] = error or "unknown read error"
            error_count += 1
            rows.append(row)
            continue

        preprocessed_text, preprocess_details = preprocess(transcript_text)
        output_path = run_dir / output_filename_for_transcript(transcript_path)
        write_preprocessed_json(
            output_path=output_path,
            source_path=transcript_path,
            raw_transcript=transcript_text,
            preprocessed_transcript=preprocessed_text,
            preprocess_details=preprocess_details,
        )

        row["status"] = "ok"
        row["raw"] = transcript_text
        row["preprocessed"] = preprocessed_text
        row["details"] = json.dumps(preprocess_details, ensure_ascii=False, indent=2)
        rows.append(row)
        processed_count += 1

    report_path.write_text(build_html_report(rows), encoding="utf-8")

    print(f"Saved comparison HTML: {report_path}")
    print(
        "Summary: "
        f"processed={processed_count}, "
        f"missing_run={missing_run_count}, "
        f"missing_transcript={missing_transcript_count}, "
        f"errors={error_count}"
    )


if __name__ == "__main__":
    main()