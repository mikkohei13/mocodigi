"""
Build an HTML table comparing specimen label data from FinBIF document.json
to AI-transcribed data from run transcript JSON files.
"""
import html
import json
from datetime import datetime
from pathlib import Path

from image_utils import get_subfolders

RUN_ID = "run_h1"
IMAGES_DIR = Path(__file__).resolve().parent / "images_lajifi"


def extract_specimen_label_parts(doc: dict) -> list[str]:
    """First gathering and first unit: taxonVerbatim, higherGeography, country, displayDateTime."""
    gatherings = doc.get("document", {}).get("gatherings") or []
    if not gatherings:
        return ["", "", "", ""]
    g = gatherings[0]
    units = g.get("units") or []
    if not units:
        return ["", "", "", ""]
    u = units[0]
    return [
        u.get("taxonVerbatim", ""),
        g.get("higherGeography", ""),
        g.get("country", ""),
        g.get("displayDateTime", ""),
    ]


def get_first_transcript(run_dir: Path) -> str | None:
    """Read data.transcript from first *_transcript.json in run_dir. Return None if missing."""
    transcripts = sorted(run_dir.glob("*_transcript.json"))
    if not transcripts:
        return None
    with open(transcripts[0], encoding="utf-8") as f:
        data = json.load(f)
    return (data.get("data") or {}).get("transcript")

def get_css() -> str:
    return """
    <style>
    table {
        width: 100%;
        border-collapse: collapse;
    }
    </style>
    <style>
    td {
        padding: 8px;
        border: 1px solid #ddd;
        vertical-align: top;
    }
    </style>
    """

def main() -> None:
    rows = []
    for subfolder in get_subfolders(str(IMAGES_DIR)):
        doc_path = subfolder / "document.json"
        if not doc_path.is_file():
            continue
        try:
            with open(doc_path, encoding="utf-8") as f:
                doc = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        run_dir = subfolder / RUN_ID
        if not run_dir.is_dir():
            continue
        transcript = get_first_transcript(run_dir)
        if transcript is None:
            continue

        document_id = doc.get("document", {}).get("documentId", "")
        parts = extract_specimen_label_parts(doc)
        rows.append((document_id, parts, transcript))

    out_path = IMAGES_DIR / f"comparison_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
        f.write(get_css())
        f.write(f"</head><body><h1>{RUN_ID} with {len(rows)} rows</h1><table>\n")
        f.write("<tr><th>Document ID</th><th>Specimen data</th><th>Transcript</th></tr>\n")
        for document_id, parts, transcript in rows:
            col_id = f"<a href='{document_id}' target='_blank'>" + html.escape(document_id) + "</a>"
            col_specimen = "<br>\n".join(html.escape(p) if p else "&nbsp;" for p in parts)
            col_transcript = html.escape(transcript).replace("\n", "<br>\n")
            f.write(f"<tr><td>{col_id}</td><td>{col_specimen}</td><td>{col_transcript}</td></tr>\n")
        f.write("</table></body></html>\n")

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
