import json
import csv
import re
import sys
from pathlib import Path

# Matches "Grid 27{any char} E:" with nothing else (ignoring surrounding spaces)
EMPTY_GRID_COORDS = re.compile(r"^\s*Grid 27.?\s*E:\s*$", re.IGNORECASE)

INPUT_FILE = Path("output/pipeline_runs/sensitive_specimens/structured_output_batch_responses/prediction-model-2026-04-29T10:58:29.018390Z/predictions.jsonl")
OUTPUT_FILE = Path("output/pipeline_runs/sensitive_specimens/compiled_specimens.tsv")

COLUMNS = ["qname", "document_long_id", "source_images", "countryInterpretation", "coordinates", "coordinateSystemInterpretation", "latitude", "longitude"]

with INPUT_FILE.open() as infile, OUTPUT_FILE.open("w", newline="") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=COLUMNS, delimiter="\t")
    writer.writeheader()

    for line_num, line in enumerate(infile, 1):
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Line {line_num}: JSON parse error: {e}", file=sys.stderr)
            continue

        structured = {}
        try:
            text = record["response"]["candidates"][0]["content"]["parts"][0]["text"]
            structured = json.loads(text)
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            print(f"Line {line_num} ({record.get('qname')}): Could not parse structured output: {e}", file=sys.stderr)

        writer.writerow({
            "qname": record.get("qname"),
            "document_long_id": record.get("document_long_id"),
            "source_images": ";".join(record.get("source_images") or []),
            "countryInterpretation": structured.get("countryInterpretation"),
            "coordinates": None if (c := structured.get("coordinates")) and EMPTY_GRID_COORDS.match(c) else c,
            "coordinateSystemInterpretation": structured.get("coordinateSystemInterpretation"),
            "latitude": structured.get("latitude"),
            "longitude": structured.get("longitude"),
        })

print(f"Done. Output written to {OUTPUT_FILE}")
