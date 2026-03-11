"""Read occurrence rows, fetch record JSON, and save first image per record."""

import csv
import json
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

API_SUFFIX = "?format=json"
LIMIT = 100000
SLEEP_TIME = 1

IMAGE_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def make_qname(parent_event_id: str) -> str:
    """Convert full Luomus URL into qname-like identifier."""
    qname = parent_event_id.strip()
    qname = qname.replace("http://id.luomus.fi/", "")
    return qname


def image_name_from_url(image_url: str) -> str:
    """Extract output filename from image URL path."""
    path_part = urlparse(image_url).path
    name = Path(path_part).name
    return name or "image.jpg"


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    input_path = script_dir.parent / "images-solanaceae" / "occurrences.txt"
    output_root = script_dir.parent / "images-solanaceae"

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")

        headers = next(reader)
        # Rows 2 and 3 are localized/translated header rows, skip both.
        next(reader, None)
        next(reader, None)

        try:
            parent_event_id_idx = headers.index("parentEventID")
        except ValueError as exc:
            raise ValueError("Missing 'parentEventID' column in header row.") from exc

        for row_number, row in enumerate(reader, start=4):
            if row_number > LIMIT:
                break
            
            if not row or parent_event_id_idx >= len(row):
                print(f"[row {row_number}] Skipping empty or malformed row.")
                continue

            parent_event_id = row[parent_event_id_idx].strip()
            if not parent_event_id:
                print(f"[row {row_number}] Missing parentEventID, skipping.")
                continue

            qname = make_qname(parent_event_id)
            qname_suffix = qname[-1] if qname else "_"
            target_dir = output_root / qname_suffix / qname
            target_dir.mkdir(parents=True, exist_ok=True)
            document_path = target_dir / "document.json"

            if document_path.exists():
                print(f"[row {row_number}] {qname}: document.json exists, skipping record.")
                continue

            api_url = f"{parent_event_id}{API_SUFFIX}"
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                data = response.json()
            except (requests.RequestException, ValueError) as exc:
                print(f"[row {row_number}] Failed to fetch JSON for {parent_event_id}: {exc}")
                continue

            with document_path.open("w", encoding="utf-8") as out_json:
                json.dump(data, out_json, ensure_ascii=False, indent=2)

            try:
                image_url = data["document"]["gatherings"][0]["units"][0]["media"][0]["fullURL"]
            except (KeyError, IndexError, TypeError):
                print(f"[row {row_number}] No first image found for {qname}.")
                continue

            image_name = image_name_from_url(image_url)
            image_path = target_dir / image_name

            try:
                time.sleep(SLEEP_TIME)
                image_response = requests.get(
                    image_url,
                    headers=IMAGE_REQUEST_HEADERS,
                    timeout=60,
                )
                image_response.raise_for_status()
            except requests.RequestException as exc:
                print(f"[row {row_number}] Failed to fetch image for {qname}: {exc}")
                continue

            with image_path.open("wb") as image_file:
                image_file.write(image_response.content)

            print(f"[row {row_number}] Saved {qname} -> {document_path.name}, {image_name}")


if __name__ == "__main__":
    main()