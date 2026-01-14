"""
Usage: set your API key to environment variable `GOOGLE_API_KEY`, then run:
    python google_geocode.py

Requirements:
    pip install requests

This script reads consolidation data from JSON files in specified folders and
sends the consolidation text to the Google Geocoding API. Results are cached
in a shelve DB to avoid repeated queries. Output TSV is saved to ./app/output
with a datetime in the filename.
"""

import os
import csv
import shelve
import time
from pathlib import Path
from urllib.parse import quote_plus
from datetime import datetime
import requests
from cache_utils import load_consolidation_cache

# Configuration
# List of folder names to process. Each contain images from a single specimen.
folder_names = [
    "images/A01 - Copy",
    "images/B01 - Copy",
    "images/B05 - Copy",
    "images/C02 - Copy",
    "images/C05 - Copy",
    "images/C13 - Copy",
    "images/C14 - Copy",
    "images/D07 - Copy",
    "images/D08 - Copy",
    "images/D11 - Copy",
    "images/D12 - Copy",
    "images/D14 - Copy",
    "images/D16 - Copy",
    "images/D17 - Copy",
    "images/D22 - Copy",
    "images/D23 - Copy",
]

run_version = "16"
branch_version = "" # Set to empty string to use just run_version, or e.g. "b" for "15b"

OUTPUT_DIR = Path("output")
CACHE_DB = OUTPUT_DIR / "google_geocode_cache.db"

# rate limiting
last_query_time = 0.0
MIN_DELAY = 1.0  # seconds between requests

def geocode_token(token, cache, api_key):
    """Return a dict with geocode result or None. Cached by token."""
    if token in cache:
        return cache[token]

    global last_query_time
    # enforce basic rate limit
    elapsed = time.time() - last_query_time
    if elapsed < MIN_DELAY:
        time.sleep(MIN_DELAY - elapsed)

    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote_plus(token)}&key={api_key}"
    resp = requests.get(url, timeout=10)
    last_query_time = time.time()

    try:
        data = resp.json()
    except Exception as e:
        print(f"Error parsing JSON response for token '{token}': {e}")
        cache[token] = {"status": "error", "error": f"invalid json: {e}"}
        return cache[token]

    status = data.get("status")
    if status != "OK":
        cache[token] = {"status": status, "results": []}
        return cache[token]

    results = []
    for r in data.get("results", []):
        loc = r.get("geometry", {}).get("location", {})
        results.append({
            "place_id": r.get("place_id"),
            "formatted_address": r.get("formatted_address"),
            "lat": loc.get("lat"),
            "lon": loc.get("lng"),
            "types": r.get("types", []),
            "raw": r,
        })

    out = {"status": status, "results": results}
    cache[token] = out
    return out


api_key = os.getenv('GOOGLE_API_KEY')
if not api_key:
    print("Error: GOOGLE_API_KEY environment variable not set")
    exit(1)

# Combine run_version and branch_version for consolidation cache
if branch_version:
    consolidation_version = f"{run_version}{branch_version}"
else:
    consolidation_version = run_version

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Generate output filename with datetime
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_TSV = OUTPUT_DIR / f"google_geocoded_results_{timestamp}.tsv"

print(f"Processing {len(folder_names)} folder(s)")
print(f"Run version: {run_version}")
if branch_version:
    print(f"Branch version: {branch_version}")
print(f"Consolidation version: {consolidation_version}")
print(f"Output file: {OUT_TSV}")
print("=" * 50)

with shelve.open(str(CACHE_DB)) as cache, open(OUT_TSV, "w", newline="", encoding="utf-8") as fout:
    fieldnames = ["folder", "text", "lat", "lon", "formatted_address", "types", "status", "selected"]
    writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()

    for folder_name in folder_names:
        print(f"\nProcessing {folder_name}...")
        base_folder = Path(folder_name)
        
        # Load consolidation data
        try:
            cache_data = load_consolidation_cache(base_folder, consolidation_version)
            consolidation_text = cache_data["data"]["consolidation"]
        except FileNotFoundError:
            print(f"Warning: Consolidation cache not found for run_{consolidation_version}, skipping...")
            writer.writerow({
                "folder": folder_name,
                "text": "",
                "lat": "",
                "lon": "",
                "formatted_address": "",
                "types": "",
                "status": "consolidation_not_found",
                "selected": False
            })
            continue
        except Exception as e:
            print(f"Error loading consolidation for {folder_name}: {e}")
            writer.writerow({
                "folder": folder_name,
                "text": "",
                "lat": "",
                "lon": "",
                "formatted_address": "",
                "types": "",
                "status": f"error_loading: {e}",
                "selected": False
            })
            continue

        if not consolidation_text or not consolidation_text.strip():
            writer.writerow({
                "folder": folder_name,
                "text": "",
                "lat": "",
                "lon": "",
                "formatted_address": "",
                "types": "",
                "status": "empty_consolidation",
                "selected": False
            })
            continue

        print("Geocoding text:", consolidation_text)

        res = geocode_token(consolidation_text, cache, api_key)

        print("Geocode result status:", res.get("status"))
        print("Number of results:", len(res.get("results", [])) if res else 0)

        if not res:
            writer.writerow({
                "folder": folder_name,
                "text": consolidation_text,
                "lat": "",
                "lon": "",
                "formatted_address": "",
                "types": "",
                "status": "no_result",
                "selected": False
            })
            continue

        status = res.get("status")
        if status == "OK" and res.get("results"):
            first = res["results"][0]
            writer.writerow({
                "folder": folder_name,
                "text": consolidation_text,
                "lat": first.get("lat"),
                "lon": first.get("lon"),
                "formatted_address": first.get("formatted_address"),
                "types": ";".join(first.get("types", [])),
                "status": status,
                "selected": True,
            })
        else:
            writer.writerow({
                "folder": folder_name,
                "text": consolidation_text,
                "lat": "",
                "lon": "",
                "formatted_address": "",
                "types": "",
                "status": status,
                "selected": False
            })

print(f"\nDone. Results written to {OUT_TSV}. Cached responses in {CACHE_DB}.")
