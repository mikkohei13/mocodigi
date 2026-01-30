"""
Fetch specimen records from FinBIF API and display their names.

Requires FINBIF_ACCESS_TOKEN in environment (see .env).
See FinBIF_API.md for API details.
"""

import json
import os
import sys
import time

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

IMAGES_DIR = "images_lajifi"

# Browser-like User-Agent for image requests (some servers block scripts)
IMAGE_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def sanitize_id(identifier: str) -> str:
    """Make a full identifier safe for dir/file names by replacing : and / with _."""
    return identifier.replace(":", "_").replace("/", "_")


def sanitize_document_id(doc_id: str) -> str:
    """Make documentId safe for use as directory name; uses full id so it stays unique."""
    return sanitize_id(doc_id)


def media_filename(media: dict, default_ext: str = "jpg") -> str:
    """Filename from full media id so document.json and file link easily; id is sanitized for uniqueness."""
    media_id = media.get("id", "")
    safe = sanitize_id(media_id)
    url = media.get("fullURL", "")
    ext = url.split(".")[-1].lower() if url and "." in url.split("/")[-1] else default_ext
    if len(ext) > 4 or not ext.isalnum():
        ext = default_ext
    return f"{safe}.{ext}"


def first_image_url(specimen: dict) -> str | None:
    """Return fullURL of the first IMAGE media in specimen, or None if none."""
    for media, _ in iter_specimen_images(specimen):
        return media.get("fullURL")
    return None


def iter_specimen_images(specimen: dict):
    """Yield (media_dict, filename) for each IMAGE media in a specimen."""
    doc = specimen.get("document", {})
    for gathering in doc.get("gatherings", []):
        for unit in gathering.get("units", []):
            for media in unit.get("media", []):
                if media.get("mediaType") == "IMAGE" and media.get("fullURL"):
                    yield media, media_filename(media)


def fetch_finbif(url: str) -> dict:
    """Fetch JSON from FinBIF API. Takes full API URL, returns response JSON."""
    token = os.getenv("FINBIF_ACCESS_TOKEN")
    if not token:
        raise ValueError("Set FINBIF_ACCESS_TOKEN in environment or .env")
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():

    SKIP_DIGITARIUM_IMAGES = False
    order_by = "&orderBy=RANDOM:42"

    page_size = 100
    url = f"https://api.laji.fi/warehouse/query/unit/list?pageSize={page_size}&page=1&cache=false&collectionId=HR.168&recordBasis=PRESERVED_SPECIMEN&hasUnitMedia=true{order_by}"

#    url = f"https://api.laji.fi/warehouse/query/unit/list?pageSize={page_size}&page=1&cache=false&collectionId=HR.168&recordBasis=PRESERVED_SPECIMEN&hasUnitMedia=true&selected=document.collectionId,document.documentId,document.licenseId,document.secureLevel,document.secureReasons,document.sourceId,gathering.conversions.wgs84CenterPoint.lat,gathering.conversions.wgs84CenterPoint.lon,gathering.displayDateTime,gathering.gatheringId,gathering.interpretations.coordinateAccuracy,gathering.interpretations.municipalityDisplayname,gathering.interpretations.sourceOfCoordinates,gathering.locality,gathering.team,unit.abundanceString,unit.linkings.taxon.id,unit.linkings.taxon.qname,unit.linkings.taxon.scientificName,unit.linkings.taxon.vernacularName,unit.notes,unit.recordBasis,unit.taxonVerbatim,unit.unitId"
    

    print(url)
    specimen_data = fetch_finbif(url)

    specimens = specimen_data.get("results", [])

    print(f"Found {len(specimens)} specimen(s)\n")

    for i, list_item in enumerate(specimens, 1):

        doc_id = list_item["document"]["documentId"]
        single_specimen_url = f"https://api.laji.fi/warehouse/query/single?documentId={doc_id}"
        single_specimen_data = fetch_finbif(single_specimen_url)
        specimen = (
            single_specimen_data
            if "document" in single_specimen_data
            else single_specimen_data.get("results", [{}])[0]
        )

        if SKIP_DIGITARIUM_IMAGES:
            first_url = first_image_url(specimen)
            if first_url and "digitarium.fi" in first_url:
                print(f"  {doc_id}: skip (first image is digitarium.fi)")
                continue

        dir_name = sanitize_document_id(doc_id)
        out_dir = os.path.join(IMAGES_DIR, dir_name)
        os.makedirs(out_dir, exist_ok=True)

        doc_path = os.path.join(out_dir, "document.json")
        with open(doc_path, "w", encoding="utf-8") as f:
            json.dump(specimen, f, ensure_ascii=False, indent=2)
        print(f"  {dir_name}: saved {doc_path}")

        for media, filename in iter_specimen_images(specimen):
            time.sleep(1)
            img_url = media["fullURL"]
            print(f"  {doc_id}: fetching {img_url}")

            img_path = os.path.join(out_dir, filename)
            resp = requests.get(img_url, headers=IMAGE_REQUEST_HEADERS, timeout=30)
            if not resp.ok:
                print("  Image fetch failed — response details:", file=sys.stderr)
                print(f"    status: {resp.status_code}", file=sys.stderr)
                print(f"    final URL: {resp.url}", file=sys.stderr)
                print(f"    response headers: {dict(resp.headers)}", file=sys.stderr)
                body_preview = resp.content[:500] if resp.content else b""
                print(f"    body preview ({len(body_preview)} bytes): {body_preview!r}", file=sys.stderr)
                resp.raise_for_status()
            with open(img_path, "wb") as f:
                f.write(resp.content)
            print(f"    {filename}")


if __name__ == "__main__":
    main()
