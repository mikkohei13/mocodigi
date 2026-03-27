"""
Step 0: Download document images from FinBIF based on a laji.fi images search URL.

This script is settings-driven and writes durable run artifacts to:
  app/output/pipeline_runs/<run_id>/download_images.json
  app/output/pipeline_runs/<run_id>/download_images.records.jsonl

On-disk output (image dataset):
  app/<image_folder_name>/<lastChar>/<qname>/
    - document.json
    - <image>.jpg

Selection:
- Discover matching documents by querying /warehouse/query/unit/list using the query
  parameters parsed from the provided `search_url`.
- For each discovered document, fetch the full document and download the first IMAGE
  media found in document.gatherings[].units[].media[] traversal.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests

from utils.files import (
    append_jsonl,
    load_json,
    load_jsonl_rows,
    resolve_path as resolve_path_from_root,
    save_json,
    validate_json_file,
)
from utils.runtime import (
    RUN_STATUS_FAILED,
    RUN_STATUS_FINISHED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_RUNNING,
    RUN_STATUS_TERMINATED,
    RunTerminatedError,
    install_termination_handlers,
    log,
    now_iso,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "download_images_settings.json"

IMAGE_REQUEST_HEADERS = {
    # Browser-like User-Agent for image requests (some servers block scripts).
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

SUMMARY_FLUSH_EVERY = 200
PAGE_SIZE = 100
SLEEP_SECONDS_BETWEEN_DOCS = 0.2
SLEEP_SECONDS_BETWEEN_IMAGE_DOWNLOADS = 0.2

FINBIF_BASE_URL = "https://api.laji.fi"
FINBIF_UNIT_LIST_URL = f"{FINBIF_BASE_URL}/warehouse/query/unit/list"
FINBIF_DOCUMENT_URL = f"{FINBIF_BASE_URL}/warehouse/query/document"


def validate_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = raw.get("settings", {})
    required = ["run_id", "image_folder_name", "search_url"]
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def extract_qname(document_id_long: str) -> str:
    value = (document_id_long or "").strip()
    if not value:
        return ""
    if "/" not in value:
        return value
    return value.rsplit("/", 1)[-1].strip()


def iter_document_images(document_payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """
    Yield media objects in traversal order.

    FinBIF documents usually store images under:
      document.gatherings[].units[].media[]
    """

    doc = document_payload.get("document", {}) or {}
    for gathering in doc.get("gatherings", []) or []:
        for unit in gathering.get("units", []) or []:
            for media in unit.get("media", []) or []:
                if (media or {}).get("mediaType") == "IMAGE" and media.get("fullURL"):
                    yield media


def first_document_image(document_payload: dict[str, Any]) -> dict[str, Any] | None:
    for media in iter_document_images(document_payload):
        return media
    return None


def image_filename_from_url(image_url: str) -> str:
    """
    Create an output filename guaranteed to have a `.jpg` extension
    (Step 1 only scans `.jpg` files).
    """

    parsed = urlparse(image_url or "")
    path = parsed.path or ""
    name = Path(path).name
    if not name:
        return "image.jpg"

    p = Path(name)
    ext = p.suffix.lower()
    if ext == ".jpg":
        return name
    if ext == ".jpeg":
        return f"{p.stem}.jpg"
    # If FinBIF returns a different extension, normalize to .jpg for step-1 compatibility.
    return f"{p.stem or 'image'}.jpg"


def resolve_images_root(project_root: Path, image_folder_name: str) -> Path:
    value = str(image_folder_name).strip()
    if value.startswith("app/") or value.startswith("app\\"):
        return resolve_path_from_root(project_root, value)
    if value.startswith("/"):
        return Path(value)
    return project_root / "app" / value


def fetch_finbif_json(*, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    token = os.getenv("FINBIF_ACCESS_TOKEN")
    if not token:
        raise ValueError("Set FINBIF_ACCESS_TOKEN in environment or .env")
    resp = requests.get(
        url,
        params=params,
        headers={
            "Authorization": f"Bearer {token}",
            "API-Version": "1",
            "Accept": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected JSON payload from FinBIF: {type(payload).__name__}")
    return payload


def fetch_image_bytes(image_url: str) -> bytes:
    resp = requests.get(
        image_url,
        headers=IMAGE_REQUEST_HEADERS,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.content


def parse_search_url_query_params(search_url: str) -> dict[str, list[str]]:
    """
    Parse the query parameters from the provided laji.fi search URL and return them
    as `dict[str, list[str]]` suitable for re-encoding.

    We intentionally don't assume which parameters exist; we pass them through.
    """

    parsed = urlparse(search_url or "")
    if not parsed.query:
        return {}
    return parse_qs(parsed.query, keep_blank_values=True)


def build_specimen_folder(images_root: Path, document_id_long: str) -> tuple[str, str, Path]:
    qname = extract_qname(document_id_long)
    last_char = qname[-1] if qname else "_"
    folder = images_root / last_char / qname
    return qname, last_char, folder


def pick_if_already_materialized(folder: Path) -> bool:
    doc_path = folder / "document.json"
    if not doc_path.exists():
        return False
    jpgs = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".jpg"])
    return len(jpgs) > 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step 0: Download document images from FinBIF based on a laji.fi images search URL."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of new documents to process (default: no limit).",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be 0 or greater.")
    return args


def main() -> None:
    args = parse_args()

    started_at_now = now_iso()
    log("Starting Step 0: download_images")

    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    validate_json_file(SETTINGS_PATH)
    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)

    run_id = str(settings["run_id"]).strip()
    search_url = str(settings["search_url"]).strip()
    image_folder_name = str(settings["image_folder_name"]).strip()

    images_root = resolve_images_root(PROJECT_ROOT, image_folder_name)

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    output_file = run_output_dir / "download_images.json"
    records_file = output_file.with_name("download_images.records.jsonl")

    limit_caused_incomplete = False

    all_query_params = parse_search_url_query_params(search_url)

    env_project = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    del env_project  # Step 0 doesn't use GCS, but keep the pattern consistent with other scripts.

    # Early materialization check (images_root could be created in a previous run).
    images_root.mkdir(parents=True, exist_ok=True)

    existing_output: dict[str, Any] | None = None
    existing_status: str | None = None
    if output_file.exists():
        existing_output = load_json(output_file)
        existing_status = existing_output.get("run_status")
        if existing_status == RUN_STATUS_FINISHED:
            log(
                f"Run output already exists with status '{RUN_STATUS_FINISHED}'. "
                f"Not starting run: {output_file}"
            )
            return
        log(
            f"Found existing run output with status '{existing_status or 'unknown'}'; attempting to resume."
        )

    processed_by_document_id: dict[str, dict[str, Any]] = {}
    if records_file.exists():
        for item in load_jsonl_rows(records_file):
            document_id_long = str(item.get("document_id_long", "")).strip()
            if document_id_long:
                processed_by_document_id[document_id_long] = item

    def build_counts() -> dict[str, int]:
        processed = sum(
            1
            for rec in processed_by_document_id.values()
            if rec.get("status") in ("downloaded", "skipped")
        )
        failed = sum(1 for rec in processed_by_document_id.values() if rec.get("status") == "failed")
        return {"processed": processed, "failed": failed, "limit": int(args.limit) if args.limit is not None else -1}

    run_output: dict[str, Any] = {
        "format_version": "0.1",
        "type": "pipeline_output",
        "run_status": RUN_STATUS_RUNNING,
        "started_at": (existing_output or {}).get("started_at", started_at_now),
        "finished_at": None,
        "last_updated_at": now_iso(),
        "error": None,
        "settings": {
            "run_id": run_id,
            "image_folder_name": image_folder_name,
            "image_root": str(images_root),
            "search_url": search_url,
        },
        "data": {"counts": build_counts(), "documents_discovered": None},
    }

    def persist_run(status: str, *, error: str | None = None, finished: bool = False) -> None:
        run_output["run_status"] = status
        run_output["error"] = error
        run_output["last_updated_at"] = now_iso()
        run_output["finished_at"] = now_iso() if finished else None
        run_output["data"]["counts"] = build_counts()
        save_json(output_file, run_output)

    # Persist immediately so interruptions always leave a run-status record.
    persist_run(RUN_STATUS_RUNNING)

    install_termination_handlers()

    processed_since_flush = 0
    newly_processed = 0
    documents_discovered = 0

    try:
        page = 1
        while True:
            log(f"Querying FinBIF discovery page {page}")

            # Build params for unit list query:
            # - Pass through all `search_url` query params unchanged.
            # - We intentionally ignore any `hasUnitMedia` value in `search_url`
            #   because it doesn't matter whether units have media; we download the
            #   first image found inside each fetched document.
            params: dict[str, Any] = dict(all_query_params)
            params.pop("hasUnitMedia", None)
            params.update(
                {
                "pageSize": [str(PAGE_SIZE)],
                "page": [str(page)],
                "cache": ["true"],
                # Keep payload minimal for discovery.
                "selected": ["document.documentId"],
                }
            )

            payload = fetch_finbif_json(url=FINBIF_UNIT_LIST_URL, params=params)
            results = payload.get("results", []) or []
            if not results:
                break

            for row in results:
                document_id_long = (
                    row.get("document", {}).get("documentId") if isinstance(row, dict) else None
                )
                document_id_long = str(document_id_long or "").strip()
                if not document_id_long:
                    continue

                documents_discovered += 1

                if document_id_long in processed_by_document_id and processed_by_document_id[document_id_long].get(
                    "status"
                ) in ("downloaded", "skipped"):
                    continue

                qname, last_char, specimen_folder = build_specimen_folder(images_root, document_id_long)
                specimen_folder.mkdir(parents=True, exist_ok=True)
                log(
                    f"Processing specimen: qname={qname}, "
                    f"documentId={document_id_long}, page={page}"
                )

                record: dict[str, Any] = {
                    "document_id_long": document_id_long,
                    "qname": qname,
                    "last_char": last_char,
                    "specimen_folder": str(specimen_folder),
                    "status": "skipped",
                    "error": None,
                    "selected_image_filename": None,
                    "selected_image_full_url": None,
                }

                if pick_if_already_materialized(specimen_folder):
                    log(f"Skipping {qname}: document.json and at least one .jpg already exist")
                    record["status"] = "skipped"
                    processed_by_document_id[document_id_long] = record
                    append_jsonl(records_file, record)
                    processed_since_flush += 1
                    newly_processed += 1
                    if args.limit is not None and newly_processed >= args.limit:
                        limit_caused_incomplete = True
                        break
                    continue

                # Fetch full document JSON.
                record["status"] = "skipped"
                document_payload = fetch_finbif_json(
                    url=FINBIF_DOCUMENT_URL,
                    params={"documentId": document_id_long},
                )

                media = first_document_image(document_payload)
                if not media:
                    log(f"Skipping {qname}: no IMAGE media found in document payload")
                    record["error"] = "No IMAGE media found in document.gatherings[].units[].media[]"
                    processed_by_document_id[document_id_long] = record
                    append_jsonl(records_file, record)
                    processed_since_flush += 1
                    newly_processed += 1
                    if args.limit is not None and newly_processed >= args.limit:
                        limit_caused_incomplete = True
                        break
                    continue

                image_url = str(media.get("fullURL") or "").strip()
                if not image_url:
                    log(f"Skipping {qname}: selected media has no fullURL")
                    record["error"] = "Media fullURL missing"
                    processed_by_document_id[document_id_long] = record
                    append_jsonl(records_file, record)
                    processed_since_flush += 1
                    newly_processed += 1
                    if args.limit is not None and newly_processed >= args.limit:
                        limit_caused_incomplete = True
                        break
                    continue

                image_filename = image_filename_from_url(image_url)
                image_path = specimen_folder / image_filename
                log(f"Downloading image for {qname}: {image_url}")

                time.sleep(SLEEP_SECONDS_BETWEEN_DOCS)

                image_bytes = fetch_image_bytes(image_url)
                time.sleep(SLEEP_SECONDS_BETWEEN_IMAGE_DOWNLOADS)

                with image_path.open("wb") as f:
                    f.write(image_bytes)

                document_json_path = specimen_folder / "document.json"
                with document_json_path.open("w", encoding="utf-8") as f:
                    json.dump(document_payload, f, ensure_ascii=False, indent=2)

                record["selected_image_filename"] = image_filename
                record["selected_image_full_url"] = image_url
                record["status"] = "downloaded"
                log(f"Saved {qname} -> {image_path.name} + document.json")
                processed_by_document_id[document_id_long] = record
                append_jsonl(records_file, record)

                processed_since_flush += 1
                newly_processed += 1

                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0

                if args.limit is not None and newly_processed >= args.limit:
                    limit_caused_incomplete = True
                    break

            if limit_caused_incomplete:
                break

            page += 1
            last_page = payload.get("lastPage")
            if last_page is not None:
                try:
                    if page > int(last_page):
                        break
                except (TypeError, ValueError):
                    pass

    except RunTerminatedError as exc:
        log(str(exc))
        persist_run(RUN_STATUS_TERMINATED, error=str(exc), finished=True)
        raise SystemExit(1) from exc
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        log(f"Run failed: {error_message}")
        persist_run(RUN_STATUS_FAILED, error=error_message, finished=True)
        raise

    final_status = RUN_STATUS_PARTIAL if limit_caused_incomplete else RUN_STATUS_FINISHED
    run_output["data"]["documents_discovered"] = documents_discovered
    persist_run(final_status, finished=True)
    log(f"Wrote run output: {output_file}")
    log(f"Run status: {final_status}")
    log(f"Counts: {build_counts()}")


if __name__ == "__main__":
    main()

