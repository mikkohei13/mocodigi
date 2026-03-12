"""Settings-driven specimen intake + GCS upload pipeline.

Step 1:
- Read specimen folders from input directory.
- Parse qname from document.json -> document.documentId.

Step 2:
- Upload selected JPG image per specimen to GCS.
- Write one run-level JSON output file.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from google.cloud import storage


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "specimen_pipeline_settings.json"


def now_iso() -> str:
    return datetime.now().isoformat()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def extract_qname(document_id: str) -> str:
    value = (document_id or "").strip()
    if not value:
        return ""
    if "/" not in value:
        return value
    return value.rsplit("/", 1)[-1].strip()


def pick_jpgs(folder: Path) -> list[Path]:
    return sorted(
        [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".jpg"],
        key=lambda p: p.name.lower(),
    )


def validate_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = raw.get("settings", {})
    required = [
        "run_id",
        "google_cloud_project",
        "input_dir",
        "gcs_bucket",
        "gcs_prefix",
    ]
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")

    return settings


def upload_file_to_gcs(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    local_file: Path,
) -> str:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))
    return f"gs://{bucket_name}/{blob_name}"


def build_blob_name(gcs_prefix: str, run_id: str, qname: str, image_name: str) -> str:
    prefix = gcs_prefix.strip("/")
    if prefix:
        return f"{prefix}/{run_id}/{qname}/{image_name}"
    return f"{run_id}/{qname}/{image_name}"


def main() -> None:
    '''
    Todo:
    - Make run fail if output with run id already exists, and run has completed successfully.
    '''
    
    started_at = now_iso()

    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)

    input_dir = resolve_path(settings["input_dir"])

    output_file_name = f"app/output/pipeline_runs/{settings['run_id']}.json"
    output_file = resolve_path(output_file_name)

    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_dir}")

    storage_client = storage.Client(project=settings["google_cloud_project"])
    specimen_folders = sorted([p for p in input_dir.iterdir() if p.is_dir()], key=lambda p: p.name)

    uploaded = 0
    failed = 0
    skipped = 0
    valid_specimens = 0
    specimen_records: list[dict[str, Any]] = []

    for folder in specimen_folders:
        record: dict[str, Any] = {
            "specimen_folder": str(folder),
            "status": "skipped",
            "error": None,
            "notes": [],
            "document_id_long": None,
            "qname": None,
            "jpg_count_in_folder": 0,
            "selected_image": None,
            "local_image_path": None,
            "gcs_uri": None,
        }

        document_path = folder / "document.json"
        jpg_files = pick_jpgs(folder)
        record["jpg_count_in_folder"] = len(jpg_files)

        if len(jpg_files) > 1:
            record["notes"].append(f"Found {len(jpg_files)} JPG files; selected first_sorted_jpg.")

        if not document_path.exists():
            record["error"] = "Missing document.json"
            skipped += 1
            specimen_records.append(record)
            continue

        if not jpg_files:
            record["error"] = "No JPG image found"
            skipped += 1
            specimen_records.append(record)
            continue

        selected_image = jpg_files[0]
        record["selected_image"] = selected_image.name
        record["local_image_path"] = str(selected_image)

        try:
            document_payload = load_json(document_path)
            document_id_long = document_payload.get("document", {}).get("documentId", "")
            qname = extract_qname(document_id_long)
        except Exception as exc:
            record["error"] = f"Failed to parse document.json: {exc}"
            skipped += 1
            specimen_records.append(record)
            continue

        if not document_id_long or not qname:
            record["error"] = "Missing or invalid document.documentId"
            skipped += 1
            specimen_records.append(record)
            continue

        record["document_id_long"] = document_id_long
        record["qname"] = qname
        valid_specimens += 1

        blob_name = build_blob_name(
            gcs_prefix=settings["gcs_prefix"],
            run_id=settings["run_id"],
            qname=qname,
            image_name=selected_image.name,
        )

        try:
            gcs_uri = upload_file_to_gcs(
                client=storage_client,
                bucket_name=settings["gcs_bucket"],
                blob_name=blob_name,
                local_file=selected_image,
            )
            record["gcs_uri"] = gcs_uri
            record["status"] = "uploaded"
            uploaded += 1
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = f"GCS upload failed: {exc}"
            failed += 1

        specimen_records.append(record)

    finished_at = now_iso()
    run_output = {
        "format_version": "0.1",
        "type": "pipeline_output",
        "started_at": started_at,
        "finished_at": finished_at,
        "settings": {
            "run_id": settings["run_id"],
            "google_cloud_project": settings["google_cloud_project"],
            "google_cloud_location": settings.get("google_cloud_location", "europe-west1"),
            "input_dir": str(input_dir),
            "gcs_bucket": settings["gcs_bucket"],
            "gcs_prefix": settings["gcs_prefix"],
            "output_file": str(output_file)
        },
        "data": {
            "counts": {
                "total_folders": len(specimen_folders),
                "valid_specimens": valid_specimens,
                "uploaded": uploaded,
                "failed": failed,
                "skipped": skipped,
            },
            "specimens": specimen_records,
        },
    }

    save_json(output_file, run_output)
    print(f"Wrote run output: {output_file}")
    print(
        "Counts: "
        f"total={len(specimen_folders)}, valid={valid_specimens}, "
        f"uploaded={uploaded}, failed={failed}, skipped={skipped}"
    )


if __name__ == "__main__":
    main()
