"""Settings-driven specimen intake + GCS upload pipeline.

Step 1:
- Read specimen folders from input directory.
- Parse qname from document.json -> document.documentId.

Step 2:
- Upload selected JPG image per specimen to GCS.
- Write one run-level JSON output file.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from google.cloud import storage

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "specimen_pipeline_settings.json"


def now_iso() -> str:
    return datetime.now().isoformat()


def log(message: str) -> None:
    print(f"[{now_iso()}] {message}")


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
        "input_dir",
        "gcs_bucket",
        "gcs_location",
        "gcs_prefix",
    ]
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")

    return settings


def resolve_adc_credentials_from_env() -> Path | None:
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    credentials_root = os.getenv("GOOGLE_CREDENTIALS_PATH", "").strip()
    if not credentials_path:
        return None

    candidate = Path(credentials_path)
    if candidate.is_absolute():
        if candidate.exists():
            return candidate
        if credentials_root:
            root_path = resolve_path(credentials_root)
            joined = root_path / credentials_path.lstrip("/")
            if joined.exists():
                return joined
        return candidate

    if credentials_root:
        root_path = resolve_path(credentials_root)
        return root_path / credentials_path
    return resolve_path(credentials_path)


def upload_file_to_gcs(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    local_file: Path,
) -> str:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))
    # Ensure the uploaded object is visible before reporting success.
    if not blob.exists(client=client):
        raise RuntimeError(f"Uploaded object not found after upload: gs://{bucket_name}/{blob_name}")
    return f"gs://{bucket_name}/{blob_name}"


def build_blob_name(gcs_prefix: str, qname: str, image_name: str) -> str:
    prefix = gcs_prefix.strip("/")
    if prefix:
        return f"{prefix}/{qname}/{image_name}"
    return f"{qname}/{image_name}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run specimen intake + GCS upload pipeline.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of specimen folders to process (default: no limit).",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be 0 or greater.")
    return args


def main() -> None:
    '''
    Todo:
    - Make run fail if output with run id already exists, and run has completed successfully.
    '''
    
    args = parse_args()
    started_at = now_iso()
    log("Starting specimen pipeline run")

    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)
    gcs_bucket = settings["gcs_bucket"]
    gcs_location = settings["gcs_location"]
    gcs_prefix = settings["gcs_prefix"]

    input_dir = resolve_path(settings["input_dir"])

    output_file_name = f"app/output/pipeline_runs/{settings['run_id']}.json"
    output_file = resolve_path(output_file_name)

    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_dir}")

    env_project = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    project_id = env_project or settings.get("google_cloud_project", "")
    if not project_id:
        raise ValueError(
            "Missing Google Cloud project. Set GOOGLE_CLOUD_PROJECT in environment."
        )

    adc_credentials_file = resolve_adc_credentials_from_env()
    if adc_credentials_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(adc_credentials_file)
    emulator_host = os.getenv("STORAGE_EMULATOR_HOST", "").strip()

    log(f"Settings file: {SETTINGS_PATH}")
    log(f"Limit: {args.limit}")
    log(f"Project: {project_id}")
    log(f"Input directory: {input_dir}")
    log(f"Bucket target: gs://{gcs_bucket}/{gcs_prefix.strip('/')}")
    log(f"Configured bucket location: {gcs_location}")
    if args.limit is None:
        log("Specimen limit: none")
    else:
        log(f"Specimen limit: {args.limit}")
    if adc_credentials_file:
        log(f"ADC credentials file: {adc_credentials_file}")
    else:
        log("ADC credentials file: default Google auth resolution")
    if emulator_host:
        log(f"STORAGE_EMULATOR_HOST is set: {emulator_host}")

    storage_client = storage.Client(project=project_id)
    specimen_folders = sorted([p for p in input_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    discovered_folders = len(specimen_folders)

    log(f"Found {discovered_folders} specimen folders")

    # Limit the number of specimen folders to process
    if args.limit is not None:
        specimen_folders = specimen_folders[:args.limit]

    log(f"Processing {len(specimen_folders)} specimen folders")

    uploaded = 0
    failed = 0
    skipped = 0
    valid_specimens = 0
    specimen_records: list[dict[str, Any]] = []

    for folder in specimen_folders:
        log(f"Processing folder: {folder.name}")
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
            log(f"Skipped {folder.name}: missing document.json")
            skipped += 1
            specimen_records.append(record)
            continue

        if not jpg_files:
            record["error"] = "No JPG image found"
            log(f"Skipped {folder.name}: no JPG image found")
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
            log(f"Skipped {folder.name}: failed to parse document.json ({exc})")
            skipped += 1
            specimen_records.append(record)
            continue

        if not document_id_long or not qname:
            record["error"] = "Missing or invalid document.documentId"
            log(f"Skipped {folder.name}: missing/invalid document.documentId")
            skipped += 1
            specimen_records.append(record)
            continue

        record["document_id_long"] = document_id_long
        record["qname"] = qname
        valid_specimens += 1

        blob_name = build_blob_name(
            gcs_prefix=gcs_prefix,
            qname=qname,
            image_name=selected_image.name,
        )
        target_uri = f"gs://{gcs_bucket}/{blob_name}"
        log(f"Uploading {selected_image.name} -> {target_uri}")

        try:
            gcs_uri = upload_file_to_gcs(
                client=storage_client,
                bucket_name=gcs_bucket,
                blob_name=blob_name,
                local_file=selected_image,
            )
            record["gcs_uri"] = gcs_uri
            record["status"] = "uploaded"
            log(f"Uploaded successfully: {gcs_uri}")
            uploaded += 1
        except Exception as exc:
            record["status"] = "failed"
            record["error"] = f"GCS upload failed: {exc}"
            log(f"Upload failed for {folder.name}: {exc}")
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
            "google_cloud_project": project_id,
            "gcs_location": gcs_location,
            "input_dir": str(input_dir),
            "gcs_bucket": gcs_bucket,
            "gcs_prefix": gcs_prefix,
            "output_file": str(output_file)
        },
        "data": {
            "counts": {
                "total_folders": len(specimen_folders),
                "discovered_folders": discovered_folders,
                "valid_specimens": valid_specimens,
                "uploaded": uploaded,
                "failed": failed,
                "skipped": skipped,
            },
            "specimens": specimen_records,
        },
    }

    save_json(output_file, run_output)
    log(f"Wrote run output: {output_file}")
    log(
        "Counts: "
        f"processed={len(specimen_folders)}, valid={valid_specimens}, "
        f"uploaded={uploaded}, failed={failed}, skipped={skipped}"
    )
    log(f"Try listing uploaded files with: gcloud storage ls -l --recursive gs://{gcs_bucket}/{gcs_prefix.strip('/')}")


if __name__ == "__main__":
    main()
