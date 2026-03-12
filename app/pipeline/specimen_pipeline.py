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
import signal
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
RUN_STATUS_RUNNING = "running"
RUN_STATUS_FINISHED = "finished"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_TERMINATED = "terminated"


class RunTerminatedError(Exception):
    pass


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
    args = parse_args()
    started_at_now = now_iso()
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
    all_specimen_folders = sorted([p for p in input_dir.iterdir() if p.is_dir()], key=lambda p: p.name)
    discovered_folders = len(all_specimen_folders)

    log(f"Found {discovered_folders} specimen folders")

    existing_output: dict[str, Any] | None = None
    existing_status = None
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
            f"Found existing run output with status '{existing_status or 'unknown'}'; "
            "attempting to resume."
        )

    records_by_folder: dict[str, dict[str, Any]] = {}
    if existing_output:
        existing_records = existing_output.get("data", {}).get("specimens", [])
        for item in existing_records:
            folder_key = str(item.get("specimen_folder", "")).strip()
            if folder_key:
                records_by_folder[folder_key] = item

    pending_folders = [
        folder
        for folder in all_specimen_folders
        if records_by_folder.get(str(folder), {}).get("status") != "uploaded"
    ]
    if args.limit is None:
        specimen_folders = pending_folders
    else:
        specimen_folders = pending_folders[:args.limit]
    limit_caused_incomplete = args.limit is not None and len(pending_folders) > len(specimen_folders)

    log(f"Pending specimen folders: {len(pending_folders)}")
    log(f"Processing {len(specimen_folders)} specimen folders this run")

    def ordered_records() -> list[dict[str, Any]]:
        return [
            records_by_folder[str(folder)]
            for folder in all_specimen_folders
            if str(folder) in records_by_folder
        ]

    def build_counts(specimen_records: list[dict[str, Any]]) -> dict[str, int]:
        uploaded = sum(1 for rec in specimen_records if rec.get("status") == "uploaded")
        failed = sum(1 for rec in specimen_records if rec.get("status") == "failed")
        skipped = sum(1 for rec in specimen_records if rec.get("status") == "skipped")
        valid_specimens = sum(1 for rec in specimen_records if rec.get("qname"))
        return {
            "total_folders": discovered_folders,
            "discovered_folders": discovered_folders,
            "valid_specimens": valid_specimens,
            "uploaded": uploaded,
            "failed": failed,
            "skipped": skipped,
        }

    run_output = {
        "format_version": "0.1",
        "type": "pipeline_output",
        "run_status": RUN_STATUS_RUNNING,
        "started_at": (existing_output or {}).get("started_at", started_at_now),
        "finished_at": None,
        "last_updated_at": now_iso(),
        "error": None,
        "settings": {
            "run_id": settings["run_id"],
            "google_cloud_project": project_id,
            "gcs_location": gcs_location,
            "input_dir": str(input_dir),
            "gcs_bucket": gcs_bucket,
            "gcs_prefix": gcs_prefix,
            "output_file": str(output_file),
        },
        "data": {
            "counts": build_counts(ordered_records()),
            "specimens": ordered_records(),
        },
    }

    def persist_run(status: str, *, error: str | None = None, finished: bool = False) -> None:
        run_output["run_status"] = status
        run_output["error"] = error
        run_output["last_updated_at"] = now_iso()
        run_output["finished_at"] = now_iso() if finished else None
        specimen_records = ordered_records()
        run_output["data"]["counts"] = build_counts(specimen_records)
        run_output["data"]["specimens"] = specimen_records
        save_json(output_file, run_output)

    # Persist immediately so interruptions always leave a run-status record.
    persist_run(RUN_STATUS_RUNNING)

    def handle_termination_signal(signum: int, frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        raise RunTerminatedError(f"Received termination signal: {signal_name}")

    signal.signal(signal.SIGINT, handle_termination_signal)
    signal.signal(signal.SIGTERM, handle_termination_signal)

    try:
        for folder in specimen_folders:
            folder_key = str(folder)
            previous = records_by_folder.get(folder_key)
            if previous and previous.get("status") == "uploaded":
                log(f"Skipping already uploaded folder: {folder.name}")
                continue

            log(f"Processing folder: {folder.name}")
            record: dict[str, Any] = {
                "specimen_folder": folder_key,
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
                records_by_folder[folder_key] = record
                persist_run(RUN_STATUS_RUNNING)
                continue

            if not jpg_files:
                record["error"] = "No JPG image found"
                log(f"Skipped {folder.name}: no JPG image found")
                records_by_folder[folder_key] = record
                persist_run(RUN_STATUS_RUNNING)
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
                records_by_folder[folder_key] = record
                persist_run(RUN_STATUS_RUNNING)
                continue

            if not document_id_long or not qname:
                record["error"] = "Missing or invalid document.documentId"
                log(f"Skipped {folder.name}: missing/invalid document.documentId")
                records_by_folder[folder_key] = record
                persist_run(RUN_STATUS_RUNNING)
                continue

            record["document_id_long"] = document_id_long
            record["qname"] = qname

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
            except Exception as exc:
                record["status"] = "failed"
                record["error"] = f"GCS upload failed: {exc}"
                log(f"Upload failed for {folder.name}: {exc}")

            records_by_folder[folder_key] = record
            persist_run(RUN_STATUS_RUNNING)

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
    persist_run(final_status, finished=True)
    counts = run_output["data"]["counts"]
    log(f"Wrote run output: {output_file}")
    log(f"Run status: {final_status}")
    log(
        "Counts: "
        f"processed={counts['total_folders']}, valid={counts['valid_specimens']}, "
        f"uploaded={counts['uploaded']}, failed={counts['failed']}, skipped={counts['skipped']}"
    )
    log(
        f"Try listing uploaded files with: "
        f"gcloud storage ls -l --recursive gs://{gcs_bucket}/{gcs_prefix.strip('/')}"
    )


if __name__ == "__main__":
    main()
