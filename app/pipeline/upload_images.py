"""Settings-driven specimen intake + GCS upload pipeline.

Step 1:
- Read specimen folders from input directory.
- Parse qname from document.json -> document.documentId.

Step 2:
- Upload all JPG images per specimen to GCS.
- Write one run-level summary JSON + per-image JSONL records.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import tempfile
from typing import Any

from google.cloud import storage
import PIL.Image
from utils.files import (
    append_jsonl,
    load_json,
    load_jsonl_rows,
    resolve_path as resolve_path_from_root,
    save_json,
)
from utils.gcp import resolve_adc_credentials_from_env, upload_file_to_gcs_blob
from utils.pipeline_config import archive_pipeline_settings, load_step_settings
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
SETTINGS_PATH = SCRIPT_DIR / "settings" / "upload_images_settings.json"
SUMMARY_FLUSH_EVERY = 200

# If an input JPG is very large, resize it before uploading to GCS.
MAX_IMAGE_BYTES = 10 * 1024 * 1024
RESIZE_SCALE = 0.5
JPEG_QUALITY = 90  # "compression 0.9" -> 90/100 in JPEG terms


def record_key_from_payload(payload: dict[str, Any]) -> str:
    folder_key = str(payload.get("specimen_folder", "")).strip()
    image_filename = str(
        payload.get("image_filename")
        or payload.get("selected_image")
        or ""
    ).strip()
    if image_filename:
        return f"{folder_key}|{image_filename}"
    return f"{folder_key}|__folder__"


def load_records_from_jsonl(path: Path) -> dict[str, dict[str, Any]]:
    records_by_key: dict[str, dict[str, Any]] = {}
    for payload in load_jsonl_rows(path):
        key = record_key_from_payload(payload)
        if key and not key.startswith("|"):
            records_by_key[key] = payload
    return records_by_key


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


def validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
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

    merged_settings, _ = load_step_settings("upload_images", SETTINGS_PATH)
    settings = validate_settings(merged_settings)
    gcs_bucket = settings["gcs_bucket"]
    gcs_location = settings["gcs_location"]
    gcs_prefix = settings["gcs_prefix"]

    input_dir = resolve_path_from_root(PROJECT_ROOT, settings["input_dir"])

    run_id = str(settings["run_id"]).strip()
    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    output_file = run_output_dir / "upload_images.json"
    records_file = output_file.with_name("upload_images.records.jsonl")
    archive_pipeline_settings(run_output_dir)

    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_dir}")

    env_project = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    project_id = env_project or settings.get("google_cloud_project", "")
    if not project_id:
        raise ValueError(
            "Missing Google Cloud project. Set GOOGLE_CLOUD_PROJECT in environment."
        )

    adc_credentials_file = resolve_adc_credentials_from_env(
        lambda path_str: resolve_path_from_root(PROJECT_ROOT, path_str)
    )
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

    def discover_specimen_folders(root: Path) -> list[Path]:
        """
        Support both directory layouts:
        - flat: <root>/<qname>/<document.json> (or <root>/<qname>/document.json where qname folder is the specimen folder)
        - nested: <root>/<lastChar>/<qname>/document.json
        """

        candidates: set[Path] = set()
        if not root.exists():
            return []

        # Depth 1: root/<specimen_folder>
        for p in root.iterdir():
            if not p.is_dir():
                continue
            if (p / "document.json").exists():
                candidates.add(p)

        # Depth 2: root/<lastChar>/<qname>
        for last_char in root.iterdir():
            if not last_char.is_dir():
                continue
            for qname_dir in last_char.iterdir():
                if not qname_dir.is_dir():
                    continue
                if (qname_dir / "document.json").exists():
                    candidates.add(qname_dir)

        return sorted(candidates, key=lambda p: str(p))

    all_specimen_folders = discover_specimen_folders(input_dir)
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

    records_by_key: dict[str, dict[str, Any]] = load_records_from_jsonl(records_file)

    pending_folders = list(all_specimen_folders)
    if args.limit is None:
        specimen_folders = pending_folders
    else:
        specimen_folders = pending_folders[:args.limit]
    limit_caused_incomplete = args.limit is not None and len(pending_folders) > len(specimen_folders)

    log(f"Pending specimen folders: {len(pending_folders)}")
    log(f"Processing {len(specimen_folders)} specimen folders this run")

    def build_counts() -> dict[str, int]:
        uploaded = sum(1 for rec in records_by_key.values() if rec.get("status") == "uploaded")
        failed = sum(1 for rec in records_by_key.values() if rec.get("status") == "failed")
        skipped = sum(1 for rec in records_by_key.values() if rec.get("status") == "skipped")
        valid_specimens = sum(1 for rec in records_by_key.values() if rec.get("qname"))
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
            "run_id": run_id,
            "google_cloud_project": project_id,
            "gcs_location": gcs_location,
            "input_dir": str(input_dir),
            "gcs_bucket": gcs_bucket,
            "gcs_prefix": gcs_prefix,
        },
        "data": {
            "counts": build_counts(),
            "records_logged": len(records_by_key),
        },
    }

    def persist_run(status: str, *, error: str | None = None, finished: bool = False) -> None:
        run_output["run_status"] = status
        run_output["error"] = error
        run_output["last_updated_at"] = now_iso()
        run_output["finished_at"] = now_iso() if finished else None
        run_output["data"]["counts"] = build_counts()
        run_output["data"]["records_logged"] = len(records_by_key)
        save_json(output_file, run_output)

    # Persist immediately so interruptions always leave a run-status record.
    persist_run(RUN_STATUS_RUNNING)

    install_termination_handlers()

    processed_since_flush = 0

    try:
        for folder in specimen_folders:
            folder_key = str(folder)
            log(f"Processing folder: {folder.name}")
            base_record: dict[str, Any] = {
                "specimen_folder": folder_key,
                "status": "skipped",
                "error": None,
                "document_id_long": None,
                "qname": None,
                "image_filename": None,
                "image_index": None,
                "gcs_uri": None,
            }

            document_path = folder / "document.json"
            jpg_files = pick_jpgs(folder)

            if not document_path.exists():
                record = dict(base_record)
                record["error"] = "Missing document.json"
                log(f"Skipped {folder.name}: missing document.json")
                records_by_key[record_key_from_payload(record)] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            if not jpg_files:
                record = dict(base_record)
                record["error"] = "No JPG image found"
                log(f"Skipped {folder.name}: no JPG image found")
                records_by_key[record_key_from_payload(record)] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            try:
                document_payload = load_json(document_path)
                document_id_long = document_payload.get("document", {}).get("documentId", "")
                qname = extract_qname(document_id_long)
            except Exception as exc:
                record = dict(base_record)
                record["error"] = f"Failed to parse document.json: {exc}"
                log(f"Skipped {folder.name}: failed to parse document.json ({exc})")
                records_by_key[record_key_from_payload(record)] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            if not document_id_long or not qname:
                record = dict(base_record)
                record["error"] = "Missing or invalid document.documentId"
                log(f"Skipped {folder.name}: missing/invalid document.documentId")
                records_by_key[record_key_from_payload(record)] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            for image_index, selected_image in enumerate(jpg_files, start=1):
                record = dict(base_record)
                record["document_id_long"] = document_id_long
                record["qname"] = qname
                record["image_filename"] = selected_image.name
                record["image_index"] = image_index

                record_key = record_key_from_payload(record)
                previous = records_by_key.get(record_key)
                if previous and previous.get("status") == "uploaded":
                    record["status"] = "skipped"
                    record["gcs_uri"] = previous.get("gcs_uri")
                    append_jsonl(records_file, record)
                    records_by_key[record_key] = record
                    processed_since_flush += 1
                    if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                        persist_run(RUN_STATUS_RUNNING)
                        processed_since_flush = 0
                    continue

                blob_name = build_blob_name(
                    gcs_prefix=gcs_prefix,
                    qname=qname,
                    image_name=selected_image.name,
                )
                target_uri = f"gs://{gcs_bucket}/{blob_name}"
                log(f"Uploading {selected_image.name} -> {target_uri}")

                try:
                    local_file = selected_image
                    tmp_resized_path: Path | None = None
                    if selected_image.stat().st_size > MAX_IMAGE_BYTES:
                        log(
                            f"Image >10MB ({selected_image.stat().st_size} bytes); resizing to 50% "
                            f"(JPEG quality={JPEG_QUALITY}) before upload."
                        )
                        with tempfile.TemporaryDirectory() as tmp_dir:
                            tmp_resized_path = (
                                Path(tmp_dir)
                                / f"{selected_image.stem}.resized.jpg"
                            )
                            with PIL.Image.open(selected_image) as im:
                                if im.mode != "RGB":
                                    im = im.convert("RGB")
                                new_w = max(1, int(im.width * RESIZE_SCALE))
                                new_h = max(1, int(im.height * RESIZE_SCALE))
                                resized = im.resize(
                                    (new_w, new_h),
                                    resample=PIL.Image.Resampling.LANCZOS,
                                )
                                resized.save(
                                    tmp_resized_path,
                                    format="JPEG",
                                    quality=JPEG_QUALITY,
                                    optimize=True,
                                )

                            gcs_uri = upload_file_to_gcs_blob(
                                client=storage_client,
                                bucket_name=gcs_bucket,
                                blob_name=blob_name,
                                local_file=tmp_resized_path,
                            )

                    else:
                        gcs_uri = upload_file_to_gcs_blob(
                            client=storage_client,
                            bucket_name=gcs_bucket,
                            blob_name=blob_name,
                            local_file=local_file,
                        )

                    record["gcs_uri"] = gcs_uri
                    record["status"] = "uploaded"
                    log(f"Uploaded successfully: {gcs_uri}")
                except Exception as exc:
                    record["status"] = "failed"
                    record["error"] = f"GCS upload failed: {exc}"
                    log(f"Upload failed for {folder.name}/{selected_image.name}: {exc}")

                records_by_key[record_key] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0

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
