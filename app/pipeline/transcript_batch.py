"""Settings-driven specimen transcription batch submission pipeline.

Step 2:
- Read uploaded specimen image records from step-1 records JSONL.
- Build Gemini batch input JSONL and upload it to GCS.
- Submit a Vertex Gemini batch job for transcription.
- Persist one run-level summary JSON + per-specimen JSONL records.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import google.genai as genai
import google.auth
from google.cloud import storage
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.genai import types

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "transcribe_batch_settings.json"
RUN_STATUS_RUNNING = "running"
RUN_STATUS_FINISHED = "finished"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_TERMINATED = "terminated"
SUMMARY_FLUSH_EVERY = 200


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


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))
        f.write("\n")


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def parse_gs_uri(uri: str) -> tuple[str, str]:
    value = (uri or "").strip()
    if not value.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")
    without_scheme = value[5:]
    if "/" not in without_scheme:
        return without_scheme, ""
    bucket, blob = without_scheme.split("/", 1)
    return bucket, blob


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


def resolve_project_id(
    *,
    settings: dict[str, Any],
    source_summary: dict[str, Any] | None,
) -> str:
    settings_project = str(settings.get("google_cloud_project", "")).strip()
    env_project = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    source_project = ""
    if source_summary:
        source_project = str(
            source_summary.get("settings", {}).get("google_cloud_project", "")
        ).strip()

    candidates = [("settings.google_cloud_project", settings_project), ("GOOGLE_CLOUD_PROJECT", env_project), ("step1_summary.settings.google_cloud_project", source_project)]
    non_empty = [(label, value) for label, value in candidates if value]
    if not non_empty:
        raise ValueError(
            "Missing Google Cloud project. Set settings.google_cloud_project or GOOGLE_CLOUD_PROJECT."
        )

    distinct_values = {value for _, value in non_empty}
    if len(distinct_values) > 1:
        details = ", ".join(f"{label}={value}" for label, value in non_empty)
        raise ValueError(
            "Conflicting Google Cloud project values detected: "
            f"{details}. Use one consistent project for step-1 uploads and step-2 batch submission."
        )

    return non_empty[0][1]


def fetch_project_number(project_id: str) -> str | None:
    try:
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(GoogleAuthRequest())
        request = Request(
            f"https://cloudresourcemanager.googleapis.com/v1/projects/{project_id}",
            headers={"Authorization": f"Bearer {credentials.token}"},
        )
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        project_number = str(payload.get("projectNumber", "")).strip()
        return project_number or None
    except Exception as exc:
        log(f"Warning: could not resolve project number for IAM preflight: {exc}")
        return None


def bucket_member_has_any_role(
    *,
    storage_client: storage.Client,
    bucket_name: str,
    member: str,
    accepted_roles: set[str],
) -> tuple[bool, str | None]:
    try:
        bucket = storage_client.bucket(bucket_name)
        policy = bucket.get_iam_policy(requested_policy_version=3)
    except Exception as exc:
        return False, f"could not read IAM policy for bucket '{bucket_name}': {exc}"

    for binding in policy.bindings:
        role = str(binding.get("role", ""))
        members = set(binding.get("members", []))
        if role in accepted_roles and member in members:
            return True, None
    return False, None


def ensure_vertex_service_agent_can_access_gcs(
    *,
    storage_client: storage.Client,
    project_id: str,
    input_uris: list[str],
    output_uri_prefix: str,
) -> None:
    project_number = fetch_project_number(project_id)
    if not project_number:
        return

    vertex_service_agent = f"service-{project_number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
    vertex_member = f"serviceAccount:{vertex_service_agent}"

    input_buckets = {parse_gs_uri(uri)[0] for uri in input_uris if uri.startswith("gs://")}
    output_bucket, _ = parse_gs_uri(output_uri_prefix)

    input_reader_roles = {
        "roles/storage.objectViewer",
        "roles/storage.objectAdmin",
        "roles/storage.admin",
        "roles/storage.legacyBucketReader",
        "roles/storage.legacyObjectReader",
    }
    output_writer_roles = {
        "roles/storage.objectCreator",
        "roles/storage.objectAdmin",
        "roles/storage.admin",
        "roles/storage.legacyBucketWriter",
    }

    for bucket_name in sorted(input_buckets):
        has_access, policy_error = bucket_member_has_any_role(
            storage_client=storage_client,
            bucket_name=bucket_name,
            member=vertex_member,
            accepted_roles=input_reader_roles,
        )
        if policy_error:
            log(f"Warning: {policy_error}")
            continue
        if not has_access:
            raise PermissionError(
                "Vertex batch runtime service agent cannot read input files. Grant at least "
                f"'roles/storage.objectViewer' on bucket '{bucket_name}' to '{vertex_service_agent}'. "
                f"Example: gcloud storage buckets add-iam-policy-binding gs://{bucket_name} "
                f"--member=\"serviceAccount:{vertex_service_agent}\" --role=\"roles/storage.objectViewer\""
            )

    has_output_access, output_policy_error = bucket_member_has_any_role(
        storage_client=storage_client,
        bucket_name=output_bucket,
        member=vertex_member,
        accepted_roles=output_writer_roles,
    )
    if output_policy_error:
        log(f"Warning: {output_policy_error}")
        return
    if not has_output_access:
        raise PermissionError(
            "Vertex batch runtime service agent cannot write output files. Grant at least "
            f"'roles/storage.objectCreator' on bucket '{output_bucket}' to '{vertex_service_agent}'. "
            f"Example: gcloud storage buckets add-iam-policy-binding gs://{output_bucket} "
            f"--member=\"serviceAccount:{vertex_service_agent}\" --role=\"roles/storage.objectCreator\""
        )


def upload_file_to_gcs(
    storage_client: storage.Client,
    local_file: Path,
    target_uri: str,
) -> str:
    bucket_name, blob_name = parse_gs_uri(target_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))
    if not blob.exists(client=storage_client):
        raise RuntimeError(f"Uploaded object not found after upload: {target_uri}")
    return target_uri


def load_records_from_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            item = line.strip()
            if not item:
                continue
            try:
                payload = json.loads(item)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def latest_step1_records_by_folder(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_folder: dict[str, dict[str, Any]] = {}
    for row in records:
        folder_key = str(row.get("specimen_folder", "")).strip()
        if not folder_key:
            continue
        by_folder[folder_key] = row
    return by_folder


def build_batch_request_row(
    *,
    gcs_uri: str,
    system_message: str,
    user_prompt: str,
    temperature: float,
) -> dict[str, Any]:
    return {
        "request": {
            "systemInstruction": {"parts": [{"text": system_message}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": user_prompt},
                        {"fileData": {"fileUri": gcs_uri, "mimeType": "image/jpeg"}},
                    ],
                }
            ],
            "generationConfig": {
                "temperature": temperature,
            },
        }
    }


def validate_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = raw.get("settings", {})
    required = [
        "run_id",
        "source_run_id",
        "gcs_bucket",
        "gcs_location",
        "gcs_prefix",
        "model",
        "temperature",
        "system_message",
        "user_prompt",
    ]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit Vertex Gemini transcription batch job from step-1 uploaded records."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of eligible uploaded specimens to include in this batch.",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be 0 or greater.")
    return args


def main() -> None:
    args = parse_args()
    started_at_now = now_iso()
    log("Starting transcription batch submission run")

    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)
    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()
    gcs_bucket = str(settings["gcs_bucket"]).strip()
    gcs_location = str(settings["gcs_location"]).strip()
    gcs_prefix = str(settings["gcs_prefix"]).strip("/")
    model_name = str(settings["model"]).strip()
    temperature = float(settings["temperature"])
    system_message = str(settings["system_message"]).strip()
    user_prompt = str(settings["user_prompt"]).strip()
    source_records_file_setting = str(
        settings.get(
            "source_records_file",
            f"app/output/pipeline_runs/{source_run_id}.records.jsonl",
        )
    ).strip()
    source_records_file = resolve_path(source_records_file_setting)
    source_summary_file_setting = str(
        settings.get(
            "source_summary_file",
            f"app/output/pipeline_runs/{source_run_id}.json",
        )
    ).strip()
    source_summary_file = resolve_path(source_summary_file_setting)
    source_summary: dict[str, Any] | None = None
    if source_summary_file.exists():
        source_summary = load_json(source_summary_file)

    output_base = resolve_path(f"app/output/pipeline_runs/{run_id}.transcript_batch.json")
    output_file = output_base
    records_file = output_base.with_name(f"{output_base.stem}.records.jsonl")
    local_batch_input_file = output_base.with_name(f"{output_base.stem}.input.jsonl")

    batch_input_uri = f"gs://{gcs_bucket}/{gcs_prefix}/batch_jobs/{run_id}/requests.jsonl"
    batch_output_uri_prefix = f"gs://{gcs_bucket}/{gcs_prefix}/batch_jobs/{run_id}/output"

    project_id = resolve_project_id(settings=settings, source_summary=source_summary)
    vertex_location = os.getenv("GOOGLE_CLOUD_LOCATION", "").strip()
    if not vertex_location:
        raise ValueError("Missing Vertex location. Set GOOGLE_CLOUD_LOCATION in environment.")

    adc_credentials_file = resolve_adc_credentials_from_env()
    if adc_credentials_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(adc_credentials_file)

    log(f"Settings file: {SETTINGS_PATH}")
    log(f"Run id: {run_id}")
    log(f"Source run id: {source_run_id}")
    log(f"Source records file: {source_records_file}")
    log(f"Source summary file: {source_summary_file}")
    log(f"Project: {project_id}")
    log(f"Vertex location: {vertex_location}")
    log(f"Configured GCS location: {gcs_location}")
    log(f"Model: {model_name}")
    log(f"Batch input URI: {batch_input_uri}")
    log(f"Batch output URI prefix: {batch_output_uri_prefix}")
    log(f"Limit: {args.limit}")
    if adc_credentials_file:
        log(f"ADC credentials file: {adc_credentials_file}")
    else:
        log("ADC credentials file: default Google auth resolution")

    if not source_records_file.exists():
        raise FileNotFoundError(f"Step-1 records file not found: {source_records_file}")

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
        existing_job_name = (
            existing_output.get("data", {}).get("batch_job", {}).get("name")
            if isinstance(existing_output, dict)
            else None
        )
        if existing_job_name:
            log(f"Batch job already submitted in existing run: {existing_job_name}")
            return

    storage_client = storage.Client(project=project_id)
    client = genai.Client(
        vertexai=True,
        project=project_id,
        location=vertex_location,
        http_options=types.HttpOptions(api_version="v1"),
    )

    source_rows = load_records_from_jsonl(source_records_file)
    source_latest_by_folder = latest_step1_records_by_folder(source_rows)
    source_specimens = sorted(source_latest_by_folder.values(), key=lambda item: str(item.get("specimen_folder", "")))

    records_by_folder: dict[str, dict[str, Any]] = {}
    if records_file.exists():
        for item in load_records_from_jsonl(records_file):
            folder_key = str(item.get("specimen_folder", "")).strip()
            if folder_key:
                records_by_folder[folder_key] = item

    def build_counts() -> dict[str, int]:
        eligible = sum(1 for rec in records_by_folder.values() if rec.get("status") == "eligible")
        queued = sum(1 for rec in records_by_folder.values() if rec.get("status") == "queued")
        skipped = sum(1 for rec in records_by_folder.values() if rec.get("status") == "skipped")
        failed = sum(1 for rec in records_by_folder.values() if rec.get("status") == "failed")
        return {
            "source_rows_total": len(source_rows),
            "source_specimens_latest": len(source_specimens),
            "eligible": eligible,
            "queued": queued,
            "skipped": skipped,
            "failed": failed,
        }

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
            "source_run_id": source_run_id,
            "source_records_file": str(source_records_file),
            "google_cloud_project": project_id,
            "vertex_location": vertex_location,
            "gcs_location": gcs_location,
            "model": model_name,
            "temperature": temperature,
            "batch_input_uri": batch_input_uri,
            "batch_output_uri_prefix": batch_output_uri_prefix,
            "output_file": str(output_file),
            "records_file": str(records_file),
            "local_batch_input_file": str(local_batch_input_file),
        },
        "data": {
            "counts": build_counts(),
            "records_logged": len(records_by_folder),
            "batch_job": None,
        },
    }

    def persist_run(status: str, *, error: str | None = None, finished: bool = False) -> None:
        run_output["run_status"] = status
        run_output["error"] = error
        run_output["last_updated_at"] = now_iso()
        run_output["finished_at"] = now_iso() if finished else None
        run_output["data"]["counts"] = build_counts()
        run_output["data"]["records_logged"] = len(records_by_folder)
        save_json(output_file, run_output)

    persist_run(RUN_STATUS_RUNNING)

    def handle_termination_signal(signum: int, frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        raise RunTerminatedError(f"Received termination signal: {signal_name}")

    signal.signal(signal.SIGINT, handle_termination_signal)
    signal.signal(signal.SIGTERM, handle_termination_signal)

    queued_records: list[dict[str, Any]] = []
    queued_input_uris: list[str] = []
    pending_specimens = source_specimens
    if args.limit is not None:
        pending_specimens = source_specimens[: args.limit]
    limit_caused_incomplete = args.limit is not None and len(source_specimens) > len(pending_specimens)

    processed_since_flush = 0

    try:
        for specimen in pending_specimens:
            folder_key = str(specimen.get("specimen_folder", "")).strip()
            record = {
                "specimen_folder": folder_key,
                "qname": specimen.get("qname"),
                "gcs_uri": specimen.get("gcs_uri"),
                "status": "skipped",
                "error": None,
                "notes": [],
            }

            status = str(specimen.get("status", "")).strip()
            gcs_uri = str(specimen.get("gcs_uri", "")).strip()

            if status != "uploaded":
                record["error"] = f"Source record status is '{status or 'missing'}', expected 'uploaded'."
                records_by_folder[folder_key] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            if not gcs_uri.startswith("gs://"):
                record["error"] = "Missing or invalid gcs_uri in source record."
                records_by_folder[folder_key] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            batch_row = build_batch_request_row(
                gcs_uri=gcs_uri,
                system_message=system_message,
                user_prompt=user_prompt,
                temperature=temperature,
            )
            queued_records.append(batch_row)
            queued_input_uris.append(gcs_uri)
            record["status"] = "eligible"
            records_by_folder[folder_key] = record
            append_jsonl(records_file, record)

            processed_since_flush += 1
            if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                persist_run(RUN_STATUS_RUNNING)
                processed_since_flush = 0

        if not queued_records:
            raise ValueError("No eligible uploaded records found to submit in batch request.")

        local_batch_input_file.parent.mkdir(parents=True, exist_ok=True)
        with local_batch_input_file.open("w", encoding="utf-8") as f:
            for row in queued_records:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")
        log(f"Wrote local batch input JSONL: {local_batch_input_file}")

        uploaded_input_uri = upload_file_to_gcs(
            storage_client=storage_client,
            local_file=local_batch_input_file,
            target_uri=batch_input_uri,
        )
        log(f"Uploaded batch input JSONL: {uploaded_input_uri}")
        ensure_vertex_service_agent_can_access_gcs(
            storage_client=storage_client,
            project_id=project_id,
            input_uris=[uploaded_input_uri] + queued_input_uris,
            output_uri_prefix=batch_output_uri_prefix,
        )

        batch_job = client.batches.create(
            model=model_name,
            src=uploaded_input_uri,
            config=types.CreateBatchJobConfig(dest=batch_output_uri_prefix),
        )

        batch_job_payload = (
            batch_job.model_dump(mode="json", exclude_none=False)
            if hasattr(batch_job, "model_dump")
            else {"name": getattr(batch_job, "name", None), "state": str(getattr(batch_job, "state", ""))}
        )
        run_output["data"]["batch_job"] = batch_job_payload

        for folder_key, record in records_by_folder.items():
            if record.get("status") == "eligible":
                queued_record = dict(record)
                queued_record["status"] = "queued"
                queued_record["batch_job_name"] = batch_job_payload.get("name")
                records_by_folder[folder_key] = queued_record
                append_jsonl(records_file, queued_record)

        final_status = RUN_STATUS_PARTIAL if limit_caused_incomplete else RUN_STATUS_FINISHED
        persist_run(final_status, finished=True)

        log(f"Submitted batch job: {batch_job_payload.get('name')}")
        log(f"Batch job state: {batch_job_payload.get('state')}")
        log(f"Wrote run output: {output_file}")
        log(f"Run status: {final_status}")

    except RunTerminatedError as exc:
        log(str(exc))
        persist_run(RUN_STATUS_TERMINATED, error=str(exc), finished=True)
        raise SystemExit(1) from exc
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        log(f"Run failed: {error_message}")
        persist_run(RUN_STATUS_FAILED, error=error_message, finished=True)
        raise


if __name__ == "__main__":
    main()
