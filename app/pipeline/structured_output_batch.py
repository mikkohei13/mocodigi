"""Settings-driven structured output batch submission pipeline.

Step 6:
- Read preprocessed transcript rows from step-5 output JSONL.
- Build Gemini batch input JSONL for structured extraction.
- Submit a Vertex Gemini batch job for structurization.
- Persist one run-level summary JSON + per-specimen JSONL records.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import google.auth
import google.genai as genai
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from google.genai import types
from utils.files import (
    append_jsonl,
    load_json,
    load_jsonl_rows,
    resolve_path as resolve_path_from_root,
    save_json,
)
from utils.gcp import parse_gs_uri, resolve_adc_credentials_from_env, upload_file_to_gcs_uri
from utils.pipeline_config import archive_pipeline_settings, load_step_settings
from utils.runtime import (
    RUN_STATUS_FAILED,
    RUN_STATUS_FINISHED,
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
SETTINGS_PATH = SCRIPT_DIR / "settings" / "structured_output_settings.json"
SUMMARY_FLUSH_EVERY = 200
SUPPORTED_BATCH_MODELS = {
    "gemini-3.1-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-pro",
}


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

    candidates = [
        ("settings.google_cloud_project", settings_project),
        ("GOOGLE_CLOUD_PROJECT", env_project),
        ("step5_summary.settings.google_cloud_project", source_project),
    ]
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
            f"{details}. Use one consistent project for step-5 and step-6."
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

    vertex_service_agent = (
        f"service-{project_number}@gcp-sa-aiplatform.iam.gserviceaccount.com"
    )
    vertex_member = f"serviceAccount:{vertex_service_agent}"

    input_buckets = {
        parse_gs_uri(uri)[0] for uri in input_uris if uri.startswith("gs://")
    }
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


def load_records_from_jsonl(path: Path) -> list[dict[str, Any]]:
    return load_jsonl_rows(path)


def normalize_specimen_key(record: dict[str, Any], row_index: int) -> str:
    qname = str(record.get("qname", "")).strip()
    if qname:
        return f"qname:{qname}"
    document_long_id = str(record.get("document_long_id", "")).strip()
    if document_long_id:
        return f"doc:{document_long_id}"
    source_prediction_file = str(record.get("source_prediction_file", "")).strip()
    source_row_index = record.get("source_row_index")
    return f"fallback:{source_prediction_file}#{source_row_index or row_index}"


def group_step5_records_by_specimen(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_specimen: dict[str, list[dict[str, Any]]] = {}
    for row_index, row in enumerate(records, start=1):
        key = normalize_specimen_key(row, row_index)
        by_specimen.setdefault(key, []).append(row)
    return by_specimen


def row_sort_key(row: dict[str, Any]) -> tuple[int, str, str, int]:
    image_index_raw = row.get("image_index")
    try:
        image_index = int(image_index_raw) if image_index_raw is not None else 10_000_000
    except (TypeError, ValueError):
        image_index = 10_000_000

    image_filename = str(row.get("image_filename", "")).strip()
    source_prediction_file = str(row.get("source_prediction_file", "")).strip()
    source_row_index_raw = row.get("source_row_index")
    try:
        source_row_index = int(source_row_index_raw) if source_row_index_raw is not None else 10_000_000
    except (TypeError, ValueError):
        source_row_index = 10_000_000
    return (image_index, image_filename, source_prediction_file, source_row_index)


def build_aggregated_transcript(rows: list[dict[str, Any]]) -> tuple[str, list[str], int]:
    sorted_rows = sorted(rows, key=row_sort_key)
    segments: list[str] = []
    source_images: list[str] = []
    for index, row in enumerate(sorted_rows, start=1):
        data = row.get("data", {})
        text = ""
        if isinstance(data, dict):
            text = str(data.get("preprocessed_transcript", "")).strip()
        if not text:
            continue
        image_filename = str(row.get("image_filename", "")).strip()
        image_index = row.get("image_index")
        label = image_filename or f"image_{index}.jpg"
        if image_index is not None:
            label = f"{label} (index={image_index})"
        source_images.append(image_filename or f"image_{index}")
        segments.append(f"[Image {index}: {label}]\n{text}")
    return "\n\n".join(segments).strip(), source_images, len(sorted_rows)


def build_batch_request_row(
    *,
    aggregated_transcript: str,
    document_long_id: str,
    qname: str,
    image_count: int,
    source_images: list[str],
    model_name: str,
    system_message: str,
    user_prompt: str,
    temperature: float,
    response_json_schema: dict[str, Any],
) -> dict[str, Any]:
    generation_config: dict[str, Any] = {
        "temperature": temperature,
        "responseMimeType": "application/json",
        "responseSchema": response_json_schema,
    }
    if model_name == "gemini-3.1-pro-preview":
        generation_config["thinkingConfig"] = {"thinkingLevel": "LOW"}
    elif model_name == "gemini-3-flash-preview":
        generation_config["thinkingConfig"] = {"thinkingLevel": "MINIMAL"}
    elif model_name == "gemini-2.5-pro":
        generation_config["thinkingConfig"] = {"thinkingBudget": 128}

    return {
        "document_long_id": document_long_id,
        "qname": qname or None,
        "image_count": image_count,
        "source_images": source_images,
        "request": {
            "systemInstruction": {"parts": [{"text": system_message}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{user_prompt}{aggregated_transcript}"}],
                }
            ],
            "generationConfig": generation_config,
        },
    }


def validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
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
        "schema_file",
    ]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")

    model_name = str(settings.get("model", "")).strip()
    if model_name not in SUPPORTED_BATCH_MODELS:
        raise ValueError(
            "Unsupported settings.model. Use one of: "
            + ", ".join(sorted(SUPPORTED_BATCH_MODELS))
        )
    return settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit Vertex Gemini structured output batch job from step-5 records."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of eligible preprocessed specimens to include in this batch.",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be 0 or greater.")
    return args


def main() -> None:
    args = parse_args()
    started_at_now = now_iso()
    log("Starting structured output batch submission run")

    merged_settings, _ = load_step_settings("structured_output_batch", SETTINGS_PATH)
    settings = validate_settings(merged_settings)
    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()
    gcs_bucket = str(settings["gcs_bucket"]).strip()
    gcs_location = str(settings["gcs_location"]).strip()
    gcs_prefix = str(settings["gcs_prefix"]).strip("/")
    model_name = str(settings["model"]).strip()
    temperature = float(settings["temperature"])
    system_message = str(settings["system_message"]).strip()
    user_prompt = str(settings["user_prompt"])
    schema_file = resolve_path_from_root(PROJECT_ROOT, str(settings["schema_file"]).strip())
    if not schema_file.exists():
        raise FileNotFoundError(f"Structured output schema file not found: {schema_file}")
    response_json_schema = load_json(schema_file)
    source_summary_file_setting = str(
        settings.get(
            "source_summary_file",
            f"app/output/pipeline_runs/{source_run_id}/preprocess_structure.json",
        )
    ).strip()
    source_summary_file = resolve_path_from_root(PROJECT_ROOT, source_summary_file_setting)
    if not source_summary_file.exists():
        raise FileNotFoundError(f"Step-5 summary file not found: {source_summary_file}")
    source_summary = load_json(source_summary_file)

    summary_output_jsonl = str(source_summary.get("data", {}).get("output_jsonl", "")).strip()
    source_records_file_setting = str(
        settings.get(
            "source_records_file",
            summary_output_jsonl
            or f"app/output/pipeline_runs/{source_run_id}/preprocess_structure.jsonl",
        )
    ).strip()
    source_records_file = resolve_path_from_root(PROJECT_ROOT, source_records_file_setting)

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    output_base = run_output_dir / "structured_output_batch.json"
    output_file = output_base
    records_file = output_base.with_name("structured_output_batch.records.jsonl")
    local_batch_input_file = output_base.with_name("structured_output_batch.input.jsonl")
    archive_pipeline_settings(run_output_dir)

    batch_input_uri = (
        f"gs://{gcs_bucket}/{gcs_prefix}/batch_jobs/{run_id}/structured_requests.jsonl"
    )
    batch_output_uri_prefix = (
        f"gs://{gcs_bucket}/{gcs_prefix}/batch_jobs/{run_id}/structured_output"
    )

    project_id = resolve_project_id(settings=settings, source_summary=source_summary)
    vertex_location = os.getenv("GOOGLE_CLOUD_LOCATION", "").strip()
    if not vertex_location:
        raise ValueError("Missing Vertex location. Set GOOGLE_CLOUD_LOCATION in environment.")

    adc_credentials_file = resolve_adc_credentials_from_env(
        lambda path_str: resolve_path_from_root(PROJECT_ROOT, path_str)
    )
    if adc_credentials_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(adc_credentials_file)

    log(f"Settings file: {SETTINGS_PATH}")
    log(f"Run id: {run_id}")
    log(f"Source run id: {source_run_id}")
    log(f"Source summary file: {source_summary_file}")
    log(f"Source records file: {source_records_file}")
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
        raise FileNotFoundError(f"Step-5 records file not found: {source_records_file}")

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
    source_grouped = group_step5_records_by_specimen(source_rows)
    source_specimens = sorted(source_grouped.items(), key=lambda item: item[0])

    records_by_key: dict[str, dict[str, Any]] = {}
    if records_file.exists():
        for item in load_records_from_jsonl(records_file):
            key = str(item.get("record_key", "")).strip()
            if key:
                records_by_key[key] = item

    def build_counts() -> dict[str, int]:
        eligible = sum(
            1 for rec in records_by_key.values() if rec.get("status") == "eligible"
        )
        queued = sum(
            1 for rec in records_by_key.values() if rec.get("status") == "queued"
        )
        skipped = sum(
            1 for rec in records_by_key.values() if rec.get("status") == "skipped"
        )
        failed = sum(1 for rec in records_by_key.values() if rec.get("status") == "failed")
        return {
            "source_rows_total": len(source_rows),
            "source_specimens_grouped": len(source_specimens),
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
            "source_summary_file": str(source_summary_file),
            "source_records_file": str(source_records_file),
            "google_cloud_project": project_id,
            "vertex_location": vertex_location,
            "gcs_location": gcs_location,
            "model": model_name,
            "temperature": temperature,
            "batch_input_uri": batch_input_uri,
            "batch_output_uri_prefix": batch_output_uri_prefix,
            "schema_file": str(schema_file),
        },
        "data": {
            "counts": build_counts(),
            "records_logged": len(records_by_key),
            "batch_job": None,
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

    persist_run(RUN_STATUS_RUNNING)
    install_termination_handlers()

    queued_records: list[dict[str, Any]] = []
    pending_specimens = source_specimens
    if args.limit is not None:
        pending_specimens = source_specimens[: args.limit]

    processed_since_flush = 0

    try:
        for row_index, (specimen_key, specimen_rows) in enumerate(pending_specimens, start=1):
            representative = sorted(specimen_rows, key=row_sort_key)[0]
            record_key = specimen_key
            qname = str(representative.get("qname", "")).strip() or None
            document_long_id = str(representative.get("document_long_id", "")).strip()
            source_prediction_file = str(representative.get("source_prediction_file", "")).strip()
            source_row_index = representative.get("source_row_index")

            record = {
                "record_key": record_key,
                "qname": qname,
                "document_long_id": document_long_id or None,
                "source_prediction_file": source_prediction_file or None,
                "source_row_index": source_row_index,
                "image_count": len(specimen_rows),
                "source_images": [],
                "status": "skipped",
                "error": None,
                "notes": [],
            }

            aggregated_transcript, source_images, image_count = build_aggregated_transcript(specimen_rows)
            record["source_images"] = source_images
            record["image_count"] = image_count
            if not aggregated_transcript:
                record["error"] = (
                    "No non-empty data.preprocessed_transcript values found in grouped step-5 rows."
                )
                records_by_key[record_key] = record
                append_jsonl(records_file, record)
                processed_since_flush += 1
                if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                    persist_run(RUN_STATUS_RUNNING)
                    processed_since_flush = 0
                continue

            batch_row = build_batch_request_row(
                aggregated_transcript=aggregated_transcript,
                document_long_id=document_long_id,
                qname=qname or "",
                image_count=image_count,
                source_images=source_images,
                model_name=model_name,
                system_message=system_message,
                user_prompt=user_prompt,
                temperature=temperature,
                response_json_schema=response_json_schema,
            )
            queued_records.append(batch_row)
            record["status"] = "eligible"
            records_by_key[record_key] = record
            append_jsonl(records_file, record)

            processed_since_flush += 1
            if processed_since_flush >= SUMMARY_FLUSH_EVERY:
                persist_run(RUN_STATUS_RUNNING)
                processed_since_flush = 0

        if not queued_records:
            raise ValueError(
                "No eligible preprocessed records found to submit in batch request."
            )

        local_batch_input_file.parent.mkdir(parents=True, exist_ok=True)
        with local_batch_input_file.open("w", encoding="utf-8") as f:
            for row in queued_records:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")
        log(f"Wrote local batch input JSONL: {local_batch_input_file}")

        uploaded_input_uri = upload_file_to_gcs_uri(
            client=storage_client,
            local_file=local_batch_input_file,
            target_uri=batch_input_uri,
        )
        log(f"Uploaded batch input JSONL: {uploaded_input_uri}")
        ensure_vertex_service_agent_can_access_gcs(
            storage_client=storage_client,
            project_id=project_id,
            input_uris=[uploaded_input_uri],
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
            else {
                "name": getattr(batch_job, "name", None),
                "state": str(getattr(batch_job, "state", "")),
            }
        )
        run_output["data"]["batch_job"] = batch_job_payload

        for record_key, record in records_by_key.items():
            if record.get("status") == "eligible":
                queued_record = dict(record)
                queued_record["status"] = "queued"
                queued_record["batch_job_name"] = batch_job_payload.get("name")
                records_by_key[record_key] = queued_record
                append_jsonl(records_file, queued_record)

        persist_run(RUN_STATUS_FINISHED, finished=True)

        log(f"Submitted batch job: {batch_job_payload.get('name')}")
        log(f"Batch job state: {batch_job_payload.get('state')}")
        log(f"Wrote run output: {output_file}")
        log(f"Run status: {RUN_STATUS_FINISHED}")

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
