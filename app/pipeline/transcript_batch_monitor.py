"""Monitor Vertex batch job and download responses.

Step 3:
- Read submitted batch job metadata from step-2 summary.
- Poll job state with fixed interval until terminal state or timeout.
- Download batch response files from GCS to local filesystem.
- Persist one run-level summary JSON + append-only JSONL records.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import google.genai as genai
from google.cloud import storage
from google.genai import types

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "transcribe_batch_monitor_settings.json"
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


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))
        f.write("\n")


def load_records_jsonl(path: Path) -> list[dict[str, Any]]:
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
    bucket, prefix = without_scheme.split("/", 1)
    return bucket, prefix


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


def is_terminal_job_state(job_state: str) -> bool:
    terminal_states = {
        "JOB_STATE_SUCCEEDED",
        "JOB_STATE_FAILED",
        "JOB_STATE_CANCELLED",
        "JOB_STATE_PAUSED",
        "JOB_STATE_EXPIRED",
    }
    return job_state in terminal_states


def validate_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = raw.get("settings", {})
    required = [
        "run_id",
        "source_run_id",
        "download_dir",
    ]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor Vertex batch job and download raw responses when completed."
    )
    parser.add_argument(
        "--timeout-hours",
        type=float,
        default=24.0,
        help="Maximum time to wait for terminal job state before ending as partial (default: 24).",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=300,
        help="Polling interval in seconds (default: 300).",
    )
    args = parser.parse_args()
    if args.timeout_hours <= 0:
        parser.error("--timeout-hours must be greater than 0.")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be greater than 0.")
    return args


def main() -> None:
    args = parse_args()
    started_at_now = now_iso()
    started_monotonic = time.monotonic()
    timeout_seconds = int(args.timeout_hours * 3600)

    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)

    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()
    download_dir = resolve_path(str(settings["download_dir"]).strip())
    source_summary_file = resolve_path(
        str(
            settings.get(
                "source_summary_file",
                f"app/output/pipeline_runs/{source_run_id}/{source_run_id}.transcript_batch.json",
            )
        ).strip()
    )

    run_output_dir = resolve_path(f"app/output/pipeline_runs/{run_id}")
    output_file = run_output_dir / f"{run_id}.transcript_batch_monitor.json"
    records_file = output_file.with_name(f"{output_file.stem}.records.jsonl")
    download_root = run_output_dir / "transcript_batch_responses"

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    if not project_id:
        raise ValueError("Missing Google Cloud project. Set GOOGLE_CLOUD_PROJECT in environment.")
    vertex_location = os.getenv("GOOGLE_CLOUD_LOCATION", "").strip()
    if not vertex_location:
        raise ValueError("Missing Vertex location. Set GOOGLE_CLOUD_LOCATION in environment.")

    adc_credentials_file = resolve_adc_credentials_from_env()
    if adc_credentials_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(adc_credentials_file)

    log(f"Settings file: {SETTINGS_PATH}")
    log(f"Run id: {run_id}")
    log(f"Source run id: {source_run_id}")
    log(f"Source summary file: {source_summary_file}")
    log(f"Project: {project_id}")
    log(f"Vertex location: {vertex_location}")
    log(f"Download root: {download_root}")
    log(f"Poll interval seconds: {args.poll_seconds}")
    log(f"Timeout seconds: {timeout_seconds}")

    if not source_summary_file.exists():
        raise FileNotFoundError(f"Step-2 summary file not found: {source_summary_file}")
    step2_summary = load_json(source_summary_file)
    batch_job = step2_summary.get("data", {}).get("batch_job", {})
    if not isinstance(batch_job, dict):
        raise ValueError("Invalid step-2 batch_job payload in source summary.")

    batch_job_name = str(batch_job.get("name", "")).strip()
    if not batch_job_name:
        raise ValueError("Missing batch job name in step-2 summary data.batch_job.name")

    batch_output_uri_prefix = str(
        step2_summary.get("settings", {}).get("batch_output_uri_prefix", "")
    ).strip()
    if not batch_output_uri_prefix.startswith("gs://"):
        raise ValueError("Missing or invalid batch_output_uri_prefix in step-2 summary settings.")

    existing_output: dict[str, Any] | None = None
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

    records_events = load_records_jsonl(records_file)

    def build_counts() -> dict[str, int]:
        poll_events = sum(1 for event in records_events if event.get("event") == "poll")
        download_events = sum(1 for event in records_events if event.get("event") == "download")
        downloaded_files = sum(
            1
            for event in records_events
            if event.get("event") == "download" and event.get("status") == "downloaded"
        )
        skipped_existing = sum(
            1
            for event in records_events
            if event.get("event") == "download" and event.get("status") == "skipped_existing"
        )
        download_failures = sum(
            1
            for event in records_events
            if event.get("event") == "download" and event.get("status") == "failed"
        )
        return {
            "poll_events": poll_events,
            "download_events": download_events,
            "downloaded_files": downloaded_files,
            "skipped_existing_files": skipped_existing,
            "download_failures": download_failures,
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
            "google_cloud_project": project_id,
            "vertex_location": vertex_location,
            "batch_job_name": batch_job_name,
            "batch_output_uri_prefix": batch_output_uri_prefix,
            "download_dir": str(download_dir),
            "run_output_dir": str(run_output_dir),
            "download_root": str(download_root),
            "output_file": str(output_file),
            "records_file": str(records_file),
            "poll_seconds": args.poll_seconds,
            "timeout_hours": args.timeout_hours,
        },
        "data": {
            "counts": build_counts(),
            "records_logged": len(records_events),
            "job_state": None,
            "job_payload": None,
        },
    }

    def persist_run(status: str, *, error: str | None = None, finished: bool = False) -> None:
        run_output["run_status"] = status
        run_output["error"] = error
        run_output["last_updated_at"] = now_iso()
        run_output["finished_at"] = now_iso() if finished else None
        run_output["data"]["counts"] = build_counts()
        run_output["data"]["records_logged"] = len(records_events)
        save_json(output_file, run_output)

    def add_event(payload: dict[str, Any]) -> None:
        records_events.append(payload)
        append_jsonl(records_file, payload)

    persist_run(RUN_STATUS_RUNNING)

    def handle_termination_signal(signum: int, frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        raise RunTerminatedError(f"Received termination signal: {signal_name}")

    signal.signal(signal.SIGINT, handle_termination_signal)
    signal.signal(signal.SIGTERM, handle_termination_signal)

    client = genai.Client(
        vertexai=True,
        project=project_id,
        location=vertex_location,
        http_options=types.HttpOptions(api_version="v1"),
    )
    storage_client = storage.Client(project=project_id)

    try:
        last_job_payload: dict[str, Any] | None = None
        last_job_state = ""
        while True:
            elapsed = int(time.monotonic() - started_monotonic)
            if elapsed >= timeout_seconds:
                add_event(
                    {
                        "event": "timeout",
                        "batch_job_name": batch_job_name,
                        "elapsed_seconds": elapsed,
                        "timeout_seconds": timeout_seconds,
                        "last_state": last_job_state or None,
                    }
                )
                run_output["data"]["job_state"] = last_job_state or None
                run_output["data"]["job_payload"] = last_job_payload
                persist_run(
                    RUN_STATUS_PARTIAL,
                    error=f"Polling timeout reached after {elapsed} seconds.",
                    finished=True,
                )
                log(f"Timeout reached after {elapsed} seconds; ending with status '{RUN_STATUS_PARTIAL}'.")
                return

            job = client.batches.get(name=batch_job_name)
            if hasattr(job, "model_dump"):
                job_payload = job.model_dump(mode="json", exclude_none=False)
            else:
                job_payload = {
                    "name": getattr(job, "name", None),
                    "state": str(getattr(job, "state", "")),
                }
            job_state = str(job_payload.get("state", "")).strip()
            last_job_state = job_state
            last_job_payload = job_payload
            run_output["data"]["job_state"] = job_state
            run_output["data"]["job_payload"] = job_payload

            add_event(
                {
                    "event": "poll",
                    "batch_job_name": batch_job_name,
                    "state": job_state,
                    "elapsed_seconds": elapsed,
                    "polled_at": now_iso(),
                }
            )
            persist_run(RUN_STATUS_RUNNING)
            log(f"Batch job state: {job_state or 'UNKNOWN'}")

            if is_terminal_job_state(job_state):
                break

            time.sleep(args.poll_seconds)

        if last_job_state != "JOB_STATE_SUCCEEDED":
            persist_run(
                RUN_STATUS_FAILED,
                error=f"Batch job ended in terminal non-success state: {last_job_state}",
                finished=True,
            )
            log(f"Batch job finished with non-success state: {last_job_state}")
            return

        bucket_name, prefix = parse_gs_uri(batch_output_uri_prefix)
        bucket = storage_client.bucket(bucket_name)
        blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
        if not blobs:
            raise FileNotFoundError(f"No output blobs found under {batch_output_uri_prefix}")

        log(f"Downloading {len(blobs)} output blobs from {batch_output_uri_prefix}")
        for blob in blobs:
            if blob.name.endswith("/"):
                continue

            relative_path = blob.name[len(prefix) :].lstrip("/") if blob.name.startswith(prefix) else blob.name
            local_path = download_root / relative_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            event_base = {
                "event": "download",
                "gcs_uri": f"gs://{bucket_name}/{blob.name}",
                "local_path": str(local_path),
                "size_bytes": int(blob.size or 0),
            }

            if local_path.exists() and local_path.stat().st_size == int(blob.size or 0):
                event = dict(event_base)
                event["status"] = "skipped_existing"
                add_event(event)
                continue

            try:
                blob.download_to_filename(str(local_path))
                event = dict(event_base)
                event["status"] = "downloaded"
                add_event(event)
            except Exception as exc:
                event = dict(event_base)
                event["status"] = "failed"
                event["error"] = str(exc)
                add_event(event)
                log(f"Download failed for {blob.name}: {exc}")

        final_counts = build_counts()
        if final_counts["download_failures"] > 0:
            persist_run(
                RUN_STATUS_FAILED,
                error=f"One or more output blob downloads failed ({final_counts['download_failures']}).",
                finished=True,
            )
            log("Run failed due to output download failures.")
            return

        persist_run(RUN_STATUS_FINISHED, finished=True)
        log(f"Run status: {RUN_STATUS_FINISHED}")
        log(f"Downloaded files: {final_counts['downloaded_files']}")
        log(f"Skipped existing files: {final_counts['skipped_existing_files']}")
        log(f"Wrote run output: {output_file}")

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
