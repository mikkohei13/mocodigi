"""Preprocess downloaded transcript predictions for structure submission.

Step 4:
- Read downloaded prediction rows from step-3 monitor output.
- Apply transcript preprocessing rules.
- Save successful preprocessed rows to JSONL for the next submit step.
- Persist one run-level summary JSON + append-only JSONL records.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

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
    RUN_STATUS_RUNNING,
    RUN_STATUS_TERMINATED,
    RunTerminatedError,
    install_termination_handlers,
    log,
    now_iso,
)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "preprocess_structure_settings.json"


def validate_settings(raw: dict[str, Any]) -> dict[str, Any]:
    settings = raw.get("settings", {})
    required = ["run_id", "source_run_id"]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def extract_qname(payload: dict[str, Any]) -> str:
    contents = payload.get("request", {}).get("contents", [])
    if not isinstance(contents, list):
        return ""
    for content in contents:
        if not isinstance(content, dict):
            continue
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            file_data = part.get("fileData")
            if not isinstance(file_data, dict):
                continue
            file_uri = str(file_data.get("fileUri", "")).strip()
            if not file_uri.startswith("gs://"):
                continue
            uri_parts = [segment for segment in file_uri.split("/") if segment]
            if len(uri_parts) >= 4:
                return uri_parts[-2]
    return ""


def extract_transcript(payload: dict[str, Any]) -> str:
    candidates = payload.get("response", {}).get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        return ""
    first_candidate = candidates[0]
    if not isinstance(first_candidate, dict):
        return ""
    content = first_candidate.get("content", {})
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts", [])
    if not isinstance(parts, list):
        return ""
    for part in parts:
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str):
            return text
    return ""


def preprocess_transcript(text: str) -> tuple[str, dict[str, object]]:
    details: dict[str, object] = {}
    if not text:
        return text, details

    # If multiple Luomus specimen identifiers are present, keep text as-is and flag.
    if text.count("id.luomus.fi") > 1:
        details["multiple_specimens"] = True
        return text, details

    # Extract QUADR.
    quadr_pattern = re.compile(r"QU[A-Z]{1,2}R\.")
    quadr_match = quadr_pattern.search(text)
    if quadr_match:
        text = quadr_pattern.sub("", text, count=1).strip()
        details["remove_quadr"] = True
    else:
        details["remove_quadr"] = False

    # Extract http-uri identifier label data
    # Allow for spaces or dots in the identifier, convert to dots for the http-uri
    luomus_pattern = re.compile(
        r"(http://id\.luomus\.fi/)\s*([A-Za-z]{1,4}[.\s][0-9]+)\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*",
        flags=re.IGNORECASE,
    )
    luomus_match = luomus_pattern.search(text)
    if luomus_match:
        base_uri, specimen_id, specimen_date = luomus_match.groups()
        details["http_uri"] = f"{base_uri}{specimen_id.replace(' ', '.')}"
        details["digitization_date"] = specimen_date
        details["missing_http_uri"] = False
        text = luomus_pattern.sub("", text, count=1).strip()
    else:
        details["missing_http_uri"] = True

    # Extract H-number from English stamp
    helsinki_with_number_pattern = re.compile(
        r"MUSEUM[\s.,]*BOTANICUM[\s.,]*UNIV[\s.,]*(?:\(\s*H[\s.,]*\)[\s.,]*)?HELSINKI[\s.,]*([0-9]{4,8})",
        flags=re.IGNORECASE,
    )
    helsinki_with_number_match = helsinki_with_number_pattern.search(text)
    if helsinki_with_number_match:
        h_number = int(helsinki_with_number_match.group(1))
        if 1000 <= h_number <= 10_000_000:
            text = helsinki_with_number_pattern.sub("", text, count=1).strip()
            details["h_number"] = h_number
            details["missing_h_number"] = False
        else:
            details["missing_h_number"] = True
    else:
        details["missing_h_number"] = True

    # Extract H-number from Latin stamp
    if details["missing_h_number"]:
        helsinki_with_number_pattern = re.compile(
            r"MUSEUM\s*BOTANICUM\s*UNIV\.?\s*HELSINGIENSIS\s*([0-9]{4,8})",
            flags=re.IGNORECASE,
        )
        helsinki_with_number_match = helsinki_with_number_pattern.search(text)
        if helsinki_with_number_match:
            h_number = int(helsinki_with_number_match.group(1))
            if 1000 <= h_number <= 10_000_000:
                text = helsinki_with_number_pattern.sub("", text, count=1).strip()
                details["h_number"] = h_number
                details["missing_h_number"] = False

    # Remove old Helsinki herbarium names
    helsinki_patterns = [
        re.compile(
            r"MUSEUM[\s.,]*BOTANICUM[\s.,]*UNIV[\s.,]*(?:\(\s*H\s*\)[\s.,]*)?HELSINKI",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"MUSEUM[\s.,]*BOTANICUM[\s.,]*UNIVERSITATIS[\s.,]*HELSINKI",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"HERBARIUM[\s.,]*MUSEI[\s.,]*HELSINGIENSIS",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"HORTUS[\s.,]*BOTANICUS[\s.,]*UNIVERSITATIS[\s.,]*HELSINGIENSIS",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"Botanical[\s.,]*Museum[\s.,]*University[\s.,]*of[\s.,]*Helsinki",
            flags=re.IGNORECASE,
        ),
    ]
    for pattern in helsinki_patterns:
        if pattern.search(text):
            text = pattern.sub("", text, count=1).strip()

    details["multiple_helsinki"] = (
        "Helsinki" in text or "Helsingin" in text or "Helsingfors" in text or "Helsingie" in text
    )
    return text, details


def to_project_relative(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def main() -> None:
    started_at_now = now_iso()

    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(f"Settings file not found: {SETTINGS_PATH}")

    validate_json_file(SETTINGS_PATH)
    raw_settings_payload = load_json(SETTINGS_PATH)
    settings = validate_settings(raw_settings_payload)

    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()
    source_summary_file = resolve_path_from_root(
        PROJECT_ROOT,
        f"app/output/pipeline_runs/{source_run_id}/transcript_batch_monitor.json",
    )

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    output_file = run_output_dir / "preprocess_structure.json"
    records_file = run_output_dir / "preprocess_structure.records.jsonl"
    preprocessed_jsonl_file = run_output_dir / "preprocess_structure.jsonl"

    if not source_summary_file.exists():
        raise FileNotFoundError(f"Step-3 summary file not found: {source_summary_file}")
    step3_summary = load_json(source_summary_file)
    responses_folder = str(step3_summary.get("data", {}).get("responses_folder", "")).strip()
    if not responses_folder:
        raise ValueError("Missing responses folder in step-3 summary data.responses_folder")
    responses_root = resolve_path_from_root(PROJECT_ROOT, responses_folder)
    if not responses_root.exists() or not responses_root.is_dir():
        raise FileNotFoundError(f"Responses folder not found: {responses_root}")

    prediction_files = sorted(responses_root.rglob("predictions.jsonl"))
    if not prediction_files:
        raise FileNotFoundError(f"No predictions.jsonl files found under {responses_root}")

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
            "starting a new processing pass."
        )

    records_events = load_jsonl_rows(records_file)

    def build_counts() -> dict[str, int]:
        process_events = [
            event for event in records_events if str(event.get("event", "")) == "process"
        ]
        succeeded = sum(1 for event in process_events if event.get("status") == "success")
        failed = sum(1 for event in process_events if event.get("status") == "failed")
        return {
            "prediction_files": len(prediction_files),
            "processed_rows": len(process_events),
            "succeeded_rows": succeeded,
            "failed_rows": failed,
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
        },
        "data": {
            "counts": build_counts(),
            "records_logged": len(records_events),
            "responses_folder": to_project_relative(responses_root),
            "prediction_files": [to_project_relative(path) for path in prediction_files],
            "output_jsonl": to_project_relative(preprocessed_jsonl_file),
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
    install_termination_handlers()

    try:
        preprocessed_jsonl_file.parent.mkdir(parents=True, exist_ok=True)
        with preprocessed_jsonl_file.open("w", encoding="utf-8") as preprocessed_file_handle:
            for prediction_file in prediction_files:
                rows = load_jsonl_rows(prediction_file)
                for row_index, payload in enumerate(rows, start=1):
                    document_long_id = str(payload.get("document_long_id", "")).strip()
                    qname = extract_qname(payload)
                    transcript = extract_transcript(payload)

                    base_event = {
                        "event": "process",
                        "source_prediction_file": to_project_relative(prediction_file),
                        "source_row_index": row_index,
                        "document_long_id": document_long_id or None,
                        "qname": qname or None,
                    }

                    if not transcript:
                        error_message = "Missing transcript text in response.candidates[0].content.parts[].text"
                        event = dict(base_event)
                        event["status"] = "failed"
                        event["error"] = error_message
                        add_event(event)
                        log(
                            "Preprocess failed: "
                            f"{error_message} | file={prediction_file} row={row_index}"
                        )
                        continue

                    preprocessed_transcript, preprocess_details = preprocess_transcript(transcript)
                    output_row = {
                        "document_long_id": document_long_id or None,
                        "qname": qname or None,
                        "source_prediction_file": to_project_relative(prediction_file),
                        "source_row_index": row_index,
                        "data": {
                            "preprocessed_transcript": preprocessed_transcript,
                            "preprocess_details": preprocess_details,
                        },
                    }
                    preprocessed_file_handle.write(json.dumps(output_row, ensure_ascii=False))
                    preprocessed_file_handle.write("\n")

                    event = dict(base_event)
                    event["status"] = "success"
                    add_event(event)

        persist_run(RUN_STATUS_FINISHED, finished=True)
        final_counts = build_counts()
        log(f"Prediction files scanned: {final_counts['prediction_files']}")
        log(f"Preprocessed rows: {final_counts['succeeded_rows']}")
        log(f"Failed rows: {final_counts['failed_rows']}")
        log(f"Wrote preprocessed JSONL: {preprocessed_jsonl_file}")
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
