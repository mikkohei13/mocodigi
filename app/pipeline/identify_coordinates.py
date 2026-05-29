"""Identify coordinates in batch transcription responses.

Step 3C:
- Read step-3 downloaded predictions.jsonl files.
- Extract image identifier and transcript text per row.
- Find the first coordinate match in the transcript (if any).
- Write a tab-separated report (.tsv) and run metadata (identify_coordinates.json).
- If identify_coordinates.json already has run_status finished, exit without re-running.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any, Callable

from utils.files import load_json, load_jsonl_rows, resolve_path as resolve_path_from_root, save_json
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


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SETTINGS_PATH = SCRIPT_DIR / "settings" / "identify_coordinates_settings.json"

# Grid coordinates with a separator (space, hyphen, or colon), optional surrounding spaces.
# Groups 1 and 2 capture northing and easting for the length-difference check.
_GRID_WITH_SEP = re.compile(r"\b(\d{3,7})\s*[-: ]\s*(\d{3,7})\b")

# Grid coordinates without a separator: 5–14 consecutive digits at a word boundary.
_GRID_NO_SEP = re.compile(r"\b\d{5,14}\b")


def _find_finnish_uniform_grid(text: str) -> str:
    # Try separator form first; northing and easting lengths may differ by at most 1.
    for m in _GRID_WITH_SEP.finditer(text):
        if abs(len(m.group(1)) - len(m.group(2))) <= 1:
            return m.group(0)
    # Fall back to no-separator form (5–14 total digits).
    m = _GRID_NO_SEP.search(text)
    return m.group(0) if m else ""


# Ordered list of (name, finder function). First match wins.
COORDINATE_FINDERS: list[tuple[str, Callable[[str], str]]] = [
    ("finnish_uniform_grid", _find_finnish_uniform_grid),
]


def validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
    required = ["run_id", "source_run_id"]
    missing = [key for key in required if settings.get(key) in (None, "")]
    if missing:
        raise ValueError(f"Missing required settings keys: {missing}")
    return settings


def extract_transcript(payload: dict[str, Any]) -> str:
    # If image has no labels, response might be empty. Prepare for that by returning an empty string.
    candidates = payload.get("response", {}).get("candidates")
    if not isinstance(candidates, list) or not candidates:
        print(f"No candidates in payload: {payload}")
        return ""
    return payload["response"]["candidates"][0]["content"]["parts"][0]["text"]


def find_first_coordinate(text: str) -> str:
    for _name, finder in COORDINATE_FINDERS:
        result = finder(text)
        if result:
            return result
    return ""


def process_prediction_row(payload: dict[str, Any]) -> dict[str, str]:
    return {
        "specimen_id": payload["document_long_id"],
        "specimen_qname": payload["qname"],
        "image_filename": payload["image_filename"],
        "matched_coordinates": find_first_coordinate(extract_transcript(payload)),
    }


def to_project_relative(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def main() -> None:
    started_at_now = now_iso()

    merged_settings, _ = load_step_settings("identify_coordinates", SETTINGS_PATH)
    settings = validate_settings(merged_settings)

    run_id = str(settings["run_id"]).strip()
    source_run_id = str(settings["source_run_id"]).strip()

    run_output_dir = resolve_path_from_root(PROJECT_ROOT, f"app/output/pipeline_runs/{run_id}")
    output_file = run_output_dir / "identify_coordinates.json"
    output_tsv = run_output_dir / "identify_coordinates.tsv"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    archive_pipeline_settings(run_output_dir)

    source_summary_file = resolve_path_from_root(
        PROJECT_ROOT,
        f"app/output/pipeline_runs/{source_run_id}/transcript_batch_monitor.json",
    )
    if not source_summary_file.exists():
        raise FileNotFoundError(f"Step-3 summary file not found: {source_summary_file}")

    step3_summary = load_json(source_summary_file)
    responses_folder = str(step3_summary.get("data", {}).get("responses_folder", "")).strip()
    if not responses_folder:
        raise ValueError("Missing responses folder in step-3 summary data.responses_folder")

    responses_root = resolve_path_from_root(PROJECT_ROOT, responses_folder)
    if not responses_root.is_dir():
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
            "source_summary_file": to_project_relative(source_summary_file),
        },
        "data": {
            "responses_folder": to_project_relative(responses_root),
            "prediction_files": [to_project_relative(path) for path in prediction_files],
            "output_tsv": to_project_relative(output_tsv),
            "counts": {
                "prediction_files": len(prediction_files),
                "rows": 0,
                "matched_coordinates": 0,
            },
        },
    }

    def persist_run(status: str, *, error: str | None = None, finished: bool = False) -> None:
        run_output["run_status"] = status
        run_output["error"] = error
        run_output["last_updated_at"] = now_iso()
        run_output["finished_at"] = now_iso() if finished else None
        save_json(output_file, run_output)

    persist_run(RUN_STATUS_RUNNING)
    install_termination_handlers()

    try:
        rows: list[dict[str, str]] = []
        for prediction_file in prediction_files:
            for payload in load_jsonl_rows(prediction_file):
                rows.append(process_prediction_row(payload))

        matched_count = sum(1 for row in rows if row["matched_coordinates"])
        run_output["data"]["counts"] = {
            "prediction_files": len(prediction_files),
            "rows": len(rows),
            "matched_coordinates": matched_count,
        }

        with output_tsv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["specimen_id", "specimen_qname", "image_filename", "matched_coordinates"],
                delimiter="\t",
            )
            writer.writeheader()
            writer.writerows(rows)

        persist_run(RUN_STATUS_FINISHED, finished=True)
        log(f"Source summary file: {source_summary_file}")
        log(f"Responses folder: {responses_root}")
        log(f"Prediction files: {len(prediction_files)}")
        log(f"Rows: {len(rows)} ({matched_count} with coordinates)")
        log(f"Wrote coordinate report: {output_tsv}")
        log(f"Run status: {RUN_STATUS_FINISHED}")
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
