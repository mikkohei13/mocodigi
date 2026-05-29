"""Load and merge pipeline-wide and step-specific settings.

The pipeline uses two layers of settings files:

1. `app/pipeline/settings/pipeline_settings.json` holds run-level orchestration:
   - `target_run_id`: where this run's outputs are written.
   - `source_run_id` (optional): which prior run folder to read inputs from.
     Omit or leave empty for straight-through runs; defaults to `target_run_id`.
   - Shared infra: `gcs_bucket`, `gcs_location`, `gcs_prefix`.
2. Each step has its own `<step>_settings.json` with step-specific params
   (prompts, model, schema path, etc.).

`load_step_settings` returns a single flat dict that the step scripts consume,
plus the raw pipeline payload (useful for archival). `archive_pipeline_settings`
copies the pipeline config into the run folder on first use so every run
folder is self-describing.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from utils.files import load_json, validate_json_file


PIPELINE_SETTINGS_PATH = (
    Path(__file__).resolve().parent.parent / "settings" / "pipeline_settings.json"
)

SHARED_KEYS = ("gcs_bucket", "gcs_location", "gcs_prefix")


def load_step_settings(
    step_name: str,
    step_settings_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load pipeline_settings.json and merge it with a step settings file.

    The returned dict contains:
    - `target_run_id`: output folder for this run (required, from pipeline settings)
    - `source_run_id`: input folder to read from (defaults to `target_run_id`)
    - shared GCS fields
    - all step-specific keys from the step settings file

    `target_run_id` and `source_run_id` are always sourced from pipeline_settings.json.
    Any `target_run_id` or `source_run_id` keys in step files are silently ignored.
    """
    if not PIPELINE_SETTINGS_PATH.exists():
        raise FileNotFoundError(
            f"Pipeline settings file not found: {PIPELINE_SETTINGS_PATH}"
        )
    validate_json_file(PIPELINE_SETTINGS_PATH)
    pipeline_payload = load_json(PIPELINE_SETTINGS_PATH)
    pipeline_settings = pipeline_payload.get("settings", {}) or {}

    if not step_settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {step_settings_path}")
    validate_json_file(step_settings_path)
    step_payload = load_json(step_settings_path)
    step_settings = step_payload.get("settings", {}) or {}

    target_run_id = str(pipeline_settings.get("target_run_id", "")).strip()
    if not target_run_id:
        raise ValueError(
            "Missing pipeline_settings.settings.target_run_id in "
            f"{PIPELINE_SETTINGS_PATH}"
        )

    source_run_id = str(pipeline_settings.get("source_run_id", "")).strip() or target_run_id

    merged: dict[str, Any] = {
        "target_run_id": target_run_id,
        "source_run_id": source_run_id,
    }
    for shared_key in SHARED_KEYS:
        if shared_key in pipeline_settings:
            merged[shared_key] = pipeline_settings[shared_key]

    # Step-specific keys are merged last; pipeline file is the sole source for
    # target_run_id and source_run_id.
    for key, value in step_settings.items():
        if key in ("target_run_id", "source_run_id"):
            continue
        merged[key] = value

    return merged, pipeline_payload


def archive_pipeline_settings(run_output_dir: Path) -> Path | None:
    """Copy pipeline_settings.json into the run folder if not already present.

    Idempotent and safe to call on every step invocation. Returns the
    destination path when present (new or existing), or None if the source
    file is missing.
    """
    if not PIPELINE_SETTINGS_PATH.exists():
        return None
    run_output_dir.mkdir(parents=True, exist_ok=True)
    destination = run_output_dir / PIPELINE_SETTINGS_PATH.name
    if not destination.exists():
        shutil.copy2(PIPELINE_SETTINGS_PATH, destination)
    return destination
