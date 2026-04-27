"""Load and merge pipeline-wide and step-specific settings.

The pipeline uses two layers of settings files:

1. `app/pipeline/settings/pipeline_settings.json` holds run-level orchestration
   (the target `run_id`, optional per-step `source_run_ids` overrides for
   branching, and shared infra like GCS bucket/location/prefix).
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

    The returned settings dict has the same shape step scripts expect today:
    `run_id`, `source_run_id`, shared GCS fields, and whatever step-specific
    keys the step file defines. `source_run_id` resolves from
    `pipeline.source_run_ids[step_name]` when present, otherwise falls back
    to `pipeline.run_id` so straight-through runs need no overrides.
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

    run_id = str(pipeline_settings.get("run_id", "")).strip()
    if not run_id:
        raise ValueError(
            "Missing pipeline_settings.settings.run_id in "
            f"{PIPELINE_SETTINGS_PATH}"
        )

    source_run_ids = pipeline_settings.get("source_run_ids", {}) or {}
    if not isinstance(source_run_ids, dict):
        raise ValueError(
            "pipeline_settings.settings.source_run_ids must be an object if present."
        )
    source_run_id = str(source_run_ids.get(step_name, run_id)).strip()
    if not source_run_id:
        raise ValueError(
            f"Empty source_run_id resolved for step '{step_name}'. Check "
            f"pipeline_settings.source_run_ids.{step_name} or run_id."
        )

    merged: dict[str, Any] = {
        "run_id": run_id,
        "source_run_id": source_run_id,
    }
    for shared_key in SHARED_KEYS:
        if shared_key in pipeline_settings:
            merged[shared_key] = pipeline_settings[shared_key]

    # Step-specific keys take effect on top of shared ones, but the pipeline
    # file is the single source of truth for run_id / source_run_id.
    for key, value in step_settings.items():
        if key in ("run_id", "source_run_id"):
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
