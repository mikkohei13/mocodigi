# Specimen Pipeline (GCS Upload Step)

This pipeline step reads specimen folders from a configured input directory, extracts a specimen identifier (`qname`) from each folder's `document.json` (`document.documentId`), selects one JPG image, and uploads that image to Google Cloud Storage.

## How It Works

1. Load settings from `app/pipeline/settings/specimen_pipeline_settings.json`.
2. Discover specimen folders under `settings.input_dir`.
3. For each pending folder:
   - Validate `document.json` and JPG presence.
   - Extract `qname` from `document.documentId`.
   - Build target GCS object path as `<gcs_prefix>/<qname>/<image_name>`.
   - Upload to `gs://<gcs_bucket>/...`.
4. Persist run status and per-folder records for resumable execution.

## Run Lifecycle and Resume

- `run_id` identifies one logical run.
- If a summary file for the same `run_id` exists with status `finished`, the script does not start.
- If status is `running`, `partial`, `failed`, or `terminated`, the script resumes.
- Already successful folders (`status == uploaded`) are skipped on resume.
- With `--limit`, the script processes only part of pending folders and ends with `partial` if work remains.

## Artifacts Used (Inputs)

- **Settings JSON**: `app/pipeline/settings/specimen_pipeline_settings.json`
  - Required keys: `run_id`, `input_dir`, `gcs_bucket`, `gcs_location`, `gcs_prefix`
- **Specimen folder contents**:
  - `document.json` containing `document.documentId`
  - One or more `.jpg` files (first sorted JPG is selected)
- **Environment/config**:
  - `GOOGLE_CLOUD_PROJECT`
  - Google ADC credentials via standard environment resolution:
    - `GOOGLE_APPLICATION_CREDENTIALS`
    - `GOOGLE_CREDENTIALS_PATH`

## Artifacts Created (Outputs)

- **Run summary JSON**: `app/output/pipeline_runs/<run_id>.json`
  - Run metadata, current status, timestamps, error (if any), aggregate counts, and paths
- **Per-specimen JSONL records**: `app/output/pipeline_runs/<run_id>.records.jsonl`
  - One append-only JSON record per processed folder event
  - Used as the main detailed execution log and resume source

## Run Status Values

- `running`: run is in progress
- `partial`: run ended intentionally before all pending folders were processed (typically due to `--limit`)
- `finished`: run completed successfully for all pending folders
- `failed`: run aborted due to an unhandled exception
- `terminated`: run interrupted by termination signal (e.g. SIGINT/SIGTERM)
