# Specimen Pipeline

Technical reference for pipeline steps in `app/pipeline`.

## Current Steps

### Step 1: Upload specimen image to GCS

- Script: `app/pipeline/specimen_pipeline.py`
- Settings: `app/pipeline/settings/specimen_pipeline_settings.json`
- Input:
  - `settings.input_dir` specimen folders
  - each folder uses `document.json` (`document.documentId`) + first sorted `.jpg`
- Output:
  - uploads image to `gs://<gcs_bucket>/<gcs_prefix>/<qname>/<image_name>`
  - writes run artifacts:
    - `app/output/pipeline_runs/<run_id>.json`
    - `app/output/pipeline_runs/<run_id>.records.jsonl`

### Step 2: Submit transcription batch job to Gemini (Vertex)

- Script: `app/pipeline/transcript_batch.py`
- Settings: `app/pipeline/settings/transcribe_batch_settings.json`
- Input:
  - step-1 records: `app/output/pipeline_runs/<source_run_id>.records.jsonl`
  - uses records with `status == "uploaded"` and valid `gcs_uri`
  - environment:
    - `GOOGLE_CLOUD_PROJECT`
    - `GOOGLE_CLOUD_LOCATION`
    - ADC via `GOOGLE_APPLICATION_CREDENTIALS` / `GOOGLE_CREDENTIALS_PATH`
- Behavior:
  - builds Gemini batch request JSONL from uploaded image `gcs_uri` values
  - uploads batch input to
    - `gs://<gcs_bucket>/<gcs_prefix>/batch_jobs/<run_id>/requests.jsonl`
  - submits Vertex batch job (model from settings)
  - configures batch output prefix:
    - `gs://<gcs_bucket>/<gcs_prefix>/batch_jobs/<run_id>/output`
- Output:
  - writes run artifacts:
    - `app/output/pipeline_runs/<run_id>.transcript_batch.json`
    - `app/output/pipeline_runs/<run_id>.transcript_batch.records.jsonl`
  - stores submitted batch job metadata in summary (`data.batch_job`)

### Step 3: Monitor batch job and download raw output

- Script: `app/pipeline/transcript_batch_monitor.py`
- Settings: `app/pipeline/settings/transcribe_batch_monitor_settings.json`
- Input:
  - step-2 summary: `app/output/pipeline_runs/<source_run_id>.transcript_batch.json`
  - uses `data.batch_job.name` and `settings.batch_output_uri_prefix`
  - environment:
    - `GOOGLE_CLOUD_PROJECT`
    - `GOOGLE_CLOUD_LOCATION`
    - ADC via `GOOGLE_APPLICATION_CREDENTIALS` / `GOOGLE_CREDENTIALS_PATH`
- Behavior:
  - polls Vertex batch job with fixed interval (`--poll-seconds`, default 300)
  - timeout via `--timeout-hours` (default 24); timeout ends run as `partial`
  - when job succeeds, downloads raw output blobs under output prefix to local directory
  - local destination root:
    - `<settings.download_dir>/<run_id>.transcript_batch_monitor.raw/`
- Output:
  - writes run artifacts:
    - `app/output/pipeline_runs/<run_id>.transcript_batch_monitor.json`
    - `app/output/pipeline_runs/<run_id>.transcript_batch_monitor.records.jsonl`

## Run Commands

- Required environment:
  - `GOOGLE_CLOUD_PROJECT`
  - `GOOGLE_CLOUD_LOCATION`
  - `GOOGLE_APPLICATION_CREDENTIALS` (or equivalent ADC setup)
- Step 1:
  - `python3 app/pipeline/specimen_pipeline.py`
  - optional partial run: `python3 app/pipeline/specimen_pipeline.py --limit 100`
- Step 2:
  - `python3 app/pipeline/transcript_batch.py`
  - optional partial run: `python3 app/pipeline/transcript_batch.py --limit 100`
- Step 3:
  - `python3 app/pipeline/transcript_batch_monitor.py`
  - custom timeout: `python3 app/pipeline/transcript_batch_monitor.py --timeout-hours 24`
  - custom poll interval: `python3 app/pipeline/transcript_batch_monitor.py --poll-seconds 300`

## Shared Patterns (All Steps)

- Settings-driven execution:
  - one settings file per step in `app/pipeline/settings`
  - explicit required keys validated at startup
- Standard run lifecycle:
  - `running`, `partial`, `finished`, `failed`, `terminated`
- Resume behavior:
  - if summary status is `finished`, do not run again
  - otherwise resume using existing summary/records where applicable
- Deterministic processing:
  - stable ordering (`sorted`) for folders/records
  - `--limit` supported for partial controlled runs
- State and audit artifacts:
  - summary JSON (run-level status + counts + settings snapshot)
  - append-only JSONL records (item-level events and errors)
- Logging:
  - timestamped log lines via shared style (`[ISO_TIMESTAMP] message`)
- Failure handling:
  - termination signals mapped to `terminated`
  - unhandled exceptions mapped to `failed` with persisted error message

## Step Implementation Rules (for new steps)

- Keep the same run/status contract and output folder: `app/output/pipeline_runs/`.
- Persist early (`running`) before heavy work starts.
- Write item-level records incrementally; avoid in-memory-only state.
- Include enough metadata in summary for the next step to continue without recomputation.
- Do not overwrite previous step artifacts; create step-specific output files.
