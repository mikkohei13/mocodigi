# Pipeline Overview

This specimen digitization pipeline has three steps/scripts in `app/pipeline`. Each step is settings-driven and writes durable run artifacts to `app/output/pipeline_runs/<run_id>/`.

## Step Contracts

### Step 1: `upload_images.py`

- Scans specimen folders, and for each, validates minimum inputs (`document.json` + at least one `.jpg`), and uploads the selected image to GCS.
- **Settings:** `app/pipeline/settings/upload_images_settings.json`
- **Input contract:** specimen folders from `settings.input_dir`
  - per folder: `document.json` with `document.documentId`
  - first sorted `.jpg` is used as the source image
- **Output contract:**
  - log file: `upload_images.json`
  - images uploaded: `upload_images.records.jsonl`
  - uploaded URI shape: `gs://<bucket>/<prefix>/<qname>/<image_name>`
- **Record status model:** `uploaded`, `skipped`, `failed`

### Step 2: `transcript_batch.py`

- Reads step-1 records, keeps only uploaded entries with valid `gs://` URIs, builds a image to text transcription batch request JSONL, uploads it, and creates a Vertex batch job.
- **Settings:** `app/pipeline/settings/transcribe_batch_settings.json`
- **Input contract:** step-1 records from `source_run_id` (default `upload_images.records.jsonl`)
- **Selection rule:** only records where `status == "uploaded"` and `gcs_uri` is valid
- **Batch I/O:**
  - input JSONL uploaded to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/requests.jsonl`
  - output prefix set to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/output`
- **Output contract:**
  - log file: `transcript_batch.json` (includes `data.batch_job`)
  - transcription jobs submitted: `transcript_batch.records.jsonl`
- **Record status model:** `eligible`, `queued`, `skipped`, `failed`

### Step 3: `transcript_batch_monitor.py`

- Reads batch metadata from step-2 summary, polls until a terminal state, and downloads raw batch transcriptions when the job succeeds.
- **Settings:** `app/pipeline/settings/transcribe_batch_monitor_settings.json`
- **Input contract:** step-2 summary with `data.batch_job.name` and `settings.batch_output_uri_prefix`
- **Polling behavior:** poll every `--poll-seconds` until terminal state or `--timeout-hours`
- **Download behavior:** on success, download all blobs under batch output prefix to `transcript_batch_responses/`
- **Output contract:**
  - log file: `transcript_batch_monitor.json`
  - polling status log: `transcript_batch_monitor.records.jsonl`
  - raw transcription response files: `transcript_batch_responses/<vertex-output-subpath>/...` (for example `prediction-*/predictions.jsonl`)
- **Event model in records:** `poll`, `download`, `timeout`

## Run State and Persistence Model

- Common run statuses (shared runtime contract): `running`, `finished`, `partial`, `failed`, `terminated`.
- Every step persists `running` state before heavy work, then continuously appends JSONL records.
- Summary JSON is periodically refreshed with counters and last update timestamps.
- If summary already has `finished`, the step is treated as complete and not rerun.
- Partial execution is explicit (`--limit` for steps 1/2, timeout path in step 3) and persisted as `partial`.

## Design Principles

- **Deterministic processing:** sorted folder/record iteration keeps runs stable and reproducible.
- **Append-only audit trail:** item/event history is never rewritten; JSONL captures observable decisions and errors.
- **Crash/interrupt resilience:** signal handling maps SIGINT/SIGTERM to `terminated` with persisted state.
- **Loose coupling between steps:** each step depends only on prior step artifacts (summary + records), not in-memory state.
- **Strict startup validation:** required settings and key dependencies are verified before expensive remote operations.
- **Operational clarity over abstraction:** simple scripts with explicit contracts, logs, and persisted counters.

## Minimal Runtime Requirements

- Environment:
  - `GOOGLE_CLOUD_PROJECT`
  - `GOOGLE_CLOUD_LOCATION` (required for steps 2 and 3)
  - ADC via `GOOGLE_APPLICATION_CREDENTIALS` (or equivalent default auth)
- Commands:
  - `python3 app/pipeline/upload_images.py [--limit N]`
  - `python3 app/pipeline/transcript_batch.py [--limit N]`
  - `python3 app/pipeline/transcript_batch_monitor.py [--poll-seconds S] [--timeout-hours H]`
