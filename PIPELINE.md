# Pipeline Overview

This specimen digitization pipeline has four steps/scripts in `app/pipeline`. Each step is settings-driven and writes durable run artifacts to `app/output/pipeline_runs/<run_id>/`.

## Todo

- See todo's below
- Add sample settings files to Git
- Harmonize and clarify pipeline artifact naming and contracts
- Make it easier to create settings: not all should require a source_run_id to be set manually
- Overall pipeline management, with Luigi?
- Clean up scripts; keep report scripts

## Step Contracts

### Step 0: Download images from FinBIF API

- Downloads the first available `IMAGE` media belonging to each FinBIF document.
- **Settings:** `app/pipeline/settings/download_images_settings.json`
- **Input contract:** user provides a `search_url` in the settings (a `laji.fi/observation/images?...` URL).
- **How filtering works:** Step 0 parses query parameters from `search_url` and uses them in FinBIF `/warehouse/query/unit/list` to discover matching `document.documentId` values.
- **On-disk output contract:**
  - per document output folder: `<image_root>/<last_char_of_document_qname>/<qname>/`
  - `document.json` is saved into that folder (full FinBIF document JSON payload)
  - one downloaded image is saved into that folder as a `.jpg`
- **Record status model:** `downloaded`, `skipped`, `failed`

### Step 1: Upload images to Google Cloud Storage (GCS) - `upload_images.py`

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

### Step 2: Create Vertex Gemini batch job - `transcript_batch.py`

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

### Step 3: Monitor Vertex Gemini batch job and download responses - `transcript_batch_monitor.py`

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

### Step 4: Generate transcript report for reviewing transcriptions - `transcript_report.py`

- Reads step-3 summary output, loads raw batch prediction rows from downloaded `predictions.jsonl` files, and produces a human-readable transcript report.
- **Settings:** `app/pipeline/settings/transcript_report_settings.json`
- **Input contract:** step-3 summary from `source_run_id` (`transcript_batch_monitor.json`) with `data.responses_folder`
- **Response parsing:** transcript text from `response.candidates[].content.parts[].text`; specimen id derived from `request.contents[].parts[].fileData.fileUri`
- **Output contract:**
  - HTML report: `transcript_report.html` (table with columns `specimen id` and `transcript`, sorted by specimen id)

### Step 5: Preprocess transcriptions by extracting data using regular algorithms - `preprocess_structure.py`

- Reads downloaded batch prediction rows from step-3 output, extracts transcript text, and applies deterministic regex-based preprocessing/cleanup before structurization.
- **Settings:** `app/pipeline/settings/preprocess_structure_settings.json`
- **Input contract:** step-3 summary from `source_run_id` (`transcript_batch_monitor.json`) with `data.responses_folder`; expects one or more `predictions.jsonl` files under that folder.
- **Preprocess behavior:** keeps source metadata (`document_long_id`, `qname`, source file/index), flags multi-specimen transcripts, and extracts/removes known label patterns (for example QUADR., Luomus http-id + digitization date, Helsinki herbarium stamps/H-number).
- **Output contract:**
  - log file: `preprocess_structure.json`
  - process event log: `preprocess_structure.records.jsonl`
  - preprocessed rows for next step: `preprocess_structure.jsonl`
- **Record status model:** `success`, `failed` (event: `process`)
- Todo:
  - Compare that H-number and http-uri match with the original data
  - Manually check duplicate http-uri specimens?

### Step 6: Submit data to the Vertex Gemini batch job for structurization - `structured_output_batch.py`

- Reads step-5 output (`preprocess_structure.json`) and resolves the preprocessed transcript JSONL from `data.output_jsonl`.
- Keeps one latest record per specimen key, builds a text to structured-data batch request JSONL, uploads it, and creates a Vertex batch job.
- **Settings:** `app/pipeline/settings/structured_output_settings.json`
- **Input contract:** step-5 summary from `source_run_id` (default `preprocess_structure.json`) with `data.output_jsonl` pointing to step-5 rows.
- **Selection rule:** only records where `data.preprocessed_transcript` exists and is non-empty.
- **Batch I/O:**
  - input JSONL uploaded to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/structured_requests.jsonl`
  - output prefix set to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/structured_output`
- **Output contract:**
  - log file: `structured_output_batch.json` (includes `data.batch_job`)
  - structured-output jobs submitted: `structured_output_batch.records.jsonl`
  - local batch payload copy: `structured_output_batch.input.jsonl`
- **Record status model:** `eligible`, `queued`, `skipped`, `failed`

### Step 7: Monitor Vertex Gemini batch job and download responses - `structured_output_batch_monitor.py`

- Reads batch metadata from step-6 summary, polls until a terminal state, and downloads raw structured-output responses when the job succeeds.
- **Settings:** `app/pipeline/settings/structured_output_batch_monitor_settings.json`
- **Input contract:** step-6 summary with `data.batch_job.name` and `settings.batch_output_uri_prefix`
- **Polling behavior:** poll every `--poll-seconds` until terminal state or `--timeout-hours`
- **Download behavior:** on success, download all blobs under batch output prefix to `structured_output_batch_responses/`
- **Output contract:**
  - log file: `structured_output_batch_monitor.json`
  - polling status log: `structured_output_batch_monitor.records.jsonl`
  - raw structured-output response files: `structured_output_batch_responses/<vertex-output-subpath>/...` (for example `prediction-*/predictions.jsonl`)
- **Event model in records:** `poll`, `download`, `timeout`

### Step 8: Do quality control analysis and report

- To be done later.

### Step 9: Export data to Kotka format

- To be done later.

### Step 10: Clean up
- Todo:
  - Delete uploaded images from GCS
  - Delete uploaded prompt data from GCS
  - Delete batch job responses from GCS


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
  - `python3 app/pipeline/download_images.py [--limit N]`
  - `python3 app/pipeline/transcript_batch.py [--limit N]`
  - `python3 app/pipeline/transcript_batch_monitor.py [--poll-seconds S] [--timeout-hours H]`
- `python3 app/pipeline/transcript_report.py`
