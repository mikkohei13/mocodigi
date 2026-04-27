# Pipeline Overview

This specimen digitization pipeline has multiple steps/scripts in `app/pipeline`. Each step is settings-driven and writes durable run artifacts to `app/output/pipeline_runs/<run_id>/`.

## Quick Start

1. Make sure your `.env` (or shell) sets `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION`, `GOOGLE_APPLICATION_CREDENTIALS`, and `FINBIF_ACCESS_TOKEN`.
2. Edit `app/pipeline/settings/pipeline_settings.json` and pick a `run_id` (this is where all step outputs will be written).
3. Edit any step-specific files in `app/pipeline/settings/` you care about (prompts, model, search URL, schema file, etc.).
4. Run the steps one at a time from the repository root, e.g. `python3 app/pipeline/download_images.py`. See `Minimal Runtime Requirements` at the bottom for the full command list.
5. Watch each step's summary JSON (`app/output/pipeline_runs/<run_id>/<step>.json`) for `run_status: finished` before running the next step.

### Settings layout

The pipeline uses two kinds of settings files:

- **`pipeline_settings.json` (one file, run-level):** `run_id`, optional `source_run_ids`, and shared GCS fields (`gcs_bucket`, `gcs_location`, `gcs_prefix`). This is the only file you touch to start a new run.
- **`<step>_settings.json` (one per step):** step-specific params only (prompts, model, temperature, schema file, search URL, input dir, etc.). These rarely change between runs.

Each step merges the two files at load time. Scripts never read `run_id` or GCS fields from step files.

### Branching a run (reuse expensive prior outputs)

`source_run_ids` is a small map in `pipeline_settings.json` that lets any step read its input from a different past run folder instead of the current `run_id`:

```json
{
  "settings": {
    "run_id": "h_lichen_collection-200",
    "source_run_ids": {
      "structured_output_batch": "h_lichen_collection"
    }
  }
}
```

With this config, `structured_output_batch.py` reads its step-5 input from `app/output/pipeline_runs/h_lichen_collection/` (the completed transcription run) but writes its own output into `app/output/pipeline_runs/h_lichen_collection-200/`. Any step not listed in `source_run_ids` defaults its `source_run_id` to the current `run_id`, so plain straight-through runs need no overrides.

The step keys used in `source_run_ids` match the script base names: `download_images`, `upload_images`, `transcript_batch`, `transcript_batch_monitor`, `transcript_report`, `preprocess_structure`, `structured_output_batch`, `structured_output_batch_monitor`, `structured_output_report`.

### Archival

Every step copies `pipeline_settings.json` into its run folder on first write, and stores the effective merged settings inside each step's summary JSON. Both files in `app/pipeline/settings/` and the per-run folder under `app/output/pipeline_runs/<run_id>/` form the permanent audit trail.

## Todo

- See todo's below
- Add sample settings files to Git
- Harmonize and clarify pipeline artifact naming and contracts
- Overall pipeline management, with Luigi? (decided against; see design notes)
- Clean up scripts; keep report scripts

## Step Contracts

### Step 0: Download images from FinBIF API

- Downloads all available `IMAGE` media belonging to each FinBIF document.
- **Settings:** `app/pipeline/settings/download_images_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** user provides a `search_url` in the settings (a `laji.fi/observation/images?...` URL).
- **How filtering works:** Step 0 parses query parameters from `search_url` and uses them in FinBIF `/warehouse/query/unit/list` to discover matching `document.documentId` values.
- **On-disk output contract:**
  - per document output folder: `<image_root>/<last_char_of_document_qname>/<qname>/`
  - `document.json` is saved into that folder (full FinBIF document JSON payload)
  - downloaded images are saved as `.jpg` files; first keeps base filename, subsequent images use suffixes (`_2`, `_3`, ...)
- **Record fields:** `document_id_long`, `qname`, `last_char`, `specimen_folder`, `selected_image_filename` (first image), `selected_image_full_url`, `downloaded_image_filenames` (list), `downloaded_image_full_urls` (list), `skipped_existing_image_filenames` (list)
- **Summary data:** includes `data.documents_discovered` counter
- **Record status model:** `downloaded` (at least one new image saved), `skipped` (no images or all already existed), `failed`

### Step 1: Upload images to Google Cloud Storage (GCS) - `upload_images.py`

- Scans specimen folders, and for each, validates minimum inputs (`document.json` + at least one `.jpg`), and uploads every `.jpg` image to GCS.
- **Settings:** `app/pipeline/settings/upload_images_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** specimen folders from `settings.input_dir`
  - per folder: `document.json` with `document.documentId`
  - all sorted `.jpg` files are used as source images
  - supports both flat (`<root>/<qname>/`) and nested (`<root>/<lastChar>/<qname>/`) layouts
- **Output contract:**
  - log file: `upload_images.json`
  - uploaded image records: `upload_images.records.jsonl` (one row per image; `--limit` applies to specimen folders, not images)
  - uploaded URI shape: `gs://<bucket>/<prefix>/<qname>/<image_name>`
- **Record fields:** `specimen_folder`, `document_id_long`, `qname`, `jpg_count_in_folder`, `image_filename`, `image_index`, `image_stem`, `local_image_path`, `gcs_uri`; `selected_image` is a backwards-compatible alias for `image_filename`
- **Record status model:** `uploaded`, `skipped`, `failed`

### Step 2: Create Vertex Gemini batch job - `transcript_batch.py`

- Reads step-1 records, keeps only uploaded entries with valid `gs://` URIs, builds image-to-text transcription batch request JSONL (one request per image), uploads it, and creates a Vertex batch job.
- **Settings:** `app/pipeline/settings/transcribe_batch_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** step-1 records from the resolved `source_run_id` folder (default `upload_images.records.jsonl`)
- **Selection rule:** only records where `status == "uploaded"` and `gcs_uri` is valid
- **Identity propagation:** image-level metadata (`image_filename`, `image_index`, specimen fields) is carried in request rows.
- **IAM preflight:** verifies the Vertex service agent has read access to the input bucket(s) and write access to the output bucket before submitting the job.
- **Batch I/O:**
  - input JSONL uploaded to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/requests.jsonl`
  - output prefix set to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/output`
- **Output contract:**
  - log file: `transcript_batch.json` (includes `data.batch_job`)
  - transcription jobs submitted: `transcript_batch.records.jsonl`
  - local batch payload copy: `transcript_batch.input.jsonl`
- **Record status model:** `eligible`, `queued`, `skipped`, `failed`

### Step 3: Monitor Vertex Gemini batch job and download responses - `transcript_batch_monitor.py`

- Reads batch metadata from step-2 summary, polls until a terminal state, and downloads raw batch transcriptions when the job succeeds.
- **Settings:** `app/pipeline/settings/transcribe_batch_monitor_settings.json` (+ `pipeline_settings.json`)
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
- **Settings:** `app/pipeline/settings/transcript_report_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** step-3 summary (`transcript_batch_monitor.json` in the resolved `source_run_id` folder) with `data.responses_folder`
- **Response parsing:** transcript text from `response.candidates[].content.parts[].text`; specimen id derived from `request.contents[].parts[].fileData.fileUri`
- **Output contract:**
  - HTML report: `transcript_report.html` (table with columns `specimen id`, `image`, and `transcript`, sorted by specimen id)

### Step 5: Preprocess transcriptions by extracting data using regular algorithms - `preprocess_structure.py`

- Reads downloaded batch prediction rows from step-3 output, extracts transcript text, and applies deterministic regex-based preprocessing/cleanup before structurization.
- **Settings:** `app/pipeline/settings/preprocess_structure_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** step-3 summary (`transcript_batch_monitor.json` in the resolved `source_run_id` folder) with `data.responses_folder`; expects one or more `predictions.jsonl` files under that folder.
- **Preprocess behavior:** keeps source metadata (`document_long_id`, `qname`, `image_filename`, `image_index`, source file/index), flags multi-specimen transcripts, and extracts/removes known label patterns (for example QUADR., Luomus http-id + digitization date, Helsinki herbarium stamps/H-number).
- **`preprocess_details` flags:** `multiple_specimens`, `remove_quadr`, `http_uri`, `digitization_date`, `missing_http_uri`, `h_number`, `missing_h_number`, `multiple_helsinki`
- **Output contract:**
  - log file: `preprocess_structure.json` (includes `data.output_jsonl`, `data.prediction_files`)
  - process event log: `preprocess_structure.records.jsonl`
  - preprocessed rows for next step: `preprocess_structure.jsonl`
- **Record status model:** `success`, `failed` (event: `process`)
- Todo:
  - Compare that H-number and http-uri match with the original data
  - Manually check duplicate http-uri specimens?

### Step 6: Submit data to the Vertex Gemini batch job for structurization - `structured_output_batch.py`

- Reads step-5 output (`preprocess_structure.json`) and resolves the preprocessed transcript JSONL from `data.output_jsonl`.
- Groups all step-5 rows by specimen key (`qname` preferred, then `document_long_id`) and builds one text-to-structured-data batch request per specimen by aggregating all image-level preprocessed transcripts in deterministic order (sorted by `image_index`, then filename, then source row index).
- Each request includes the aggregated transcript as labeled sections (`[Image N: <filename> (index=N)]`) followed by the transcript text.
- **Settings:** `app/pipeline/settings/structured_output_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** step-5 summary (`preprocess_structure.json` in the resolved `source_run_id` folder) with `data.output_jsonl` pointing to step-5 rows.
- **Selection rule:** only grouped specimens with at least one non-empty `data.preprocessed_transcript` across their image rows.
- **IAM preflight:** verifies the Vertex service agent has read access to the input bucket and write access to the output bucket before submitting the job.
- **Response schema:** loaded from the JSON Schema file referenced by `settings.schema_file` (default `app/pipeline/schemas/herbarium_specimen.schema.json`) and passed to Gemini as `generationConfig.responseSchema`. Swap specimen types (e.g. pinned insects) by pointing `schema_file` at a different JSON Schema.
- **Batch I/O:**
  - input JSONL uploaded to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/structured_requests.jsonl`
  - output prefix set to `gs://<bucket>/<prefix>/batch_jobs/<run_id>/structured_output`
- **Output contract:**
  - log file: `structured_output_batch.json` (includes `data.batch_job`)
  - structured-output job records: `structured_output_batch.records.jsonl` (one row per specimen; includes `image_count`, `source_images`, `source_row_count`)
  - local batch payload copy: `structured_output_batch.input.jsonl`
- **Record status model:** `eligible`, `queued`, `skipped`, `failed`

### Step 7: Monitor Vertex Gemini batch job and download responses - `structured_output_batch_monitor.py`

- Reads batch metadata from step-6 summary, polls until a terminal state, and downloads raw structured-output responses when the job succeeds.
- **Settings:** `app/pipeline/settings/structured_output_batch_monitor_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** step-6 summary with `data.batch_job.name` and `settings.batch_output_uri_prefix`
- **Polling behavior:** poll every `--poll-seconds` until terminal state or `--timeout-hours`
- **Download behavior:** on success, download all blobs under batch output prefix to `structured_output_batch_responses/`
- **Output contract:**
  - log file: `structured_output_batch_monitor.json`
  - polling status log: `structured_output_batch_monitor.records.jsonl`
  - raw structured-output response files: `structured_output_batch_responses/<vertex-output-subpath>/...` (for example `prediction-*/predictions.jsonl`)
- **Event model in records:** `poll`, `download`, `timeout`

### Step 7B: Generate structured output report for reviewing extraction results - `structured_output_report.py`

- Reads step-7 monitor summary output, loads raw batch prediction rows from downloaded `predictions.jsonl` files, and produces a human-readable HTML report of structured JSON per specimen.
- **Settings:** `app/pipeline/settings/structured_output_report_settings.json` (+ `pipeline_settings.json`)
- **Input contract:** step-7 summary (`structured_output_batch_monitor.json` in the resolved `source_run_id` folder) with `data.responses_folder`
- **Response parsing:** structured data from `response.candidates[].content.parts[].text` (JSON); specimen id from top-level `qname`; image list from `source_images` when present
- **Output contract:**
  - HTML report: `structured_output_report.html` (table with columns `Specimen ID`, `Source images`, and `Structured output` with pretty-printed JSON, sorted by specimen id)

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
  - `GOOGLE_CLOUD_LOCATION` (required for steps 2, 3, 6, and 7)
  - ADC via `GOOGLE_APPLICATION_CREDENTIALS` (or equivalent default auth)
- Commands:
  - `python3 app/pipeline/download_images.py [--limit N]`
  - `python3 app/pipeline/upload_images.py [--limit N]`
  - `python3 app/pipeline/transcript_batch.py [--limit N]`
  - `python3 app/pipeline/transcript_batch_monitor.py [--poll-seconds S] [--timeout-hours H]`
  - `python3 app/pipeline/transcript_report.py`
  - `python3 app/pipeline/preprocess_structure.py`
  - `python3 app/pipeline/structured_output_batch.py [--limit N]`
  - `python3 app/pipeline/structured_output_batch_monitor.py [--poll-seconds S] [--timeout-hours H]`
  - `python3 app/pipeline/structured_output_report.py`
