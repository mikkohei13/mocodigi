---
name: Specimen Pipeline Options
overview: Refine the pipeline plan to explicitly use stored JSON config files for step parameters, while keeping manual-first execution and machine-readable logging for reproducible reruns.
todos:
  - id: define-json-config-contract
    content: Define and validate JSON config schema for input/transform/output per step.
    status: pending
  - id: define-run-lineage
    content: Add run lineage fields (input_run_id, output_run_id, parent_run_id) for branching and replay.
    status: pending
  - id: define-run-manifest
    content: Design JSONL manifest and state transitions for async batch submit/collect lifecycle.
    status: pending
  - id: define-jsonl-logs
    content: Define immutable JSONL run log record format with resolved params and provider metadata.
    status: pending
  - id: manual-first-execution
    content: Implement manual step triggers that consume only stored JSON configs.
    status: pending
  - id: prepare-option2-wrapper
    content: Keep interfaces stable so a lightweight workflow library can wrap the same stages later.
    status: pending
isProject: false
---

# Pipeline Options for Async Gemini Workflow

## Scope

- Focus on two implementation paths:
  - Option 1: Custom orchestrator code
  - Option 2: Same custom core + lightweight open-source workflow library
- Keep approach simple and iterative; avoid premature optimization.

## Baseline in Current Repo

- Existing stages and helpers already separate concerns:
  - Transcription script: [/Users/mikko/Documents/code/mocodigi/app/scripts/digitize.py](/Users/mikko/Documents/code/mocodigi/app/scripts/digitize.py)
  - Pre-structure/local transform script: [/Users/mikko/Documents/code/mocodigi/app/scripts/pre_structure.py](/Users/mikko/Documents/code/mocodigi/app/scripts/pre_structure.py)
  - Cache helpers: [/Users/mikko/Documents/code/mocodigi/app/utils/cache_utils.py](/Users/mikko/Documents/code/mocodigi/app/utils/cache_utils.py)
  - Gemini helpers: [/Users/mikko/Documents/code/mocodigi/app/utils/gemini_utils.py](/Users/mikko/Documents/code/mocodigi/app/utils/gemini_utils.py)

## Required Parameter Model

- Every step has persisted editable parameters with three blocks:
  - `input` (folder traversal scope + `input_run_id`)
  - `transform` (model, temperature, prompt/system message, and optional named local transform functions)
  - `output` (`output_run_id`)
- `input_run_id` and `output_run_id` stay independent so steps can branch/fork.

## Small Update: JSON Config Files

- Add explicit JSON config files as the canonical source of run intent.
- Keep configs in a dedicated directory (example: `app/config/pipeline/`).
- Use two JSON layers:
  - `profile.json` for shared defaults and environment-level settings
  - One JSON file per step (or one pipeline JSON with per-step sections)
- Prefer schema-validated JSON to keep edits safe and machine-checkable.
- Example config shape:

```json
{
  "step": "transcription",
  "input": {
    "root_folder": "images-solanaceae-trial",
    "input_run_id": "h1"
  },
  "transform": {
    "provider": "gemini",
    "model": "gemini-3.1-pro-preview",
    "temperature": 0.0,
    "thinking_budget": 128,
    "max_output_chars": 1000,
    "system_message": "Your task is to ..."
  },
  "output": {
    "output_run_id": "h2"
  }
}
```

## Logging and Re-run Strategy

- Emit machine-readable execution logs (JSONL) for each run/step.
- Log records should include:
  - step name, specimen key, timestamps, action, status
  - resolved parameter snapshot (or hash + stored snapshot)
  - input and output run IDs
  - provider batch IDs, retry/backoff metadata, errors
- Treat logs as immutable run records.
- Allow “rerun from prior run record” by generating a new config snapshot from logged parameters (optionally edited), while preserving lineage (`parent_run_id`).

## Option 1 (Recommended First): Custom Orchestrator

- Build a thin orchestrator that reads JSON configs and runs one stage manually at a time:
  - submit transcription batch
  - collect transcription results
  - run local preprocess
  - submit structurization batch
  - collect structurization results
- Persist state transitions in a simple manifest (JSONL first).

## Option 2: Custom Core + Open-Source Workflow Wrapper

- Keep the exact same JSON config contract and stage logic.
- Add a light workflow library only for retries, scheduling, and run observability.
- This keeps migration low-risk: orchestration changes, core stage code stays.

## Phase Plan

- Phase 1 (manual trigger):
  - Trigger each step manually using stored JSON configs only.
  - Verify branching by reusing one upstream run as input for multiple downstream output run IDs.
  - Verify logs are sufficient to reconstruct and replay a run.
- Phase 2 (single pipeline trigger):
  - Add one end-to-end trigger that consumes the same JSON configs.
  - Keep manual step entrypoints for debugging and partial reruns.

## Architecture View

```mermaid
flowchart LR
  loadConfig[LoadJsonConfig] --> submitTx[SubmitTranscriptionBatch]
  submitTx --> collectTx[CollectTranscriptionResults]
  collectTx --> localPre[RunLocalPreprocess]
  localPre --> submitStruct[SubmitStructurizeBatch]
  submitStruct --> collectStruct[CollectStructurizeResults]
  collectStruct --> writeLogs[WriteJsonlRunLogs]
  writeLogs --> writeOutputs[WriteRunOutputs]
```



