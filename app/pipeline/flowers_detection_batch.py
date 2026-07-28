"""Flowers Detection - Analyze plant images from Google Cloud Storage with Gemini.

Processes images from GCS bucket, sends them to Gemini for analysis with a custom prompt,
and saves results as JSON.

Usage:
    docker-compose run --rm pipeline python -m app.pipeline.flowers_detection_batch
    or: python app/pipeline/flowers_detection_batch.py

Configuration (edit below):
    BUCKET: GCS bucket name
    PREFIX: Folder prefix in bucket
    OUTPUT_FILE: Output JSON file path
    PROMPT: Question to ask Gemini about each image
    LIMIT: Max images to process (None for all)
    MAX_OUTPUT_TOKENS: Max tokens for model output response (e.g. 10)

Output (results.json):
    [{"image": "path/to/image.jpg", "has_flowers": "yes"}, ...]
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import google.genai as genai
from google.cloud import storage
from google.genai import types

from utils.gcp import parse_gs_uri, resolve_adc_credentials_from_env, upload_file_to_gcs_uri

from dotenv import load_dotenv
load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Configuration
BUCKET = "mocodigi"
PREFIX = "mocodigi-pipeline/"
OUTPUT_FILE = "results.json"
PROMPT = "Does this herbarium specimen image show visible flowers? " \
    "Answer only 'yes' or 'no'. or 'unknown'. " \
    "If you don't know the answer, answer 'unknown'. " \
    "Do nothing else, do not add any extra text or explanation. " \
    "Don't think too much, just answer the question. " 
LIMIT = 200
MAX_OUTPUT_TOKENS = 200
POLL_TIMEOUT_HOURS = 24
MODEL = "gemini-2.5-flash"
DELAY = 60 


def list_gcs_images(bucket_name: str, prefix: str) -> list[dict[str, str]]:
    """List image URIs from GCS bucket."""
    bucket = storage.Client().bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    images = []
    for blob in blobs:
        if blob.name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
            images.append({"name": blob.name, "uri": f"gs://{bucket_name}/{blob.name}"})
    return sorted(images, key=lambda x: x["name"])


def build_batch_request(images: list[dict[str, str]], model: str, prompt: str) -> dict[str, Any]:
    """Build batch request JSON."""
    requests = []
    for img in images:
        generation_config: dict[str, Any] = {"temperature": 0}
        if MAX_OUTPUT_TOKENS is not None:
            generation_config["maxOutputTokens"] = MAX_OUTPUT_TOKENS

        requests.append({
            "image_key": img["name"],
            "request": {
                "contents": [{
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                        {"fileData": {"fileUri": img["uri"], "mimeType": "image/jpeg"}},
                    ],
                }],
                "generationConfig": generation_config,
            }
        })
    return {"requests": requests}


def submit_batch_job(project_id: str, location: str, bucket: str, run_id: str, batch_data: dict) -> str:
    """Submit batch job and return job name."""
    client = genai.Client(vertexai=True, project=project_id, location=location, 
                          http_options=types.HttpOptions(api_version="v1"))
    
    # Upload request file
    batch_input_uri = f"gs://{bucket}/batch_jobs/{run_id}/request.jsonl"
    temp_batch_file = Path(tempfile.gettempdir()) / f"batch_{run_id}.jsonl"
    with open(temp_batch_file, "w") as f:
        for req in batch_data["requests"]:
            f.write(json.dumps(req) + "\n")
    
    storage_client = storage.Client(project=project_id)
    upload_file_to_gcs_uri(
        client=storage_client,
        local_file=temp_batch_file,
        target_uri=batch_input_uri,
    )
    
    # Submit batch
    output_uri = f"gs://{bucket}/batch_jobs/{run_id}/output"
    job = client.batches.create(
        model=MODEL,
        src=batch_input_uri,
        config=types.CreateBatchJobConfig(dest=output_uri),
    )
    return job.name


def poll_and_download(project_id: str, location: str, job_name: str, bucket: str, run_id: str, timeout_hours: float = 24):
    """Poll job and download results when complete."""
    client = genai.Client(vertexai=True, project=project_id, location=location,
                          http_options=types.HttpOptions(api_version="v1"))
    storage_client = storage.Client(project=project_id)
    
    start = time.time()
    timeout = timeout_hours * 3600
    
    while time.time() - start < timeout:
        job = client.batches.get(name=job_name)
        state = str(job.state)
        print(f"Job state: {state}")
        
        if "JOB_STATE_SUCCEEDED" in state:
            # Download results
            output_uri = f"gs://{bucket}/batch_jobs/{run_id}/output"
            bucket_name, prefix = parse_gs_uri(output_uri)
            gcs_bucket = storage_client.bucket(bucket_name)
            blobs = gcs_bucket.list_blobs(prefix=prefix)
            
            results = []
            for blob in blobs:
                if blob.name.endswith('.jsonl'):
                    content = blob.download_as_text()
                    for line in content.strip().split('\n'):
                        if line:
                            results.append(json.loads(line))
            return results
        elif "JOB_STATE_FAILED" in state or "JOB_STATE_CANCELLED" in state:
            raise Exception(f"Job failed with state: {state}")
        
        print(f"Waiting... ({int(time.time() - start)}s elapsed)")
        time.sleep(DELAY)
    
    raise TimeoutError(f"Job did not complete within {timeout_hours} hours")


def cleanup_batch_folder(bucket: str, run_id: str, project_id: str):
    """Delete temporary batch_jobs folder from GCS."""
    try:
        storage_client = storage.Client(project=project_id)
        gcs_bucket = storage_client.bucket(bucket)
        prefix = f"batch_jobs/{run_id}/"
        blobs = gcs_bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            blob.delete()
        print(f"Cleaned up {prefix}")
    except Exception as e:
        print(f"Warning: Could not clean up batch folder: {e}")


def main():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    location = os.getenv("GOOGLE_CLOUD_LOCATION", "europe-west1").strip()
    
    if not project_id:
        raise ValueError("Set GOOGLE_CLOUD_PROJECT env var")
    
    adc = resolve_adc_credentials_from_env(lambda p: p)
    if adc:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(adc)
    
    print(f"Listing images from gs://{BUCKET}/{PREFIX}...")
    images = list_gcs_images(BUCKET, PREFIX)
    if LIMIT:
        images = images[:LIMIT]
    print(f"Found {len(images)} images")
    
    print("Building batch request...")
    batch_data = build_batch_request(images, MODEL, PROMPT)
    
    run_id = str(int(time.time()))
    print(f"Submitting batch job (run_id: {run_id})...")
    job_name = submit_batch_job(project_id, location, BUCKET, run_id, batch_data)
    print(f"Job: {job_name}")
    
    print(f"Polling job (timeout: {POLL_TIMEOUT_HOURS}h)...")
    results = poll_and_download(project_id, location, job_name, BUCKET, run_id, POLL_TIMEOUT_HOURS)
    
    # Parse results and save as JSON
    output = []
    total_prompt_tokens = 0
    total_candidate_tokens = 0
    total_thought_tokens = 0
    total_tokens = 0

    for result in results:
        image_key = result.get("image_key", "unknown")
        answer = "unknown"
        
        # Log error or safety block if present in batch output
        if "error" in result:
            print(f"Warning: {image_key} returned batch error: {result['error']}")

        try:
            candidates = result.get("response", {}).get("candidates", [])
            if candidates and len(candidates) > 0:
                finish_reason = candidates[0].get("finishReason", "")
                if finish_reason and finish_reason != "STOP":
                    print(f"Warning: {image_key} finishReason: {finish_reason}")
                
                content = candidates[0].get("content", {})
                parts = content.get("parts", [])
                if parts:
                    answer = parts[0].get("text", "unknown").strip().lower()
        except (KeyError, TypeError, IndexError):
            pass

        usage = result.get("response", {}).get("usageMetadata", {})
        total_prompt_tokens += usage.get("promptTokenCount", 0)
        total_candidate_tokens += usage.get("candidatesTokenCount", 0)
        total_thought_tokens += usage.get("thoughtsTokenCount", 0) 
        total_tokens += usage.get("totalTokenCount", 0)

        output.append({"image": image_key, "has_flowers": answer, "url": f"https://console.cloud.google.com/storage/browser/_details/mocodigi/{image_key};tab=live_object?authuser=1"})
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"Saved {len(output)} results to {OUTPUT_FILE}")

    if results:
        print("\n--- Token Usage Summary ---")
        print(f"Total Prompt Tokens:     {total_prompt_tokens:,}")
        print(f"Total Candidate Tokens:     {total_candidate_tokens:,}")
        print(f"Total Thought Tokens:     {total_thought_tokens:,}")
        print(f"Total Tokens Used:       {total_tokens:,}")
        print(f"Average Tokens / Image:  {total_tokens / len(results):.1f}")
    
    # Clean up temporary batch folder from GCS
    # cleanup_batch_folder(BUCKET, run_id, project_id)


if __name__ == "__main__":
    main()
