from image_utils import collect_image_files_from_folders, load_image_as_part, get_subfolders
from gemini_utils import get_gemini_client, generate_transcription_with_stream_capture
from cache_utils import cache_exists, load_cache, save_cache
import hashlib
import json
from datetime import datetime


def post_process_transcript(text_content: str) -> str:
    text_content = text_content.replace("Transcription:", "").strip()
    return text_content


def save_raw_gemini_response(cache_path, response_payload: dict) -> str:
    """
    Save raw streamed Gemini payload next to transcript cache JSON.
    """
    raw_response_path = cache_path.with_name(f"{cache_path.stem}_gemini_response.json")
    with open(raw_response_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "format_version": "0.1",
                "type": "gemini_stream_capture",
                "datetime": datetime.now().isoformat(),
                "data": response_payload
            },
            f,
            indent=2,
            ensure_ascii=False
        )
    return str(raw_response_path)

# Configuration
# List of folder names to process
folder_names = [
    "images/kio",
]

# Or every subfolder in a folder
folder_names = get_subfolders("images-solanaceae-trial")

model_name = "gemini-2.5-flash"
model_name = "gemini-3.1-pro-preview"

temperature = 0.0
run_version = "solanaceae2"
max_chars = 1000

# True = Vertex AI Express Mode (API key from Google Cloud). False = Gemini Developer API (aistudio.google.com).
USE_VERTEX_AI = True

# Note: HERBARIUM
system_prompt = """
Your task is to accurately transcribe handwritten and typewritten herbarium specimen labels based on a photograph, minimizing CER and WER. Work character by character, word by word, line by line, label by label, transcribing the text exactly as it appears on the labels. To maintain the authenticity of the historical text, retain spelling errors, grammar, syntax, capitalization, and punctuation. Transcribe all the text on the labels. They may be in any language using Latin alphabet with diacritics, and may also contain numbers, dates, codes and abbreviations. In your final response write Transcription: followed only by your transcription. Do not include any other text, conversational filler, or descriptions of the labels in your response.
"""

# Initialize the Gemini client
client = get_gemini_client(use_vertex_ai=USE_VERTEX_AI)

# Collect all image files from all folders
all_image_files = collect_image_files_from_folders(folder_names)

if not all_image_files:
    print("No image files found in any of the specified folders")
    exit(1)

print(f"System prompt: {system_prompt}")
print(f"Temperature: {temperature}")
print(f"Model: {model_name}")
print(f"Total images to process: {len(all_image_files)}")
print("-" * 50)

# Process each image
for image_file in all_image_files:
    print(f"\nProcessing: {image_file}")
    
    try:
        # Check if cache exists
        if cache_exists(image_file, run_version):
            print("Cache found, skipping transcription...")
            cached = load_cache(image_file, run_version)
            response_text = cached["data"]["raw_transcript"]
        else:
            print("No cache found, doing transcription...")
            # Load image and create part using helper function
            image_part = load_image_as_part(image_file)
            
            # Generate transcription using Gemini API
            response_payload = generate_transcription_with_stream_capture(
                client=client,
                image_part=image_part,
                model_name=model_name,
                system_prompt=system_prompt,
                temperature=temperature,
                max_chars=max_chars
            )
            response_text = response_payload["transcript_text"]

            processed_response_text = post_process_transcript(response_text)
            
            # Save to cache
            cache_path = save_cache(
                image_file=image_file,
                raw_transcript=response_text,
                transcript=processed_response_text,
                model_name=model_name,
                prompt=system_prompt,
                temperature=temperature,
                run_version=run_version
            )
            print(f"Saved to cache: {cache_path}")

            raw_response_path = save_raw_gemini_response(cache_path, response_payload)
            print(f"Saved raw Gemini stream response: {raw_response_path}")
        
        # Print the response
        print("Response:")
        print(response_text)
        print("-" * 50)
        
    except Exception as e:
        print(f"Error processing {image_file.name}: {e}")
        print("-" * 50)
