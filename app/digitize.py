from image_utils import collect_image_files_from_folders, load_image_as_part
from gemini_utils import get_gemini_client, generate_transcription
from cache_utils import cache_exists, load_cache, save_cache
import hashlib


def post_process_transcript(text_content: str) -> str:
    text_content = text_content.replace("Transcription:", "").strip()
    return text_content

# Configuration
# List of folder names to process
folder_names = [
#    "images/A02_double",
#    "images/C02_double",
    "images/A01",
    "images/B01",
    "images/B05",
    "images/C02",
    "images/C05",
    "images/C14",
]

model_name = "gemini-3-pro-preview"
model_name = "gemini-2.5-flash"

system_prompt = """
Your task is to accurately transcribe handwritten and typewritten biological specimen labels based on a photograph, minimizing the CER and WER. Work character by character, word by word, line by line, label by label, transcribing the text exactly as it appears on the labels. To maintain the authenticity of the historical text, retain spelling errors, grammar, syntax, capitalization, and punctuation. Transcribe all the text on the labels. They may be in any language using Latin alphabet with diacritics, and may also contain numbers, dates, codes and abbreviations. In your final response write Transcription: followed only by your transcription. Do not include any other text, conversational filler, or descriptions of the labels in your response.
"""
temperature = 0.0
run_version = "14"

# Initialize the Gemini client
client = get_gemini_client()

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
        else:
            print("No cache found, doing transcription...")
            # Load image and create part using helper function
            image_part = load_image_as_part(image_file)
            
            # Generate transcription using Gemini API
            response_text = generate_transcription(
                client=client,
                image_part=image_part,
                model_name=model_name,
                system_prompt=system_prompt,
                temperature=temperature
            )

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
        
        # Print the response
        print("Response:")
        print(response_text)
        print("-" * 50)
        
    except Exception as e:
        print(f"Error processing {image_file.name}: {e}")
        print("-" * 50)
