from image_utils import collect_image_files_from_folders, load_image_as_part
from gemini_utils import get_gemini_client, generate_transcription
from cache_utils import cache_exists, load_cache, save_cache
import hashlib
# Configuration
# List of folder names to process
folder_names = [
    "images/A02_single",
    "images/C11",
#    "images/A01",
#    "images/B01",
#    "images/C01",
#    "images/C02_single",
]

model_name = "gemini-3-pro-preview"
model_name = "gemini-2.5-flash"

system_prompt = """
Your task is to accurately transcribe handwritten and typewritten biological specimen labels based on a photograph, minimizing the CER and WER. Work character by character, word by word, line by line, label by label, transcribing the text exactly as it appears on the labels. To maintain the authenticity of the historical text, retain spelling errors, grammar, syntax, capitalization, and punctuation. Transcribe all the text on the labels. They may be in any language using Latin alphabet with diacritics, and may also contain numbers, dates, codes and abbreviations. In your final response write Transcription: followed only by your transcription. Do not include any other text, conversational filler, or descriptions of the labels in your response.
"""
temperature = 0.0
run_version = "9"

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
            print("Cache found, loading from cache...")
            cache_data = load_cache(image_file, run_version)
            response_text = cache_data["data"]["transcription"]
            print("(Loaded from cache)")
        else:
            print("No cache found, generating transcription...")
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
            
            # Save to cache
            cache_path = save_cache(
                image_file=image_file,
                transcription=response_text,
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
