import os
import google.genai as genai
from google.genai import types
from image_utils import get_image_files_from_folder, load_image_as_part

# Configuration
# List of folder names to process
folder_names = [
    "images/A02_single",
    "images/C02_single",
]

model_name = "gemini-3-pro-preview"
model_name = "gemini-2.5-flash"

system_prompt = """
Your task is to accurately transcribe hand- and machine-written biological specimen labels based on photographs, minimizing the CER and WER. Work character by character, word by word, line by line, transcribing the text exactly as it appears on the labels. To maintain the authenticity of the historical text, retain spelling errors, grammar, syntax, capitalization, and punctuation. Transcribe all the text on the labels. They may have names in any language. In your final response write Transcription: followed only by your transcription.
"""
temperature = 0.0

api_key = os.getenv("GEMINI_API_KEY")

# Initialize the Gemini client
if not api_key:
    raise ValueError("API key is not set. Please set GEMINI_API_KEY environment variable.")

client = genai.Client(api_key=api_key)

# Collect all image files from all folders
all_image_files = []
for folder_name in folder_names:
    try:
        image_files = get_image_files_from_folder(folder_name)
        all_image_files.extend(image_files)
        print(f"Found {len(image_files)} image(s) in '{folder_name}'")
    except (FileNotFoundError, ValueError) as e:
        print(f"Warning: {e}")
        continue

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
        # Load image and create part using helper function
        image_part = load_image_as_part(image_file)
        
        # Generate content with the image and system prompt
        response = client.models.generate_content(
            model=model_name,
            contents=[image_part],
            config=types.GenerateContentConfig(
                temperature=temperature,
                system_instruction=system_prompt,
                thinking_config=types.ThinkingConfig(thinking_budget=128)
            )
        )
        
        # Print the response
        print("Response:")
        print(response.text)
        print("-" * 50)
        
    except Exception as e:
        print(f"Error processing {image_file.name}: {e}")
        print("-" * 50)
