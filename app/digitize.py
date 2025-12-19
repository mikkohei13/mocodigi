import os
import io
import google.genai as genai
from google.genai import types
from pathlib import Path
import PIL.Image

# Configuration
# Set this to either a folder path or a single image file path
# Example: "bug_1" for folder, or "bug_1/image0002.jpg" for single image

image_path = "bug_test/image0052 - Copy.jpg"
image_path = "bug_test/image0018.jpg"

system_prompt = """
Your task is to accurately transcribe hand- and machine-written biological specimen labels based on photographs, minimizing the CER and WER. Work character by character, word by word, line by line, transcribing the text exactly as it appears on the labels. To maintain the authenticity of the historical text, retain spelling errors, grammar, syntax, capitalization, and punctuation. Transcribe all the text on the labels. They may have names in any language. In your final response write Transcription: followed only by your transcription.
"""
temperature = 0.0

api_key = os.getenv("GEMINI_API_KEY")

# Initialize the Gemini client
if not api_key:
    raise ValueError("API key is not set. Please set GEMINI_API_KEY environment variable.")

client = genai.Client(api_key=api_key)
model_name = "gemini-3-pro-preview"
#model_name = "gemini-2.5-flash"

# Determine if image_path is a file or folder
image_path_obj = Path(image_path)
if not image_path_obj.exists():
    raise FileNotFoundError(f"Path '{image_path}' does not exist")

# Supported image extensions
image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

# Check if it's a single file or a folder
if image_path_obj.is_file():
    # Single image file
    if image_path_obj.suffix.lower() not in image_extensions:
        raise ValueError(f"'{image_path}' is not a supported image file")
    image_files = [image_path_obj]
    print(f"Processing single image: {image_path}")
elif image_path_obj.is_dir():
    # Folder - get all image files
    image_files = [
        f for f in image_path_obj.iterdir()
        if f.suffix.lower() in image_extensions
    ]
    if not image_files:
        print(f"No image files found in '{image_path}'")
        exit(1)
    print(f"Found {len(image_files)} image(s) in '{image_path}'")
else:
    raise ValueError(f"'{image_path}' is neither a file nor a directory")

print(f"System prompt: {system_prompt}")
print(f"Temperature: {temperature}")
print(f"Model: {model_name}")

print("-" * 50)

# Process each image
for image_file in sorted(image_files):
    print(f"\nProcessing: {image_file.name}")
    
    try:
        # Load the image and convert to bytes
        image = PIL.Image.open(image_file)
        
        # Convert PIL Image to bytes
        img_bytes = io.BytesIO()
        image.save(img_bytes, format=image.format or 'JPEG')
        img_bytes.seek(0)
        image_bytes = img_bytes.read()
        
        # Determine MIME type based on file extension
        mime_type_map = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.webp': 'image/webp'
        }
        mime_type = mime_type_map.get(image_file.suffix.lower(), 'image/jpeg')
        
        # Create image part
        image_part = types.Part.from_bytes(data=image_bytes, mime_type=mime_type)
        
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
