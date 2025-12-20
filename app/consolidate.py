from image_utils import get_image_files_from_folder
from gemini_utils import get_gemini_client
from cache_utils import load_cache
from pathlib import Path
import json
from datetime import datetime
from google.genai import types

# Configuration
# List of folder names to process
folder_names = [
    "images/A01",
]

model_name = "gemini-2.5-flash"

system_prompt = """
Your task is to consolidate and refine multiple transcriptions of the same labels of a single biological specimen. You will receive transcriptions from multiple images. Review all the transcriptions, identify any duplicated text, inconsistencies, errors, or areas that need clarification. Provide a consolidated, refined version that maintains the authenticity of the historical text while improving accuracy where possible. In your final response write Consolidation: followed only by your consolidated transcription. Don't include any other text in your response.
"""

temperature = 0.0
run_version = "7"

debug = False

# Initialize the Gemini client
client = get_gemini_client()


def get_consolidation_cache_path(base_folder: Path, run_version: str) -> Path:
    """
    Get the consolidation cache file path.
    
    Args:
        base_folder: Path to the base folder (first folder in folder_names)
        run_version: Version string to include in filename
        
    Returns:
        Path to the consolidation cache file in the artifacts subdirectory
    """
    artifacts_dir = base_folder / "artifacts"
    cache_filename = f"consolidation_{run_version}.json"
    return artifacts_dir / cache_filename


def consolidation_cache_exists(base_folder: Path, run_version: str) -> bool:
    """
    Check if a consolidation cache file exists.
    
    Args:
        base_folder: Path to the base folder
        run_version: Version string to include in filename
        
    Returns:
        True if cache file exists, False otherwise
    """
    cache_path = get_consolidation_cache_path(base_folder, run_version)
    return cache_path.exists()


def load_consolidation_cache(base_folder: Path, run_version: str) -> dict:
    """
    Load consolidation result from cache file.
    
    Args:
        base_folder: Path to the base folder
        run_version: Version string to include in filename
        
    Returns:
        Dictionary containing cached consolidation data
        
    Raises:
        FileNotFoundError: If cache file doesn't exist
        json.JSONDecodeError: If cache file is invalid JSON
    """
    cache_path = get_consolidation_cache_path(base_folder, run_version)
    with open(cache_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_consolidation_cache(
    base_folder: Path,
    consolidation: str,
    concatenated_transcriptions: str,
    model_name: str,
    prompt: str,
    temperature: float,
    run_version: str
) -> Path:
    """
    Save consolidation result to cache file.
    
    Args:
        base_folder: Path to the base folder
        consolidation: Consolidated text
        concatenated_transcriptions: Original concatenated transcriptions
        model_name: Model name used
        prompt: System prompt used
        temperature: Temperature setting used
        run_version: Run version string
        
    Returns:
        Path to the saved cache file
    """
    cache_path = get_consolidation_cache_path(base_folder, run_version)
    
    # Create artifacts directory if it doesn't exist
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    cache_data = {
        "format_version": "0.1",
        "type": "consolidation",
        "datetime": datetime.now().isoformat(),
        "settings": {
            "run_version": run_version,
            "model": model_name,
            "prompt": prompt,
            "temperature": temperature
        },
        "data": {
            "consolidation": consolidation,
            "concatenated_transcriptions": concatenated_transcriptions
        }
    }
    
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)
    
    return cache_path


def generate_consolidation(
    client,
    text_content: str,
    model_name: str,
    system_prompt: str,
    temperature: float = 0.0,
    thinking_budget: int = 128
) -> str:
    """
    Generate consolidation for concatenated transcriptions using Gemini API.
    
    Args:
        client: Initialized Gemini client
        text_content: Concatenated transcription text
        model_name: Name of the Gemini model to use
        system_prompt: System instruction prompt
        temperature: Temperature for generation (default: 0.0)
        thinking_budget: Thinking budget for the model (default: 128)
        
    Returns:
        Response text from the API
    """
    # Pass text content directly (no need for Part object for text-only content)
    response = client.models.generate_content(
        model=model_name,
        contents=[text_content],
        config=types.GenerateContentConfig(
            temperature=temperature,
            system_instruction=system_prompt,
            thinking_config=types.ThinkingConfig(thinking_budget=thinking_budget)
        )
    )
    
    return response.text


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
print(f"Run version: {run_version}")
print(f"Total images to process: {len(all_image_files)}")
print("-" * 50)

# Get base folder for cache file location (use first folder)
base_folder = Path(folder_names[0]) if folder_names else Path("images")

# Check if consolidation cache exists
if consolidation_cache_exists(base_folder, run_version):
    print("Consolidation cache found, loading from cache...")
    cache_data = load_consolidation_cache(base_folder, run_version)
    consolidation_text = cache_data["data"]["consolidation"]
    print("(Loaded from cache)")
else:
    print("No consolidation cache found, generating consolidation...")
    
    # Collect all transcriptions from all images
    all_transcriptions = []
    for image_file in all_image_files:
        try:
            cache_data = load_cache(image_file, run_version)
            transcription = cache_data["data"]["transcription"]
            # Remove "Transcription:" prefix if present
            if transcription.startswith("Transcription:"):
                transcription = transcription[len("Transcription:"):].lstrip()
            all_transcriptions.append(f"\n{transcription}\n")
            print(f"Loaded transcription from {image_file.name}")
        except FileNotFoundError:
            print(f"Warning: No transcription cache found for {image_file.name}, skipping...")
            continue
    
    if not all_transcriptions:
        print("No transcriptions found to consolidate")
        exit(1)
    
    # Concatenate all transcriptions
    concatenated_text = ""
    transcript_count = 0
    for transcription in all_transcriptions:
        transcript_count += 1

        # Content before each transcription
        concatenated_text += f"# Transcript {transcript_count}:\n\n"

        concatenated_text += transcription

        # Content after each transcription
        concatenated_text += "\n\n"

    print(f"Concatenated {len(all_transcriptions)} transcription(s)")
    
    # Debug mode: exit before submitting to Gemini
    if debug:
        print("DEBUG EXIT:")
        print("\nConcatenated text that would be sent to Gemini:")
        print(concatenated_text)
        exit(0)
    
    # Generate consolidation using Gemini API
    print("Submitting to Gemini for consolidation...")
    consolidation_text = generate_consolidation(
        client=client,
        text_content=concatenated_text,
        model_name=model_name,
        system_prompt=system_prompt,
        temperature=temperature
    )
    
    # Save to consolidation cache
    cache_path = save_consolidation_cache(
        base_folder=base_folder,
        consolidation=consolidation_text,
        concatenated_transcriptions=concatenated_text,
        model_name=model_name,
        prompt=system_prompt,
        temperature=temperature,
        run_version=run_version
    )
    print(f"Saved to consolidation cache: {cache_path}")

# Print the consolidation result
print("\n" + "=" * 50)
print("Consolidation Result:")
print("=" * 50)
print(consolidation_text)
print("=" * 50)

