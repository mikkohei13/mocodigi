from image_utils import get_image_files_from_folder
from gemini_utils import get_gemini_client, generate_consolidation
from cache_utils import load_cache
from pathlib import Path
import json
from datetime import datetime

# Configuration
# List of folder names to process
folder_names = [
    "images/A01",
    "images/C11",
]

model_name = "gemini-2.5-flash"
model_name = "gemini-3-pro-preview"

system_prompt = """
Your task is to consolidate and refine multiple raw transcripts into a single, coherent set of label text for one biological specimen. 

You will receive between 2 and 10 raw transcripts derived from different image angles of the same specimen. The specimen carries 1 to 5 separate physical labels. Note that each raw transcript is likely incomplete, fragmented, or partially overlapping with others. 

Review all the input transcripts to reconstruct the full text of the original labels. Identify duplicated text across the inputs, resolve inconsistencies, and stitch together partial fragments. Provide a final version that maintains the authenticity of the historical text, including abbreviations, codes, dates, etc. exactly as they appear. 

# Rules:

- Ignore transcribed text that is clearly noise, such as scattered characters unconnected to words.
- Merge overlapping text. If multiple transcripts contain the same text (even in varied formats), consolidate them into the single most accurate representation.
- Be careful with numbers, only consolidate them if they appear similarly in multiple transcripts.
- Maintain the separation between distinct physical labels using line breaks.
- Fix obvious OCR typos based on context.
- Be strictly accurate with numbers. Do not combine fragments of numbers unless the match is exact in several similar transcripts. If there is a conflict or ambiguity between numbers in different transcripts, do not guess. Instead ignore such numbers.

In your final response, write "Consolidation:" followed only by your consolidated transcription. Do not include any other text, conversational filler, or descriptions of the labels in your response.

# Context:

- The specimen could have been collected anywhere in the world, probably in the 1900s.
- The labels may be in any language using the Latin alphabet with diacritics.
- The specimen is an insect belonging to Hemiptera.
- The labels often contain the following types of information, but **capture all legible content even if it does not fit these categories**:
  - **Locality names:** country, region, abbreviation, coordinates.
  - **Collection Data:** dates (months often in Roman numerals), and collector names (sometimes with 'leg' or 'coll').
  - **Taxonomy:** binomial scientific names, author names, and determiner names (sometimes with 'det').
  - **Curatorial:** loan info, catalog numbers, type status.

"""

temperature = 0.0
run_version = "9"

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


print(f"System prompt: {system_prompt}")
print(f"Temperature: {temperature}")
print(f"Model: {model_name}")
print(f"Run version: {run_version}")
print(f"Processing {len(folder_names)} specimen(s)")
print("=" * 50)

# Process each folder (specimen) separately
for folder_name in folder_names:
    print(f"\n{'=' * 50}")
    print(f"Processing specimen: {folder_name}")
    print(f"{'=' * 50}")
    
    # Get image files from this folder only
    try:
        image_files = get_image_files_from_folder(folder_name)
        print(f"Found {len(image_files)} image(s) in '{folder_name}'")
    except (FileNotFoundError, ValueError) as e:
        print(f"Warning: {e}, skipping...")
        continue
    
    if not image_files:
        print(f"No image files found in '{folder_name}', skipping...")
        continue
    
    # Get base folder for cache file location
    base_folder = Path(folder_name)
    
    # Check if consolidation cache exists
    if consolidation_cache_exists(base_folder, run_version):
        print("Consolidation cache found, loading from cache...")
        cache_data = load_consolidation_cache(base_folder, run_version)
        consolidation_text = cache_data["data"]["consolidation"]
        print("(Loaded from cache)")
    else:
        print("No consolidation cache found, generating consolidation...")
        
        # Collect all transcriptions from images in this folder
        all_transcriptions = []
        for image_file in image_files:
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
            print(f"No transcriptions found to consolidate for '{folder_name}', skipping...")
            continue
        
        # Concatenate all transcriptions
        concatenated_text = ""
        transcript_count = 0
        for transcription in all_transcriptions:
            transcript_count += 1

            # Content before each transcription
            concatenated_text += f"## Transcript {transcript_count}:\n\n"

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

    # Print the consolidation result for this specimen
    print("\n" + "-" * 50)
    print(f"Consolidation Result for {folder_name}:")
    print("-" * 50)
    print(consolidation_text)
    print("-" * 50)

print("\n" + "=" * 50)
print("All specimens processed")
print("=" * 50)

