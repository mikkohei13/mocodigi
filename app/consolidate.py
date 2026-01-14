from image_utils import get_image_files_from_folder
from gemini_utils import get_gemini_client, generate_consolidation
from cache_utils import (
    load_cache,
    consolidation_cache_exists,
    load_consolidation_cache,
    save_consolidation_cache
)
from rag_utils import get_rag_content
from pathlib import Path


def post_process_consolidation(text_content: str) -> str:
    text_content = text_content.replace("Consolidation:", "").strip()
    return text_content


# Configuration
# List of folder names to process
folder_names = [
    "images/A01 - Copy",
    "images/B01 - Copy",
    "images/B05 - Copy",
    "images/C02 - Copy",
    "images/C05 - Copy",
    "images/C13 - Copy",
    "images/C14 - Copy",
    "images/D07 - Copy",
    "images/D08 - Copy",
    "images/D11 - Copy",
    "images/D12 - Copy",
    "images/D14 - Copy",
    "images/D16 - Copy",
    "images/D17 - Copy",
    "images/D22 - Copy",
    "images/D23 - Copy",
]

model_name = "gemini-3-pro-preview"
model_name = "gemini-2.5-flash"

temperature = 0.0
run_version = "16"
branch_version = "" # Set to empty string to use just run_version, or e.g. "b" for "15b"

# Combine run_version and branch_version for consolidation cache
consolidation_version = f"{run_version}{branch_version}"

debug = False

system_prompt = """
Your task is to consolidate and refine multiple raw transcripts into a single, coherent set of label text for one biological specimen. 

You will receive between 2 and 10 raw transcripts derived from different image angles of the same specimen. The specimen carries 1 to 5 separate physical labels. Note that each raw transcript is likely incomplete, fragmented, or partially overlapping with others. 

Review all the input transcripts to reconstruct the full text of the original labels. Identify duplicated text across the inputs, resolve inconsistencies, and stitch together partial fragments. Provide a final version that maintains the authenticity of the historical text, including abbreviations, codes, dates, etc. exactly as they appear. 

# Rules:

- Ignore transcribed text that is clearly noise, such as scattered characters unconnected to words.
- Merge overlapping text. If multiple transcripts contain the same text (even in varied formats), consolidate them into the single most accurate representation.
- Maintain the separation between distinct physical labels using line breaks.
- Fix obvious OCR typos based on context.
- Be strictly accurate with numbers. Do not combine fragments of numbers unless the match is exact in several similar transcripts. If there is a conflict or ambiguity between numbers in different transcripts, do not guess. Instead ignore such numbers.

In your final response, write "Consolidation:" followed only by your consolidated transcript. Do not include any other text, conversational filler, or descriptions of the labels in your response.
"""

# Initialize the Gemini client
client = get_gemini_client()

#print(f"System prompt: {system_prompt}")
print(f"Temperature: {temperature}")
print(f"Model: {model_name}")
print(f"Run version: {run_version}")
print(f"Branch version: {branch_version}")
print(f"Consolidation version: {consolidation_version}")
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
    if consolidation_cache_exists(base_folder, consolidation_version):
        print("Consolidation cache found, loading from cache...")
        cache_data = load_consolidation_cache(base_folder, consolidation_version)
        consolidation_text = cache_data["data"]["consolidation"]
        print("(Loaded from cache)")
    else:
        print("No consolidation cache found, generating consolidation...")
        
        # Collect all transcripts from images in this folder
        all_transcripts = []
        for image_file in image_files:
            try:
                cache_data = load_cache(image_file, run_version)
                transcript = cache_data["data"]["transcript"]

                all_transcripts.append(f"\n{transcript}\n")
                print(f"Loaded transcript from {image_file.name}")
            except FileNotFoundError:
                print(f"Warning: No transcript cache found for {image_file.name}, skipping...")
                continue
        
        if not all_transcripts:
            print(f"No transcripts found to consolidate for '{folder_name}', skipping...")
            continue
        
        # Concatenate all transcripts
        transcripts_content = ""
        transcript_count = 0
        for transcript in all_transcripts:
            transcript_count += 1

            # Content before each transcript
            transcripts_content += f"## Transcript {transcript_count}:\n"

            transcripts_content += transcript

            # Content after each transcript
            transcripts_content += "\n"

        print(f"Concatenated {len(all_transcripts)} transcript(s)")

        # Add RAG
        rag_content = get_rag_content(folder_name, transcripts_content)

        # Combine all
        content = rag_content + "\n\n" + transcripts_content

        print(system_prompt)
        print(content)

        # Debug mode: exit before submitting to Gemini
        if debug:
            print("DEBUG EXIT:")
            exit(0)
        
        # Generate consolidation using Gemini API
        print("Submitting to Gemini for consolidation...")
        consolidation_text = generate_consolidation(
            client=client,
            text_content=transcripts_content,
            model_name=model_name,
            system_prompt=system_prompt,
            temperature=temperature
        )

        processed_consolidation_text = post_process_consolidation(consolidation_text)
        
        # Save to consolidation cache
        cache_path = save_consolidation_cache(
            base_folder=base_folder,
            raw_consolidation=consolidation_text,
            consolidation=processed_consolidation_text,
            concatenated_transcripts=transcripts_content,
            model_name=model_name,
            prompt=system_prompt,
            temperature=temperature,
            run_version=consolidation_version
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

