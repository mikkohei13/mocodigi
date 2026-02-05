"""Convert transcribed specimen data to Darwin Core (DwC) format using Gemini."""
import json
import re
import time
from pathlib import Path
from datetime import datetime

from image_utils import get_subfolders
from gemini_utils import get_gemini_client, generate_consolidation
from cache_utils import (
    consolidation_cache_exists,
    load_consolidation_cache,
)
from comparison_utils import values_equal

# Configuration (hardcoded)
RUN_VERSION = "h1"
folder_names = [
    "images_lajifi/http___id.luomus.fi_C.512411",
]

# Or every subfolder in a folder
folder_names = get_subfolders("images_lajifi")

model_name = "gemini-2.5-flash"
model_name = "gemini-3-pro-preview"

temperature = 0.0
USE_VERTEX_AI = True
MAX_CHARS = 4096

# Fields that contain semicolon-separated values (compared as sets, ignoring order)
SEMICOLON_LIST_FIELDS = {"identifiedBy", "catalogNumber", "recordedBy"}

# Original by MH
SYSTEM_PROMPT_1 = """From this raw specimen data transcription, extract following Darwin Core (DwC) data fields:

- country (Country name in English)
- verbatimLocality (The full geographic locality string including possible descriptions)
- locality (Only locality names)
- scientificName
- scientificNameAuthorship
- identifiedBy (Determiner or "det" name, multiple names separated by semicolon)
- collectionCode (Name of the collection the specimen originally came from, usually not Finnish. Do not shorten the name in any way.)
- institutionCode (If transcription contains "UNIV. (H). HELSINKI" or such, institution code is "H")
- eventDate (Collecting date in YYYY-MM-DD format)
- catalogNumber (Multiple numbers separated by semicolon, usually one of them is a HTTP-URI in format "http://id.luomus.fi/{letters}.{numbers}")
- recordedBy (Collector or "leg" name, multiple names separated by semicolon)


Return Darwin Core data in JSON format. Do not include any other text, conversational filler, or descriptions in your response.
"""

# Based on https://github.com/gbif-norway/gpt-prompts/blob/master/llm-comparison.txt
SYSTEM_PROMPT_2 = """You are an expert herbarium digitization system, working on OCR text extracted from the images of scanned herbarium specimens. First correct any obvious OCR errors, and then extract ONLY the following Darwin Core terms:

- country: Name of the country or major administrative unit for the Location.
- verbatimLocality: the full geographic locality string including possible descriptions
- locality: A spatial region or named place.
- scientificName: Full scientific name, not containing identification qualifications.
- scientificNameAuthorship: Full authorship information, not containing identification qualifications.
- identifiedBy: Person, group, or organization assigning the Taxon to the subject.
- collectionCode: Name of the collection the specimen originally came from, usually not Finnish. Do not shorten the name in any way.
- institutionCode: If transcription contains "UNIV. (H). HELSINKI" or such, institution code is "H"
- eventDate: date of the Event in YYYY-MM-DD format. Don't populate missing values.
- catalogNumber: Unique identifier for the record in the dataset or collection.
- recordedBy: List of people, groups, or organizations responsible for recording the original Occurrence.

If there are multiple valid values for a term, separate them with ";". If you can't identify information for a specific term, and/or the term is blank, skip the term in the response. IMPORTANT: respond ONLY in valid JSON.
"""

SYSTEM_PROMPT_3 = """Your task is to accurately convert biological specimen label text into Darwin Core (DwC) json format. Do not include any other text or conversational filler into your response. Resulting JSON should have structure like this, using only these standard DwC fields. Don't include anything else than the json when responding.

{
  "country": [Country name in English],
  "verbatimLocality": [The full geographic locality string including possible descriptions],
  "locality": [Only locality names],
  "scientificName: [Name],
  "scientificNameAuthorship: [Authors + year, if available],
  "identifiedBy": [Determiner or "det" name, multiple names separated by semicolon],
  "collectionCode": [Name of the collection the specimen originally came from, usually not Finnish. Do not shorten the name in any way.],
  "institutionCode": [If transcription contains UNIV. (H). HELSINKI or such, institution code is "H"],
  "eventDate": [Collecting date in YYYY-MM-DD format],
  "catalogNumber": [Multiple numbers separated by semicolon, usually one of them is a HTTP-URI in format "http://id.luomus.fi/{letters}.{numbers}"],
  "recordedBy": [Collector or "leg" name, multiple names separated by semicolon],
}
"""

# List of system prompts for triple-call consensus (same prompt for now, can be customized)
SYSTEM_PROMPTS = [
    SYSTEM_PROMPT_1,  # Prompt variation 1
    SYSTEM_PROMPT_2,  # Prompt variation 2
    SYSTEM_PROMPT_3,  # Prompt variation 3
]


def load_free_text(run_dir: Path) -> str | None:
    """Load free-text from consolidation.json or first *_transcript.json in run_dir. Returns None if neither exists."""
    consolidation_path = run_dir / "consolidation.json"
    if consolidation_path.exists():
        data = load_consolidation_cache(run_dir.parent, RUN_VERSION)
        return data["data"]["consolidation"]

    transcripts = sorted(run_dir.glob("*_transcript.json"))
    if not transcripts:
        return None
    with open(transcripts[0], "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["data"]["transcript"]


def parse_json_response(raw: str) -> dict | None:
    """Parse LLM response as JSON. Strips markdown code blocks. Returns None on failure."""
    text = raw.strip()
    # Strip optional ```json ... ``` or ``` ... ```
    match = re.match(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", text, re.DOTALL)
    if match:
        text = match.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def calculate_consensus(responses: list[dict]) -> tuple[dict, dict]:
    """
    Calculate consensus from multiple LLM responses.
    
    Uses comparison utilities to handle:
    - None/null equivalence (missing keys = null values)
    - Semicolon-separated lists (compared as sets, ignoring order)
    
    Args:
        responses: List of parsed JSON responses (dicts)
        
    Returns:
        Tuple of (consensus_data, match_proportions):
        - consensus_data: Dict with agreed values for each field
        - match_proportions: Dict with match proportion (1.0, 0.667, or 0.333) for each field
    """
    consensus_data = {}
    match_proportions = {}
    
    # Collect all keys across all responses
    all_keys = set()
    for r in responses:
        all_keys.update(r.keys())
    
    num_responses = len(responses)
    
    for key in all_keys:
        # Get values for this key from all responses (None if missing)
        values = [r.get(key) for r in responses]
        
        # Determine if this field uses semicolon-separated list comparison
        use_semicolon = key in SEMICOLON_LIST_FIELDS
        
        # Group values by equality using comparison utils
        # Each group is (representative_value, count, first_index)
        groups = []
        for i, val in enumerate(values):
            found_group = False
            for group in groups:
                if values_equal(val, group[0], semicolon_list=use_semicolon):
                    group[1] += 1
                    found_group = True
                    break
            if not found_group:
                groups.append([val, 1, i])
        
        # Find the group with the most matches
        groups.sort(key=lambda g: (-g[1], g[2]))  # Sort by count desc, then by first occurrence
        most_common_value, count, _ = groups[0]
        
        # Calculate match proportion
        if count == num_responses:
            proportion = 1.0
        elif count >= 2:
            proportion = round(count / num_responses, 3)
        else:
            proportion = round(1 / num_responses, 3)
        
        # Set consensus value: majority value if 2+ agree, otherwise first response's value
        if count >= 2:
            consensus_data[key] = most_common_value
        else:
            # No majority - use first response's value
            consensus_data[key] = values[0]
        
        match_proportions[key] = proportion
    
    return consensus_data, match_proportions


def main() -> None:
    client = get_gemini_client(use_vertex_ai=USE_VERTEX_AI)

    print(f"Run version: {RUN_VERSION}")
    print(f"Model: {model_name}")
    print(f"Temperature: {temperature}")
    print(f"Processing {len(folder_names)} specimen(s)")
    print("=" * 50)

    for folder_name in folder_names:
        print(f"\n{'=' * 50}")
        print(f"Processing specimen: {folder_name}")
        print("=" * 50)

        base_folder = Path(folder_name)
        run_dir = base_folder / f"run_{RUN_VERSION}"

        if not run_dir.exists():
            print(f"Run directory not found: {run_dir}, skipping...")
            continue

        free_text = load_free_text(run_dir)
        if free_text is None:
            print("No consolidation.json or *_transcript.json found in run_h1, skipping...")
            continue

        out_path = run_dir / "darwin_core.json"
        if out_path.exists():
            print(f"Output file already exists: {out_path}, skipping...")
            continue

        print("Submitting for Darwin Core extraction (triple-call consensus):")
        print("--")
        print(free_text)
        print("-" * 50)

        # Make 3 Gemini calls with different system prompts
        raw_responses = []
        parsed_responses = []
        
        for i, system_prompt in enumerate(SYSTEM_PROMPTS):
            print(f"\nCall {i + 1}/{len(SYSTEM_PROMPTS)}...")
            time.sleep(2)
            
            response_text = generate_consolidation(
                client=client,
                text_content=free_text,
                model_name=model_name,
                system_prompt=system_prompt,
                temperature=temperature,
                max_chars=MAX_CHARS,
            )
            
            raw_responses.append(response_text)
            parsed = parse_json_response(response_text)
            parsed_responses.append(parsed)
            
            print(f"Response {i + 1}:")
            print(response_text)
            print("-" * 50)

        # Filter out failed parses
        valid_responses = [r for r in parsed_responses if r is not None]
        
        cache_data = {
            "format_version": "0.1",
            "type": "darwin_core",
            "datetime": datetime.now().isoformat(),
            "settings": {
                "run_version": RUN_VERSION,
                "model": model_name,
                "prompts": SYSTEM_PROMPTS,
                "temperature": temperature,
            },
            "call_data": parsed_responses,
        }

        if len(valid_responses) >= 2:
            # Calculate consensus from valid responses
            consensus_data, match_proportions = calculate_consensus(valid_responses)
            cache_data["data"] = consensus_data
            cache_data["match"] = match_proportions
            print(f"Consensus calculated from {len(valid_responses)} valid responses")
        elif len(valid_responses) == 1:
            # Only one valid response - use it without match data
            cache_data["data"] = valid_responses[0]
            cache_data["errors"] = {"insufficient_valid_responses": True}
            print("Only 1 valid response - no consensus calculated")
        else:
            # No valid responses - store first raw response
            cache_data["data"] = raw_responses[0]
            cache_data["errors"] = {"json_malformed": True, "all_responses_failed": True}
            print("All responses failed to parse as JSON")

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)

        print(f"Saved: {out_path}")

    print("\n" + "=" * 50)
    print("Done")


if __name__ == "__main__":
    main()
