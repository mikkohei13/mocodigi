import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from cache_utils import load_consolidation_cache
from gemini_utils import get_gemini_client
from image_utils import get_subfolders


class HerbariumSpecimen(BaseModel):
    """Structured data extracted from a herbarium specimen label."""

    collectionName: Optional[str] = Field(
        default=None,
        description=(
            "Full name of the source collection, herbarium, and/or museum."
        ),
    )
    specimenIdentifier: Optional[str] = Field(
        default=None,
        description=(
            "Museum accession, catalog number, or other identifier assigned to the specimen."
        ),
    )
    collectorFieldNumber: Optional[str] = Field(
        default=None,
        description=(
            "Identifier given by the collector in the field, usually a number."
        ),
    )
    scientificName: Optional[str] = Field(
        default=None,
        description=(
            "Scientific name without authorship, preferring the most recent determination if multiple are present."
        ),
    )
    scientificNameAuthorship: Optional[str] = Field(
        default=None,
        description=(
            "Author citation for the scientificName, and year if available."
        ),
    )
    identifiedBy: Optional[str] = Field(
        default=None,
        description=(
            "Name(s) of the person(s) who determined the scientific name, often indicated by 'det.', 'determ.', or 'conf.'. Use the most recent determination if multiple are present. Separate multiple names with a semicolon."
        ),
    )
    dateIdentified: Optional[str] = Field(
        default=None,
        description=(
            "Year or full date of the (most recent) determination."
        ),
    )
    family: Optional[str] = Field(
        default=None,
        description=(
            "Scientific family name if explicitly stated on the label, typically ending in 'aceae' or 'ae'."
        ),
    )
    eventDate: Optional[str] = Field(
        default=None,
        description="Collection date.",
    )
    eventDateInterpretation: Optional[str] = Field(
        default=None,
        description="Interpretation of the collection date, in one of the following formats: 'YYYY-MM-DD', 'YYYY-MM', or'YYYY'. None if this cannot be determined.",
    )
    localityDescription: Optional[str] = Field(
        default=None,
        description=(
            "Full locality description, preserving original wording and language. May include country, region, site name, and/or directions."
        ),
    )
    country: Optional[str] = Field(
        default=None,
        description=(
            "Country name, which may be a historical or non-English."
        ),
    )
    countryInterpretation: Optional[str] = Field(
        default=None,
        description=(
            "Current, interpreted non-verbatim country name in English."
        ),
    )
    stateProvince: Optional[str] = Field(
        default=None,
        description=(
            "State, province, department, or equivalent first-level administrative unit."
        ),
    )
    municipality: Optional[str] = Field(
        default=None,
        description=(
            "Municipality, county, district, or equivalent second-level administrative unit."
        ),
    )
    coordinates: Optional[str] = Field(
        default=None,
        description=(
            "Verbatim coordinate string including punctuation — may be in a modern or historical format."
        ),
    )
    coordinateSystemInterpretation: Optional[str] = Field(
        default=None,
        description=(
            "Interpretation of the coordinate system used, e.g. 'UTM', 'WGS84', or such. None if this cannot be determined."
        ),
    )
    latitude: Optional[str] = Field(
        default=None,
        description=(
            "Latitude verbatim."
        ),
    )
    longitude: Optional[str] = Field(
        default=None,
        description=(
            "Longitude verbatim."
        ),
    )
    elevation: Optional[str] = Field(
        default=None,
        description=(
            "Elevation or altitude including units if available."
        ),
    )
    habitat: Optional[str] = Field(
        default=None,
        description=(
            "Habitat description, vegetation community, substrate, microhabitat, or such."

        ),
    )
    recordedBy: Optional[str] = Field(
        default=None,
        description=(
            "Name(s) of the collector(s), often indicated by 'leg.', 'Coll.', or similar. Separate multiple names with a semicolon."
        ),
    )
    occurrenceRemarks: Optional[str] = Field(
        default=None,
        description=(
            "Descriptive notes about the specimen, occurrence, or collecting event."

        ),
    )
    nonWildInterpretation: Optional[bool] = Field(
        default=None,
        description=(
            "Interpretation of wildness: True if the specimen was collected from a cultivated plant or botanic garden. False if collected from a wild population. None if this cannot be determined."
        ),
    )

SYSTEM_PROMPT = """Your task is to accurately convert OCR text extracted from the images of scanned herbarium specimens into structured json format.

Be careful checking whether the specimen has been collected from another country than where it is stored in.

Extract values only, not labels. Strip any field label, prefix, or keyword that introduces the value on the label.

Extract all field values verbatim — copy text exactly as it appears on the label, preserving original spelling, language, abbreviations, and punctuation. Only deviate from verbatim text where a field description explicitly asks for interpretation."""

# Configuration (hardcoded)

folder_names = [
    "images-solanaceae-trial/C.319050",
]

# Or every subfolder in a folder
folder_names = get_subfolders("images-solanaceae-trial")

RUN_VERSION = "solanaceae2"

# MODEL_NAME = "gemini-2.5-flash"
MODEL_NAME = "gemini-3.1-pro-preview"

TEMPERATURE = 0.0


def load_free_text(run_dir: Path) -> str | None:
    """Load free text from *_preprocessed.json, then consolidation, then transcript."""
    preprocessed_files = sorted(run_dir.glob("*_preprocessed.json"))
    if preprocessed_files:
        with open(preprocessed_files[0], "r", encoding="utf-8") as f:
            data = json.load(f)
        return data["data"]["preprocessed_transcript"]

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


def submit_structured_extraction(client, free_text: str) -> HerbariumSpecimen:
    """Submit text to Gemini and validate response against HerbariumSpecimen schema."""
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=free_text,
        config={
            "temperature": TEMPERATURE,
            "system_instruction": SYSTEM_PROMPT,
            "response_mime_type": "application/json",
            "response_json_schema": HerbariumSpecimen.model_json_schema(),
        },
    )
    return HerbariumSpecimen.model_validate_json(response.text)


def main() -> None:
    client = get_gemini_client(use_vertex_ai=True)

    print(f"Run version: {RUN_VERSION}")
    print(f"Model: {MODEL_NAME}")
    print(f"Temperature: {TEMPERATURE}")
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
            print("No consolidation.json or *_transcript.json found, skipping...")
            continue

        out_path = run_dir / "structured_output.json"
        if out_path.exists():
            print(f"Output file already exists: {out_path}, skipping...")
            continue

        print("Submitting for structured extraction:")
        print("--")
        print(free_text)
        print("-" * 50)

        cache_data = {
            "format_version": "0.1",
            "type": "structured_output",
            "datetime": datetime.now().isoformat(),
            "settings": {
                "run_version": RUN_VERSION,
                "model": MODEL_NAME,
                "prompt": SYSTEM_PROMPT,
                "temperature": TEMPERATURE,
            },
        }

        try:
            specimen = submit_structured_extraction(client, free_text)
            cache_data["data"] = specimen.model_dump(exclude_none=True)
        except Exception as exc:  # keep processing other specimens on per-item failure
            cache_data["data"] = {}
            cache_data["error"] = str(exc)
            print(f"Structured extraction failed: {exc}")

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)

        print(f"Saved: {out_path}")

    print("\n" + "=" * 50)
    print("Done")


if __name__ == "__main__":
    main()

