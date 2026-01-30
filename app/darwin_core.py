"""Convert transcribed specimen data to Darwin Core (DwC) format using Gemini."""
import json
import re
from pathlib import Path
from datetime import datetime

from gemini_utils import get_gemini_client, generate_consolidation
from cache_utils import (
    consolidation_cache_exists,
    load_consolidation_cache,
)

# Configuration (hardcoded)
RUN_VERSION = "h1"
folder_names = [
    "images_lajifi/http___id.luomus.fi_C.512411",
]
model_name = "gemini-2.5-flash"
model_name = "gemini-3-pro-preview"

temperature = 0.0
USE_VERTEX_AI = True
MAX_CHARS = 4096

SYSTEM_PROMPT = """From this raw specimen data transcription, extract following Darwin Core (DwC) data fields. Retrun them in a json format.
country, locality, scientificName, scientificNameAuthorship, institutionCode, eventDate, catalogNumber, recordNumber, recordedBy."""


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

        print("Submitting to Gemini for Darwin Core extraction...")
        response_text = generate_consolidation(
            client=client,
            text_content=free_text,
            model_name=model_name,
            system_prompt=SYSTEM_PROMPT,
            temperature=temperature,
            max_chars=MAX_CHARS,
        )

        parsed = parse_json_response(response_text)
        out_path = run_dir / "darwin_core.json"
        run_dir.mkdir(parents=True, exist_ok=True)

        if parsed is not None:
            cache_data = {
                "format_version": "0.1",
                "type": "darwin_core",
                "datetime": datetime.now().isoformat(),
                "settings": {
                    "run_version": RUN_VERSION,
                    "model": model_name,
                    "prompt": SYSTEM_PROMPT,
                    "temperature": temperature,
                },
                "data": parsed,
            }
        else:
            cache_data = {"malformed": response_text}

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)

        print(f"Saved: {out_path}")

    print("\n" + "=" * 50)
    print("Done")


if __name__ == "__main__":
    main()
