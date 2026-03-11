"""Write random transcript samples to a text file."""

from pathlib import Path
import json
import random

# Configuration
BASE_FOLDER = Path(__file__).resolve().parent.parent / "images_lajifi"
RUN_ID = "run_h1"
FOLDERS_TO_SAMPLE = 99
OUTPUT_FILE = f"{BASE_FOLDER}/sample_transcripts.txt"


def get_transcript_files(folder: Path, run_id: str) -> list[Path]:
    """Return transcript files from folder/run_id."""
    run_folder = folder / run_id
    if not run_folder.exists() or not run_folder.is_dir():
        return []
    return sorted(run_folder.glob("*_transcript.json"))


def extract_transcript_text(transcript_file: Path) -> str:
    """Read transcript text from a transcript JSON file."""
    with transcript_file.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("data", {}).get("transcript", "").strip()


def main() -> None:
    if not BASE_FOLDER.exists() or not BASE_FOLDER.is_dir():
        print(f"Base folder '{BASE_FOLDER}' does not exist.")
        return

    folders = [f for f in BASE_FOLDER.iterdir() if f.is_dir()]
    if not folders:
        print(f"No subfolders found in '{BASE_FOLDER}'.")
        return

    sample_size = min(FOLDERS_TO_SAMPLE, len(folders))
    sampled_folders = random.sample(folders, sample_size)

    written_count = 0
    with OUTPUT_FILE.open("w", encoding="utf-8") as out:
        for folder in sampled_folders:
            transcript_files = get_transcript_files(folder, RUN_ID)
            if not transcript_files:
                continue

            for transcript_file in transcript_files:
                transcript = extract_transcript_text(transcript_file)
                if not transcript:
                    continue

                out.write(f"\n# Specimen {written_count + 1}:\n\n")
#                out.write(f"Folder: {folder.name}\n")
#                out.write(f"File: {transcript_file.name}\n")
#                out.write("Transcript:\n")
                out.write(f"{transcript}\n")
#                out.write(f"{'-' * 80}\n\n")
                written_count += 1

    print(f"Wrote {written_count} transcript(s) to '{OUTPUT_FILE}'.")


if __name__ == "__main__":
    main()

