"""Calculate mean response token count from a JSONL file."""

from __future__ import annotations

import json
from pathlib import Path

pipeline_run_id = "herbariumgenerale_random100"
pipeline_run_id = "herbariumgenerale_random100_pro2.5"
pipeline_run_id = "solanaceae-0"


def main() -> None:
    input_file = resolve_input_file()

    total = 0
    thoughts = 0
    prompt = 0
    candidates = 0
    count = 0

    with input_file.open("r", encoding="utf-8") as infile:
        for line in infile:
            if not line.strip():
                continue
            record = json.loads(line)
            usage = record["response"]["usageMetadata"]
            total += usage["totalTokenCount"]
            thoughts += usage["thoughtsTokenCount"]
            prompt += usage["promptTokenCount"]
            candidates += usage["candidatesTokenCount"]
            count += 1

    if count == 0:
        print("No records found.")
        return

    mean_total = total / count
    mean_thoughts = thoughts / count
    mean_prompt = prompt / count
    mean_candidates = candidates / count

    print(f"Pipeline run id: {pipeline_run_id}")
    print(f"Records: {count}")
    print(f"Mean promptTokenCount: {mean_prompt:.2f}")
    print(f"Mean candidatesTokenCount: {mean_candidates:.2f}")
    print(f"Mean thoughtsTokenCount: {mean_thoughts:.2f}")
    print(f"Mean totalTokenCount: {mean_total:.2f}")


def resolve_input_file() -> Path:
    app_dir = Path(__file__).resolve().parent.parent
    run_dir = app_dir / "output/pipeline_runs" / pipeline_run_id
    matches = sorted(
        run_dir.glob("transcript_batch_responses/*/predictions.jsonl"),
        key=lambda path: path.as_posix(),
    )

    if not matches:
        raise FileNotFoundError(
            f"No predictions.jsonl found under: {run_dir}/transcript_batch_responses/*/"
        )

    if len(matches) > 1:
        raise RuntimeError(
            "Multiple predictions.jsonl files found. Narrow pipeline_run_id or pick one:\n"
            + "\n".join(str(path) for path in matches)
        )

    return matches[0]


if __name__ == "__main__":
    main()

