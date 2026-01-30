# Script that verifies transcript data against ground truth

from cache_utils import load_consolidation_cache, load_alignment_cache
from pathlib import Path
from collections import Counter
import unicodedata


# Configuration
# Set to "consolidation" or "alignment" to test the corresponding data type
DATATYPE = "consolidation"

# List of folder names to process. Each contain images from a single specimen.
folder_names = [
    "images/D01",
    "images/D02",
    "images/D03",
]

run_version = "17"
branch_version = "" # Set to empty string to use just run_version, or e.g. "b" for "15b"


def normalize_text(text: str) -> str:
    """Remove whitespace and convert to lowercase."""
    return ''.join(text.split()).lower()


def filter_alphanumeric(text: str) -> str:
    """Keep only letters and numbers, including those with diacritics."""
    return ''.join(c for c in text if unicodedata.category(c).startswith(('L', 'N')))


def count_characters(text: str) -> Counter:
    """Count occurrences of each character."""
    return Counter(text)


def compare_texts(gt_text: str, consolidation_text: str, alphanumeric_only: bool = False) -> tuple[float, int]:
    """
    Compare ground truth and consolidation texts.
    
    Args:
        gt_text: Ground truth text
        consolidation_text: Consolidation text
        alphanumeric_only: If True, only compare letters and numbers
    
    Returns:
        Tuple of (match_percentage, mismatch_count)
    """
    gt_normalized = normalize_text(gt_text)
    consolidation_normalized = normalize_text(consolidation_text)
    
    if alphanumeric_only:
        gt_normalized = filter_alphanumeric(gt_normalized)
        consolidation_normalized = filter_alphanumeric(consolidation_normalized)
    
    gt_chars = count_characters(gt_normalized)
    consolidation_chars = count_characters(consolidation_normalized)
    
    # Get all unique characters from both texts
    all_chars = set(gt_chars.keys()) | set(consolidation_chars.keys())
    
    # Count matches and mismatches
    matches = 0
    mismatches = 0
    
    for char in all_chars:
        gt_count = gt_chars.get(char, 0)
        consolidation_count = consolidation_chars.get(char, 0)
        
        # Count matching characters (min of the two counts)
        matches += min(gt_count, consolidation_count)
        # Count mismatches (difference in counts)
        mismatches += abs(gt_count - consolidation_count)
    
    # Calculate match percentage based on matches vs total characters considered
    total_chars_considered = matches + mismatches
    if total_chars_considered == 0:
        match_percentage = 0.0
    else:
        match_percentage = (matches / total_chars_considered) * 100
    
    return match_percentage, mismatches


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def normalize_line(line: str) -> str:
    """Normalize a line for comparison (strip whitespace)."""
    return line.strip()


def normalize_for_wer(text: str) -> str:
    """
    Normalize text for WER-style tokenization where whitespace/punctuation variants
    like "J.A.Smith" vs "J. A. Smith" shouldn't matter.

    Rules:
    - casefold for case-insensitive matching
    - treat any non-letter / non-number (including punctuation and "/" ) as a separator
    - collapse whitespace
    """
    folded = text.casefold()
    out_chars: list[str] = []
    for c in folded:
        if unicodedata.category(c).startswith(("L", "N")):
            out_chars.append(c)
        else:
            out_chars.append(" ")
    return " ".join("".join(out_chars).split())


def normalize_for_cer(text: str) -> str:
    """
    Normalize text for CER where whitespace and punctuation should not matter.
    This uses `normalize_for_wer` then removes all spaces.
    """
    return normalize_for_wer(text).replace(" ", "")


def _generate_spans(lines: list[str], max_span_lines: int) -> list[tuple[int, int, str]]:
    """
    Generate consecutive-line spans.

    Returns tuples of (start_idx, end_idx_exclusive, span_text).
    """
    spans: list[tuple[int, int, str]] = []
    n = len(lines)
    for start in range(n):
        for length in range(1, max_span_lines + 1):
            end = start + length
            if end > n:
                break
            # Join with space so line breaks don't affect matching/tokenization.
            span_text = " ".join(normalize_line(l) for l in lines[start:end]).strip()
            if span_text:
                spans.append((start, end, span_text))
    return spans


def match_spans(
    gt_lines: list[str],
    consolidation_lines: list[str],
    *,
    max_gt_span_lines: int = 3,
    max_consolidation_span_lines: int = 2,
    similarity_threshold: float = 0.5,
) -> list[tuple[int, int, int, int, float]]:
    """
    Match spans of consecutive lines between GT and consolidation using similarity.
    
    Args:
        gt_lines: List of ground truth lines.
        consolidation_lines: List of consolidation lines.
        max_gt_span_lines: Max consecutive GT lines to consider as a single unit.
        max_consolidation_span_lines: Max consecutive consolidation lines to consider as a single unit.
        similarity_threshold: Minimum normalized similarity to accept a match.
    
    Returns:
        List of tuples (gt_start, gt_end, cons_start, cons_end, similarity_score)
        where similarity_score is normalized (0-1, higher is better).
    """
    gt_spans = _generate_spans(gt_lines, max_gt_span_lines)
    cons_spans = _generate_spans(consolidation_lines, max_consolidation_span_lines)

    candidates: list[tuple[float, int, int, int, int]] = []
    # candidates entries: (similarity, gt_start, gt_end, cons_start, cons_end)
    for gt_start, gt_end, gt_text in gt_spans:
        gt_norm = normalize_for_wer(gt_text)
        if not gt_norm:
            continue
        for cons_start, cons_end, cons_text in cons_spans:
            cons_norm = normalize_for_wer(cons_text)
            if not cons_norm:
                continue

            distance = levenshtein_distance(gt_norm, cons_norm)
            max_len = max(len(gt_norm), len(cons_norm))
            similarity = 1.0 if max_len == 0 else 1.0 - (distance / max_len)
            if similarity >= similarity_threshold:
                candidates.append((similarity, gt_start, gt_end, cons_start, cons_end))

    # Greedy global selection: pick best matches first, ensure spans don't overlap.
    candidates.sort(key=lambda x: (-x[0], -((x[2] - x[1]) + (x[4] - x[3])), x[1], x[3]))

    used_gt_indices: set[int] = set()
    used_cons_indices: set[int] = set()
    matches: list[tuple[int, int, int, int, float]] = []

    for similarity, gt_start, gt_end, cons_start, cons_end in candidates:
        gt_range = range(gt_start, gt_end)
        cons_range = range(cons_start, cons_end)
        if any(i in used_gt_indices for i in gt_range):
            continue
        if any(i in used_cons_indices for i in cons_range):
            continue

        matches.append((gt_start, gt_end, cons_start, cons_end, similarity))
        used_gt_indices.update(gt_range)
        used_cons_indices.update(cons_range)

    return matches


def calculate_wer(reference: str, hypothesis: str) -> tuple[float, int, int, int]:
    """
    Calculate Word Error Rate (WER) using edit distance.
    
    Args:
        reference: Reference text
        hypothesis: Hypothesis text
    
    Returns:
        Tuple of (WER, substitutions, insertions, deletions)
        WER is a percentage (0-100)
    """
    # Tokenize into words
    ref_words = reference.split()
    hyp_words = hypothesis.split()
    
    if len(ref_words) == 0:
        if len(hyp_words) == 0:
            return 0.0, 0, 0, 0
        else:
            return 100.0, 0, len(hyp_words), 0
    
    # Calculate edit distance at word level
    # Use dynamic programming
    dp = [[0] * (len(hyp_words) + 1) for _ in range(len(ref_words) + 1)]
    
    # Initialize
    for i in range(len(ref_words) + 1):
        dp[i][0] = i
    for j in range(len(hyp_words) + 1):
        dp[0][j] = j
    
    # Fill DP table
    for i in range(1, len(ref_words) + 1):
        for j in range(1, len(hyp_words) + 1):
            if ref_words[i-1] == hyp_words[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = min(
                    dp[i-1][j] + 1,      # deletion
                    dp[i][j-1] + 1,      # insertion
                    dp[i-1][j-1] + 1     # substitution
                )
    
    total_errors = dp[len(ref_words)][len(hyp_words)]
    
    # Backtrack to count substitutions, insertions, deletions
    substitutions = 0
    insertions = 0
    deletions = 0
    
    i, j = len(ref_words), len(hyp_words)
    while i > 0 or j > 0:
        if i > 0 and j > 0 and ref_words[i-1] == hyp_words[j-1]:
            i -= 1
            j -= 1
        elif i > 0 and j > 0 and dp[i][j] == dp[i-1][j-1] + 1:
            substitutions += 1
            i -= 1
            j -= 1
        elif j > 0 and dp[i][j] == dp[i][j-1] + 1:
            insertions += 1
            j -= 1
        else:
            deletions += 1
            i -= 1
    
    wer = (total_errors / len(ref_words)) * 100
    return wer, substitutions, insertions, deletions


def calculate_cer(reference: str, hypothesis: str) -> tuple[float, int, int, int]:
    """
    Calculate Character Error Rate (CER) using edit distance.
    
    Args:
        reference: Reference text
        hypothesis: Hypothesis text
    
    Returns:
        Tuple of (CER, substitutions, insertions, deletions)
        CER is a percentage (0-100)
    """
    if len(reference) == 0:
        if len(hypothesis) == 0:
            return 0.0, 0, 0, 0
        else:
            return 100.0, 0, len(hypothesis), 0
    
    # Build DP table for edit distance and backtracking
    dp = [[0] * (len(hypothesis) + 1) for _ in range(len(reference) + 1)]
    
    for i in range(len(reference) + 1):
        dp[i][0] = i
    for j in range(len(hypothesis) + 1):
        dp[0][j] = j
    
    for i in range(1, len(reference) + 1):
        for j in range(1, len(hypothesis) + 1):
            if reference[i-1] == hypothesis[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = min(
                    dp[i-1][j] + 1,
                    dp[i][j-1] + 1,
                    dp[i-1][j-1] + 1
                )
    
    distance = dp[len(reference)][len(hypothesis)]
    
    # Backtrack to count substitutions, insertions, deletions
    substitutions = 0
    insertions = 0
    deletions = 0
    
    i, j = len(reference), len(hypothesis)
    while i > 0 or j > 0:
        if i > 0 and j > 0 and reference[i-1] == hypothesis[j-1]:
            i -= 1
            j -= 1
        elif i > 0 and j > 0 and dp[i][j] == dp[i-1][j-1] + 1:
            substitutions += 1
            i -= 1
            j -= 1
        elif j > 0 and dp[i][j] == dp[i][j-1] + 1:
            insertions += 1
            j -= 1
        else:
            deletions += 1
            i -= 1
    
    cer = (distance / len(reference)) * 100
    return cer, substitutions, insertions, deletions


def calculate_wer_cer_two_level(gt_text: str, consolidation_text: str) -> tuple[float, float, dict]:
    """
    Calculate WER and CER using two-level matching (order-insensitive).
    
    First matches spans of consecutive lines between GT and consolidation
    (to be robust to line wrapping), then calculates WER/CER on matched spans.
    Unmatched GT lines count as deletions; unmatched consolidation lines count
    as insertions.
    
    Args:
        gt_text: Ground truth text
        consolidation_text: Consolidation text
    
    Returns:
        Tuple of (WER, CER, details_dict)
        details_dict contains breakdown of errors
    """
    # Split into lines (GT preserves original label line breaks; consolidation may not)
    gt_lines = [line for line in gt_text.split("\n")]
    consolidation_lines = [line for line in consolidation_text.split("\n")]

    # Match spans (order-flex at span granularity)
    span_matches = match_spans(
        gt_lines,
        consolidation_lines,
        max_gt_span_lines=3,
        max_consolidation_span_lines=2,
        similarity_threshold=0.5,
    )

    # Calculate WER/CER on matched spans using normalization that ignores punctuation and spacing
    total_ref_words = 0
    total_ref_chars = 0
    total_word_errors = 0
    total_char_errors = 0
    
    matched_gt_indices: set[int] = set()
    matched_cons_indices: set[int] = set()

    # Process matched spans
    for gt_start, gt_end, cons_start, cons_end, similarity in span_matches:
        matched_gt_indices.update(range(gt_start, gt_end))
        matched_cons_indices.update(range(cons_start, cons_end))

        gt_span_text = " ".join(normalize_line(l) for l in gt_lines[gt_start:gt_end]).strip()
        cons_span_text = " ".join(normalize_line(l) for l in consolidation_lines[cons_start:cons_end]).strip()

        gt_for_wer = normalize_for_wer(gt_span_text)
        cons_for_wer = normalize_for_wer(cons_span_text)

        # WER (word-level)
        wer, _, _, _ = calculate_wer(gt_for_wer, cons_for_wer)
        ref_words = len(gt_for_wer.split())
        total_ref_words += ref_words
        total_word_errors += (wer / 100.0) * ref_words

        # CER (character-level), ignoring spaces entirely
        gt_for_cer = normalize_for_cer(gt_span_text)
        cons_for_cer = normalize_for_cer(cons_span_text)
        cer, _, _, _ = calculate_cer(gt_for_cer, cons_for_cer)
        ref_chars = len(gt_for_cer)
        total_ref_chars += ref_chars
        total_char_errors += (cer / 100.0) * ref_chars
    
    # Count unmatched GT lines as deletions (all tokens/chars are missing)
    for gt_idx, gt_line in enumerate(gt_lines):
        if gt_idx not in matched_gt_indices:
            gt_line_norm = normalize_line(gt_line)
            if not gt_line_norm:
                continue
            gt_for_wer = normalize_for_wer(gt_line_norm)
            gt_for_cer = normalize_for_cer(gt_line_norm)
            if gt_for_wer:
                ref_words = len(gt_for_wer.split())
                total_ref_words += ref_words
                total_word_errors += ref_words
            if gt_for_cer:
                ref_chars = len(gt_for_cer)
                total_ref_chars += ref_chars
                total_char_errors += ref_chars
    
    # Count unmatched consolidation lines as insertions (errors).
    # Note: Insertions don't increase the reference length, only error count.
    for cons_idx, cons_line in enumerate(consolidation_lines):
        if cons_idx not in matched_cons_indices:
            cons_line_norm = normalize_line(cons_line)
            if not cons_line_norm:
                continue
            cons_for_wer = normalize_for_wer(cons_line_norm)
            cons_for_cer = normalize_for_cer(cons_line_norm)
            if cons_for_wer:
                total_word_errors += len(cons_for_wer.split())
            if cons_for_cer:
                total_char_errors += len(cons_for_cer)
    
    # Calculate final WER and CER
    if total_ref_words == 0:
        wer = 0.0
    else:
        wer = (total_word_errors / total_ref_words) * 100
    
    if total_ref_chars == 0:
        cer = 0.0
    else:
        cer = (total_char_errors / total_ref_chars) * 100
    
    details = {
        # Backward-compatible key names (now they mean "spans"/"lines used by spans")
        "matched_lines": len(span_matches),
        "unmatched_gt_lines": len(
            [i for i, line in enumerate(gt_lines) if i not in matched_gt_indices and normalize_line(line)]
        ),
        "unmatched_cons_lines": len(
            [
                i
                for i, line in enumerate(consolidation_lines)
                if i not in matched_cons_indices and normalize_line(line)
            ]
        ),
        "matched_spans": len(span_matches),
        "total_ref_words": total_ref_words,
        "total_ref_chars": total_ref_chars,
    }
    
    return wer, cer, details


# Validate DATATYPE
if DATATYPE not in ("consolidation", "alignment"):
    raise ValueError(f"DATATYPE must be 'consolidation' or 'alignment', got '{DATATYPE}'")

# Combine run_version and branch_version for cache
if branch_version:
    cache_version = f"{run_version}{branch_version}"
else:
    cache_version = run_version

# Process each folder
print("=" * 50)
print(f"Comparing {DATATYPE} to ground truth")
print(f"Run version: {run_version}")
if branch_version:
    print(f"Branch version: {branch_version}")
print(f"Cache version: {cache_version}")
print("=" * 50)

match_percentages = []
alphanumeric_match_percentages = []
wer_values = []
cer_values = []

for folder_name in folder_names:
    print(f"\n{folder_name}:")
    
    base_folder = Path(folder_name)
    
    # Load ground truth
    gt_path = base_folder / "gt.txt"
    if not gt_path.exists():
        print(f"Warning: {gt_path} not found, skipping...")
        continue
    
    with open(gt_path, 'r', encoding='utf-8') as f:
        gt_text = f.read()
    
    # Load data based on DATATYPE
    try:
        if DATATYPE == "consolidation":
            cache_data = load_consolidation_cache(base_folder, cache_version)
            data_text = cache_data["data"]["consolidation"]
        else:  # DATATYPE == "alignment"
            cache_data = load_alignment_cache(base_folder, cache_version)
            data_text = cache_data["data"]["alignment"]
    except FileNotFoundError:
        print(f"Warning: {DATATYPE.capitalize()} cache not found for run_{cache_version}, skipping...")
        continue
    
    # Print texts
    print("Ground truth:")
    print(gt_text)
    print(f"\n{DATATYPE.capitalize()}:")
    print(data_text)
    
    # Compare (all characters)
    match_percentage, mismatch_count = compare_texts(gt_text, data_text, alphanumeric_only=False)
    
    # Compare (alphanumeric only)
    alphanumeric_match_percentage, alphanumeric_mismatch_count = compare_texts(gt_text, data_text, alphanumeric_only=True)
    
    print(f"\nMatch percentage (all characters): {match_percentage:.2f}%")
    print(f"Mismatch character count (all characters): {mismatch_count}")
    print(f"Match percentage (alphanumeric only): {alphanumeric_match_percentage:.2f}%")
    print(f"Mismatch character count (alphanumeric only): {alphanumeric_mismatch_count}")
    
    # Calculate WER and CER using two-level span matching + normalization
    wer, cer, details = calculate_wer_cer_two_level(gt_text, data_text)
    print(f"\nWord Error Rate (WER, span-matched, normalized): {wer:.2f}%")
    print(f"Character Error Rate (CER, span-matched, normalized): {cer:.2f}%")
    print(f"Matched spans: {details['matched_spans']}")
    print(f"Unmatched GT lines: {details['unmatched_gt_lines']}")
    print(f"Unmatched {DATATYPE} lines: {details['unmatched_cons_lines']}")
    
    match_percentages.append(match_percentage)
    alphanumeric_match_percentages.append(alphanumeric_match_percentage)
    wer_values.append(wer)
    cer_values.append(cer)

# Calculate and print averages
if match_percentages:
    average_match = sum(match_percentages) / len(match_percentages)
    average_alphanumeric_match = sum(alphanumeric_match_percentages) / len(alphanumeric_match_percentages)
    average_wer = sum(wer_values) / len(wer_values)
    average_cer = sum(cer_values) / len(cer_values)
    print("\n" + "=" * 50)
    print(f"Average match percentage (all characters): {average_match:.2f}%")
    print(f"Average match percentage (alphanumeric only): {average_alphanumeric_match:.2f}%")
    print(f"Average Word Error Rate (WER, span-matched, normalized): {average_wer:.2f}%")
    print(f"Average Character Error Rate (CER, span-matched, normalized): {average_cer:.2f}%")
    print("=" * 50)

