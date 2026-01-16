from image_utils import get_image_files_from_folder
from cache_utils import (
    load_cache,
    alignment_cache_exists,
    load_alignment_cache,
    save_alignment_cache
)
from pathlib import Path
import re


def is_contained_or_duplicate(s1: str, s2: str, similarity_threshold: float = 0.80) -> tuple[bool, str]:
    """
    Check if one string is contained in another or if they are duplicates.
    
    Args:
        s1: First string
        s2: Second string
        similarity_threshold: Threshold for considering strings similar (0.0-1.0)
        
    Returns:
        Tuple of (is_duplicate, action) where action is:
        - "s1_contains_s2": s1 contains s2, use s1
        - "s2_contains_s1": s2 contains s1, use s2
        - "": not duplicates
    """
    s1_lower = s1.lower()
    s2_lower = s2.lower()
    
    # Check if one is contained in the other (exact match)
    if s1_lower in s2_lower:
        return (True, "s2_contains_s1")
    if s2_lower in s1_lower:
        return (True, "s1_contains_s2")
    
    # Normalize by removing whitespace and punctuation differences for comparison
    # Keep only alphanumeric and common separators
    s1_normalized = re.sub(r'[^\w\s]', '', s1_lower)
    s2_normalized = re.sub(r'[^\w\s]', '', s2_lower)
    
    # Remove all whitespace for containment check
    s1_compact = ''.join(s1_normalized.split())
    s2_compact = ''.join(s2_normalized.split())
    
    if s1_compact in s2_compact:
        return (True, "s2_contains_s1")
    if s2_compact in s1_compact:
        return (True, "s1_contains_s2")
    
    # Check similarity by finding longest common subsequence
    # If a large portion of one string is in the other, they're likely duplicates
    if len(s1_compact) == 0 or len(s2_compact) == 0:
        return (False, "")
    
    # Find longest common substring
    def lcs_length(s1, s2):
        """Find length of longest common substring."""
        m, n = len(s1), len(s2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        max_len = 0
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if s1[i-1] == s2[j-1]:
                    dp[i][j] = dp[i-1][j-1] + 1
                    max_len = max(max_len, dp[i][j])
                else:
                    dp[i][j] = 0
        return max_len
    
    lcs = lcs_length(s1_compact, s2_compact)
    min_len = min(len(s1_compact), len(s2_compact))
    
    if min_len > 0 and lcs / min_len >= similarity_threshold:
        # They're very similar, prefer the longer one
        if len(s1) >= len(s2):
            return (True, "s1_contains_s2")
        else:
            return (True, "s2_contains_s1")
    
    return (False, "")


def find_overlap(s1: str, s2: str, min_overlap: int = 3) -> tuple[int, str]:
    """
    Find the best overlap between two strings (case-insensitive).
    
    Args:
        s1: First string
        s2: Second string
        min_overlap: Minimum overlap length required
        
    Returns:
        Tuple of (overlap_length, merge_direction) where merge_direction is:
        - "s1_then_s2": s2's prefix overlaps with s1's suffix (merge: s1 + s2[overlap:])
        - "s2_then_s1": s1's prefix overlaps with s2's suffix (merge: s2 + s1[overlap:])
        - "": no sufficient overlap found
    """
    s1_lower = s1.lower()
    s2_lower = s2.lower()
    
    max_overlap = 0
    best_direction = ""
    
    # Check if s2's prefix overlaps with s1's suffix (s1 then s2)
    for i in range(min_overlap, min(len(s1_lower), len(s2_lower)) + 1):
        if s1_lower[-i:] == s2_lower[:i]:
            if i > max_overlap:
                max_overlap = i
                best_direction = "s1_then_s2"
    
    # Check if s1's prefix overlaps with s2's suffix (s2 then s1)
    for i in range(min_overlap, min(len(s1_lower), len(s2_lower)) + 1):
        if s2_lower[-i:] == s1_lower[:i]:
            if i > max_overlap:
                max_overlap = i
                best_direction = "s2_then_s1"
    
    return (max_overlap, best_direction)


def merge_fragments(fragments: list[str], min_overlap: int = 3) -> str:
    """
    Merge fragments using Overlap-Layout-Consensus algorithm.
    
    Args:
        fragments: List of text fragments to merge
        min_overlap: Minimum overlap length required for merging
        
    Returns:
        Merged text string
    """
    if not fragments:
        return ""
    
    if len(fragments) == 1:
        return fragments[0]
    
    # Filter out empty fragments
    fragments = [f.strip() for f in fragments if f.strip()]
    
    if not fragments:
        return ""
    
    # First pass: remove duplicates and contained fragments
    # Check if any fragment is contained in or duplicates another
    fragments_to_remove = set()
    for i in range(len(fragments)):
        if i in fragments_to_remove:
            continue
        for j in range(i + 1, len(fragments)):
            if j in fragments_to_remove:
                continue
            is_dup, action = is_contained_or_duplicate(fragments[i], fragments[j])
            if is_dup:
                if action == "s1_contains_s2":
                    # fragments[i] contains fragments[j], remove j
                    fragments_to_remove.add(j)
                elif action == "s2_contains_s1":
                    # fragments[j] contains fragments[i], remove i
                    fragments_to_remove.add(i)
                    break  # i is removed, move to next i
    
    # Remove duplicate/contained fragments
    fragments = [f for idx, f in enumerate(fragments) if idx not in fragments_to_remove]
    
    if len(fragments) == 1:
        return fragments[0]
    
    if not fragments:
        return ""
    
    # Greedy OLC: repeatedly find best overlap and merge
    while len(fragments) > 1:
        best_overlap = 0
        best_i = 0
        best_j = 0
        best_direction = ""
        
        # Find the pair with the best overlap
        for i in range(len(fragments)):
            for j in range(i + 1, len(fragments)):
                # Check for duplicates again (in case merging created new duplicates)
                is_dup, action = is_contained_or_duplicate(fragments[i], fragments[j])
                if is_dup:
                    # Skip this pair, one is a duplicate of the other
                    continue
                
                overlap, direction = find_overlap(fragments[i], fragments[j], min_overlap)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_i = i
                    best_j = j
                    best_direction = direction
        
        if best_overlap == 0:
            # No overlaps found, check if any are duplicates one more time
            # If all remaining fragments are unique and don't overlap, return the longest/most complete
            if len(fragments) > 1:
                # Return the longest fragment as it's likely the most complete
                return max(fragments, key=len)
            return fragments[0] if fragments else ""
        
        # Merge the best pair
        if best_direction == "s1_then_s2":
            # s2's prefix overlaps with s1's suffix
            # Merge: s1 + s2[overlap:]
            merged = fragments[best_i] + fragments[best_j][best_overlap:]
        else:  # best_direction == "s2_then_s1"
            # s1's prefix overlaps with s2's suffix
            # Merge: s2 + s1[overlap:]
            merged = fragments[best_j] + fragments[best_i][best_overlap:]
        
        # Remove the two fragments and add the merged one
        fragments = [f for idx, f in enumerate(fragments) if idx not in (best_i, best_j)]
        fragments.append(merged)
    
    return fragments[0] if fragments else ""


def align_transcripts(transcripts: list[str], min_overlap: int = 3) -> str:
    """
    Align multiple transcripts using OLC algorithm.
    
    Args:
        transcripts: List of transcript strings
        min_overlap: Minimum overlap length required for merging
        
    Returns:
        Aligned and merged transcript string
    """
    # Clean and normalize transcripts
    cleaned_transcripts = []
    for transcript in transcripts:
        # Remove extra whitespace but preserve structure
        cleaned = transcript.strip()
        if cleaned:
            cleaned_transcripts.append(cleaned)
    
    if not cleaned_transcripts:
        return ""
    
    # Use OLC to merge fragments
    aligned = merge_fragments(cleaned_transcripts, min_overlap)
    
    return aligned


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

run_version = "16"
branch_version = ""  # Set to empty string to use just run_version, or e.g. "b" for "15b"

# Combine run_version and branch_version for alignment cache
alignment_version = f"{run_version}{branch_version}"

# Minimum overlap length for merging fragments
min_overlap = 3

print(f"Run version: {run_version}")
print(f"Branch version: {branch_version}")
print(f"Alignment version: {alignment_version}")
print(f"Min overlap: {min_overlap}")
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
    
    # Check if alignment cache exists
    if alignment_cache_exists(base_folder, alignment_version):
        print("Alignment cache found, loading from cache...")
        cache_data = load_alignment_cache(base_folder, alignment_version)
        alignment_text = cache_data["data"]["alignment"]
        print("(Loaded from cache)")
    else:
        print("No alignment cache found, generating alignment...")
        
        # Collect all transcripts from images in this folder
        all_transcripts = []
        for image_file in image_files:
            try:
                cache_data = load_cache(image_file, run_version)
                transcript = cache_data["data"]["transcript"]

                all_transcripts.append(transcript)
                print(f"Loaded transcript from {image_file.name}")
            except FileNotFoundError:
                print(f"Warning: No transcript cache found for {image_file.name}, skipping...")
                continue
        
        if not all_transcripts:
            print(f"No transcripts found to align for '{folder_name}', skipping...")
            continue
        
        print(f"Collected {len(all_transcripts)} transcript(s)")
        
        # Align transcripts using OLC algorithm
        print("Aligning transcripts using Overlap-Layout-Consensus...")
        alignment_text = align_transcripts(all_transcripts, min_overlap)
        
        # Prepare concatenated transcripts for cache (similar to consolidate.py format)
        transcripts_content = ""
        transcript_count = 0
        for transcript in all_transcripts:
            transcript_count += 1
            transcripts_content += f"## Transcript {transcript_count}:\n"
            transcripts_content += f"\n{transcript}\n"
            transcripts_content += "\n"
        
        # Save to alignment cache
        cache_path = save_alignment_cache(
            base_folder=base_folder,
            raw_alignment=alignment_text,
            alignment=alignment_text,
            concatenated_transcripts=transcripts_content,
            model_name="OLC-alignment",
            prompt=f"Overlap-Layout-Consensus alignment with min_overlap={min_overlap}",
            temperature=0.0,
            run_version=alignment_version
        )
        print(f"Saved to alignment cache: {cache_path}")

    # Print the alignment result for this specimen
    print("\n" + "-" * 50)
    print(f"Alignment Result for {folder_name}:")
    print("-" * 50)
    print(alignment_text)
    print("-" * 50)

print("\n" + "=" * 50)
print("All specimens processed")
print("=" * 50)

