# Script that verifies transcript data against ground truth

from cache_utils import load_consolidation_cache
from pathlib import Path
from collections import Counter
import unicodedata


# Configuration
# List of folder names to process. Each contain images from a single specimen.
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


# Combine run_version and branch_version for consolidation cache
if branch_version:
    consolidation_version = f"{run_version}{branch_version}"
else:
    consolidation_version = run_version

# Process each folder
print("=" * 50)
print("Comparing consolidation to ground truth")
print(f"Run version: {run_version}")
if branch_version:
    print(f"Branch version: {branch_version}")
print(f"Consolidation version: {consolidation_version}")
print("=" * 50)

match_percentages = []
alphanumeric_match_percentages = []

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
    
    # Load consolidation
    try:
        cache_data = load_consolidation_cache(base_folder, consolidation_version)
        consolidation_text = cache_data["data"]["consolidation"]
    except FileNotFoundError:
        print(f"Warning: Consolidation cache not found for run_{consolidation_version}, skipping...")
        continue
    
    # Print texts
    print("Ground truth:")
    print(gt_text)
    print("\nConsolidation:")
    print(consolidation_text)
    
    # Compare (all characters)
    match_percentage, mismatch_count = compare_texts(gt_text, consolidation_text, alphanumeric_only=False)
    
    # Compare (alphanumeric only)
    alphanumeric_match_percentage, alphanumeric_mismatch_count = compare_texts(gt_text, consolidation_text, alphanumeric_only=True)
    
    print(f"\nMatch percentage (all characters): {match_percentage:.2f}%")
    print(f"Mismatch character count (all characters): {mismatch_count}")
    print(f"Match percentage (alphanumeric only): {alphanumeric_match_percentage:.2f}%")
    print(f"Mismatch character count (alphanumeric only): {alphanumeric_mismatch_count}")
    
    match_percentages.append(match_percentage)
    alphanumeric_match_percentages.append(alphanumeric_match_percentage)

# Calculate and print averages
if match_percentages:
    average_match = sum(match_percentages) / len(match_percentages)
    average_alphanumeric_match = sum(alphanumeric_match_percentages) / len(alphanumeric_match_percentages)
    print("\n" + "=" * 50)
    print(f"Average match percentage (all characters): {average_match:.2f}%")
    print(f"Average match percentage (alphanumeric only): {average_alphanumeric_match:.2f}%")
    print("=" * 50)

