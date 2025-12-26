'''
Script to find problematic rows.
'''

import csv
import json
import sys
from pathlib import Path
from collections import defaultdict

# FinBIF occurrences data file
input_path = "./secret/HBF.114852-old-invertebrate-specimens/occurrences_random_sample.txt"
input_path = "./secret/HBF.114852-old-invertebrate-specimens/occurrences.txt"

# Output file
output_path = "./data/problematic.txt"

# Target strings to search for (rows that contain these)
target_strings = [
    "hyönteistietokanta:1611-3430-4198-2848",
    "hyönteistietokanta:2352-0990-6557-1404",
    "hyönteistietokanta:9743-1844-6551-7528"
]

# Increase CSV field size limit to handle large fields
csv.field_size_limit(10 * 1024 * 1024)

# Read all lines from the file
with open(input_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find problematic rows and their context
results = []
for i, line in enumerate(lines):
    # Check if line contains any of the target strings
    for target in target_strings:
        if target in line:
            # Get previous, current, and next lines
            prev_line = lines[i - 1].rstrip('\n') if i > 0 else ""
            curr_line = line.rstrip('\n')
            next_line = lines[i + 1].rstrip('\n') if i < len(lines) - 1 else ""
            
            results.append({
                'prev': prev_line,
                'curr': curr_line,
                'next': next_line
            })
            break  # Only add once per line

# Write results to output file
with open(output_path, 'w', encoding='utf-8') as f:
    for i, result in enumerate(results):
        if i > 0:
            f.write("--\n")
        f.write(result['prev'] + "\n")
        f.write(result['curr'] + "\n")
        f.write(result['next'] + "\n")

print(f"Found {len(results)} problematic rows. Output saved to {output_path}")

