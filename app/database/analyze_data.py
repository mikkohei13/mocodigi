import csv
import json
import sys
from pathlib import Path
from collections import defaultdict

# Field name to analyze (change this to analyze different fields)
field_name = "recordedBy"
field_name = "county"
field_name = "verbatimLocality"

# Filter by country: True for Finnish (FI), False for international (not FI)
finnish = False

# FinBIF occurrences data file
input_path = "./secret/HBF.114852-old-invertebrate-specimens/occurrences_random_sample.txt"
input_path = "./secret/HBF.114852-old-invertebrate-specimens/occurrences.txt"

# Output path based on field name and country filter
prefix = "FI" if finnish else "INTL"
output_path = f"./data/{prefix}-{field_name}.json"

# Dictionary to count occurrences
name_counts = defaultdict(int)

# Increase field size limit to handle large fields
csv.field_size_limit(sys.maxsize)

# Read the TSV file
# Note: occurrences.txt has 3 header rows:
# Row 1: Darwin Core (DwC) field names (used as column names)
# Row 2: Finnish field names (skip)
# Row 3: English field names (skip)
with open(input_path, 'r', encoding='utf-8') as f:
    # Use QUOTE_NONE to ignore quotation marks in data
    reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
    
    # Skip rows 2 and 3 (Finnish and English field names)
    next(reader, None)  # Skip row 2
    next(reader, None)  # Skip row 3
    
    # Process each row
    for row in reader:
        # Filter rows based on finnish boolean
        country_code = row.get('countryCode', '')
        if (finnish and country_code == 'FI') or (not finnish and country_code != 'FI'):
            field_value = row.get(field_name, '')
            
            if field_value:
                # Split by comma, trim whitespace, and convert to lowercase
                names = [name.strip().lower() for name in field_value.split(',')]
                
                # Count each name
                for name in names:
                    if name:  # Skip empty strings
                        name_counts[name] += 1

# Create output directory if it doesn't exist
output_dir = Path(output_path).parent
output_dir.mkdir(parents=True, exist_ok=True)

# Save as JSON
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(dict(name_counts), f, indent=2, ensure_ascii=False)

print(f"Processed {len(name_counts)} unique {field_name} values")
print(f"Results saved to {output_path}")
