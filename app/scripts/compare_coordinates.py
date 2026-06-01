"""Join compiled_specimens.tsv (File A) with identify_coordinates.tsv (File B)
on document_long_id == specimen_id, prefixing columns with A. and B."""

import csv
import os

FILE_A = "/app/output/pipeline_runs/sensitive_specimens/compiled_specimens.tsv"
FILE_B = "/app/output/pipeline_runs/sensitive_specimens_e/identify_coordinates.tsv"
OUTPUT = "/app/output/pipeline_runs/sensitive_specimens/compare_coordinates.tsv"

KEY_A = "document_long_id"
KEY_B = "specimen_id"


def read_tsv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return list(reader), reader.fieldnames


rows_a, cols_a = read_tsv(FILE_A)
rows_b, cols_b = read_tsv(FILE_B)

# Index File B by its primary key for fast lookup
index_b = {row[KEY_B]: row for row in rows_b}

out_cols_a = [f"A.{c}" for c in cols_a]
out_cols_b = [f"B.{c}" for c in cols_b]
out_cols = out_cols_a + out_cols_b

empty_b = {f"B.{c}": "" for c in cols_b}

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=out_cols, delimiter="\t")
    writer.writeheader()
    for row_a in rows_a:
        out = {f"A.{k}": v for k, v in row_a.items()}
        match = index_b.get(row_a[KEY_A])
        if match:
            out.update({f"B.{k}": v for k, v in match.items()})
        else:
            out.update(empty_b)
        writer.writerow(out)

print(f"Written {len(rows_a)} rows to {OUTPUT}")
