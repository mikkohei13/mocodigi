"""Populate localities_finland table from GeoNames data file."""
import sys
import csv
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection


def main(max_rows=100):
    """Read top max_rows from input file and populate database."""
    input_file = "./data/FI.txt"
    
    print(f"Reading data from {input_file}...")
    
    conn = get_db_connection()
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            # Read TSV file
            reader = csv.DictReader(f, delimiter='\t')
            
            # Read top rows
            rows_to_insert = []
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                
                # Skip rows where feature_class is spot, undersea or road
                if row.get('feature_class') in ('S', 'U', 'R'):
                    continue
                
                rows_to_insert.append({
                    'id': int(row['geonameid']),
                    'name': row['name'],
                    'source': 'geonames',
                    'updated': datetime.now()
                })
        
        # Insert into database
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO localities_finland (id, name, source, updated)
                VALUES (%(id)s, %(name)s, %(source)s, %(updated)s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    source = EXCLUDED.source,
                    updated = EXCLUDED.updated
            """
            
            cur.executemany(insert_query, rows_to_insert)
            conn.commit()
            
            print(f"Successfully inserted {len(rows_to_insert)} rows into localities_finland table.")
    
    except Exception as e:
        print(f"Error populating database: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main(1000000)
