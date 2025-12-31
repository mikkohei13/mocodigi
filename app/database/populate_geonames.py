"""Populate localities_finland table from GeoNames data file."""
import sys
import csv
from datetime import datetime
from pathlib import Path
import psycopg2.extras

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection


def main(max_rows=100, batch_size=50000):
    """Read top max_rows from input file and populate database in batches."""
    input_file = "./data/FI_sample.txt"
    input_file = "./data/allCountries_1k.txt"
    input_file = "./data/allCountries.txt"
    
    print(f"Reading data from {input_file}...")
    print(f"Batch size: {batch_size:,} rows")
    
    conn = get_db_connection()
    try:
        # Drop index for faster inserts
        print("Dropping index for faster inserts...")
        with conn.cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS idx_localities_finland_name_trgm;")
        conn.commit()
        
        insert_query = """
            INSERT INTO localities_finland (id, feature_class, name, source, updated)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                feature_class = EXCLUDED.feature_class,
                name = EXCLUDED.name,
                source = EXCLUDED.source,
                updated = EXCLUDED.updated
        """
        
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            
            # Skip header row
            next(reader, None)
            
            batch = []
            total_inserted = 0
            rows_processed = 0
            
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                
                # Skip rows where feature_class (index 6) is spot, undersea or road
                if len(row) > 6 and row[6] in ('S', 'U', 'R'):
                    continue
                
                rows_processed += 1
                batch.append((
                    int(row[0]),  # geonameid
                    row[6] if len(row) > 6 else None,  # feature_class
                    row[1],  # name
                    'geonames',  # source
                    datetime.now()  # updated
                ))
                
                # Insert batch when it reaches batch_size
                if len(batch) >= batch_size:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur, insert_query, batch, template=None, page_size=batch_size
                        )
                    conn.commit()
                    total_inserted += len(batch)
                    print(f"Inserted {total_inserted:,} rows (processed {rows_processed:,} from file)...")
                    batch = []
            
            # Insert remaining rows
            if batch:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur, insert_query, batch, template=None, page_size=len(batch)
                    )
                conn.commit()
                total_inserted += len(batch)
        
        print(f"Successfully inserted {total_inserted:,} rows into localities_finland table.")
        
        # Rebuild index
        print("Rebuilding index...")
        with conn.cursor() as cur:
            cur.execute("""
                CREATE INDEX idx_localities_finland_name_trgm 
                ON localities_finland USING GIN (name gin_trgm_ops)
            """)
        conn.commit()
        print("Index rebuilt successfully.")
    
    except Exception as e:
        print(f"Error populating database: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main(10000000)
