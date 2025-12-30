"""Ensure all performance indexes exist and are optimized."""
import sys
from pathlib import Path

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection


def main():
    """Ensure pg_trgm extension and GIN index exist, and update statistics."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Ensure pg_trgm extension is enabled
            print("Ensuring pg_trgm extension is enabled...")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            conn.commit()
            print("✓ pg_trgm extension enabled")
            
            # 2. Ensure GIN index exists on name column
            print("Ensuring GIN index exists on name column...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_localities_finland_name_trgm 
                ON localities_finland USING GIN (name gin_trgm_ops);
            """)
            conn.commit()
            print("✓ GIN index on name column created/verified")
            
            # 3. Update table statistics for better query planning
            print("Updating table statistics...")
            cur.execute("ANALYZE localities_finland;")
            conn.commit()
            print("✓ Table statistics updated")
            
            # 4. Check index usage information
            print("\nChecking index information...")
            cur.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE tablename = 'localities_finland'
                ORDER BY indexname;
            """)
            indexes = cur.fetchall()
            
            if indexes:
                print(f"\nFound {len(indexes)} index(es) on localities_finland:")
                for schema, table, index, definition in indexes:
                    print(f"  - {index}")
                    print(f"    {definition}")
            else:
                print("  No indexes found (this is unexpected)")
            
            # 5. Check table size and row count
            print("\nChecking table statistics...")
            cur.execute("""
                SELECT 
                    COUNT(*) as row_count,
                    pg_size_pretty(pg_total_relation_size('localities_finland')) as total_size,
                    pg_size_pretty(pg_relation_size('localities_finland')) as table_size,
                    pg_size_pretty(pg_indexes_size('localities_finland')) as indexes_size
                FROM localities_finland;
            """)
            stats = cur.fetchone()
            if stats:
                row_count, total_size, table_size, indexes_size = stats
                print(f"  Rows: {row_count:,}")
                print(f"  Table size: {table_size}")
                print(f"  Indexes size: {indexes_size}")
                print(f"  Total size: {total_size}")
            
            print("\n✓ All indexes verified and optimized!")
    
    except Exception as e:
        print(f"Error ensuring indexes: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

