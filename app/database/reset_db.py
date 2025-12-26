"""Reset database by dropping and recreating tables."""
import sys
from pathlib import Path

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection, execute_sql_file


def main():
    """Drop all tables and recreate schema."""
    schema_file = Path(__file__).parent / "schema.sql"
    
    print("Resetting database...")
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Drop all tables (CASCADE to handle dependencies)
            cur.execute("DROP TABLE IF EXISTS localities CASCADE;")
            conn.commit()
            print("Dropped existing tables.")
        
        # Recreate schema
        execute_sql_file(conn, schema_file)
        print("Schema recreated successfully.")
    except Exception as e:
        print(f"Error resetting database: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

