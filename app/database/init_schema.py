"""Initialize database schema."""
import sys
from pathlib import Path

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection, execute_sql_file


def main():
    """Initialize database schema from schema.sql"""
    schema_file = Path(__file__).parent / "schema.sql"
    
    print(f"Initializing schema from {schema_file}...")
    
    conn = get_db_connection()
    try:
        execute_sql_file(conn, schema_file)
        print("Schema initialized successfully.")
    except Exception as e:
        print(f"Error initializing schema: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

