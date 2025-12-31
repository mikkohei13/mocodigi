"""Reset database by dropping and recreating tables."""
import sys
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection, execute_sql_file


def main():
    """Drop all tables and recreate schema. Force termination of all connections."""
    schema_file = Path(__file__).parent / "schema.sql"
    
    print("Resetting database...")
    
    conn = get_db_connection()
    try:
        # Set autocommit mode to avoid transaction locks
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            # Get our own connection PID
            cur.execute("SELECT pg_backend_pid();")
            our_pid = cur.fetchone()[0]
            
            # Terminate all other connections to the database (force disconnect)
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                AND pid != pg_backend_pid()
                AND pid != %s;
            """, (our_pid,))
            terminated = cur.rowcount
            if terminated > 0:
                print(f"Terminated {terminated} active connection(s).")
            
            # Drop all tables with CASCADE (force deletion)
            cur.execute("DROP TABLE IF EXISTS localities_finland CASCADE;")
            print("Dropped existing tables.")
            
            # Drop extension if it exists (will be recreated by schema)
            cur.execute("DROP EXTENSION IF EXISTS pg_trgm CASCADE;")
            print("Dropped extensions.")
        
        # Switch back to normal transaction mode for schema creation
        conn.set_isolation_level(0)  # Default isolation level
        
        # Recreate schema
        execute_sql_file(conn, schema_file)
        print("Schema recreated successfully.")
    except Exception as e:
        print(f"Error resetting database: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

