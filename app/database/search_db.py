"""Search for localities in the database."""
import sys
from pathlib import Path

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection


search_string = "Kuha"


def main():
    """Search for search_string in the database and print matches."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Search for matches in the name field (case-insensitive)
            query = """
                SELECT id, feature_class, name, source, updated
                FROM localities_finland
                WHERE name ILIKE %s
                ORDER BY name
            """
            
            # Use % for pattern matching (matches anywhere in the name)
            search_pattern = f"%{search_string}%"
            cur.execute(query, (search_pattern,))
            
            results = cur.fetchall()
            
            if results:
                print(f"Found {len(results)} match(es) for '{search_string}':")
                for row in results:
                    id_val, feature_class, name, source, updated = row
                    print(f"{id_val}, {feature_class}, {name}, {source}, {updated}")
            else:
                print(f"No matches found for '{search_string}'")
    
    except Exception as e:
        print(f"Error searching database: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
