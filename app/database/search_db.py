"""Search for localities in the database using hybrid fuzzy matching."""
import sys
from pathlib import Path
import time

# Add parent directory to path to import db_utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db_utils import get_db_connection
from Levenshtein import distance as levenshtein_distance


search_string = "sammatti"

# Number of candidates to fetch from database using pg_trgm (fast filtering)
# Then we'll calculate Levenshtein distance for these and re-sort
CANDIDATE_LIMIT = 200

# Number of final results to display
RESULT_LIMIT = 200


def main():
    """
    Search for search_string using hybrid approach:
    1. Fast filtering with pg_trgm similarity (uses GIN index)
    2. Precise ranking with Levenshtein distance in Python
    3. Sort by Levenshtein distance (lower = better match)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Step 1: Get top candidates using pg_trgm (fast with GIN index)
            # This efficiently filters from potentially millions of rows
            query = """
                SELECT id, feature_class, name, source, updated,
                       similarity(name, %s) AS sim_score
                FROM localities_finland
                WHERE similarity(name, %s) > 0.1
                ORDER BY sim_score DESC, name
                LIMIT %s
            """
            
            time_start = time.time()
            cur.execute(query, (search_string, search_string, CANDIDATE_LIMIT))
            candidates = cur.fetchall()
            time_end = time.time()
            print(f"Time taken: {time_end - time_start} seconds")

            if not candidates:
                print(f"No matches found for '{search_string}'")
                return
            
            # Step 2: Calculate Levenshtein distance for each candidate
            # Levenshtein distance: lower = better match (0 = exact match)
            results_with_levenshtein = []
            for row in candidates:
                id_val, feature_class, name, source, updated, sim_score = row
                lev_dist = levenshtein_distance(search_string.lower(), name.lower())
                results_with_levenshtein.append({
                    'id': id_val,
                    'feature_class': feature_class,
                    'name': name,
                    'source': source,
                    'updated': updated,
                    'sim_score': sim_score,
                    'levenshtein': lev_dist
                })
            
            # Step 3: Sort by Levenshtein distance (primary), then by similarity (secondary)
            results_with_levenshtein.sort(key=lambda x: (x['levenshtein'], -x['sim_score']))
            
            # Step 4: Display top results
            final_results = results_with_levenshtein[:RESULT_LIMIT]
            
            print(f"Found {len(candidates)} candidate(s) for '{search_string}', showing top {len(final_results)}:")
            print(f"{'ID':<8} {'Class':<6} {'Name':<40} {'Source':<8} {'Sim':<8} {'Lev':<6} {'Updated'}")
            print("-" * 110)
            for result in final_results:
                print(f"{result['id']:<8} "
                      f"{result['feature_class'] or '':<6} "
                      f"{result['name']:<40} "
                      f"{result['source']:<8} "
                      f"{result['sim_score']:<8.4f} "
                      f"{result['levenshtein']:<6} "
                      f"{result['updated']}")
    
    except Exception as e:
        print(f"Error searching database: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
