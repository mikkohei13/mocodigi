"""Database connection utilities."""
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_db_connection():
    """
    Create and return a database connection using environment variables.
    
    Returns:
        psycopg2.connection: Database connection object
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "testuser"),
        password=os.getenv("DB_PASSWORD", "testpass"),
        database=os.getenv("DB_NAME", "testdb")
    )


def execute_sql_file(conn, sql_file_path):
    """
    Execute SQL commands from a file.
    
    Args:
        conn: Database connection
        sql_file_path: Path to SQL file
    """
    with open(sql_file_path, 'r') as f:
        sql = f.read()
    
    with conn.cursor() as cur:
        cur.execute(sql)
    
    conn.commit()

