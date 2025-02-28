import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()

config.read('dwh.cfg')
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
ARN = config['IAM_ROLE']['ARN']


def drop_tables(cur, conn):
    """Drop existing database tables to ensure clean schema setup.

    Executes each SQL query in the global `drop_table_queries` list (expected DROP TABLE IF EXISTS commands).
    Commits transaction after each query execution to maintain database consistency.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor for executing SQL commands
        conn (psycopg2.extensions.connection): Database connection for transaction commits

    Note:
        Requires pre-defined global list `drop_table_queries` containing DROP statements.
        Designed to be idempotent - safe for repeated execution.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Initialize database schema by creating fact and dimension tables.

    Executes each SQL query in the global `create_table_queries` list (expected CREATE TABLE statements).
    Commits transaction after each query execution to ensure schema changes persist.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor for executing SQL commands
        conn (psycopg2.extensions.connection): Database connection for transaction commits

    Note:
        Requires pre-defined global list `create_table_queries` containing CREATE statements.
        Typically follows drop_tables() to ensure fresh schema creation.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Orchestrates database schema management lifecycle.

    Process Flow:
    1. Loads cluster configuration from dwh.cfg
    2. Establishes database connection
    3. Drops existing tables (if any)
    4. Creates fresh tables with defined schema
    5. Ensures proper connection cleanup

    Typical Use Case:
    - Database initialization
    - Schema reset during development/testing
    - Infrastructure-as-code deployments

    Note:
        Requires valid [CLUSTER] section in dwh.cfg with host, dbname, user, password, port.
        Execution order is critical: drop_tables() must precede create_tables().
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
