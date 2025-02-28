import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data into staging tables from external sources (S3 buckets).

    Executes each SQL query in the global `copy_table_queries` list (expected to be COPY commands).
    Commits transaction after each query execution to ensure data integrity.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor for executing SQL commands.
        conn (psycopg2.extensions.connection): Database connection for transaction commits.

    Note:
        Requires pre-defined global list `copy_table_queries` containing valid COPY statements.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transform and load data from staging tables into analytical schema.

    Executes each SQL query in the global `insert_table_queries` list (INSERT-SELECT statements).
    Commits transaction after each query execution to maintain data consistency.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor for executing SQL commands.
        conn (psycopg2.extensions.connection): Database connection for transaction commits.

    Note:
        Requires pre-defined global list `insert_table_queries` containing valid INSERT statements.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Orchestrate the complete ETL pipeline.

    1. Loads configuration from dwh.cfg
    2. Establishes database connection
    3. Populates staging tables
    4. Transforms data into analytical tables
    5. Ensures proper resource cleanup

    Process Flow:
    - Parses cluster credentials from configuration file
    - Initializes database connection objects
    - Executes staging data loading via load_staging_tables()
    - Performs dimensional modeling via insert_tables()
    - Closes database connection upon completion

    Note:
        Requires valid [CLUSTER] section in dwh.cfg with host, dbname, user, password, port.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
