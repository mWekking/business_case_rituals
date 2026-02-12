import duckdb
from datetime import datetime, timedelta, timezone
from pathlib import Path

## Create DuckDB tables


def connect(db_path):
    # Make sure the parent folder exists before opening the database file.
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Open (or create) the DuckDB database.
    return duckdb.connect(db_path)


def run_sql_file(connection, path):
    # Read a SQL file from disk and execute it as one script.
    with open(path, "r") as f:
        connection.execute(f.read())


def init_raw_tables(connection):
    # Create all raw GitHub tables from SQL files.
    # JSON columns store the original API payload untouched.
    run_sql_file(connection, "extract/sql/sys_pull_requests.sql")
    run_sql_file(connection, "extract/sql/sys_issues.sql")
    run_sql_file(connection, "extract/sql/sys_commits.sql")
