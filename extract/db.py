import duckdb
from datetime import datetime, timedelta, timezone
from pathlib import Path

## Create DuckDB tables

def connect(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)

def run_sql_file(connection, path):
    with open(path, "r") as f:
        connection.execute(f.read())

def init_raw_tables(connection):
    # Use JSON type for raw payload. DuckDB accepts JSON strings inserted into JSON columns.
    run_sql_file(connection, "extract/sql/sys_pull_requests.sql")
    run_sql_file(connection, "extract/sql/sys_issues.sql")
    run_sql_file(connection, "extract/sql/sys_commits.sql")
