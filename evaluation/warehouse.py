"""Read-only access to the DuckDB warehouse built by dbt."""
from __future__ import annotations

import os

import duckdb

DUCKDB_PATH = os.getenv("GRONTIA_DUCKDB_PATH", "/opt/grontia/warehouse/grontia.duckdb")
# Must match the md: path of the motherduck output in analytics/dbt/profiles.yml
MOTHERDUCK_DB = "grontia"
STAGING, MARTS = '"main_STAGING"', '"main_MARTS"'


_motherduck: duckdb.DuckDBPyConnection | None = None


def connect() -> duckdb.DuckDBPyConnection:
    # Read wherever DAG 40 publishes: with the motherduck target the local file
    # is no longer refreshed, and staging there is made of tables (no S3 needed)
    if os.getenv("GRONTIA_DBT_TARGET") == "motherduck":
        # Opening md: costs a few seconds, so the web app shares one connection
        # and hands each request its own cursor (DuckDB's per-thread handle)
        global _motherduck
        if _motherduck is None:
            _motherduck = duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")
        return _motherduck.cursor()

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    # Staging models are views over the silver Parquet files in S3
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    con.execute("install httpfs; load httpfs;")
    con.execute(f"set s3_endpoint='{endpoint.replace('http://', '').replace('https://', '')}'")
    con.execute(f"set s3_use_ssl={'true' if endpoint.startswith('https') else 'false'}")
    con.execute("set s3_url_style='path'")
    con.execute(f"set s3_access_key_id='{os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}'")
    con.execute(f"set s3_secret_access_key='{os.getenv('MINIO_SECRET_KEY', 'minioadmin')}'")
    return con


def rows(con: duckdb.DuckDBPyConnection, sql: str, *params) -> list[dict]:
    cur = con.execute(sql, list(params))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def one(con: duckdb.DuckDBPyConnection, sql: str, *params) -> dict | None:
    r = rows(con, sql, *params)
    return r[0] if r else None
