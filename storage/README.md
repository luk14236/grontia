# Storage

This folder documents storage conventions only. Data files must NOT be committed to git.

Storage is S3-compatible object storage, served locally by SeaweedFS (see `orchestration/airflow/docker-compose.yml`):

- S3 API: http://localhost:9000 (credentials `minioadmin` / `minioadmin`, env vars `MINIO_*`)
- File browser: http://localhost:8888/buckets/

Buckets:
- `bronze`: raw files as ingested, `{source}/{source}_bronze/{dataset}/ingestion_date=YYYY-MM-DD/`
- `silver`: cleaned and deduplicated Parquet, `{source}/{dataset}/`
- `gold`: reserved; the gold layer is built by dbt into DuckDB / MotherDuck
