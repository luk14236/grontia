# Grontia — Airflow

Local Airflow 3 stack with S3-compatible storage (SeaweedFS), run with Docker Compose.

## Run

```bash
cp ../../.env.example .env
docker compose up -d
docker compose ps
```

- Airflow UI: http://localhost:8080 (admin / admin)
- S3 API: http://localhost:9000, file browser: http://localhost:8888/buckets/

## Contents

| Path | What |
|---|---|
| `dags/00_smoke_test.py` | Airflow health check |
| `dags/01_read_datasets_catalog.py` | Validates the dataset catalog |
| `dags/11_cbs_bronze_ingestion_local.py` | Legacy: CBS ingestion to local disk (`include/output/`) |
| `dags/30_multi_source_daily_ingestion.py` | **Main DAG**: CBS, PDOK, KNMI and NDW into the `bronze` bucket, daily |
| `include/configs/datasets.yml` | Dataset catalog read by the DAGs |
| `docker-compose.yml` | Postgres, Airflow services, SeaweedFS and bucket creation |
| `Dockerfile`, `requirements.txt` | Airflow image with project dependencies |

Trigger the main DAG from the UI or with:

```bash
docker compose exec airflow-scheduler airflow dags trigger 30_multi_source_daily_ingestion
```

Next steps (bronze → silver → dbt) are in [`docs/PROJETO_COMPLETO.md`](../../docs/PROJETO_COMPLETO.md#13-como-rodar-o-pipeline-completo).
