# Grontia — Airflow

Local Airflow 3 stack with S3-compatible storage (SeaweedFS), run with Docker Compose.

## Run

```bash
cp ../../.env.example .env
docker compose up -d
docker compose ps
```

- Airflow UI: http://localhost:8080, user `admin`. Airflow 3's simple auth
  manager generates the password on first start; read it with
  `docker exec airflow-airflow-api-server-1 cat /opt/airflow/simple_auth_manager_passwords.json.generated`
  (it changes when the api-server container is recreated)
- S3 API: http://localhost:9000, file browser: http://localhost:8888/buckets/

## Contents

| Path | What |
|---|---|
| `dags/00_smoke_test.py` | Airflow health check |
| `dags/01_read_datasets_catalog.py` | Validates the dataset catalog |
| `dags/11_cbs_bronze_ingestion_local.py` | Legacy: CBS ingestion to local disk (`include/output/`) |
| `dags/30_multi_source_daily_ingestion.py` | **Main DAG**: CBS, PDOK, KNMI and NDW into the `bronze` bucket, daily |
| `dags/40_silver_and_marts.py` | Bronze → silver (PySpark) → `dbt build`, triggered by DAG 30 |
| `dags/50_evaluate_address.py` | Manual: evaluate one address from a form in the UI |
| `dags/60_ndw_traffic_live.py` | NDW traffic snapshot every 5 minutes, hourly rollup, pruning |
| `dags/grontia_assets.py` | Asset that links DAG 30 to DAG 40 |
| `dags/grontia_alerts.py` | Failure and data-quality alerts (log, webhook, e-mail) |
| `dags/grontia_ndw.py` | NDW DATEX II XML parser shared by DAGs 30 and 60 |
| `include/configs/datasets.yml` | Dataset catalog read by the DAGs |
| `docker-compose.yml` | Postgres, Airflow services, SeaweedFS and bucket creation |
| `Dockerfile`, `requirements.txt` | Airflow image with project dependencies |
| `requirements-pipeline.txt` | PySpark + dbt, installed in a separate venv inside the image |

Trigger the main DAG from the UI or with:

```bash
docker compose exec airflow-scheduler airflow dags trigger 30_multi_source_daily_ingestion
```

When DAG 30 finishes it publishes the `s3://bronze/daily` asset, which starts
DAG 40 (silver + dbt). The marts land in `/opt/grontia/warehouse/grontia.duckdb`
(volume `warehouse`). To reprocess a date, trigger DAG 40 with
`{"ingestion_date": "YYYY-MM-DD"}`.

## Running unattended

- Every long-running service has `restart: unless-stopped`, so the stack comes
  back with Docker. `docker compose stop` keeps it stopped.
- After a reboot Docker starts all containers at once; `autoheal` restarts any
  Airflow container that stays unhealthy (e.g. it started before Postgres).
- With `catchup=False`, a run missed while the machine was off runs once as soon
  as the scheduler is back.
- On Windows + WSL, Docker only runs while WSL does. Register a logon task that
  starts WSL: `scripts/local/register-wsl-autostart.ps1` (run in PowerShell).
  Also disable sleep while plugged in, or the 00:00 UTC run waits for wake-up.

Manual steps outside Airflow are in [`docs/PROJETO_COMPLETO.md`](../../docs/PROJETO_COMPLETO.md#13-como-rodar-o-pipeline-completo).
