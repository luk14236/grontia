# Grontia – Project Structure Rationale

## 1. Purpose of This Document

This document explains **why the Grontia repository is structured the way it is**, and how this structure supports:

- Multiple independent data sources
- A shared analytical model
- Incremental evolution without large refactors
- A zero-cost, open-source stack that runs entirely on a laptop

---

## 2. High-Level Design Principles

### 2.1 Domain-Oriented Datasets

Each data source (CBS, PDOK, KNMI, NDW) has its own ingestion function, bronze prefix, silver folder and dbt staging folder. A broken source does not block the others: every dataset is an independent task in the DAG.

### 2.2 Separation of Concerns

The structure explicitly separates:

- **Orchestration** – when things run (Airflow)
- **Processing** – how raw data becomes clean data (PySpark)
- **Analytics & Modeling** – how clean data becomes answers (dbt)

### 2.3 Shared Analytical Model

Sources are ingested independently, but converge in dbt marts, keyed mainly by CBS `(region_code, period_code)`.

### 2.4 Open Source Only

Earlier iterations targeted Azure (ADLS, Container Apps) and Databricks. That was dropped: the project must run for free and without a cloud account. The removed code is still available in git history.

---

## 3. Top-Level Structure Overview

```text
grontia/
  orchestration/airflow/   when things run
  processing/local/        bronze -> silver
  analytics/dbt/           silver -> staging -> marts
  docs/                    documentation, decisions, diagrams
  .github/workflows/       CI
```

---

## 4. Orchestration Layer (`orchestration/airflow`)

**Responsibility:** fetch data from the sources on a schedule and land it, unchanged, in the bronze bucket.

**Key decisions:**

- Airflow 3 runs in Docker Compose together with SeaweedFS (S3-compatible storage)
- Ingestion is lightweight (HTTP + upload); heavy transformations do not run inside Airflow
- Datasets are declared in `include/configs/datasets.yml`, and the DAG builds its tasks from that file
- Each bronze partition (`ingestion_date=YYYY-MM-DD`) is cleared before writing, so re-runs are idempotent

The main DAG is `30_multi_source_daily_ingestion`. `00` and `01` are health checks; `11` is a legacy CBS-to-disk variant.

---

## 5. Processing Layer (`processing/local`)

**Responsibility:** turn raw bronze files into clean, deduplicated Parquet in the silver bucket.

`bronze_to_silver.py` has one transform per source (`transform_cbs`, `transform_pdok`, `transform_knmi`, `transform_ndw`). Each one flattens the raw format, adds ingestion metadata, deduplicates by natural key and overwrites only its own `ingestion_date` partition.

PySpark is used here on purpose, as a learning goal; the data volumes would also fit in DuckDB.

There is no Spark gold step: the gold layer is built only by dbt.

---

## 6. Analytics & Modeling Layer (`analytics/dbt`)

**Responsibility:** transform silver Parquet into business-ready analytical models, with tests.

dbt-duckdb reads Parquet straight from S3 (`httpfs`), materializes into a local DuckDB file (`dev`) or MotherDuck (`motherduck`).

### 6.1 Staging Models

Organized by source:

```text
models/staging/
  cbs/
  pdok/
  knmi/
  ndw/
```

Staging models standardize names (Dutch → English), cast types, keep the latest snapshot per natural key, and act as the contract between processing and analytics.

### 6.2 Marts (Gold Layer)

```text
models/marts/
```

Cross-source joins and aggregations live here. **No pipeline writes gold data directly**: all analytical logic flows through dbt.

An intermediate layer (`models/intermediate/`) will be introduced only when marts start repeating the same joins.

---

## 7. Documentation (`docs/`)

- `PROJETO_COMPLETO.md` – full project documentation (Portuguese)
- `architecture/decisions/` – decision records like this one
- `architecture/diagrams/` – Mermaid sources and the ERD generator

---

## 8. Why This Structure Scales

- A new CBS table is usually one entry in `datasets.yml` plus one staging model
- A new source needs one ingestion function, one silver transform and one staging folder
- Analytical logic stays stable while ingestion changes
- Moving to a hosted S3 or warehouse means changing endpoints and credentials, not code
