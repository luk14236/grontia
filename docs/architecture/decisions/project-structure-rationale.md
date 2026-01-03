# Grontia – Project Structure Rationale

## 1. Purpose of This Document

This document explains **why the Grontia project repository is structured the way it is**, and how this structure supports:

- Multiple independent data pipelines  
- Different ingestion patterns (batch and streaming)  
- A shared analytical model  
- Incremental evolution without large refactors  

The goal is not only to organize files, but to **encode architectural decisions directly into the repository layout**.

---

## 2. High-Level Design Principles

### 2.1 Domain-Oriented Pipelines

Each data source or domain (CBS, PDOK, NDW, KNMI, etc.) is treated as an **independent pipeline**, with its own ingestion, processing logic, and orchestration.

This avoids:

- Tight coupling between unrelated data sources  
- Accidental cross-dependencies  
- The “single giant pipeline” anti-pattern  

---

### 2.2 Separation of Concerns

The structure explicitly separates:

- **Orchestration** – when things run  
- **Processing** – how data is transformed  
- **Analytics & Modeling** – how data is consumed  
- **Infrastructure** – how resources are provisioned  

Each layer evolves independently.

---

### 2.3 Shared Analytical Model

Although pipelines are independent, they converge into a **shared analytical model**, enabling cross-domain analysis without coupling ingestion logic.

---

## 3. Top-Level Structure Overview

```text
GRONTIA/
  orchestration/
  processing/
  analytics/
  infrastructure/
  docs/
  README.md
```

Each top-level folder represents a **distinct responsibility** in the data platform.

---

## 4. Orchestration Layer (`orchestration/airflow`)

**Responsibility:**  
Control *when* pipelines run and *in which order*.

**Key decisions:**

- Apache Airflow is used strictly as an **orchestrator**
- No heavy data processing runs inside Airflow
- Airflow only triggers external systems (Databricks, Snowflake, dbt)

**Why this matters:**

- DAGs remain simple and readable  
- Operational risk is reduced  
- This matches real-world production patterns  

Each data source has its own DAG, for example:

- `cbs_batch_ingestion.py`  
- `pdok_batch_ingestion.py`  
- `ndw_streaming_ingestion.py`  

This keeps pipelines observable and independently deployable.

---

## 5. Processing Layer (`processing/`)

### 5.1 Common Processing Utilities (`processing/common`)

**Responsibility:**  
Provide reusable building blocks shared across pipelines.

Examples include:

- API pagination logic  
- Storage path conventions  
- Dataset configuration  
- Data quality helpers  

**Why this exists:**  
Without a shared processing layer, common logic would be duplicated across sources, increasing maintenance cost and inconsistency.

This layer acts as an internal **data processing framework**.

---

### 5.2 Databricks Processing by Domain (`processing/databricks`)

Each data source has its own folder:

```text
processing/databricks/
  cbs/
  pdok/
  ndw/
  knmi/
```

Inside each domain:

- `extract/` handles ingestion into Bronze  
- `transform/` handles transformation into Silver  

**Why domain separation matters:**

- Each source has unique schemas and update patterns  
- Failures are isolated  
- Ownership and responsibility are clear  

---

## 6. Analytics & Modeling Layer (`analytics/dbt`)

**Responsibility:**  
Transform clean Silver data into business-ready analytical models.

---

### 6.1 Staging Models

Organized by source:

```text
models/staging/
  cbs/
  pdok/
  ndw/
  knmi/
```

Staging models:

- Standardize naming  
- Apply light transformations  
- Act as contracts between processing and analytics  

---

### 6.2 Intermediate Models

Used to combine domains logically:

```text
models/intermediate/
  geography/
  housing/
  mobility/
  climate/
```

This layer enables:

- Cross-domain joins  
- Reusable transformations  
- Decoupling marts from raw source structure  

---

### 6.3 Marts (Gold Layer)

```text
models/marts/
```

This is where:

- Business facts and dimensions live  
- Decision-support models are created  
- BI tools and APIs consume data  

**Key principle:**  
No pipeline writes Gold data directly. All analytical logic flows through dbt.

---

## 7. Infrastructure Layer (`infrastructure/terraform`)

**Responsibility:**  
Provision and manage cloud resources.

The structure separates:

- `modules/` – reusable Terraform components  
- `envs/` – environment-specific configuration (dev, prod)  

This enables:

- Infrastructure as Code  
- Reproducibility  
- Environment isolation  

---

## 8. Documentation (`docs/`)

Architecture and decision documentation is treated as **first-class code**.

Folders include:

- `architecture/` – system design  
- `decisions/` – architectural decision records (ADRs)  
- `diagrams/` – visual representations of pipelines  

This ensures decisions are explicit, traceable, and evolvable.

---

## 9. Why This Structure Scales

This structure allows Grontia to:

- Add new data sources without refactoring existing pipelines  
- Support both batch and streaming ingestion  
- Evolve from learning-focused to production-grade  
- Keep analytical logic stable while ingestion changes  
- Reflect real-world data engineering practices  

---

## 10. Final Notes

This repository structure is intentional.

It encodes:

- Architectural intent  
- Operational boundaries  
- Long-term scalability decisions  

As the project grows, this structure ensures that **complexity grows linearly, not exponentially**.
