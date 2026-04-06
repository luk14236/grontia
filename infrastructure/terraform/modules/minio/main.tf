# MinIO — Local Storage (Azure Reference Module)
#
# MinIO is the local equivalent of Azure Data Lake Storage Gen2.
# It exposes an S3-compatible API, so code written against MinIO
# works against ADLS with minimal changes (just swap credentials/endpoint).
#
# LOCAL: runs via Docker Compose (see orchestration/airflow/docker-compose.override.yml)
#   - API:     http://localhost:9000
#   - Console: http://localhost:9001
#   - User:    minioadmin / minioadmin
#   - Buckets: bronze, silver, gold (auto-created on startup)
#
# AZURE EQUIVALENT: Azure Data Lake Storage Gen2 (see modules/storage/main.tf)
#
# This module is intentionally empty — MinIO is not an Azure resource.
# It exists here as documentation of the local ↔ cloud mapping.
#
# To provision the Azure equivalent, use modules/storage.
