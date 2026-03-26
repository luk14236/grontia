# Grontia - Multi-Source Hourly Data Ingestion

## Overview

The **Multi-Source Hourly Ingestion DAG** (`30_multi_source_hourly_ingestion_adls.py`) automatically ingests data from all configured sources every hour:

- **CBS** (Statistics Netherlands) - Socioeconomic and demographic data
- **PDOK** (Public Services on the Map) - Geospatial data
- **KNMI** (Royal Netherlands Meteorological Institute) - Weather and climate data
- **NDW** (National Data Warehouse) - Traffic and mobility data

## Features

### ⏰ **Unified Hourly Schedule**
- Runs every hour at minute 0 (`0 * * * *`)
- Processes all data sources in parallel
- No backfilling to avoid duplicate data

### 🔍 **Smart Data Availability Checks**
- Each source has its own data availability check
- Skips sources/datasets that don't have updates
- Reduces unnecessary API calls and storage costs

### 📊 **Source-Specific Ingestion Logic**
- **CBS**: OData API with TableInfos, DataProperties, TypedDataSet
- **PDOK**: Geospatial services (placeholder - needs implementation)
- **KNMI**: Weather data APIs (placeholder - needs implementation)
- **NDW**: Traffic data APIs (placeholder - needs implementation)

### ☁️ **Unified Azure Storage**
- All data goes to the same ADLS Gen2 bronze layer
- Partitioned by `source/source_bronze/dataset_name/ingestion_date=YYYY-MM-DD`
- Consistent metadata format across all sources

### 🛡️ **Reliability Features**
- Automatic retries (3 attempts with 5-minute delays)
- Comprehensive error handling and logging
- Data validation after ingestion
- Graceful failure handling per source

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Check Data    │ -> │   Ingest Data   │ -> │   Validate      │
│   Available?    │    │   from Source   │    │   Ingestion     │
│  (Per Source)   │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             │
│   Save to ADLS  │ <- │   Partition by  │ <-----------┘
│   Bronze Layer  │    │   Date/Source   │
└─────────────────┘    └─────────────────┘
```

## Data Flow

### 1. **Data Availability Check**
Each source implements its own check:
- **CBS**: Checks if TableInfos endpoint is accessible
- **PDOK/KNMI/NDW**: Placeholder checks (need implementation)

### 2. **Source-Specific Ingestion**
- **CBS**: Downloads JSON from OData endpoints
- **PDOK**: Downloads geospatial data (to be implemented)
- **KNMI**: Downloads weather measurements (to be implemented)
- **NDW**: Downloads traffic data (to be implemented)

### 3. **Data Partitioning**
```
bronze/
├── cbs/cbs_bronze/{dataset}/ingestion_date=2026-03-27/
├── pdok/pdok_bronze/{dataset}/ingestion_date=2026-03-27/
├── knmi/knmi_bronze/{dataset}/ingestion_date=2026-03-27/
└── ndw/ndw_bronze/{dataset}/ingestion_date=2026-03-27/
```

### 4. **Metadata Tracking**
Each ingestion creates a `_summary.json` with:
```json
{
  "source": "cbs",
  "dataset_name": "neighbourhood_key_figures",
  "ingestion_datetime": "2026-03-27_14-00-00",
  "ingestion_date": "2026-03-27",
  "endpoints_ingested": ["TableInfos", "DataProperties", "TypedDataSet"],
  "total_records": 1250,
  "status": "success"
}
```

## Configuration

### Environment Variables Required
```bash
# Azure Storage (required for all sources)
AZURE_STORAGE_ACCOUNT_NAME="your-storage-account"
AZURE_STORAGE_ACCOUNT_KEY="your-storage-key"
ADLS_BRONZE_FILESYSTEM="bronze"
```

### Datasets Configuration
The DAG reads from `datasets.yml` and automatically processes all configured datasets from all sources.

## Current Implementation Status

### ✅ **Fully Implemented**
- **CBS**: Complete OData ingestion with all endpoints
- **Infrastructure**: ADLS writing, partitioning, metadata
- **Orchestration**: Parallel processing, error handling

### 🚧 **Placeholder (Needs Implementation)**
- **PDOK**: Geospatial data ingestion logic
- **KNMI**: Weather data API integration
- **NDW**: Traffic data API integration

## Scaling Considerations

### Current Limitations
- Sequential processing within each source
- No cross-source dependencies
- Fixed retry policy

### Future Optimizations
- Parallel processing within sources
- Data quality monitoring
- Incremental loading strategies
- Advanced scheduling based on data freshness

## Integration with CI/CD

The DAG is automatically deployed via GitHub Actions:

1. **Code Changes** → GitHub Actions
2. **Build Image** → Azure Container Registry
3. **Deploy DAGs** → Astronomer Cloud
4. **Hourly Execution** → Automatic multi-source ingestion

## Monitoring & Operations

### DAG Status
- **Success**: All sources ingested successfully
- **Warning**: Some sources/datasets skipped
- **Failure**: Critical ingestion errors

### Logs
```
INFO - Starting CBS ingestion for neighbourhood_key_figures
INFO - Starting PDOK ingestion for addresses
INFO - Starting KNMI ingestion for weather_stations
INFO - Starting NDW ingestion for traffic_flow
```

### Data Quality Checks
- Validates API response structure per source
- Counts records ingested
- Verifies file writes to ADLS
- Tracks ingestion metadata

## Next Steps

### Immediate
- [ ] Implement PDOK data ingestion logic
- [ ] Implement KNMI data ingestion logic
- [ ] Implement NDW data ingestion logic
- [ ] Test DAG execution with all sources

### Medium-term
- [ ] Add data quality checks per source
- [ ] Implement incremental loading
- [ ] Add alerting for failures

### Long-term
- [ ] Parallel processing optimization
- [ ] Advanced scheduling per source
- [ ] Cross-source data validation