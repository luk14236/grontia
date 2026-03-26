# Grontia - Hourly CBS Data Ingestion

## Overview

The **CBS Hourly Ingestion DAG** (`20_cbs_hourly_ingestion_adls.py`) automatically ingests CBS (Statistics Netherlands) datasets to Azure Data Lake Storage every hour.

## Features

### ⏰ **Hourly Schedule**
- Runs every hour at minute 0 (`0 * * * *`)
- No backfilling to avoid duplicate data
- Only one instance runs at a time

### 🔍 **Smart Data Checking**
- Checks if new data is available before ingestion
- Skips datasets that don't have updates
- Reduces unnecessary API calls and storage costs

### 📊 **Complete Dataset Ingestion**
- Ingests all configured bronze endpoints (TableInfos, DataProperties, TypedDataSet)
- Saves data partitioned by `ingestion_date=YYYY-MM-DD`
- Includes comprehensive metadata and validation

### ☁️ **Azure Integration**
- Direct integration with Azure Data Lake Storage Gen2
- Secure authentication via environment variables
- Optimized for cloud-native storage patterns

### 🛡️ **Reliability Features**
- Automatic retries (3 attempts with 5-minute delays)
- Comprehensive error handling and logging
- Data validation after ingestion
- Graceful failure handling

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Check Data    │ -> │   Ingest Data   │ -> │   Validate      │
│   Available?    │    │   from CBS      │    │   Ingestion     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             │
│   Save to ADLS  │ <- │   Partition by  │ <-----------┘
│   Bronze Layer  │    │   Date         │
└─────────────────┘    └─────────────────┘
```

## Data Flow

### 1. **Data Availability Check**
```python
# Checks if CBS API endpoints are accessible
url = f"{CBS_BASE}/{table_id}/TableInfos"
response = http_get_json(url)
return len(response.get("value", [])) > 0
```

### 2. **Dataset Ingestion**
```python
# Ingests all bronze endpoints for each dataset
for endpoint in ["TableInfos", "DataProperties", "TypedDataSet"]:
    data = http_get_json(f"{CBS_BASE}/{table_id}/{endpoint}")
    adls_write_text(f"{base_path}/{endpoint.lower()}.json", json.dumps(data))
```

### 3. **Data Partitioning**
```
bronze/cbs/cbs_bronze/{dataset_name}/ingestion_date=2026-03-26/
├── tableinfos.json
├── dataproperties.json
├── typeddataset.json
└── _summary.json
```

### 4. **Metadata Tracking**
```json
{
  "table_id": "84583NED",
  "dataset_name": "neighbourhood_key_figures",
  "ingestion_datetime": "2026-03-26_14-00-00",
  "ingestion_date": "2026-03-26",
  "endpoints_ingested": ["TableInfos", "DataProperties", "TypedDataSet"],
  "total_records": 1250,
  "status": "success"
}
```

## Configuration

### Environment Variables Required
```bash
# Azure Storage
AZURE_STORAGE_ACCOUNT_NAME="your-storage-account"
AZURE_STORAGE_ACCOUNT_KEY="your-storage-key"
ADLS_BRONZE_FILESYSTEM="bronze"
ADLS_BRONZE_PREFIX="cbs/cbs_bronze"
```

### Datasets Configured
- `neighbourhood_key_figures` (84583NED)
- `average_woz_value` (83765NED)
- `housing_stock` (83913NED)
- `household_income` (83914NED)

## Monitoring & Operations

### DAG Status
- **Success**: All datasets ingested successfully
- **Warning**: Some datasets skipped (no new data)
- **Failure**: Ingestion errors with automatic retry

### Logs
```
INFO - Starting ingestion for neighbourhood_key_figures (table_id: 84583NED)
INFO - Ingesting endpoint: TableInfos
INFO - Ingesting endpoint: DataProperties
INFO - Successfully ingested neighbourhood_key_figures: 1250 records
```

### Data Quality Checks
- Validates API response structure
- Counts records ingested
- Verifies file writes to ADLS
- Tracks ingestion metadata

## Scaling Considerations

### Current Limitations
- Sequential processing of datasets
- No parallel ingestion within datasets
- Fixed retry policy

### Future Optimizations
- Parallel dataset processing
- Incremental data loading
- Advanced retry strategies
- Data quality monitoring integration

## Troubleshooting

### Common Issues

#### Authentication Failed
```
WARNING - Missing AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY
```
**Solution**: Set environment variables in Airflow configuration.

#### API Rate Limiting
```
HTTPError: 429 Too Many Requests
```
**Solution**: DAG automatically retries with backoff.

#### Storage Write Failed
```
ResourceNotFoundError: File system 'bronze' not found
```
**Solution**: Verify ADLS filesystem exists and permissions.

#### Dataset Not Found
```
ValueError: Dataset 84583NED not found in catalog
```
**Solution**: Check `datasets.yml` configuration.

## Integration with CI/CD

The DAG is automatically deployed via GitHub Actions:

1. **Code Changes** → GitHub Actions
2. **Build Image** → Azure Container Registry
3. **Deploy DAGs** → Astronomer Cloud
4. **Hourly Execution** → Automatic data ingestion

## Next Steps

### Immediate
- [ ] Configure environment variables in Astronomer
- [ ] Test DAG execution manually
- [ ] Monitor first few runs

### Medium-term
- [ ] Add data quality checks
- [ ] Implement incremental loading
- [ ] Add alerting for failures

### Long-term
- [ ] Parallel processing optimization
- [ ] Advanced scheduling based on data freshness
- [ ] Integration with dbt for transformation