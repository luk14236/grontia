# Grontia CI/CD Setup Guide

## Overview
This guide explains how to set up continuous integration and deployment (CI/CD) for the Grontia project using GitHub Actions and Azure.

## Prerequisites

### Azure Resources
- Azure subscription with appropriate permissions
- Service Principal with Contributor role
- Azure Container Registry (ACR)
- Azure Storage Account for Terraform state
- Azure Databricks workspace
- Astronomer Cloud deployment

### GitHub Secrets Required
Set these secrets in your GitHub repository settings:

#### Azure Authentication
```
AZURE_SUBSCRIPTION_ID    # Your Azure subscription ID
AZURE_TENANT_ID         # Your Azure tenant ID
AZURE_CLIENT_ID         # Service Principal client ID
AZURE_CLIENT_SECRET     # Service Principal client secret
```

#### Container Registry
```
ACR_LOGIN_SERVER        # e.g., grondia.azurecr.io
ACR_USERNAME           # ACR username (usually the registry name)
ACR_PASSWORD           # ACR password or access token
```

#### Astronomer (Airflow)
```
ASTRONOMER_API_TOKEN    # API token from Astronomer Cloud
ASTRONOMER_DEPLOYMENT_ID # Deployment ID from Astronomer
```

#### Databricks
```
DATABRICKS_HOST         # Databricks workspace URL
DATABRICKS_TOKEN        # Personal access token
```

## Initial Setup

### 1. Bootstrap Infrastructure
```bash
# Set environment variables
export AZURE_SUBSCRIPTION_ID="your-subscription-id"
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"

# Run bootstrap script
./infrastructure/scripts/bootstrap.sh
```

### 2. Configure Terraform Backend
Update `infrastructure/terraform/envs/dev/backend.tf` with your storage account details.

### 3. Set GitHub Secrets
Add all required secrets in GitHub repository settings under "Secrets and variables" > "Actions".

## Deployment Workflow

### Automatic Deployment
- Push to `main` branch triggers full CI/CD pipeline
- PRs trigger validation checks only
- Manual deployment available via workflow dispatch

### Environments
- **dev**: Development environment (default)
- **prod**: Production environment

### Manual Deployment
1. Go to GitHub Actions tab
2. Select "CI/CD Pipeline" workflow
3. Click "Run workflow"
4. Choose environment (dev/prod)

## Pipeline Stages

### 1. Validate
- Terraform format and validation
- Python DAG compilation
- Code linting

### 2. Build Airflow Image
- Build Docker image from `orchestration/airflow/`
- Push to Azure Container Registry
- Tag with branch/commit info

### 3. Deploy Infrastructure
- Apply Terraform changes
- Create/update Azure resources
- Configure networking and security

### 4. Deploy DAGs
- Deploy Airflow DAGs to Astronomer Cloud
- Update task definitions

### 5. Deploy dbt
- Run dbt models on Databricks
- Update data transformations

## Troubleshooting

### Common Issues

#### Terraform Backend Not Configured
```
Error: Backend configuration changed
```
**Solution**: Update `backend.tf` with correct storage account details.

#### Azure Authentication Failed
```
Error: authentication failed
```
**Solution**: Verify service principal credentials and permissions.

#### ACR Push Failed
```
Error: denied: access forbidden
```
**Solution**: Check ACR credentials and ensure service principal has AcrPush role.

#### Astronomer Deployment Failed
```
Error: deployment failed
```
**Solution**: Verify API token and deployment ID.

### Logs and Monitoring
- GitHub Actions logs for CI/CD status
- Azure Monitor for infrastructure metrics
- Astronomer UI for DAG execution
- Databricks workspace for dbt runs

## Security Notes
- Never commit secrets to code
- Use GitHub secrets for sensitive values
- Rotate service principal credentials regularly
- Limit service principal permissions to minimum required