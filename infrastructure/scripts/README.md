# Grontia Deployment Scripts

## Overview
This directory contains scripts for deploying and managing the Grontia infrastructure.

## Scripts

### bootstrap.sh
Initializes the Azure infrastructure and prepares for deployment.

**Usage:**
```bash
# Set required environment variables
export AZURE_SUBSCRIPTION_ID="your-subscription-id"
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"

# Run bootstrap
./infrastructure/scripts/bootstrap.sh
```

### deploy.sh
Deploys the entire stack to Azure.

**Usage:**
```bash
# Deploy to dev
./infrastructure/scripts/deploy.sh dev

# Deploy to prod
./infrastructure/scripts/deploy.sh prod
```

### destroy.sh
Destroys the infrastructure (use with caution).

**Usage:**
```bash
# Destroy dev environment
./infrastructure/scripts/destroy.sh dev

# Destroy prod environment
./infrastructure/scripts/destroy.sh prod
```

## Environment Variables

### Required for All Scripts
- `AZURE_SUBSCRIPTION_ID`: Your Azure subscription ID
- `AZURE_TENANT_ID`: Your Azure tenant ID
- `AZURE_CLIENT_ID`: Service Principal client ID
- `AZURE_CLIENT_SECRET`: Service Principal client secret

### Additional for deploy.sh
- `ACR_LOGIN_SERVER`: Azure Container Registry login server
- `ASTRONOMER_API_TOKEN`: Astronomer Cloud API token
- `DATABRICKS_TOKEN`: Databricks personal access token

## Prerequisites

1. **Azure CLI**: Install and authenticate
   ```bash
   az login
   ```

2. **Terraform**: Version 1.6.0 or later
   ```bash
   terraform version
   ```

3. **Astronomer CLI**: For Airflow deployments
   ```bash
   astro version
   ```

4. **Databricks CLI**: For dbt deployments
   ```bash
   databricks --version
   ```

## Deployment Flow

1. **Bootstrap**: Initialize infrastructure and state
2. **Validate**: Check configurations and dependencies
3. **Deploy Infrastructure**: Create Azure resources
4. **Build Images**: Create and push Docker images
5. **Deploy Applications**: Deploy Airflow and dbt
6. **Validate**: Run smoke tests and health checks

## Troubleshooting

### Common Issues

#### Authentication Failed
```
Error: Unable to authenticate with Azure
```
**Solution:**
- Verify service principal credentials
- Check subscription permissions
- Ensure tenant ID is correct

#### Terraform State Locked
```
Error: state lock is held by another process
```
**Solution:**
- Wait for other deployments to complete
- Force unlock if necessary: `terraform force-unlock LOCK_ID`

#### ACR Push Failed
```
Error: access denied
```
**Solution:**
- Verify ACR credentials
- Check service principal has AcrPush role
- Ensure registry exists

### Logs and Monitoring

- **GitHub Actions**: CI/CD pipeline logs
- **Azure Monitor**: Infrastructure metrics
- **Astronomer UI**: DAG execution logs
- **Databricks Workspace**: dbt run logs
- **Terraform**: State and plan outputs

## Security

- Never commit secrets to version control
- Use GitHub secrets for sensitive values
- Rotate service principal credentials regularly
- Limit permissions to minimum required scope