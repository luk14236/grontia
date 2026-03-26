#!/bin/bash

# Grontia Deployment Script
# Deploys the entire Grontia stack to Azure

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT=${1:-dev}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Required environment variables
REQUIRED_VARS=(
    "AZURE_SUBSCRIPTION_ID"
    "AZURE_TENANT_ID"
    "AZURE_CLIENT_ID"
    "AZURE_CLIENT_SECRET"
    "ACR_LOGIN_SERVER"
    "ASTRONOMER_API_TOKEN"
    "DATABRICKS_TOKEN"
)

echo -e "${BLUE}🚀 Starting Grontia Deployment to $ENVIRONMENT${NC}"

# Validate environment variables
for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo -e "${RED}❌ Error: $var environment variable is not set${NC}"
        exit 1
    fi
done

# Validate environment
if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "prod" ]]; then
    echo -e "${RED}❌ Error: Environment must be 'dev' or 'prod'${NC}"
    exit 1
fi

# Authenticate with Azure
echo -e "${YELLOW}🔐 Authenticating with Azure...${NC}"
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID" --output none
az account set --subscription "$AZURE_SUBSCRIPTION_ID"

# Deploy infrastructure
echo -e "${YELLOW}🏗️  Deploying infrastructure...${NC}"
cd "$PROJECT_ROOT/infrastructure/terraform/envs/$ENVIRONMENT"

terraform init
terraform plan -out=tfplan -var="subscription_id=$AZURE_SUBSCRIPTION_ID" -var="acr_login_server=$ACR_LOGIN_SERVER"
terraform apply tfplan

# Build and push Docker image
echo -e "${YELLOW}🐳 Building and pushing Airflow image...${NC}"
cd "$PROJECT_ROOT"

# Login to ACR
ACR_USERNAME=$(az acr credential show --name "$(basename "$ACR_LOGIN_SERVER")" --query "username" -o tsv)
ACR_PASSWORD=$(az acr credential show --name "$(basename "$ACR_LOGIN_SERVER")" --query "passwords[0].value" -o tsv)

echo "$ACR_PASSWORD" | docker login "$ACR_LOGIN_SERVER" -u "$ACR_USERNAME" --password-stdin

# Build and push
docker build -t "$ACR_LOGIN_SERVER/grontia-airflow:latest" ./orchestration/airflow
docker push "$ACR_LOGIN_SERVER/grontia-airflow:latest"

# Deploy DAGs to Astronomer
echo -e "${YELLOW}✈️  Deploying DAGs to Astronomer...${NC}"
cd "$PROJECT_ROOT/orchestration/airflow"

# Install Astronomer CLI if not present
if ! command -v astro &> /dev/null; then
    curl -sSL https://install.astronomer.io | sudo bash -s -- v1
fi

astro login --token "$ASTRONOMER_API_TOKEN"
astro deploy "$ASTRONOMER_DEPLOYMENT_ID"

# Deploy dbt to Databricks
echo -e "${YELLOW}🔶 Deploying dbt to Databricks...${NC}"
cd "$PROJECT_ROOT/analytics/dbt"

pip install dbt-databricks
dbt deps
dbt run --target "$ENVIRONMENT"

echo -e "${GREEN}✅ Deployment to $ENVIRONMENT completed successfully!${NC}"
echo -e "${BLUE}📊 Summary:${NC}"
echo "  - Infrastructure: Deployed"
echo "  - Airflow Image: Built and pushed"
echo "  - DAGs: Deployed to Astronomer"
echo "  - dbt Models: Deployed to Databricks"