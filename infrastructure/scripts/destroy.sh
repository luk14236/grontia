#!/bin/bash

# Grontia Destroy Script
# Destroys the Grontia infrastructure (USE WITH CAUTION)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ENVIRONMENT=${1:-dev}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo -e "${RED}⚠️  WARNING: This will destroy the $ENVIRONMENT environment!${NC}"
echo -e "${RED}⚠️  This action cannot be undone!${NC}"
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " -r
if [[ ! $REPLY =~ ^yes$ ]]; then
    echo -e "${GREEN}Operation cancelled.${NC}"
    exit 0
fi

# Required environment variables
REQUIRED_VARS=(
    "AZURE_SUBSCRIPTION_ID"
    "AZURE_TENANT_ID"
    "AZURE_CLIENT_ID"
    "AZURE_CLIENT_SECRET"
)

for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo -e "${RED}❌ Error: $var environment variable is not set${NC}"
        exit 1
    fi
done

echo -e "${YELLOW}🔐 Authenticating with Azure...${NC}"
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID" --output none
az account set --subscription "$AZURE_SUBSCRIPTION_ID"

echo -e "${YELLOW}🏗️  Destroying infrastructure...${NC}"
cd "$PROJECT_ROOT/infrastructure/terraform/envs/$ENVIRONMENT"

terraform init
terraform destroy -auto-approve -var="subscription_id=$AZURE_SUBSCRIPTION_ID"

echo -e "${GREEN}✅ Environment $ENVIRONMENT destroyed successfully!${NC}"