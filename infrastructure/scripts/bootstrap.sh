#!/bin/bash

# Grontia Infrastructure Bootstrap Script
# This script initializes the Azure infrastructure for the Grontia project

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Grontia Infrastructure Bootstrap${NC}"

# Check if required environment variables are set
required_vars=("AZURE_SUBSCRIPTION_ID" "AZURE_TENANT_ID" "AZURE_CLIENT_ID" "AZURE_CLIENT_SECRET")
for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo -e "${RED}❌ Error: $var environment variable is not set${NC}"
        exit 1
    fi
done

echo -e "${YELLOW}🔐 Logging into Azure...${NC}"
az login --service-principal -u "$AZURE_CLIENT_ID" -p "$AZURE_CLIENT_SECRET" --tenant "$AZURE_TENANT_ID"
az account set --subscription "$AZURE_SUBSCRIPTION_ID"

echo -e "${YELLOW}🏗️  Initializing Terraform...${NC}"
cd infrastructure/terraform/envs/dev

terraform init

echo -e "${YELLOW}📋 Planning infrastructure deployment...${NC}"
terraform plan -out=tfplan

echo -e "${GREEN}✅ Bootstrap complete!${NC}"
echo -e "${YELLOW}💡 Next steps:${NC}"
echo "  1. Review the terraform plan: terraform show tfplan"
echo "  2. Apply the changes: terraform apply tfplan"
echo "  3. Set up GitHub Secrets for CI/CD"
echo "  4. Push code to trigger deployment"
