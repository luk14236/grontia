terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-grondia-dev"
    storage_account_name = "stgrondiadev12345"
    container_name       = "tfstate"
    key                  = "prod.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

module "networking" {
  source              = "../../modules/networking"
  environment         = var.environment
  location            = var.location
  resource_group_name = var.resource_group_name
}

module "storage" {
  source              = "../../modules/storage"
  environment         = var.environment
  location            = var.location
  resource_group_name = var.resource_group_name
  container_name      = "bronze"
}

module "databricks" {
  source              = "../../modules/databricks"
  environment         = var.environment
  location            = var.location
  resource_group_name = var.resource_group_name
  vnet_id             = module.networking.vnet_id
  subnet_id           = module.databricks.subnet_id
}

module "airflow" {
  source              = "../../modules/airflow"
  environment         = var.environment
  location            = var.location
  resource_group_name = var.resource_group_name
  vnet_id             = module.networking.vnet_id
  subnet_id           = module.airflow.subnet_id
  acr_login_server    = var.acr_login_server
}