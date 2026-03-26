variable "environment" {
  description = "Environment name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "container_name" {
  description = "Bronze container name"
  type        = string
  default     = "bronze"
}

output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.this.name
}

output "storage_account_key" {
  description = "Storage account key"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}

output "bronze_container_name" {
  description = "Bronze container name"
  value       = azurerm_storage_container.bronze.name
}

output "silver_container_name" {
  description = "Silver container name"
  value       = azurerm_storage_container.silver.name
}

output "gold_container_name" {
  description = "Gold container name"
  value       = azurerm_storage_container.gold.name
}