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

output "vnet_id" {
  description = "Virtual network ID"
  value       = azurerm_virtual_network.this.id
}

output "databricks_public_subnet_id" {
  description = "Databricks public subnet ID"
  value       = azurerm_subnet.databricks_public.id
}

output "databricks_private_subnet_id" {
  description = "Databricks private subnet ID"
  value       = azurerm_subnet.databricks_private.id
}

output "airflow_subnet_id" {
  description = "Airflow subnet ID"
  value       = azurerm_subnet.airflow.id
}