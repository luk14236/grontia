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

variable "vnet_id" {
  description = "Virtual network ID"
  type        = string
}

variable "subnet_id" {
  description = "Databricks subnet ID"
  type        = string
}

variable "public_subnet_name" {
  description = "Public subnet name"
  type        = string
  default     = "databricks-public"
}

variable "private_subnet_name" {
  description = "Private subnet name"
  type        = string
  default     = "databricks-private"
}

output "workspace_id" {
  description = "Databricks workspace ID"
  value       = azurerm_databricks_workspace.this.id
}

output "workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.this.workspace_url
}