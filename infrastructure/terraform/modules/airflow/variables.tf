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
  description = "Airflow subnet ID"
  type        = string
}

variable "acr_login_server" {
  description = "Azure Container Registry login server"
  type        = string
}

output "acr_login_server" {
  description = "ACR login server"
  value       = azurerm_container_registry.this.login_server
}

output "acr_admin_username" {
  description = "ACR admin username"
  value       = azurerm_container_registry.this.admin_username
}

output "acr_admin_password" {
  description = "ACR admin password"
  value       = azurerm_container_registry.this.admin_password
  sensitive   = true
}

output "airflow_url" {
  description = "Airflow web UI URL"
  value       = azurerm_container_app.airflow.latest_revision_fqdn
}