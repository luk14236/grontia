variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
  default     = "00000000-0000-0000-0000-000000000000" # placeholder — reference only
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "West Europe"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "grondia-prod-rg"
}

variable "acr_login_server" {
  description = "Azure Container Registry login server"
  type        = string
  default     = "grontiaprodacr.azurecr.io" # placeholder — reference only
}