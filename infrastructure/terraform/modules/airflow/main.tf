resource "azurerm_container_registry" "this" {
  name                = "grondia${var.environment}acr"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = true

  tags = {
    Environment = var.environment
    Project     = "Grontia"
  }
}

resource "azurerm_container_app_environment" "this" {
  name                       = "grondia-${var.environment}-containerapp-env"
  location                   = var.location
  resource_group_name        = var.resource_group_name
  log_analytics_workspace_id = null # Using default logging

  tags = {
    Environment = var.environment
    Project     = "Grontia"
  }
}

resource "azurerm_container_app" "airflow" {
  name                         = "grondia-${var.environment}-airflow"
  container_app_environment_id = azurerm_container_app_environment.this.id
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"

  template {
    container {
      name   = "airflow"
      image  = "${var.acr_login_server}/grondia-airflow:latest"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "AIRFLOW__CORE__EXECUTOR"
        value = "LocalExecutor"
      }

      env {
        name  = "AIRFLOW__CORE__LOAD_EXAMPLES"
        value = "False"
      }

      env {
        name  = "AIRFLOW__WEBSERVER__WEB_SERVER_PORT"
        value = "8080"
      }
    }
  }

  ingress {
    external_enabled = true
    target_port      = 8080
    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  tags = {
    Environment = var.environment
    Project     = "Grontia"
  }
}