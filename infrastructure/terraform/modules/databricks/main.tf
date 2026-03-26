resource "azurerm_databricks_workspace" "this" {
  name                = "grondia-${var.environment}-workspace"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"

  custom_parameters {
    no_public_ip        = true
    public_subnet_name  = var.public_subnet_name
    private_subnet_name = var.private_subnet_name
    virtual_network_id  = var.vnet_id
  }

  tags = {
    Environment = var.environment
    Project     = "Grontia"
  }
}

resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "Shared Autoscaling"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 20

  autoscale {
    min_workers = 1
    max_workers = 50
  }

  depends_on = [azurerm_databricks_workspace.this]
}

resource "databricks_job" "cbs_pipeline" {
  name = "CBS Bronze Pipeline"

  job_cluster {
    job_cluster_key = "job_cluster"
    new_cluster {
      spark_version = "13.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"
      num_workers   = 2
    }
  }

  task {
    task_key = "cbs_ingestion"
    job_cluster_key = "job_cluster"

    python_wheel_task {
      package_name = "grondia.processing"
      entry_point  = "cbs_ingestion"
    }
  }

  depends_on = [azurerm_databricks_workspace.this]
}