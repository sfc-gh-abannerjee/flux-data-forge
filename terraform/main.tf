# =============================================================================
# Flux Data Forge - Terraform Module
# =============================================================================
# This module provisions all Snowflake infrastructure required for
# Flux Data Forge SPCS deployment.
#
# Usage:
#   module "flux_data_forge" {
#     source = "./terraform"
#     
#     database_name    = "MY_DATABASE"
#     schema_name      = "MY_SCHEMA"
#     warehouse_name   = "MY_WAREHOUSE"
#     compute_pool_name = "MY_COMPUTE_POOL"
#   }
# =============================================================================

terraform {
  required_version = ">= 1.0.0"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 0.87.0"
    }
  }
}

# =============================================================================
# DATABASE & SCHEMA
# =============================================================================

resource "snowflake_database" "flux_db" {
  count = var.create_database ? 1 : 0
  
  name    = var.database_name
  comment = "Database for Flux Data Forge"
  
  data_retention_time_in_days = 7
}

resource "snowflake_schema" "flux_schema" {
  count = var.create_schema ? 1 : 0
  
  database = var.create_database ? snowflake_database.flux_db[0].name : var.database_name
  name     = var.schema_name
  comment  = "Schema for Flux Data Forge"
  
  is_managed = false
}

locals {
  database_name = var.create_database ? snowflake_database.flux_db[0].name : var.database_name
  schema_name   = var.create_schema ? snowflake_schema.flux_schema[0].name : var.schema_name
}

# =============================================================================
# WAREHOUSE
# =============================================================================

resource "snowflake_warehouse" "flux_wh" {
  count = var.create_warehouse ? 1 : 0
  
  name           = var.warehouse_name
  warehouse_size = var.warehouse_size
  
  auto_suspend           = 60
  auto_resume            = true
  initially_suspended    = true
  
  comment = "Warehouse for Flux Data Forge operations"
}

# =============================================================================
# IMAGE REPOSITORY
# =============================================================================

resource "snowflake_image_repository" "flux_repo" {
  name     = var.image_repository_name
  database = local.database_name
  schema   = local.schema_name
  
  # Note: Depends on schema existing
  depends_on = [snowflake_schema.flux_schema]
}

# =============================================================================
# COMPUTE POOL
# =============================================================================

resource "snowflake_compute_pool" "flux_pool" {
  count = var.create_compute_pool ? 1 : 0
  
  name            = var.compute_pool_name
  instance_family = var.compute_pool_instance_family
  min_nodes       = var.compute_pool_min_nodes
  max_nodes       = var.compute_pool_max_nodes
  
  auto_resume  = true
  auto_suspend_secs = var.compute_pool_auto_suspend_secs
  
  comment = "Compute pool for Flux Data Forge SPCS"
}

locals {
  compute_pool_name = var.create_compute_pool ? snowflake_compute_pool.flux_pool[0].name : var.compute_pool_name
}

# =============================================================================
# TARGET TABLE
# =============================================================================

resource "snowflake_table" "ami_streaming_readings" {
  database = local.database_name
  schema   = local.schema_name
  name     = var.target_table_name
  
  comment = "Streaming landing table for AMI data from Flux Data Forge"
  
  change_tracking = true
  
  column {
    name     = "METER_ID"
    type     = "VARCHAR(50)"
    nullable = false
  }
  
  column {
    name     = "READING_TIMESTAMP"
    type     = "TIMESTAMP_NTZ"
    nullable = false
  }
  
  column {
    name     = "USAGE_KWH"
    type     = "FLOAT"
    nullable = true
  }
  
  column {
    name     = "VOLTAGE"
    type     = "FLOAT"
    nullable = true
  }
  
  column {
    name     = "POWER_FACTOR"
    type     = "FLOAT"
    nullable = true
  }
  
  column {
    name     = "TEMPERATURE_C"
    type     = "FLOAT"
    nullable = true
  }
  
  column {
    name     = "TRANSFORMER_ID"
    type     = "VARCHAR(50)"
    nullable = true
  }
  
  column {
    name     = "CIRCUIT_ID"
    type     = "VARCHAR(50)"
    nullable = true
  }
  
  column {
    name     = "SUBSTATION_ID"
    type     = "VARCHAR(50)"
    nullable = true
  }
  
  column {
    name     = "SERVICE_AREA"
    type     = "VARCHAR(100)"
    nullable = true
  }
  
  column {
    name     = "CUSTOMER_SEGMENT"
    type     = "VARCHAR(50)"
    nullable = true
  }
  
  column {
    name     = "LATITUDE"
    type     = "FLOAT"
    nullable = true
  }
  
  column {
    name     = "LONGITUDE"
    type     = "FLOAT"
    nullable = true
  }
  
  column {
    name     = "IS_OUTAGE"
    type     = "BOOLEAN"
    nullable = true
    default {
      constant = "FALSE"
    }
  }
  
  column {
    name     = "DATA_QUALITY"
    type     = "VARCHAR(20)"
    nullable = true
    default {
      constant = "'VALID'"
    }
  }
  
  column {
    name     = "EMISSION_PATTERN"
    type     = "VARCHAR(50)"
    nullable = true
  }
  
  column {
    name     = "PRODUCTION_MATCHED"
    type     = "BOOLEAN"
    nullable = true
    default {
      constant = "FALSE"
    }
  }
  
  column {
    name     = "INGESTION_TIMESTAMP"
    type     = "TIMESTAMP_NTZ"
    nullable = true
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
  
  depends_on = [snowflake_schema.flux_schema]
}

# =============================================================================
# SERVICE ROLE (Optional)
# =============================================================================

resource "snowflake_role" "flux_service_role" {
  count = var.create_service_role ? 1 : 0
  
  name    = var.service_role_name
  comment = "Role for Flux Data Forge service"
}

resource "snowflake_grant_privileges_to_role" "flux_warehouse_usage" {
  count = var.create_service_role ? 1 : 0
  
  privileges = ["USAGE"]
  role_name  = snowflake_role.flux_service_role[0].name
  
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = var.create_warehouse ? snowflake_warehouse.flux_wh[0].name : var.warehouse_name
  }
}

resource "snowflake_grant_privileges_to_role" "flux_schema_usage" {
  count = var.create_service_role ? 1 : 0
  
  privileges = ["USAGE"]
  role_name  = snowflake_role.flux_service_role[0].name
  
  on_schema {
    schema_name = "\"${local.database_name}\".\"${local.schema_name}\""
  }
}

resource "snowflake_grant_privileges_to_role" "flux_table_privileges" {
  count = var.create_service_role ? 1 : 0
  
  privileges = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  role_name  = snowflake_role.flux_service_role[0].name
  
  on_schema_object {
    object_type = "TABLE"
    object_name = "\"${local.database_name}\".\"${local.schema_name}\".\"${snowflake_table.ami_streaming_readings.name}\""
  }
}
