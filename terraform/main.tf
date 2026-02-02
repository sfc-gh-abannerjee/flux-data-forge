# =============================================================================
# Flux Data Forge - Terraform Module
# =============================================================================
# This module provisions Snowflake infrastructure required for
# Flux Data Forge SPCS deployment.
#
# IMPORTANT LIMITATIONS:
# The Snowflake Terraform provider does NOT support SPCS-specific resources:
#   - Compute Pools (CREATE COMPUTE POOL)
#   - Image Repositories (CREATE IMAGE REPOSITORY)  
#   - Services (CREATE SERVICE)
#
# These must be created via SQL. See scripts/sql/ for SQL deployment scripts.
# This Terraform module handles: Database, Schema, Warehouse, and Target Table.
#
# Usage:
#   cd terraform
#   cp terraform.tfvars.example terraform.tfvars
#   # Edit terraform.tfvars with your values
#   terraform init
#   terraform plan
#   terraform apply
#   # Then run SQL scripts for SPCS resources
# =============================================================================

terraform {
  required_version = ">= 1.0.0"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = ">= 0.89.0, < 1.0.0"
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
# SPCS RESOURCES (NOT SUPPORTED BY TERRAFORM)
# =============================================================================
# The following resources must be created via SQL:
#
# 1. IMAGE REPOSITORY:
#    CREATE IMAGE REPOSITORY IF NOT EXISTS <database>.<schema>.<repo_name>;
#
# 2. COMPUTE POOL:
#    CREATE COMPUTE POOL IF NOT EXISTS <pool_name>
#        MIN_NODES = 1
#        MAX_NODES = 2
#        INSTANCE_FAMILY = CPU_X64_S
#        AUTO_RESUME = TRUE
#        AUTO_SUSPEND_SECS = 300;
#
# 3. SERVICE:
#    CREATE SERVICE ... (see scripts/sql/05_create_service.sql)
#
# Use the provided SQL scripts: scripts/sql/02_image_repository.sql,
# scripts/sql/03_compute_pool.sql, scripts/sql/05_create_service.sql
# =============================================================================

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

resource "snowflake_account_role" "flux_service_role" {
  count = var.create_service_role ? 1 : 0
  
  name    = var.service_role_name
  comment = "Role for Flux Data Forge service"
}

# Grant role to SYSADMIN for management
resource "snowflake_grant_account_role" "flux_role_to_sysadmin" {
  count = var.create_service_role ? 1 : 0
  
  role_name        = snowflake_account_role.flux_service_role[0].name
  parent_role_name = "SYSADMIN"
}
