# =============================================================================
# Flux Data Forge - Terraform Outputs
# =============================================================================

output "database_name" {
  description = "Name of the database (created or existing)"
  value       = local.database_name
}

output "schema_name" {
  description = "Name of the schema (created or existing)"
  value       = local.schema_name
}

output "warehouse_name" {
  description = "Name of the warehouse"
  value       = var.create_warehouse ? snowflake_warehouse.flux_wh[0].name : var.warehouse_name
}

output "target_table_name" {
  description = "Name of the target table"
  value       = snowflake_table.ami_streaming_readings.name
}

output "target_table_fqn" {
  description = "Fully qualified name of the target table"
  value       = "${local.database_name}.${local.schema_name}.${snowflake_table.ami_streaming_readings.name}"
}

output "service_role_name" {
  description = "Name of the service role (if created)"
  value       = var.create_service_role ? snowflake_account_role.flux_service_role[0].name : null
}

# -----------------------------------------------------------------------------
# SPCS Setup SQL (run after Terraform apply)
# -----------------------------------------------------------------------------

output "spcs_setup_sql" {
  description = "SQL commands to set up SPCS resources (run after Terraform)"
  value       = <<-EOT
    -- ==========================================================================
    -- Run these SQL commands after Terraform apply to complete SPCS setup
    -- ==========================================================================
    
    USE DATABASE ${local.database_name};
    USE SCHEMA ${local.schema_name};
    
    -- 1. Create Image Repository
    CREATE IMAGE REPOSITORY IF NOT EXISTS ${var.image_repository_name}
        COMMENT = 'Image repository for Flux Data Forge';
    
    -- Get repository URL for docker push:
    SHOW IMAGE REPOSITORIES LIKE '${var.image_repository_name}';
    
    -- 2. Create Compute Pool
    CREATE COMPUTE POOL IF NOT EXISTS ${var.compute_pool_name}
        MIN_NODES = ${var.compute_pool_min_nodes}
        MAX_NODES = ${var.compute_pool_max_nodes}
        INSTANCE_FAMILY = ${var.compute_pool_instance_family}
        AUTO_RESUME = TRUE
        AUTO_SUSPEND_SECS = ${var.compute_pool_auto_suspend_secs}
        COMMENT = 'Compute pool for Flux Data Forge';
    
    -- 3. After pushing Docker image, create service using:
    --    scripts/sql/05_create_service.sql
  EOT
}

output "docker_commands" {
  description = "Docker commands to build and push image"
  value       = <<-EOT
    # After running the SPCS setup SQL above, get the repository URL and run:
    
    # 1. Login to Snowflake registry (get URL from SHOW IMAGE REPOSITORIES)
    docker login <ORG>-<ACCOUNT>.registry.snowflakecomputing.com
    
    # 2. Build the image
    cd spcs_app
    docker build -t flux_data_forge:${var.service_image_tag} .
    
    # 3. Tag for Snowflake registry
    docker tag flux_data_forge:${var.service_image_tag} \
      <ORG>-<ACCOUNT>.registry.snowflakecomputing.com/${local.database_name}/${local.schema_name}/${var.image_repository_name}/flux_data_forge:${var.service_image_tag}
    
    # 4. Push to registry
    docker push <ORG>-<ACCOUNT>.registry.snowflakecomputing.com/${local.database_name}/${local.schema_name}/${var.image_repository_name}/flux_data_forge:${var.service_image_tag}
  EOT
}

output "create_service_sql" {
  description = "SQL to create the SPCS service (run after pushing image)"
  value       = <<-EOT
    CREATE SERVICE IF NOT EXISTS ${local.database_name}.${local.schema_name}.${var.service_name}
      IN COMPUTE POOL ${var.compute_pool_name}
      FROM SPECIFICATION $$
    spec:
      containers:
        - name: flux-data-forge
          image: /${local.database_name}/${local.schema_name}/${var.image_repository_name}/flux_data_forge:${var.service_image_tag}
          env:
            SNOWFLAKE_DATABASE: ${local.database_name}
            SNOWFLAKE_SCHEMA: ${local.schema_name}
            SNOWFLAKE_WAREHOUSE: ${var.warehouse_name}
            AMI_TABLE: ${var.target_table_name}
          resources:
            requests:
              cpu: 1
              memory: 2Gi
            limits:
              cpu: 2
              memory: 4Gi
      endpoints:
        - name: app
          port: 8080
          public: true
    $$
      COMMENT = 'Flux Data Forge - Synthetic AMI Data Generation Service';
    
    -- Get the service URL
    SHOW ENDPOINTS IN SERVICE ${local.database_name}.${local.schema_name}.${var.service_name};
  EOT
}
