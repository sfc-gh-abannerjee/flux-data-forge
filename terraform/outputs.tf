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

output "image_repository_name" {
  description = "Name of the image repository"
  value       = snowflake_image_repository.flux_repo.name
}

output "image_repository_url" {
  description = "Full URL of the image repository for docker push"
  value       = snowflake_image_repository.flux_repo.repository_url
}

output "compute_pool_name" {
  description = "Name of the compute pool"
  value       = local.compute_pool_name
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
  value       = var.create_service_role ? snowflake_role.flux_service_role[0].name : null
}

# -----------------------------------------------------------------------------
# Docker Push Command
# -----------------------------------------------------------------------------

output "docker_push_command" {
  description = "Command to push Docker image to Snowflake registry"
  value       = <<-EOT
    # 1. Login to Snowflake registry
    docker login ${snowflake_image_repository.flux_repo.repository_url}
    
    # 2. Build the image
    cd spcs_app
    docker build -t flux_data_forge:${var.service_image_tag} .
    
    # 3. Tag for Snowflake registry
    docker tag flux_data_forge:${var.service_image_tag} ${snowflake_image_repository.flux_repo.repository_url}/flux_data_forge:${var.service_image_tag}
    
    # 4. Push to registry
    docker push ${snowflake_image_repository.flux_repo.repository_url}/flux_data_forge:${var.service_image_tag}
  EOT
}

# -----------------------------------------------------------------------------
# Service Creation SQL
# -----------------------------------------------------------------------------

output "create_service_sql" {
  description = "SQL to create the SPCS service (run after pushing image)"
  value       = <<-EOT
    CREATE SERVICE IF NOT EXISTS ${local.database_name}.${local.schema_name}.${var.service_name}
      IN COMPUTE POOL ${local.compute_pool_name}
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
