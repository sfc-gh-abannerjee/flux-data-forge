# =============================================================================
# Flux Data Forge - Terraform Variables
# =============================================================================

# -----------------------------------------------------------------------------
# DATABASE & SCHEMA
# -----------------------------------------------------------------------------

variable "database_name" {
  description = "Name of the Snowflake database for Flux Data Forge"
  type        = string
  default     = "FLUX_DATA_FORGE"
}

variable "create_database" {
  description = "Whether to create a new database (false to use existing)"
  type        = bool
  default     = false
}

variable "schema_name" {
  description = "Name of the schema within the database"
  type        = string
  default     = "APP"
}

variable "create_schema" {
  description = "Whether to create a new schema (false to use existing)"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# WAREHOUSE
# -----------------------------------------------------------------------------

variable "warehouse_name" {
  description = "Name of the Snowflake warehouse"
  type        = string
  default     = "FLUX_WH"
}

variable "create_warehouse" {
  description = "Whether to create a new warehouse (false to use existing)"
  type        = bool
  default     = false
}

variable "warehouse_size" {
  description = "Size of the warehouse (XSMALL, SMALL, MEDIUM, LARGE, etc.)"
  type        = string
  default     = "XSMALL"
}

# -----------------------------------------------------------------------------
# IMAGE REPOSITORY
# -----------------------------------------------------------------------------

variable "image_repository_name" {
  description = "Name of the Snowflake image repository"
  type        = string
  default     = "FLUX_DATA_FORGE_REPO"
}

# -----------------------------------------------------------------------------
# COMPUTE POOL
# -----------------------------------------------------------------------------

variable "compute_pool_name" {
  description = "Name of the SPCS compute pool"
  type        = string
  default     = "FLUX_COMPUTE_POOL"
}

variable "create_compute_pool" {
  description = "Whether to create a new compute pool (false to use existing)"
  type        = bool
  default     = true
}

variable "compute_pool_instance_family" {
  description = "Instance family for the compute pool"
  type        = string
  default     = "CPU_X64_S"
  
  validation {
    condition     = contains(["CPU_X64_XS", "CPU_X64_S", "CPU_X64_M", "CPU_X64_L", "HIGHMEM_X64_S", "HIGHMEM_X64_M", "HIGHMEM_X64_L", "GPU_NV_S", "GPU_NV_M", "GPU_NV_L"], var.compute_pool_instance_family)
    error_message = "Invalid instance family. Must be one of: CPU_X64_XS, CPU_X64_S, CPU_X64_M, CPU_X64_L, HIGHMEM_X64_S, HIGHMEM_X64_M, HIGHMEM_X64_L, GPU_NV_S, GPU_NV_M, GPU_NV_L"
  }
}

variable "compute_pool_min_nodes" {
  description = "Minimum number of nodes in the compute pool"
  type        = number
  default     = 1
}

variable "compute_pool_max_nodes" {
  description = "Maximum number of nodes in the compute pool"
  type        = number
  default     = 2
}

variable "compute_pool_auto_suspend_secs" {
  description = "Seconds of inactivity before compute pool auto-suspends"
  type        = number
  default     = 300
}

# -----------------------------------------------------------------------------
# TARGET TABLE
# -----------------------------------------------------------------------------

variable "target_table_name" {
  description = "Name of the target table for AMI streaming data"
  type        = string
  default     = "AMI_STREAMING_READINGS"
}

# -----------------------------------------------------------------------------
# SERVICE ROLE
# -----------------------------------------------------------------------------

variable "create_service_role" {
  description = "Whether to create a dedicated role for the service"
  type        = bool
  default     = false
}

variable "service_role_name" {
  description = "Name of the service role"
  type        = string
  default     = "FLUX_DATA_FORGE_ROLE"
}

# -----------------------------------------------------------------------------
# SPCS SERVICE (for reference - service is created via SQL/spec)
# -----------------------------------------------------------------------------

variable "service_name" {
  description = "Name of the SPCS service"
  type        = string
  default     = "FLUX_DATA_FORGE_SERVICE"
}

variable "service_image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}
