# Flux Data Forge - Terraform Module

This Terraform module provisions all Snowflake infrastructure required for deploying Flux Data Forge as a Snowpark Container Service.

## Resources Created

- **Database** (optional) - Container for all objects
- **Schema** (optional) - Namespace for objects
- **Warehouse** (optional) - Compute for queries
- **Image Repository** - Stores Docker images
- **Compute Pool** (optional) - SPCS compute resources
- **Target Table** - Landing table for AMI data
- **Service Role** (optional) - Dedicated role with minimal privileges

## Prerequisites

1. **Terraform** >= 1.0.0
2. **Snowflake Provider** >= 0.87.0
3. **Snowflake Account** with SPCS enabled
4. **Authentication** configured (key pair or SSO)

## Quick Start

### 1. Configure Provider

Create `provider.tf`:

```hcl
provider "snowflake" {
  account  = "your-account"
  user     = "your-username"
  role     = "ACCOUNTADMIN"
  
  # Option 1: Key pair authentication
  private_key_path = "~/.ssh/snowflake_key.p8"
  
  # Option 2: Password authentication
  # password = var.snowflake_password
  
  # Option 3: Browser SSO
  # authenticator = "externalbrowser"
}
```

### 2. Create Configuration

Create `terraform.tfvars`:

```hcl
# Use existing database and warehouse
database_name    = "MY_DATABASE"
create_database  = false

schema_name      = "FLUX_APP"
create_schema    = true

warehouse_name   = "MY_WAREHOUSE"
create_warehouse = false

# Create new compute pool
compute_pool_name   = "FLUX_COMPUTE_POOL"
create_compute_pool = true

# Image repository
image_repository_name = "FLUX_DATA_FORGE_REPO"
```

### 3. Initialize and Apply

```bash
cd terraform

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply changes
terraform apply
```

### 4. Deploy the Application

After Terraform completes, follow the output instructions:

```bash
# Get the docker push command
terraform output docker_push_command

# Get the service creation SQL
terraform output create_service_sql
```

## Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `database_name` | Database name | `FLUX_DATA_FORGE` | No |
| `create_database` | Create new database | `false` | No |
| `schema_name` | Schema name | `APP` | No |
| `create_schema` | Create new schema | `true` | No |
| `warehouse_name` | Warehouse name | `FLUX_WH` | No |
| `create_warehouse` | Create new warehouse | `false` | No |
| `warehouse_size` | Warehouse size | `XSMALL` | No |
| `compute_pool_name` | Compute pool name | `FLUX_COMPUTE_POOL` | No |
| `create_compute_pool` | Create new compute pool | `true` | No |
| `compute_pool_instance_family` | Instance type | `CPU_X64_S` | No |
| `compute_pool_min_nodes` | Min nodes | `1` | No |
| `compute_pool_max_nodes` | Max nodes | `2` | No |
| `image_repository_name` | Image repo name | `FLUX_DATA_FORGE_REPO` | No |
| `target_table_name` | Target table name | `AMI_STREAMING_READINGS` | No |

## Outputs

| Output | Description |
|--------|-------------|
| `database_name` | Database name |
| `schema_name` | Schema name |
| `image_repository_url` | Full URL for docker push |
| `compute_pool_name` | Compute pool name |
| `target_table_fqn` | Fully qualified table name |
| `docker_push_command` | Complete docker push instructions |
| `create_service_sql` | SQL to create SPCS service |

## Example Configurations

### Minimal (Use Existing Infrastructure)

```hcl
database_name       = "EXISTING_DB"
schema_name         = "EXISTING_SCHEMA"
warehouse_name      = "EXISTING_WH"
compute_pool_name   = "EXISTING_POOL"

create_database     = false
create_schema       = false
create_warehouse    = false
create_compute_pool = false
```

### Full Setup (Create Everything)

```hcl
database_name       = "FLUX_DATA_FORGE"
schema_name         = "APP"
warehouse_name      = "FLUX_WH"
compute_pool_name   = "FLUX_POOL"

create_database     = true
create_schema       = true
create_warehouse    = true
create_compute_pool = true

warehouse_size                 = "SMALL"
compute_pool_instance_family   = "CPU_X64_M"
compute_pool_max_nodes         = 4
```

## Cleanup

To destroy all created resources:

```bash
# First, drop the SPCS service manually
# (Terraform doesn't manage the service directly)

# Then destroy infrastructure
terraform destroy
```

## Notes

- **SPCS Service**: The SPCS service itself is created via SQL (see `create_service_sql` output) rather than Terraform, because it requires the Docker image to be pushed first.
- **Compute Pool**: Creating a compute pool requires specific account permissions. If you get permission errors, use an existing pool.
- **Image Repository**: The repository URL format is: `<org>-<account>.registry.snowflakecomputing.com/<db>/<schema>/<repo>`
