# Flux Data Forge - SQL Deployment Scripts

Deploy Flux Data Forge to Snowflake SPCS using Snow CLI with variable templating.

## Script Order

Run these scripts in order:

| Script | Purpose | Prerequisites |
|--------|---------|---------------|
| `01_database_schema.sql` | Create database, schema, warehouse | None |
| `02_image_repository.sql` | Create image repository for Docker | Script 01 |
| `03_compute_pool.sql` | Create SPCS compute pool | Script 01 |
| `04_target_table.sql` | Create AMI_STREAMING_READINGS table | Script 01 |
| `05_create_service.sql` | Deploy the SPCS service | Scripts 01-04 + Docker image pushed |
| `06_validation.sql` | Validate deployment | All scripts + data generated |

## Quick Start

```bash
# Set your connection
export CONN="your_connection_name"

# 1. Create database and schema
snow sql -c $CONN -f scripts/sql/01_database_schema.sql \
    -D "database=FLUX_DATA_FORGE" \
    -D "schema=PUBLIC" \
    -D "warehouse=FLUX_DATA_FORGE_WH"

# 2. Create image repository
snow sql -c $CONN -f scripts/sql/02_image_repository.sql \
    -D "database=FLUX_DATA_FORGE" \
    -D "schema=PUBLIC" \
    -D "image_repo=FLUX_DATA_FORGE_REPO"

# 3. Create compute pool
snow sql -c $CONN -f scripts/sql/03_compute_pool.sql \
    -D "compute_pool=FLUX_DATA_FORGE_POOL" \
    -D "instance_family=CPU_X64_S" \
    -D "min_nodes=1" \
    -D "max_nodes=2"

# 4. Create target table
snow sql -c $CONN -f scripts/sql/04_target_table.sql \
    -D "database=FLUX_DATA_FORGE" \
    -D "schema=PUBLIC"

# 5. Push Docker image (see below)

# 6. Create service
snow sql -c $CONN -f scripts/sql/05_create_service.sql \
    -D "database=FLUX_DATA_FORGE" \
    -D "schema=PUBLIC" \
    -D "warehouse=FLUX_DATA_FORGE_WH" \
    -D "compute_pool=FLUX_DATA_FORGE_POOL" \
    -D "image_repo=FLUX_DATA_FORGE_REPO" \
    -D "service_name=FLUX_DATA_FORGE_SERVICE" \
    -D "image_tag=latest"

# 7. Validate
snow sql -c $CONN -f scripts/sql/06_validation.sql \
    -D "database=FLUX_DATA_FORGE" \
    -D "schema=PUBLIC" \
    -D "service_name=FLUX_DATA_FORGE_SERVICE" \
    -D "compute_pool=FLUX_DATA_FORGE_POOL"
```

## Push Docker Image (Between Steps 4 and 6)

After creating the image repository (script 02), push the Docker image:

```bash
# Get the repository URL
# Run in Snowflake: SHOW IMAGE REPOSITORIES LIKE 'FLUX_DATA_FORGE_REPO';

# Login to Snowflake registry
docker login <org>-<account>.registry.snowflakecomputing.com

# Build the image
cd flux-data-forge/spcs_app
docker build -t flux_data_forge:latest .

# Tag and push
docker tag flux_data_forge:latest <repository_url>/flux_data_forge:latest
docker push <repository_url>/flux_data_forge:latest
```

## Variable Reference

| Variable | Description | Example Values |
|----------|-------------|----------------|
| `database` | Target database name | `FLUX_DATA_FORGE` |
| `schema` | Schema name | `PUBLIC` |
| `warehouse` | Warehouse name | `FLUX_DATA_FORGE_WH` |
| `image_repo` | Image repository name | `FLUX_DATA_FORGE_REPO` |
| `compute_pool` | Compute pool name | `FLUX_DATA_FORGE_POOL` |
| `service_name` | SPCS service name | `FLUX_DATA_FORGE_SERVICE` |
| `instance_family` | Compute instance type | `CPU_X64_S`, `CPU_X64_M` |
| `min_nodes` | Minimum compute nodes | `1` |
| `max_nodes` | Maximum compute nodes | `2` |
| `image_tag` | Docker image tag | `latest` |

## Troubleshooting

### Compute Pool Not Starting
- Check SPCS is enabled for your account
- Verify you have CREATE COMPUTE POOL privilege

### Service Not Starting
- Verify Docker image was pushed successfully
- Check image path in service spec matches your repo
- View service logs: `CALL SYSTEM$GET_SERVICE_LOGS('SERVICE_NAME', '0', 'flux-data-forge', 100);`

### No Data in Table
- Ensure service is READY: `SELECT SYSTEM$GET_SERVICE_STATUS('SERVICE_NAME');`
- Check service logs for errors
- Generate data via the UI first

## Related Documentation

- [Flux Data Forge README](../../README.md)
- [SPCS Documentation](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
- [Flux Utility Platform](https://github.com/sfc-gh-abannerjee/flux-utility-solutions)
