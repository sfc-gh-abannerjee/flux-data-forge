# Flux Data Forge - SQL Deployment Scripts

This directory contains modular SQL scripts for deploying Flux Data Forge to Snowflake SPCS.

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

## Quick Deployment

### Option 1: Run All Scripts

```sql
-- In Snowflake Worksheets, run each script in order
-- Scripts 01-04 can be run immediately
-- Before script 05, you must push the Docker image
```

### Option 2: Use the Single Deployment Script

For convenience, the original `../spcs_app/deploy_spcs.sql` contains all steps in one file.

## Between Script 04 and 05: Push Docker Image

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

## Customization

Each script has configuration variables at the top. Update these before running:

```sql
-- Example from 01_database_schema.sql
SET database_name = 'MY_DATABASE';
SET schema_name = 'MY_SCHEMA';
SET warehouse_name = 'MY_WAREHOUSE';
```

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
