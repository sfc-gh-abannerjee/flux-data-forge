-- =============================================================================
-- Flux Data Forge - 05: Create SPCS Service
-- =============================================================================
-- Creates the Snowpark Container Service that runs Flux Data Forge.
--
-- Prerequisites:
--   - Run scripts 01-04 first
--   - Docker image must be pushed to the image repository
--   - Compute pool must be in IDLE or ACTIVE state
--
-- IMPORTANT: Update the image path below to match your environment!
-- =============================================================================

-- Configuration - UPDATE THESE VALUES
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET warehouse_name = 'FLUX_DATA_FORGE_WH';
SET compute_pool_name = 'FLUX_DATA_FORGE_POOL';
SET image_repo_name = 'FLUX_DATA_FORGE_REPO';
SET service_name = 'FLUX_DATA_FORGE_SERVICE';
SET image_tag = 'latest';

USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);
USE WAREHOUSE IDENTIFIER($warehouse_name);

-- Verify compute pool is ready
DESCRIBE COMPUTE POOL IDENTIFIER($compute_pool_name);

-- Create the SPCS service
-- Note: The image path format is /<database>/<schema>/<repo>/<image>:<tag>
CREATE SERVICE IF NOT EXISTS IDENTIFIER($service_name)
    IN COMPUTE POOL IDENTIFIER($compute_pool_name)
    FROM SPECIFICATION $$
spec:
  containers:
    - name: flux-data-forge
      image: /FLUX_DATA_FORGE/PUBLIC/FLUX_DATA_FORGE_REPO/flux_data_forge:latest
      env:
        SNOWFLAKE_DATABASE: FLUX_DATA_FORGE
        SNOWFLAKE_SCHEMA: PUBLIC
        SNOWFLAKE_WAREHOUSE: FLUX_DATA_FORGE_WH
        SNOWFLAKE_ROLE: SYSADMIN
        AMI_TABLE: AMI_STREAMING_READINGS
        SERVICE_AREA: HOUSTON_METRO
        # Logging level
        LOG_LEVEL: INFO
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
    EXTERNAL_ACCESS_INTEGRATIONS = ()
    COMMENT = 'Flux Data Forge - Synthetic AMI Data Generation Service';

-- Check service status (wait for READY)
SELECT SYSTEM$GET_SERVICE_STATUS($service_name);

-- Get service endpoints (the app URL)
SHOW ENDPOINTS IN SERVICE IDENTIFIER($service_name);

SELECT 
    'SPCS Service' as STEP,
    $service_name as SERVICE_NAME,
    'Check SHOW ENDPOINTS for the application URL' as NEXT_ACTION,
    'SUCCESS' as STATUS;
