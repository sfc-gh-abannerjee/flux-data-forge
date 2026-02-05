-- =============================================================================
-- Flux Data Forge - 05: Create SPCS Service
-- =============================================================================
-- Creates the Snowpark Container Service that runs Flux Data Forge.
--
-- Variables (Jinja2 syntax for Snow CLI):
--   <% database %>       - Target database name
--   <% schema %>         - Schema name (default: PUBLIC)
--   <% warehouse %>      - Warehouse name
--   <% compute_pool %>   - Compute pool name
--   <% image_repo %>     - Image repository name
--   <% service_name %>   - SPCS service name
--   <% image_tag %>      - Docker image tag (default: latest)
--
-- Usage:
--   snow sql -f scripts/sql/05_create_service.sql \
--       -D "database=FLUX_DATA_FORGE" \
--       -D "schema=PUBLIC" \
--       -D "warehouse=FLUX_DATA_FORGE_WH" \
--       -D "compute_pool=FLUX_DATA_FORGE_POOL" \
--       -D "image_repo=FLUX_DATA_FORGE_REPO" \
--       -D "service_name=FLUX_DATA_FORGE_SERVICE" \
--       -D "image_tag=latest" \
--       -c your_connection_name
--
-- Prerequisites:
--   - Run scripts 01-04 first
--   - Docker image must be pushed to the image repository
--   - Compute pool must be in IDLE or ACTIVE state
-- =============================================================================

USE DATABASE IDENTIFIER('<% database %>');
USE SCHEMA IDENTIFIER('<% schema %>');
USE WAREHOUSE IDENTIFIER('<% warehouse %>');

-- Verify compute pool is ready
DESCRIBE COMPUTE POOL IDENTIFIER('<% compute_pool %>');

-- Create the SPCS service
CREATE SERVICE IF NOT EXISTS IDENTIFIER('<% service_name %>')
    IN COMPUTE POOL IDENTIFIER('<% compute_pool %>')
    FROM SPECIFICATION $$
spec:
  containers:
    - name: flux-data-forge
      image: /<% database %>/<% schema %>/<% image_repo %>/flux_data_forge:<% image_tag %>
      env:
        SNOWFLAKE_DATABASE: <% database %>
        SNOWFLAKE_SCHEMA: <% schema %>
        SNOWFLAKE_WAREHOUSE: <% warehouse %>
        SNOWFLAKE_ROLE: SYSADMIN
        AMI_TABLE: AMI_STREAMING_READINGS
        SERVICE_AREA: HOUSTON_METRO
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
SELECT SYSTEM$GET_SERVICE_STATUS('<% service_name %>');

-- Get service endpoints (the app URL)
SHOW ENDPOINTS IN SERVICE IDENTIFIER('<% service_name %>');

SELECT 
    'SPCS Service' as STEP,
    '<% service_name %>' as SERVICE_NAME,
    'Check SHOW ENDPOINTS for the application URL' as NEXT_ACTION,
    'SUCCESS' as STATUS;
