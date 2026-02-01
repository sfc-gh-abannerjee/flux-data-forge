-- =============================================================================
-- Flux Data Forge - SPCS Deployment Script
-- =============================================================================
-- This script creates the necessary Snowflake objects for deploying
-- Flux Data Forge as a Snowpark Container Service.
--
-- INSTRUCTIONS:
-- 1. Update the configuration variables below (Section 0)
-- 2. Run the PRE-FLIGHT CHECKS section first to validate your environment
-- 3. If all checks pass, run the remaining sections in order
-- =============================================================================

-- =============================================================================
-- 0. CONFIGURATION - UPDATE THESE VALUES
-- =============================================================================
SET database_name = 'MY_DATABASE';        -- Your target database
SET schema_name = 'MY_SCHEMA';            -- Your target schema
SET warehouse_name = 'MY_WAREHOUSE';      -- Warehouse for queries
SET compute_pool_name = 'MY_COMPUTE_POOL'; -- SPCS compute pool
SET image_repo_name = 'FLUX_DATA_FORGE_REPO';  -- Image repository name
SET service_name = 'FLUX_DATA_FORGE_SERVICE';  -- Service name

-- =============================================================================
-- 1. PRE-FLIGHT CHECKS - Run these FIRST to validate your environment
-- =============================================================================
-- These queries help identify issues before deployment fails.

-- Check 1: Verify current role has necessary permissions
SELECT 
    '✓ Current Role' as CHECK_NAME,
    CURRENT_ROLE() as VALUE,
    'Ensure this role can create services, compute pools, and repositories' as NOTE;

-- Check 2: Verify account has SPCS enabled
SELECT 
    '✓ SPCS Enabled' as CHECK_NAME,
    CASE WHEN SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_08') IS NOT NULL 
         THEN 'Yes' ELSE 'Check with support' END as VALUE,
    'SPCS requires specific account enablement' as NOTE;

-- Check 3: List existing compute pools (at least one must exist or be created)
SHOW COMPUTE POOLS;

-- Check 4: Verify warehouse exists and is accessible
SHOW WAREHOUSES LIKE $warehouse_name;

-- Check 5: Check if database/schema exist
SHOW DATABASES LIKE $database_name;
SHOW SCHEMAS LIKE $schema_name IN DATABASE IDENTIFIER($database_name);

-- Check 6: Verify you can use the target database/schema
USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);
USE WAREHOUSE IDENTIFIER($warehouse_name);

-- Check 7: List existing image repositories (for reference)
SHOW IMAGE REPOSITORIES;

-- =============================================================================
-- PRE-FLIGHT SUMMARY
-- =============================================================================
-- If you see errors above, fix them before proceeding:
--
-- ERROR: "Database does not exist"
--   → CREATE DATABASE <your_database>;
--
-- ERROR: "Schema does not exist"  
--   → CREATE SCHEMA <your_database>.<your_schema>;
--
-- ERROR: "Warehouse does not exist"
--   → CREATE WAREHOUSE <your_warehouse> WAREHOUSE_SIZE = 'XSMALL';
--
-- ERROR: "Compute pool does not exist" (when creating service)
--   → Uncomment and run Section 2 below to create one
--
-- ERROR: "Insufficient privileges"
--   → USE ROLE ACCOUNTADMIN; or request privileges from admin
-- =============================================================================

-- =============================================================================
-- 2. CREATE IMAGE REPOSITORY
-- =============================================================================
CREATE IMAGE REPOSITORY IF NOT EXISTS IDENTIFIER($image_repo_name)
    COMMENT = 'Image repository for Flux Data Forge';

-- Get the repository URL - you'll need this for docker push
-- Format: <org>-<account>.registry.snowflakecomputing.com/<db>/<schema>/<repo>
SHOW IMAGE REPOSITORIES LIKE $image_repo_name;

-- =============================================================================
-- 3. CREATE COMPUTE POOL (if needed)
-- =============================================================================
-- Uncomment the block below if you need to create a new compute pool.
-- Skip if using an existing pool.

/*
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($compute_pool_name)
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'Compute pool for Flux Data Forge';

-- Wait for compute pool to be ready (ACTIVE or IDLE state)
DESCRIBE COMPUTE POOL IDENTIFIER($compute_pool_name);
*/

-- =============================================================================
-- 4. CREATE TARGET TABLE (landing zone for AMI data)
-- =============================================================================
CREATE TABLE IF NOT EXISTS AMI_STREAMING_READINGS (
    -- Core AMI fields
    METER_ID VARCHAR(50) NOT NULL COMMENT 'Unique meter identifier',
    READING_TIMESTAMP TIMESTAMP_NTZ NOT NULL COMMENT 'Meter reading timestamp',
    
    -- Measurements
    USAGE_KWH FLOAT COMMENT '15-minute interval energy usage (kWh)',
    VOLTAGE FLOAT COMMENT 'Voltage reading (V)',
    POWER_FACTOR FLOAT COMMENT 'Power factor',
    TEMPERATURE_C FLOAT COMMENT 'Ambient temperature (Celsius)',
    
    -- Infrastructure context
    TRANSFORMER_ID VARCHAR(50) COMMENT 'Associated transformer',
    CIRCUIT_ID VARCHAR(50) COMMENT 'Associated circuit',
    SUBSTATION_ID VARCHAR(50) COMMENT 'Associated substation',
    SERVICE_AREA VARCHAR(100) COMMENT 'Service territory/region',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer classification',
    
    -- Location
    LATITUDE FLOAT COMMENT 'Meter latitude',
    LONGITUDE FLOAT COMMENT 'Meter longitude',
    
    -- Status
    IS_OUTAGE BOOLEAN DEFAULT FALSE COMMENT 'Outage indicator',
    DATA_QUALITY VARCHAR(20) DEFAULT 'VALID' COMMENT 'Quality flag: VALID, ESTIMATED, OUTAGE',
    EMISSION_PATTERN VARCHAR(50) COMMENT 'Data emission pattern used',
    PRODUCTION_MATCHED BOOLEAN DEFAULT FALSE COMMENT 'Whether meter ID matched production data',
    
    -- Metadata
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Snowflake ingestion timestamp'
)
CLUSTER BY (DATE_TRUNC('DAY', READING_TIMESTAMP), METER_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = ON
COMMENT = 'Streaming landing table for AMI data from Flux Data Forge';

-- =============================================================================
-- 5. CREATE SPCS SERVICE
-- =============================================================================
-- IMPORTANT: Before running this, ensure:
-- 1. Docker image is pushed to the repository (run build_and_push.sh)
-- 2. service_spec.yaml is uploaded or inline spec is used
-- 3. Compute pool exists and is in ACTIVE or IDLE state

-- Option A: Create from uploaded spec file (if using Snowflake stage)
-- CREATE SERVICE IF NOT EXISTS IDENTIFIER($service_name)
--     IN COMPUTE POOL IDENTIFIER($compute_pool_name)
--     FROM @my_stage/service_spec.yaml
--     EXTERNAL_ACCESS_INTEGRATIONS = ()
--     COMMENT = 'Flux Data Forge - Synthetic AMI Data Generation Service';

-- Option B: Create with inline specification
CREATE SERVICE IF NOT EXISTS IDENTIFIER($service_name)
    IN COMPUTE POOL IDENTIFIER($compute_pool_name)
    FROM SPECIFICATION $$
spec:
  containers:
    - name: flux-data-forge
      image: /MY_DATABASE/MY_SCHEMA/FLUX_DATA_FORGE_REPO/flux_data_forge:latest
      env:
        SNOWFLAKE_DATABASE: MY_DATABASE
        SNOWFLAKE_SCHEMA: MY_SCHEMA
        SNOWFLAKE_WAREHOUSE: MY_WAREHOUSE
        SNOWFLAKE_ROLE: SYSADMIN
        AMI_TABLE: AMI_STREAMING_READINGS
        SERVICE_AREA: HOUSTON_METRO
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

-- =============================================================================
-- 6. VERIFY DEPLOYMENT
-- =============================================================================

-- Check service status (should be READY)
SELECT SYSTEM$GET_SERVICE_STATUS($service_name);

-- Describe service details
DESCRIBE SERVICE IDENTIFIER($service_name);

-- View service logs (useful for debugging)
-- CALL SYSTEM$GET_SERVICE_LOGS($service_name, '0', 'flux-data-forge', 100);

-- =============================================================================
-- 7. GET SERVICE URL
-- =============================================================================
-- The ingress_url is your application URL
SHOW ENDPOINTS IN SERVICE IDENTIFIER($service_name);

-- =============================================================================
-- 8. POST-DEPLOYMENT VALIDATION
-- =============================================================================
-- After getting the URL, verify the service is healthy:
-- 1. Open the ingress_url in your browser
-- 2. You should see the Flux Data Forge UI
-- 3. Try generating a small batch (Quick Demo preset)
-- 4. Check the target table for data:

SELECT COUNT(*) as ROW_COUNT, 
       MIN(READING_TIMESTAMP) as EARLIEST,
       MAX(READING_TIMESTAMP) as LATEST
FROM AMI_STREAMING_READINGS;

-- =============================================================================
-- CLEANUP (if needed)
-- =============================================================================
-- To remove the deployment:
-- DROP SERVICE IF EXISTS IDENTIFIER($service_name);
-- DROP TABLE IF EXISTS AMI_STREAMING_READINGS;
-- DROP IMAGE REPOSITORY IF EXISTS IDENTIFIER($image_repo_name);
-- DROP COMPUTE POOL IF EXISTS IDENTIFIER($compute_pool_name);  -- if you created one
