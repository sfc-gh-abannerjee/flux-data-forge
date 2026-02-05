-- =============================================================================
-- FLUX DATA FORGE - STANDALONE QUICKSTART
-- =============================================================================
-- This script creates EVERYTHING needed to run Flux Data Forge independently.
-- No other repositories or deployments required.
--
-- USAGE:
--   1. Open this file in Snowsight (Snowflake web UI) or your SQL editor
--   2. Update the CONFIGURATION section below with your values
--   3. Run the entire script (or section by section)
--
-- WHAT THIS CREATES:
--   - FLUX_DATA_FORGE database with PUBLIC schema
--   - FLUX_DATA_FORGE_WH warehouse
--   - AMI_STREAMING_READINGS table for generated data
--   - Image repository for Docker images
--   - Compute pool for SPCS service
--
-- TIME TO COMPLETE: ~5-10 minutes (compute pool startup is the slowest step)
--
-- REQUIREMENTS:
--   - ACCOUNTADMIN role (or equivalent)
--   - Docker Desktop installed on your local machine
-- =============================================================================

-- =============================================================================
-- CONFIGURATION - Change these values for your environment
-- =============================================================================
-- Database and schema names
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET warehouse_name = 'FLUX_DATA_FORGE_WH';

-- SPCS resource names
SET image_repo_name = 'FLUX_DATA_FORGE_REPO';
SET compute_pool_name = 'FLUX_DATA_FORGE_POOL';
SET service_name = 'FLUX_DATA_FORGE_SERVICE';

-- Compute pool sizing (CPU_X64_XS, CPU_X64_S, CPU_X64_M, CPU_X64_L)
-- CPU_X64_S (2 vCPU, 8GB RAM) is recommended for typical workloads
SET instance_family = 'CPU_X64_S';
SET min_nodes = 1;
SET max_nodes = 2;

-- Service configuration
SET image_tag = 'latest';
SET service_area = 'HOUSTON_METRO';

-- =============================================================================
-- STEP 1: CREATE DATABASE AND SCHEMA
-- =============================================================================
-- This creates your standalone database (not shared with other Flux repos)

CREATE DATABASE IF NOT EXISTS IDENTIFIER($database_name)
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Standalone database for Flux Data Forge - Synthetic AMI Data Generation';

USE DATABASE IDENTIFIER($database_name);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_name)
    COMMENT = 'Main schema for Flux Data Forge objects';

USE SCHEMA IDENTIFIER($schema_name);

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($warehouse_name)
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Flux Data Forge queries';

USE WAREHOUSE IDENTIFIER($warehouse_name);

SELECT '1. DATABASE SETUP COMPLETE' as STATUS, 
       CURRENT_DATABASE() as DATABASE, 
       CURRENT_SCHEMA() as SCHEMA,
       CURRENT_WAREHOUSE() as WAREHOUSE;

-- =============================================================================
-- STEP 2: CREATE TARGET TABLE FOR AMI DATA
-- =============================================================================
-- This is where all generated AMI (smart meter) data will be written

CREATE TABLE IF NOT EXISTS AMI_STREAMING_READINGS (
    -- Core AMI fields
    METER_ID VARCHAR(50) NOT NULL 
        COMMENT 'Unique meter identifier',
    READING_TIMESTAMP TIMESTAMP_NTZ NOT NULL 
        COMMENT 'Meter reading timestamp',
    
    -- Measurements
    USAGE_KWH FLOAT 
        COMMENT '15-minute interval energy usage (kWh)',
    VOLTAGE FLOAT 
        COMMENT 'Voltage reading (V)',
    POWER_FACTOR FLOAT 
        COMMENT 'Power factor (0-1)',
    TEMPERATURE_C FLOAT 
        COMMENT 'Ambient temperature (Celsius)',
    
    -- Infrastructure context (for joining with grid topology)
    TRANSFORMER_ID VARCHAR(50) 
        COMMENT 'Associated transformer',
    CIRCUIT_ID VARCHAR(50) 
        COMMENT 'Associated circuit',
    SUBSTATION_ID VARCHAR(50) 
        COMMENT 'Associated substation',
    SERVICE_AREA VARCHAR(100) 
        COMMENT 'Service territory/region',
    CUSTOMER_SEGMENT VARCHAR(50) 
        COMMENT 'Customer classification (residential, commercial, industrial)',
    
    -- Location
    LATITUDE FLOAT 
        COMMENT 'Meter latitude',
    LONGITUDE FLOAT 
        COMMENT 'Meter longitude',
    
    -- Status flags
    IS_OUTAGE BOOLEAN DEFAULT FALSE 
        COMMENT 'Outage indicator',
    DATA_QUALITY VARCHAR(20) DEFAULT 'VALID' 
        COMMENT 'Quality flag: VALID, ESTIMATED, OUTAGE',
    EMISSION_PATTERN VARCHAR(50) 
        COMMENT 'Data emission pattern used for generation',
    PRODUCTION_MATCHED BOOLEAN DEFAULT FALSE 
        COMMENT 'Whether meter ID matched production data',
    
    -- Metadata
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() 
        COMMENT 'Snowflake ingestion timestamp'
)
CLUSTER BY (DATE_TRUNC('DAY', READING_TIMESTAMP), METER_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = TRUE
COMMENT = 'Streaming landing table for AMI data from Flux Data Forge';

SELECT '2. TARGET TABLE CREATED' as STATUS, 
       'AMI_STREAMING_READINGS' as TABLE_NAME;

-- =============================================================================
-- STEP 3: CREATE IMAGE REPOSITORY
-- =============================================================================
-- This stores your Docker image in Snowflake

CREATE IMAGE REPOSITORY IF NOT EXISTS IDENTIFIER($image_repo_name)
    COMMENT = 'Image repository for Flux Data Forge container images';

-- Show the repository URL (you'll need this for docker push)
SHOW IMAGE REPOSITORIES LIKE 'FLUX_DATA_FORGE_REPO';

-- IMPORTANT: Note the repository_url from the output above!
-- You'll need it to push your Docker image in the next step.
--
-- The URL looks like: <org>-<account>.registry.snowflakecomputing.com/FLUX_DATA_FORGE/PUBLIC/FLUX_DATA_FORGE_REPO
--
-- To push an image (run these in your terminal, not here):
--   docker login <org>-<account>.registry.snowflakecomputing.com
--   docker build -t flux_data_forge:latest -f spcs_app/Dockerfile spcs_app/
--   docker tag flux_data_forge:latest <repository_url>/flux_data_forge:latest
--   docker push <repository_url>/flux_data_forge:latest

SELECT '3. IMAGE REPOSITORY CREATED' as STATUS,
       $image_repo_name as REPOSITORY_NAME,
       'Run SHOW IMAGE REPOSITORIES to get the push URL' as NEXT_STEP;

-- =============================================================================
-- STEP 4: CREATE COMPUTE POOL
-- =============================================================================
-- This provisions the compute resources for your SPCS service
-- NOTE: This step takes 1-2 minutes as Snowflake provisions the nodes

CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($compute_pool_name)
    MIN_NODES = $min_nodes
    MAX_NODES = $max_nodes
    INSTANCE_FAMILY = $instance_family
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'Compute pool for Flux Data Forge SPCS service';

-- Check compute pool status
DESCRIBE COMPUTE POOL IDENTIFIER($compute_pool_name);

-- Wait for status to be IDLE or ACTIVE before proceeding
-- You can re-run the DESCRIBE command to check status
SELECT '4. COMPUTE POOL CREATED' as STATUS,
       $compute_pool_name as POOL_NAME,
       'Wait for IDLE or ACTIVE status (1-2 min)' as NEXT_STEP;

-- =============================================================================
-- STEP 5: VERIFY IMAGE IS PUSHED
-- =============================================================================
-- Before creating the service, verify your Docker image is in the repository
-- If this returns no results, you need to push your image first!

SELECT '5. CHECK: Verify Docker image is pushed' as STATUS,
       'Run this query after pushing your image:' as INSTRUCTION;

-- Uncomment and run this after pushing your image:
-- SELECT * FROM TABLE(REGISTRY_DB.PUBLIC.LIST_IMAGES_IN_REPOSITORY(
--     REPOSITORY_NAME => CONCAT($database_name, '.', $schema_name, '.', $image_repo_name)
-- ));

-- =============================================================================
-- STEP 6: CREATE THE SPCS SERVICE
-- =============================================================================
-- IMPORTANT: Only run this AFTER:
--   1. Compute pool shows IDLE or ACTIVE status
--   2. Docker image has been pushed to the repository
--
-- Uncomment the CREATE SERVICE block below when ready

/*
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
*/

-- After creating the service, check its status:
-- SELECT SYSTEM$GET_SERVICE_STATUS('FLUX_DATA_FORGE_SERVICE');

-- Get the application URL:
-- SHOW ENDPOINTS IN SERVICE FLUX_DATA_FORGE_SERVICE;

SELECT '6. SERVICE READY TO CREATE' as STATUS,
       'Uncomment CREATE SERVICE block after pushing Docker image' as NEXT_STEP;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================
-- Run these after the service is deployed and you've generated some data

-- Check service status
-- SELECT SYSTEM$GET_SERVICE_STATUS('FLUX_DATA_FORGE_SERVICE');

-- Get service URL
-- SHOW ENDPOINTS IN SERVICE FLUX_DATA_FORGE_SERVICE;

-- Check generated data
-- SELECT COUNT(*) as ROWS_GENERATED,
--        MIN(READING_TIMESTAMP) as FIRST_READING,
--        MAX(READING_TIMESTAMP) as LAST_READING,
--        COUNT(DISTINCT METER_ID) as UNIQUE_METERS
-- FROM AMI_STREAMING_READINGS;

-- Sample data preview
-- SELECT * FROM AMI_STREAMING_READINGS LIMIT 10;

-- =============================================================================
-- CLEANUP (if needed)
-- =============================================================================
-- Uncomment these to remove all resources created by this script

-- DROP SERVICE IF EXISTS FLUX_DATA_FORGE_SERVICE;
-- DROP COMPUTE POOL IF EXISTS FLUX_DATA_FORGE_POOL;
-- DROP IMAGE REPOSITORY IF EXISTS FLUX_DATA_FORGE_REPO;
-- DROP TABLE IF EXISTS AMI_STREAMING_READINGS;
-- DROP WAREHOUSE IF EXISTS FLUX_DATA_FORGE_WH;
-- DROP DATABASE IF EXISTS FLUX_DATA_FORGE;

-- =============================================================================
-- SUMMARY
-- =============================================================================
SELECT '=== STANDALONE SETUP COMPLETE ===' as MESSAGE;
SELECT 'Database: ' || $database_name as CREATED,
       'Schema: ' || $schema_name as SCHEMA,
       'Warehouse: ' || $warehouse_name as WAREHOUSE,
       'Table: AMI_STREAMING_READINGS' as TARGET_TABLE,
       'Compute Pool: ' || $compute_pool_name as COMPUTE_POOL,
       'Image Repo: ' || $image_repo_name as IMAGE_REPO;
       
SELECT 'NEXT STEPS:' as HEADER,
       '1. Push Docker image to repository (see Step 3 output)' as STEP_1,
       '2. Wait for compute pool to reach IDLE/ACTIVE status' as STEP_2,
       '3. Uncomment and run CREATE SERVICE in Step 6' as STEP_3,
       '4. Access app via URL from SHOW ENDPOINTS' as STEP_4;
