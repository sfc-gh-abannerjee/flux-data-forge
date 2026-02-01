-- =============================================================================
-- Flux Data Forge - SPCS Deployment Script
-- =============================================================================
-- This script creates the necessary Snowflake objects for deploying
-- Flux Data Forge as a Snowpark Container Service.
--
-- Prerequisites:
-- 1. ACCOUNTADMIN role (or equivalent permissions)
-- 2. Docker image pushed to Snowflake Image Registry
-- 3. Compute pool created (or use existing)
-- =============================================================================

-- Configuration variables - UPDATE THESE
SET database_name = '<YOUR_DATABASE>';
SET schema_name = '<YOUR_SCHEMA>';
SET warehouse_name = '<YOUR_WAREHOUSE>';
SET compute_pool_name = '<YOUR_COMPUTE_POOL>';
SET image_repo_name = '<YOUR_IMAGE_REPO>';
SET service_name = 'FLUX_DATA_FORGE_SERVICE';

-- Use the target database and schema
USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

-- =============================================================================
-- 1. CREATE IMAGE REPOSITORY (if not exists)
-- =============================================================================
CREATE IMAGE REPOSITORY IF NOT EXISTS IDENTIFIER($image_repo_name)
    COMMENT = 'Image repository for Flux Data Forge';

-- Show repository URL for docker push
SHOW IMAGE REPOSITORIES LIKE $image_repo_name;

-- =============================================================================
-- 2. CREATE COMPUTE POOL (if not exists)
-- =============================================================================
-- Uncomment and modify if you need to create a compute pool
/*
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($compute_pool_name)
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    COMMENT = 'Compute pool for Flux Data Forge';
*/

-- =============================================================================
-- 3. CREATE STREAMING TABLE (landing zone for AMI data)
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
-- 4. CREATE SPCS SERVICE
-- =============================================================================
-- Drop existing service if updating
-- DROP SERVICE IF EXISTS IDENTIFIER($service_name);

CREATE SERVICE IF NOT EXISTS IDENTIFIER($service_name)
    IN COMPUTE POOL IDENTIFIER($compute_pool_name)
    FROM SPECIFICATION_FILE = 'service_spec.yaml'
    EXTERNAL_ACCESS_INTEGRATIONS = ()  -- Add if external network access needed
    COMMENT = 'Flux Data Forge - Synthetic AMI Data Generation Service';

-- =============================================================================
-- 5. GRANT ACCESS TO SERVICE ENDPOINT
-- =============================================================================
-- Grant access to specific roles as needed
-- GRANT USAGE ON SERVICE IDENTIFIER($service_name) TO ROLE <YOUR_ROLE>;

-- =============================================================================
-- 6. CHECK SERVICE STATUS
-- =============================================================================
DESCRIBE SERVICE IDENTIFIER($service_name);
SELECT SYSTEM$GET_SERVICE_STATUS($service_name);
CALL SYSTEM$GET_SERVICE_LOGS($service_name, '0', 'flux-data-forge', 100);

-- =============================================================================
-- 7. GET SERVICE ENDPOINT URL
-- =============================================================================
SHOW ENDPOINTS IN SERVICE IDENTIFIER($service_name);

-- The service URL will be shown in the INGRESS_URL column
-- Format: https://<random>-<org>-<account>.snowflakecomputing.app
