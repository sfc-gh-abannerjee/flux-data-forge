-- =============================================================================
-- Flux Data Forge - 06: Validation Queries
-- =============================================================================
-- Run these queries to validate your deployment is working correctly.
--
-- Prerequisites:
--   - All previous scripts (01-05) completed
--   - Service is in READY state
--   - You've generated some test data via the UI
-- =============================================================================

-- Configuration
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET service_name = 'FLUX_DATA_FORGE_SERVICE';
SET compute_pool_name = 'FLUX_DATA_FORGE_POOL';

USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

-- =============================================================================
-- 1. SERVICE HEALTH CHECK
-- =============================================================================

-- Check service status (should be READY)
SELECT SYSTEM$GET_SERVICE_STATUS($service_name) as SERVICE_STATUS;

-- Get service endpoints
SHOW ENDPOINTS IN SERVICE IDENTIFIER($service_name);

-- View recent service logs (useful for debugging)
-- CALL SYSTEM$GET_SERVICE_LOGS($service_name, '0', 'flux-data-forge', 50);

-- =============================================================================
-- 2. DATA VALIDATION
-- =============================================================================

-- Row count and date range
SELECT 
    COUNT(*) as TOTAL_ROWS,
    MIN(READING_TIMESTAMP) as EARLIEST_READING,
    MAX(READING_TIMESTAMP) as LATEST_READING,
    COUNT(DISTINCT METER_ID) as UNIQUE_METERS,
    COUNT(DISTINCT SERVICE_AREA) as SERVICE_AREAS
FROM AMI_STREAMING_READINGS;

-- Data quality distribution
SELECT 
    DATA_QUALITY,
    COUNT(*) as ROW_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as PERCENTAGE
FROM AMI_STREAMING_READINGS
GROUP BY DATA_QUALITY
ORDER BY ROW_COUNT DESC;

-- Recent data (last 10 rows)
SELECT *
FROM AMI_STREAMING_READINGS
ORDER BY INGESTION_TIMESTAMP DESC
LIMIT 10;

-- =============================================================================
-- 3. INFRASTRUCTURE CHECK
-- =============================================================================

-- Compute pool status
DESCRIBE COMPUTE POOL IDENTIFIER($compute_pool_name);

-- Image repository
SHOW IMAGE REPOSITORIES LIKE 'FLUX_DATA_FORGE_REPO';

-- =============================================================================
-- 4. SAMPLE ANALYTICS QUERIES
-- =============================================================================

-- Hourly usage aggregation
SELECT 
    DATE_TRUNC('HOUR', READING_TIMESTAMP) as HOUR,
    COUNT(*) as READINGS,
    ROUND(AVG(USAGE_KWH), 3) as AVG_USAGE_KWH,
    ROUND(AVG(VOLTAGE), 1) as AVG_VOLTAGE,
    SUM(CASE WHEN IS_OUTAGE THEN 1 ELSE 0 END) as OUTAGE_COUNT
FROM AMI_STREAMING_READINGS
WHERE READING_TIMESTAMP >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC
LIMIT 24;

-- Usage by customer segment
SELECT 
    CUSTOMER_SEGMENT,
    COUNT(DISTINCT METER_ID) as METERS,
    ROUND(AVG(USAGE_KWH), 3) as AVG_USAGE_KWH,
    ROUND(SUM(USAGE_KWH), 2) as TOTAL_USAGE_KWH
FROM AMI_STREAMING_READINGS
GROUP BY CUSTOMER_SEGMENT
ORDER BY TOTAL_USAGE_KWH DESC;

-- =============================================================================
-- VALIDATION SUMMARY
-- =============================================================================

SELECT 
    'Validation Complete' as STATUS,
    (SELECT COUNT(*) FROM AMI_STREAMING_READINGS) as TOTAL_ROWS,
    (SELECT SYSTEM$GET_SERVICE_STATUS($service_name)) as SERVICE_STATUS;
