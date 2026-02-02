-- =============================================================================
-- Flux Data Forge - 04: Target Table
-- =============================================================================
-- Creates the AMI_STREAMING_READINGS table where generated data lands.
--
-- Prerequisites:
--   - Run 01_database_schema.sql first
-- =============================================================================

-- Configuration
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';

USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

-- Create target table for streaming AMI data
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
-- Cluster for efficient time-range and meter queries
CLUSTER BY (DATE_TRUNC('DAY', READING_TIMESTAMP), METER_ID)
-- 7-day time travel for recovery
DATA_RETENTION_TIME_IN_DAYS = 7
-- Enable change tracking for streams/CDC
CHANGE_TRACKING = ON
COMMENT = 'Streaming landing table for AMI data from Flux Data Forge';

-- Verify table creation
DESCRIBE TABLE AMI_STREAMING_READINGS;

SELECT 
    'Target Table' as STEP,
    'AMI_STREAMING_READINGS' as TABLE_NAME,
    'Ready for streaming data' as NEXT_ACTION,
    'SUCCESS' as STATUS;
