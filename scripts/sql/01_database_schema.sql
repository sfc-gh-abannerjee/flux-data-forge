-- =============================================================================
-- Flux Data Forge - 01: Database and Schema Setup
-- =============================================================================
-- Creates the database and schema for Flux Data Forge.
--
-- Usage:
--   Run this script first to set up the foundation.
--   Update the variables below for your environment.
-- =============================================================================

-- Configuration variables
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET warehouse_name = 'FLUX_DATA_FORGE_WH';

-- Create database (if needed)
CREATE DATABASE IF NOT EXISTS IDENTIFIER($database_name)
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Database for Flux Data Forge - Synthetic AMI Data Generation';

-- Use the database
USE DATABASE IDENTIFIER($database_name);

-- Create schema (if needed)
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_name)
    COMMENT = 'Schema for Flux Data Forge objects';

-- Use the schema
USE SCHEMA IDENTIFIER($schema_name);

-- Create warehouse (if needed)
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($warehouse_name)
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Flux Data Forge queries';

USE WAREHOUSE IDENTIFIER($warehouse_name);

-- Verify setup
SELECT 
    'Database/Schema Setup' as STEP,
    CURRENT_DATABASE() as DATABASE,
    CURRENT_SCHEMA() as SCHEMA,
    CURRENT_WAREHOUSE() as WAREHOUSE,
    'SUCCESS' as STATUS;
