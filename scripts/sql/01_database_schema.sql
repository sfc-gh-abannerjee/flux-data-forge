-- =============================================================================
-- Flux Data Forge - 01: Database and Schema Setup
-- =============================================================================
-- Creates the database and schema for Flux Data Forge.
--
-- Variables (Jinja2 syntax for Snow CLI):
--   <% database %>   - Target database name (default: FLUX_DATA_FORGE)
--   <% schema %>     - Schema name (default: PUBLIC)
--   <% warehouse %>  - Warehouse name
--
-- Usage:
--   snow sql -f scripts/sql/01_database_schema.sql \
--       -D "database=FLUX_DATA_FORGE" \
--       -D "schema=PUBLIC" \
--       -D "warehouse=FLUX_DATA_FORGE_WH" \
--       -c your_connection_name
-- =============================================================================

-- Create database (if needed)
CREATE DATABASE IF NOT EXISTS IDENTIFIER('<% database %>')
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Database for Flux Data Forge - Synthetic AMI Data Generation';

-- Use the database
USE DATABASE IDENTIFIER('<% database %>');

-- Create schema (if needed)
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('<% schema %>')
    COMMENT = 'Schema for Flux Data Forge objects';

-- Use the schema
USE SCHEMA IDENTIFIER('<% schema %>');

-- Create warehouse (if needed)
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER('<% warehouse %>')
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Flux Data Forge queries';

USE WAREHOUSE IDENTIFIER('<% warehouse %>');

-- Verify setup
SELECT 
    'Database/Schema Setup' as STEP,
    CURRENT_DATABASE() as DATABASE,
    CURRENT_SCHEMA() as SCHEMA,
    CURRENT_WAREHOUSE() as WAREHOUSE,
    'SUCCESS' as STATUS;
