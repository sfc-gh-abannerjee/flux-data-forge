-- =============================================================================
-- Flux Data Forge - 02: Image Repository
-- =============================================================================
-- Creates the image repository for storing Docker images.
--
-- Prerequisites:
--   - Run 01_database_schema.sql first
-- =============================================================================

-- Configuration
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET image_repo_name = 'FLUX_DATA_FORGE_REPO';

USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

-- Create image repository
CREATE IMAGE REPOSITORY IF NOT EXISTS IDENTIFIER($image_repo_name)
    COMMENT = 'Image repository for Flux Data Forge container images';

-- Show repository details (get the URL for docker push)
SHOW IMAGE REPOSITORIES LIKE $image_repo_name;

-- The repository_url column shows the full path for docker commands:
-- Format: <org>-<account>.registry.snowflakecomputing.com/<db>/<schema>/<repo>
--
-- To push an image:
--   docker login <org>-<account>.registry.snowflakecomputing.com
--   docker tag flux_data_forge:latest <repository_url>/flux_data_forge:latest
--   docker push <repository_url>/flux_data_forge:latest

SELECT 
    'Image Repository' as STEP,
    $image_repo_name as REPOSITORY_NAME,
    'Run SHOW IMAGE REPOSITORIES to get the push URL' as NEXT_ACTION,
    'SUCCESS' as STATUS;
