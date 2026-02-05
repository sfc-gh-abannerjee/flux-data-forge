-- =============================================================================
-- Flux Data Forge - 02: Image Repository
-- =============================================================================
-- Creates the image repository for storing Docker images.
--
-- Variables (Jinja2 syntax for Snow CLI):
--   <% database %>    - Target database name
--   <% schema %>      - Schema name (default: PUBLIC)
--   <% image_repo %>  - Image repository name
--
-- Usage:
--   snow sql -f scripts/sql/02_image_repository.sql \
--       -D "database=FLUX_DATA_FORGE" \
--       -D "schema=PUBLIC" \
--       -D "image_repo=FLUX_DATA_FORGE_REPO" \
--       -c your_connection_name
-- =============================================================================

USE DATABASE IDENTIFIER('<% database %>');
USE SCHEMA IDENTIFIER('<% schema %>');

-- Create image repository
CREATE IMAGE REPOSITORY IF NOT EXISTS IDENTIFIER('<% image_repo %>')
    COMMENT = 'Image repository for Flux Data Forge container images';

-- Show repository details (get the URL for docker push)
SHOW IMAGE REPOSITORIES LIKE '<% image_repo %>';

-- The repository_url column shows the full path for docker commands:
-- Format: <org>-<account>.registry.snowflakecomputing.com/<db>/<schema>/<repo>
--
-- To push an image:
--   docker login <org>-<account>.registry.snowflakecomputing.com
--   docker tag flux_data_forge:latest <repository_url>/flux_data_forge:latest
--   docker push <repository_url>/flux_data_forge:latest

SELECT 
    'Image Repository' as STEP,
    '<% image_repo %>' as REPOSITORY_NAME,
    'Run SHOW IMAGE REPOSITORIES to get the push URL' as NEXT_ACTION,
    'SUCCESS' as STATUS;
