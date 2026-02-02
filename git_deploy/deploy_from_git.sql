-- =============================================================================
-- Flux Data Forge - Deploy from Git
-- =============================================================================
-- Deploys Flux Data Forge infrastructure by executing SQL files directly
-- from the Git repository.
--
-- Prerequisites:
--   - Run setup_git_integration.sql first
--   - Git repository must be fetched (ALTER GIT REPOSITORY ... FETCH)
--
-- Usage with Snowflake CLI:
--   snow git execute "@FLUX_DATA_FORGE_REPO/branches/main/scripts/sql" \
--     --database FLUX_DATA_FORGE --schema PUBLIC
-- =============================================================================

-- Configuration
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET git_repo_name = 'FLUX_DATA_FORGE_REPO';
SET branch = 'main';

USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

-- Fetch latest from remote
ALTER GIT REPOSITORY IDENTIFIER($git_repo_name) FETCH;

-- =============================================================================
-- EXECUTE DEPLOYMENT SCRIPTS IN ORDER
-- =============================================================================
-- Note: Each EXECUTE IMMEDIATE runs a SQL file from the Git repo

-- Step 1: Database and Schema
EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/01_database_schema.sql;

-- Step 2: Image Repository
EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/02_image_repository.sql;

-- Step 3: Compute Pool
EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/03_compute_pool.sql;

-- Step 4: Target Table
EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/04_target_table.sql;

-- =============================================================================
-- PAUSE HERE: Push Docker image before continuing
-- =============================================================================
-- Before running script 05, you must:
--   1. docker login <registry_url>
--   2. docker build -t flux_data_forge:latest ./spcs_app
--   3. docker push <repository_url>/flux_data_forge:latest
-- =============================================================================

-- Step 5: Create Service (only run after Docker image is pushed)
-- EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/05_create_service.sql;

-- Step 6: Validation
-- EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/06_validation.sql;

-- =============================================================================
-- ALTERNATIVE: Execute single deploy script
-- =============================================================================
-- If you prefer the monolithic deployment:
-- EXECUTE IMMEDIATE FROM @FLUX_DATA_FORGE_REPO/branches/main/spcs_app/deploy_spcs.sql;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 
    'Git Deployment' as STEP,
    'Infrastructure deployed from Git' as STATUS,
    'Push Docker image, then run script 05' as NEXT_ACTION;
