-- =============================================================================
-- Flux Data Forge - Git Integration Setup
-- =============================================================================
-- This script sets up Snowflake Git integration to enable deployments
-- directly from the GitHub repository.
--
-- Benefits:
--   - Deploy infrastructure from Git without local files
--   - CI/CD integration with GitHub Actions
--   - Version-controlled deployments
--
-- Prerequisites:
--   - ACCOUNTADMIN role (for creating integrations)
--   - GitHub Personal Access Token (for private repos)
-- =============================================================================

-- Configuration
SET database_name = 'FLUX_DATA_FORGE';
SET schema_name = 'PUBLIC';
SET git_repo_name = 'FLUX_DATA_FORGE_REPO';

-- For public repos, no secret needed
-- For private repos, create a secret with your GitHub PAT:
-- CREATE SECRET github_pat TYPE = PASSWORD
--     USERNAME = 'your_github_username'
--     PASSWORD = 'ghp_your_personal_access_token';

-- =============================================================================
-- 1. CREATE API INTEGRATION (one-time setup per account)
-- =============================================================================

CREATE API INTEGRATION IF NOT EXISTS git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED = TRUE
    COMMENT = 'Git API integration for Flux repositories';

-- =============================================================================
-- 2. CREATE GIT REPOSITORY
-- =============================================================================

USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($schema_name);

CREATE GIT REPOSITORY IF NOT EXISTS IDENTIFIER($git_repo_name)
    API_INTEGRATION = git_api_integration
    ORIGIN = 'https://github.com/sfc-gh-abannerjee/flux-data-forge.git'
    -- Uncomment for private repos:
    -- GIT_CREDENTIALS = github_pat
    COMMENT = 'Flux Data Forge Git repository';

-- =============================================================================
-- 3. FETCH LATEST
-- =============================================================================

ALTER GIT REPOSITORY IDENTIFIER($git_repo_name) FETCH;

-- =============================================================================
-- 4. LIST AVAILABLE BRANCHES
-- =============================================================================

SHOW GIT BRANCHES IN IDENTIFIER($git_repo_name);

-- =============================================================================
-- 5. LIST FILES IN REPO
-- =============================================================================

-- List SQL files
SELECT * 
FROM TABLE(
    DIRECTORY(@FLUX_DATA_FORGE_REPO/branches/main/)
)
WHERE RELATIVE_PATH LIKE '%.sql'
ORDER BY RELATIVE_PATH;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 
    'Git Integration' as STEP,
    $git_repo_name as REPOSITORY,
    'Run deploy_from_git.sql to deploy' as NEXT_ACTION,
    'SUCCESS' as STATUS;
