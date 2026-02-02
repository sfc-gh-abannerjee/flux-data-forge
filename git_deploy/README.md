# Flux Data Forge - Git Integration Deployment

This directory contains SQL scripts for deploying Flux Data Forge using Snowflake's native Git integration.

## Overview

Snowflake Git integration allows you to:
- Execute SQL files directly from a GitHub repository
- Deploy infrastructure without downloading files locally
- Integrate with CI/CD pipelines via GitHub Actions
- Maintain version-controlled deployments

## Quick Start

### 1. One-Time Setup

```sql
-- Run in Snowflake Worksheets with ACCOUNTADMIN role
!source setup_git_integration.sql
```

This creates:
- API integration for GitHub access
- Git repository object linked to this repo

### 2. Deploy from Git

```sql
-- Run the deployment script
!source deploy_from_git.sql
```

### 3. Using Snowflake CLI

```bash
# Fetch latest
snow git fetch FLUX_DATA_FORGE_REPO

# Execute all SQL scripts in order
snow git execute "@FLUX_DATA_FORGE_REPO/branches/main/scripts/sql" \
  --database FLUX_DATA_FORGE \
  --schema PUBLIC
```

## GitHub Actions Integration

Add this workflow to `.github/workflows/deploy.yml`:

```yaml
name: Deploy to Snowflake
on:
  push:
    branches: [main]
    paths:
      - 'scripts/sql/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: Snowflake-Labs/snowflake-cli-action@v1
        with:
          cli-version: latest
          default-config-file-path: snowflake.yml
      
      - name: Fetch Git Repository
        run: snow git fetch FLUX_DATA_FORGE_REPO
        
      - name: Deploy Infrastructure
        run: |
          snow git execute "@FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/01_database_schema.sql"
          snow git execute "@FLUX_DATA_FORGE_REPO/branches/main/scripts/sql/02_image_repository.sql"
          # ... etc
```

## Private Repository Setup

For private repositories, create a secret with your GitHub PAT:

```sql
CREATE SECRET github_pat
    TYPE = PASSWORD
    USERNAME = 'your_github_username'
    PASSWORD = 'ghp_your_personal_access_token';

-- Then reference in repository creation:
CREATE GIT REPOSITORY flux_data_forge_repo
    API_INTEGRATION = git_api_integration
    ORIGIN = 'https://github.com/your-org/flux-data-forge.git'
    GIT_CREDENTIALS = github_pat;
```

## Files in This Directory

| File | Purpose |
|------|---------|
| `setup_git_integration.sql` | One-time setup of Git integration |
| `deploy_from_git.sql` | Execute deployment scripts from Git |
| `README.md` | This documentation |

## Related Documentation

- [Snowflake Git Integration](https://docs.snowflake.com/en/developer-guide/git/git-overview)
- [Snowflake CLI Git Commands](https://docs.snowflake.com/en/developer-guide/snowflake-cli/git/overview)
- [Flux Utility Platform](https://github.com/sfc-gh-abannerjee/flux-utility-solutions)
