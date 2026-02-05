"""
Centralized configuration for Flux Data Forge SPCS application.

All database references should use this module to ensure configurability
across different deployment environments.

Environment Variables:
    SNOWFLAKE_DATABASE: Target database name (default: FLUX_DB)
"""

import os

# Primary database - configurable via environment variable
DB = os.getenv("SNOWFLAKE_DATABASE", "FLUX_DB")

# Warehouse - configurable via environment variable
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FLUX_WH")

# Schema names
SCHEMA_PRODUCTION = "PRODUCTION"
SCHEMA_APPLICATIONS = "APPLICATIONS"
SCHEMA_DEV = "DEV"


def get_table_path(schema: str, table: str) -> str:
    """Get fully qualified table path."""
    return f"{DB}.{schema}.{table}"


def get_production_table(table: str) -> str:
    """Get path to a PRODUCTION schema table."""
    return get_table_path(SCHEMA_PRODUCTION, table)


def get_applications_table(table: str) -> str:
    """Get path to an APPLICATIONS schema table."""
    return get_table_path(SCHEMA_APPLICATIONS, table)
