"""
Snowpipe Streaming Implementation - Dual Architecture Support

This module provides implementations for BOTH Snowpipe Streaming architectures:

1. CLASSIC Architecture (SQL INSERT via Snowpark):
   - The Classic Snowpipe Streaming SDK is Java-only (snowflake-ingest-sdk).
   - There is NO Python SDK for Classic architecture.
   - For Python apps, the equivalent is SQL INSERT via Snowpark session,
     which provides direct table writes without PIPE objects.
   - Client-side schema validation.
   - Channels open directly against tables.

2. HIGH-PERFORMANCE (HP) Architecture:
   - Python SDK available: pip install snowpipe-streaming
   - Import: from snowflake.ingest.streaming import StreamingIngestClient
   - Requires PIPE objects with DATA_SOURCE(TYPE => 'STREAMING')
   - Server-side schema validation with in-flight transforms
   - Up to 10 GB/s per table, sub-10-second latency
   - Throughput-based billing
   - GA on AWS since Sep 23, 2025

SDK API Reference (HP only):
- StreamingIngestClient(client_name, db_name, schema_name, pipe_name, properties)
- client.open_channel(channel_name) -> (channel, status)
- channel.append_row(row, offset_token) / append_rows(rows, start_offset, end_offset)
- channel.initiate_flush() / wait_for_flush(timeout_seconds)
- channel.close() / client.close()
- client.get_channel_statuses([channel_name]) -> dict of ChannelStatus

Author: Flux Data Forge Team
Date: January 2026
"""

import os
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading

from config import DB, SCHEMA_PRODUCTION

logger = logging.getLogger(__name__)


# ============================================================================
# ENVIRONMENT DETECTION
# ============================================================================

def is_running_in_spcs() -> bool:
    """Detect if running inside Snowpark Container Services.
    SPCS mounts secrets at /usr/local/creds/secret_string."""
    return os.path.exists("/usr/local/creds/secret_string")


# ============================================================================
# ARCHITECTURE INFO - Exported for /api/streaming/architectures endpoint
# ============================================================================

ARCHITECTURE_INFO = {
    "sql_insert": {
        "name": "SQL INSERT (via Snowpark)",
        "sdk": "snowflake-snowpark-python",
        "native_sdk": "snowflake-ingest-sdk (Java only)",
        "description": (
            "Direct table writes using SQL INSERT via Snowpark session. "
            "The native Classic Snowpipe Streaming SDK is Java-only; "
            "this Python implementation uses Snowpark SQL as the equivalent path. "
            "No PIPE object required - writes directly to tables."
        ),
        "latency": "< 1 second (SQL INSERT)",
        "throughput": "Limited by warehouse size and concurrency",
        "requires_pipe": False,
        "supports_transform": False,
        "auth": "Snowpark session (token-based or key pair)",
        "pricing": "Warehouse compute credits",
        "python_sdk_available": False,
        "key_methods": ["session.sql(INSERT INTO ...)", "Batch VALUES construction"],
        "documentation": "https://docs.snowflake.com/en/user-guide/snowpipe-streaming-overview",
        "best_for": "Simple setups, lower throughput, Snowpark-native apps",
    },
    "hp": {
        "name": "High-Performance Snowpipe Streaming",
        "sdk": "snowpipe-streaming (pip install snowpipe-streaming)",
        "native_sdk": "snowpipe-streaming (Python + Java, Rust core)",
        "description": (
            "High-performance streaming using the new SDK with PIPE objects. "
            "Server-side schema validation, in-flight transforms, and "
            "cluster-at-ingest-time support. Up to 10 GB/s per table."
        ),
        "latency": "< 10 seconds (ingest-to-query)",
        "throughput": "Up to 10 GB/s per table",
        "requires_pipe": True,
        "supports_transform": True,
        "auth": "Key pair (JWT/RSA) required",
        "pricing": "Throughput-based (credits per uncompressed GB)",
        "python_sdk_available": True,
        "key_classes": [
            "StreamingIngestClient",
            "StreamingIngestChannel",
            "ChannelStatus",
        ],
        "documentation": "https://docs.snowflake.com/en/user-guide/snowpipe-streaming-high-performance-overview",
        "best_for": "High throughput, production pipelines, real-time analytics",
    },
}

# Backward-compatible aliases
STREAMING_INFO = ARCHITECTURE_INFO["hp"]
ARCHITECTURE_INFO["classic"] = ARCHITECTURE_INFO["sql_insert"]


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class SnowpipeStreamingConfig:
    """Configuration for Snowpipe Streaming connections (both architectures)"""

    # Snowflake connection
    account: str = ""
    user: str = ""
    role: str = "SYSADMIN"
    private_key: str = ""
    private_key_path: str = "/usr/local/creds/secret_string"

    # Target location - defaults from config module
    database: str = ""
    schema: str = ""
    table: str = "AMI_STREAMING_READINGS"

    # PIPE object name (REQUIRED for HP architecture)
    pipe_name: str = "AMI_STREAMING_PIPE"

    # Client settings
    client_name: str = "flux_data_forge"
    channel_name: str = "flux_channel"

    # Architecture selection
    architecture: str = "hp"  # 'sql_insert' or 'hp'

    def __post_init__(self):
        """Load environment variables if not provided"""
        if not self.account:
            self.account = os.environ.get('SNOWFLAKE_ACCOUNT', '')
        if not self.user:
            self.user = os.environ.get('SNOWFLAKE_USER', '')
        if not self.role:
            self.role = os.environ.get('SNOWFLAKE_ROLE', 'SYSADMIN')
        if not self.database:
            self.database = os.environ.get('SNOWFLAKE_DATABASE', DB)
        if not self.schema:
            self.schema = os.environ.get('SNOWFLAKE_SCHEMA', SCHEMA_PRODUCTION)

    def get_account_url(self) -> str:
        """Get the Snowflake account URL"""
        return f"https://{self.account}.snowflakecomputing.com"

    def load_private_key(self) -> str:
        """Load private key from file or environment"""
        if self.private_key:
            return self.private_key

        key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', self.private_key_path)
        if os.path.exists(key_path):
            with open(key_path, 'r') as f:
                return f.read().strip()

        key_env = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
        if key_env:
            return key_env

        raise ValueError(
            f"Private key not found at {key_path} or in SNOWFLAKE_PRIVATE_KEY env var"
        )


# ============================================================================
# HP (HIGH-PERFORMANCE) STREAMING CLIENT
# ============================================================================

class HPStreamingClient:
    """
    High-Performance Snowpipe Streaming client.

    Uses the snowpipe-streaming SDK (snowflake.ingest.streaming module).
    Requires a PIPE object with DATA_SOURCE(TYPE => 'STREAMING').
    Python SDK: pip install snowpipe-streaming
    """

    def __init__(self, config: SnowpipeStreamingConfig):
        self.config = config
        self._client = None
        self._channel = None
        self._initialized = False
        self._lock = threading.Lock()
        self._rows_written = 0

    def initialize(self) -> bool:
        """Initialize the HP Streaming client and open channel"""
        try:
            from snowflake.ingest.streaming import StreamingIngestClient

            private_key = self.config.load_private_key()

            logger.info(f"Initializing HP Snowpipe Streaming as user {self.config.user}")
            logger.info(f"Account URL: {self.config.get_account_url()}")
            logger.info(
                f"Target PIPE: {self.config.database}.{self.config.schema}.{self.config.pipe_name}"
            )

            properties = {
                'url': self.config.get_account_url(),
                'account': self.config.account,
                'user': self.config.user,
                'private_key': private_key,
                'role': self.config.role,
                'authorization_type': 'JWT',
            }

            self._client = StreamingIngestClient(
                client_name=self.config.client_name,
                db_name=self.config.database,
                schema_name=self.config.schema,
                pipe_name=self.config.pipe_name,
                properties=properties,
            )

            self._channel, status = self._client.open_channel(
                channel_name=self.config.channel_name
            )

            logger.info(f"HP Channel '{self.config.channel_name}' opened")
            logger.info(f"Status: {status.status_code}")

            self._initialized = True
            return True

        except ImportError as e:
            logger.error(f"snowpipe-streaming package not installed: {e}")
            logger.error("Install with: pip install snowpipe-streaming")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize HP Streaming client: {e}")
            return False

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def write_rows(self, rows: List[Dict[str, Any]], offset_token: Optional[str] = None) -> int:
        """Write rows using HP SDK append_rows"""
        if not self._initialized:
            raise RuntimeError("HP Streaming client not initialized")

        if not rows:
            return 0

        with self._lock:
            try:
                if offset_token is None:
                    offset_token = (
                        f"{datetime.now(timezone.utc).isoformat()}_{len(rows)}"
                    )

                self._channel.append_rows(
                    rows=rows,
                    start_offset_token=offset_token,
                    end_offset_token=offset_token,
                )

                self._rows_written += len(rows)
                logger.debug(f"HP wrote {len(rows)} rows (total: {self._rows_written})")
                return len(rows)

            except Exception as e:
                logger.error(f"HP write_rows failed: {e}")
                raise

    def flush(self, timeout_seconds: int = 30) -> bool:
        """Flush buffered data and wait for completion"""
        if not self._channel:
            return False

        try:
            self._channel.initiate_flush()
            self._channel.wait_for_flush(timeout_seconds=timeout_seconds)
            logger.info("HP flush completed")
            return True
        except Exception as e:
            logger.error(f"HP flush failed: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get current channel status"""
        if not self._channel:
            return {'initialized': False, 'architecture': 'hp'}

        try:
            statuses = self._client.get_channel_statuses([self.config.channel_name])
            status = statuses.get(self.config.channel_name)
            if status:
                return {
                    'initialized': self._initialized,
                    'architecture': 'hp',
                    'channel_name': status.channel_name,
                    'status_code': status.status_code,
                    'rows_inserted': status.rows_inserted,
                    'rows_parsed': status.rows_parsed,
                    'rows_error': status.rows_error_count,
                    'last_error': status.last_error_message,
                    'local_rows_written': self._rows_written,
                }
            return {
                'initialized': self._initialized,
                'architecture': 'hp',
                'local_rows_written': self._rows_written,
            }
        except Exception as e:
            logger.error(f"Failed to get HP status: {e}")
            return {'initialized': self._initialized, 'architecture': 'hp', 'error': str(e)}

    def close(self, wait_for_flush: bool = True, timeout_seconds: int = 30):
        """Close channel and client gracefully"""
        if self._channel:
            try:
                if wait_for_flush:
                    self._channel.wait_for_flush(timeout_seconds=timeout_seconds)
                self._channel.close()
                logger.info("HP channel closed")
            except Exception as e:
                logger.error(f"Error closing HP channel: {e}")

        if self._client:
            try:
                self._client.close()
                logger.info("HP client closed")
            except Exception as e:
                logger.error(f"Error closing HP client: {e}")

        self._initialized = False


# ============================================================================
# SQL INSERT STREAMING CLIENT (SQL INSERT via Snowpark)
# ============================================================================

class SqlInsertClient:
    """
    SQL INSERT streaming client using Snowpark.

    The native Classic Snowpipe Streaming SDK is Java-only (snowflake-ingest-sdk).
    This Python implementation uses Snowpark SQL INSERT as the equivalent path,
    providing direct table writes without PIPE objects.
    """

    def __init__(self, config: SnowpipeStreamingConfig, session=None):
        self.config = config
        self._session = session
        self._initialized = False
        self._lock = threading.Lock()
        self._rows_written = 0
        self._batches_sent = 0

    def initialize(self, session=None) -> bool:
        """Initialize with a Snowpark session"""
        if session:
            self._session = session

        if not self._session:
            logger.error("SQL INSERT streaming requires a Snowpark session")
            return False

        target = f"{self.config.database}.{self.config.schema}.{self.config.table}"
        logger.info(f"Initializing SQL INSERT streaming to {target}")
        self._initialized = True
        return True

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def write_rows(self, rows: List[Dict[str, Any]], offset_token: Optional[str] = None) -> int:
        """Write rows using SQL INSERT via Snowpark session"""
        if not rows:
            return 0

        if not self._initialized or not self._session:
            raise RuntimeError("SQL INSERT streaming client not initialized")

        with self._lock:
            try:
                target = f"{self.config.database}.{self.config.schema}.{self.config.table}"
                columns = list(rows[0].keys())
                data = [[row.get(c) for c in columns] for row in rows]
                df = self._session.create_dataframe(data, schema=columns)
                df.write.mode("append").save_as_table(target)

                self._rows_written += len(rows)
                self._batches_sent += 1
                logger.debug(
                    f"SQL INSERT: {len(rows)} rows (total: {self._rows_written})"
                )
                return len(rows)

            except Exception as e:
                logger.error(f"SQL INSERT write_rows failed: {e}")
                raise

    def flush(self, timeout_seconds: int = 30) -> bool:
        """No-op for SQL INSERT — commits immediately"""
        return True

    def get_status(self) -> Dict[str, Any]:
        """Get current status"""
        return {
            'initialized': self._initialized,
            'architecture': 'sql_insert',
            'local_rows_written': self._rows_written,
            'batches_sent': self._batches_sent,
        }

    def close(self, wait_for_flush: bool = True, timeout_seconds: int = 30):
        """No-op for SQL INSERT — Snowpark session is managed externally"""
        self._initialized = False
        logger.info("SQL INSERT streaming client closed")


# Backward-compatible aliases
SnowpipeStreamingClient = HPStreamingClient
ClassicStreamingClient = SqlInsertClient


# ============================================================================
# SQL DDL GENERATORS
# ============================================================================

def _generate_base_table_ddl(
    database: str,
    schema: str,
    table_name: str,
    enable_change_tracking: bool = True,
) -> str:
    """Generate DDL for the streaming target table (shared by both architectures)"""
    change_tracking = "CHANGE_TRACKING = TRUE" if enable_change_tracking else ""
    return f"""
-- ============================================================================
-- SNOWPIPE STREAMING TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
    -- Core AMI fields
    METER_ID VARCHAR(50) NOT NULL COMMENT 'Unique meter identifier',
    TRANSFORMER_ID VARCHAR(50) COMMENT 'Associated transformer',
    CIRCUIT_ID VARCHAR(50) COMMENT 'Associated circuit',
    SUBSTATION_ID VARCHAR(50) COMMENT 'Associated substation',
    READING_TIMESTAMP TIMESTAMP_NTZ NOT NULL COMMENT 'Meter reading timestamp',

    -- Measurements
    USAGE_KWH FLOAT COMMENT '15-minute interval energy usage (kWh)',
    VOLTAGE FLOAT COMMENT 'Voltage reading (V)',
    POWER_FACTOR FLOAT COMMENT 'Power factor (0-1)',
    TEMPERATURE_C FLOAT COMMENT 'Ambient temperature (Celsius)',

    -- Context
    SERVICE_AREA VARCHAR(100) COMMENT 'Service territory/region',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer classification',
    LATITUDE FLOAT COMMENT 'Meter latitude',
    LONGITUDE FLOAT COMMENT 'Meter longitude',

    -- Status
    IS_OUTAGE BOOLEAN DEFAULT FALSE COMMENT 'Outage indicator',
    DATA_QUALITY VARCHAR(20) DEFAULT 'VALID' COMMENT 'Quality flag: VALID, ESTIMATED, OUTAGE',
    PRODUCTION_MATCHED BOOLEAN DEFAULT FALSE COMMENT 'Production matched indicator',
    EMISSION_PATTERN VARCHAR(20) COMMENT 'Emission pattern category',

    -- Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Row creation timestamp'
)
CLUSTER BY (DATE_TRUNC('DAY', READING_TIMESTAMP), METER_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
{change_tracking}
COMMENT = 'Snowpipe Streaming landing table for AMI data'
;
"""


def generate_sql_insert_streaming_ddl(
    database: str = None,
    schema: str = None,
    table_name: str = "AMI_STREAMING_READINGS",
    enable_change_tracking: bool = True,
) -> str:
    """
    Generate DDL for SQL INSERT architecture.

    SQL INSERT architecture writes directly to tables via Snowpark session.
    No PIPE object is needed. Only the target table is created.
    """
    database = database or DB
    schema = schema or SCHEMA_PRODUCTION
    return f"""
-- ============================================================================
-- SQL INSERT STREAMING SETUP
-- ============================================================================
-- The Classic Snowpipe Streaming SDK is Java-only.
-- For Python applications, SQL INSERT via Snowpark is the equivalent.
-- No PIPE object required - data is written directly to the table.
-- ============================================================================

{_generate_base_table_ddl(database, schema, table_name, enable_change_tracking)}

-- Grant permissions for streaming role
GRANT SELECT, INSERT ON TABLE {database}.{schema}.{table_name}
TO ROLE SYSADMIN;
"""


# Backward-compatible alias
generate_classic_streaming_ddl = generate_sql_insert_streaming_ddl


def _build_copy_into_from_columns(
    database: str,
    schema: str,
    table_name: str,
    table_columns: List[Dict[str, str]],
    cluster_clause: str = "",
) -> str:
    """
    Dynamically build a COPY INTO ... FROM (SELECT ...) clause from actual table columns.

    For each column, generates:
      - Column name in the target list
      - $1:<COL>::<TYPE> in the SELECT, with special handling for DEFAULT timestamp columns
    """
    # Columns whose default is CURRENT_TIMESTAMP — use CURRENT_TIMESTAMP() in SELECT
    _TIMESTAMP_DEFAULT_NAMES = {
        'INGESTION_TIMESTAMP', 'CREATED_AT', 'INSERTED_AT', 'LOADED_AT',
        'INSERT_TIMESTAMP', 'LOAD_TIMESTAMP',
    }

    col_names = []
    select_exprs = []

    for col in table_columns:
        name = col['name'].upper()
        sf_type = col['type'].upper()
        col_names.append(f"    {name}")

        # If this looks like an auto-timestamp column, use CURRENT_TIMESTAMP()
        if name in _TIMESTAMP_DEFAULT_NAMES and 'TIMESTAMP' in sf_type:
            select_exprs.append("        CURRENT_TIMESTAMP()")
        else:
            # Map Snowflake type string to a cast type
            cast_type = sf_type
            # Snowflake DESC TABLE returns types like "VARCHAR(50)", "NUMBER(38,0)", etc.
            # These are valid cast targets as-is.
            select_exprs.append(f"        $1:{name}::{cast_type}")

    col_list = ",\n".join(col_names)
    select_list = ",\n".join(select_exprs)

    return f"""
COPY INTO {database}.{schema}.{table_name} (
{col_list}
)
FROM (
    SELECT
{select_list}
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
)
{cluster_clause}"""


def generate_hp_streaming_ddl(
    database: str = None,
    schema: str = None,
    table_name: str = "AMI_STREAMING_READINGS",
    pipe_name: str = "AMI_STREAMING_PIPE",
    enable_transformation: bool = True,
    enable_clustering: bool = True,
    table_columns: List[Dict[str, str]] = None,
) -> str:
    """
    Generate DDL for HP architecture (SDK + PIPE object).

    HP architecture requires a PIPE object with DATA_SOURCE(TYPE => 'STREAMING').
    The PIPE defines server-side transforms applied during ingestion.

    Args:
        table_columns: Optional list of {"name": ..., "type": ...} dicts from
            DESC TABLE.  When provided the COPY INTO is built dynamically to
            match the real table schema instead of using hardcoded columns.
    """
    database = database or DB
    schema = schema or SCHEMA_PRODUCTION

    cluster_clause = ""
    if enable_clustering:
        cluster_clause = "CLUSTER_AT_INGEST_TIME = TRUE"

    if table_columns and enable_transformation:
        # ── Deterministic path: generate COPY INTO from actual table columns ──
        copy_body = _build_copy_into_from_columns(
            database, schema, table_name, table_columns, cluster_clause,
        )
    elif enable_transformation:
        # ── Fallback: 18-column schema matching generate_ami_reading() ──
        copy_body = f"""
COPY INTO {database}.{schema}.{table_name} (
    METER_ID,
    TRANSFORMER_ID,
    CIRCUIT_ID,
    SUBSTATION_ID,
    READING_TIMESTAMP,
    USAGE_KWH,
    VOLTAGE,
    POWER_FACTOR,
    TEMPERATURE_C,
    SERVICE_AREA,
    CUSTOMER_SEGMENT,
    LATITUDE,
    LONGITUDE,
    IS_OUTAGE,
    DATA_QUALITY,
    PRODUCTION_MATCHED,
    EMISSION_PATTERN,
    CREATED_AT
)
FROM (
    SELECT
        $1:METER_ID::VARCHAR(50),
        $1:TRANSFORMER_ID::VARCHAR(50),
        $1:CIRCUIT_ID::VARCHAR(50),
        $1:SUBSTATION_ID::VARCHAR(50),
        $1:READING_TIMESTAMP::TIMESTAMP_NTZ,
        $1:USAGE_KWH::FLOAT,
        $1:VOLTAGE::FLOAT,
        $1:POWER_FACTOR::FLOAT,
        $1:TEMPERATURE_C::FLOAT,
        $1:SERVICE_AREA::VARCHAR(100),
        $1:CUSTOMER_SEGMENT::VARCHAR(50),
        $1:LATITUDE::FLOAT,
        $1:LONGITUDE::FLOAT,
        $1:IS_OUTAGE::BOOLEAN,
        COALESCE($1:DATA_QUALITY::VARCHAR(20), 'VALID'),
        $1:PRODUCTION_MATCHED::BOOLEAN,
        $1:EMISSION_PATTERN::VARCHAR(20),
        CURRENT_TIMESTAMP()
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
)
{cluster_clause}"""
    else:
        copy_body = f"""
COPY INTO {database}.{schema}.{table_name}
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
{cluster_clause}"""

    return f"""
-- ============================================================================
-- HIGH-PERFORMANCE SNOWPIPE STREAMING SETUP (SDK + PIPE Object)
-- ============================================================================
-- Requires: pip install snowpipe-streaming
-- SDK import: from snowflake.ingest.streaming import StreamingIngestClient
-- Auth: Key pair (JWT/RSA) required
-- GA on AWS since Sep 23, 2025
-- ============================================================================

{_generate_base_table_ddl(database, schema, table_name)}

-- ============================================================================
-- STREAMING PIPE OBJECT
-- CRITICAL: Must use DATA_SOURCE(TYPE => 'STREAMING')
-- ============================================================================

CREATE OR REPLACE PIPE {database}.{schema}.{pipe_name}
AS
{copy_body};

-- Verify pipe creation (should show kind=STREAMING)
SHOW PIPES LIKE '{pipe_name}' IN SCHEMA {database}.{schema};

-- Grant permissions for streaming role
GRANT SELECT, INSERT ON TABLE {database}.{schema}.{table_name}
TO ROLE SYSADMIN;

GRANT EVOLVE SCHEMA ON TABLE {database}.{schema}.{table_name}
TO ROLE SYSADMIN;

GRANT OPERATE ON PIPE {database}.{schema}.{pipe_name}
TO ROLE SYSADMIN;

COMMENT ON PIPE {database}.{schema}.{pipe_name} IS
    'HP Snowpipe Streaming PIPE for AMI data ingestion';
"""


# Backward-compatible aliases
def generate_streaming_table_ddl(database: str, schema: str, table_name: str) -> str:
    """Generate table DDL (backward compatible)"""
    return _generate_base_table_ddl(database, schema, table_name)


def generate_streaming_pipe_ddl(
    database: str, schema: str, table_name: str, pipe_name: str
) -> str:
    """Generate pipe DDL (backward compatible, HP architecture)"""
    return generate_hp_streaming_ddl(
        database, schema, table_name, pipe_name,
        enable_transformation=True, enable_clustering=False
    )


def generate_full_ddl(
    database: str = None,
    schema: str = None,
    table_name: str = "AMI_STREAMING_READINGS",
    pipe_name: str = "AMI_STREAMING_PIPE",
) -> str:
    """Generate complete DDL for HP Streaming setup (backward compatible)"""
    return generate_hp_streaming_ddl(
        database, schema, table_name, pipe_name,
        enable_transformation=True, enable_clustering=True
    )


# ============================================================================
# PYTHON CLIENT CODE GENERATORS
# ============================================================================

def get_sql_insert_python_client_code(
    database: str = None,
    schema: str = None,
    table_name: str = "AMI_STREAMING_READINGS",
) -> str:
    """Generate Python client code for SQL INSERT architecture (via Snowpark)"""
    database = database or DB
    schema = schema or SCHEMA_PRODUCTION
    return f'''#!/usr/bin/env python3
"""
Classic Snowpipe Streaming - SQL INSERT via Snowpark

NOTE: The native Classic Snowpipe Streaming SDK is Java-only (snowflake-ingest-sdk).
For Python applications, SQL INSERT via Snowpark session is the equivalent approach.
No PIPE object required - data is written directly to the table.

Pros: Simple setup, no key-pair auth needed, uses existing Snowpark session
Cons: Lower throughput, warehouse compute costs, no server-side transforms
"""
from snowflake.snowpark import Session
from datetime import datetime

# Connect via Snowpark
connection_params = {{
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",  # Or use authenticator/private_key
    "role": "SYSADMIN",
    "warehouse": "FLUX_WH",
    "database": "{database}",
    "schema": "{schema}",
}}
session = Session.builder.configs(connection_params).create()

# Write data via SQL INSERT
rows = [
    {{"METER_ID": "MTR-001", "READING_TIMESTAMP": "2026-01-17 12:00:00", "USAGE_KWH": 1.5}},
    {{"METER_ID": "MTR-002", "READING_TIMESTAMP": "2026-01-17 12:00:00", "USAGE_KWH": 2.1}},
]

columns = list(rows[0].keys())
col_str = ", ".join(columns)
values_parts = []
for row in rows:
    vals = ", ".join(
        f"\\'{v}\\'" if isinstance(v, str) else str(v) for v in row.values()
    )
    values_parts.append(f"({{vals}})")

insert_sql = f"INSERT INTO {database}.{schema}.{table_name} ({{col_str}}) VALUES {{', '.join(values_parts)}}"
session.sql(insert_sql).collect()
print(f"Inserted {{len(rows)}} rows via SQL INSERT")

session.close()
'''


def get_hp_python_client_code(
    database: str = None,
    schema: str = None,
    pipe_name: str = "AMI_STREAMING_PIPE",
) -> str:
    """Generate Python client code for HP architecture"""
    database = database or DB
    schema = schema or SCHEMA_PRODUCTION
    return f'''#!/usr/bin/env python3
"""
High-Performance Snowpipe Streaming Client

Requires: pip install snowpipe-streaming
Auth: Key pair (JWT/RSA) required
PIPE object must exist before streaming (run DDL first)

Pros: 10 GB/s throughput, sub-10s latency, server-side transforms, offset tracking
Cons: Requires key-pair auth, PIPE object setup, throughput-based billing
"""
from snowflake.ingest.streaming import StreamingIngestClient

# Configuration
ACCOUNT = 'your_account_id'
USER = 'your_username'
ROLE = 'SYSADMIN'
DATABASE = '{database}'
SCHEMA = '{schema}'
PIPE_NAME = '{pipe_name}'

# Load private key (RSA key pair required)
with open('rsa_key.p8', 'r') as f:
    private_key = f.read()

# Connection properties
properties = {{
    'url': f'https://{{ACCOUNT}}.snowflakecomputing.com',
    'account': ACCOUNT,
    'user': USER,
    'private_key': private_key,
    'role': ROLE,
    'authorization_type': 'JWT',
}}

# Create client - PIPE object is required
client = StreamingIngestClient(
    client_name='my_streaming_client',
    db_name=DATABASE,
    schema_name=SCHEMA,
    pipe_name=PIPE_NAME,
    properties=properties,
)

# Open channel
channel, status = client.open_channel(channel_name='my_channel')
print(f"Channel opened: {{status.status_code}}")

# Write data - single row
channel.append_row(
    {{"METER_ID": "MTR-001", "READING_TIMESTAMP": "2026-01-17T12:00:00", "USAGE_KWH": 1.5}},
    "offset_1"
)

# Write data - batch
rows = [
    {{"METER_ID": "MTR-002", "READING_TIMESTAMP": "2026-01-17T12:00:00", "USAGE_KWH": 2.1}},
    {{"METER_ID": "MTR-003", "READING_TIMESTAMP": "2026-01-17T12:00:00", "USAGE_KWH": 1.8}},
]
channel.append_rows(rows=rows, start_offset_token="batch_1", end_offset_token="batch_1")

# Flush and wait for data to land
channel.initiate_flush()
channel.wait_for_flush(timeout_seconds=30)

# Check status
statuses = client.get_channel_statuses(["my_channel"])
ch_status = statuses["my_channel"]
print(f"Rows inserted: {{ch_status.rows_inserted}}")

# Clean up
channel.close()
client.close()
'''


# Backward-compatible alias
def get_python_client_code(
    database: str = None,
    schema: str = None,
    pipe_name: str = "AMI_STREAMING_PIPE",
) -> str:
    """Generate HP Python client code (backward compatible)"""
    return get_hp_python_client_code(database, schema, pipe_name)


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def create_streaming_client(
    config: Optional[SnowpipeStreamingConfig] = None,
    architecture: str = "hp",
    session=None,
):
    """
    Factory function to create the appropriate streaming client.

    Args:
        config: Streaming configuration (created with defaults if None)
        architecture: 'sql_insert' for SQL INSERT, 'hp' for HP SDK (overridden by config.architecture if set)
        session: Snowpark session (required for sql_insert, ignored for HP)

    Returns:
        SqlInsertClient or HPStreamingClient
    """
    if config is None:
        config = SnowpipeStreamingConfig(architecture=architecture)
    else:
        # Prefer config.architecture when provided
        if config.architecture:
            architecture = config.architecture

    if architecture == "sql_insert" or architecture == "classic":
        client = SqlInsertClient(config, session=session)
    else:
        client = HPStreamingClient(config)

    return client


# Backward-compatible alias
def create_snowpipe_client(
    config: Optional[SnowpipeStreamingConfig] = None,
) -> HPStreamingClient:
    """Factory function to create HP Snowpipe Streaming client (backward compatible)"""
    if config is None:
        config = SnowpipeStreamingConfig()
    return HPStreamingClient(config)
