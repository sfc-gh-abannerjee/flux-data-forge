"""
Snowpipe Streaming Implementation

This module provides the implementation for Snowpipe Streaming using the
snowpipe-streaming SDK (snowflake.ingest.streaming module).

Key Points:
1. Snowpipe Streaming ALWAYS requires a PIPE object with DATA_SOURCE(TYPE => 'STREAMING')
2. The SDK is installed via: pip install snowpipe-streaming
3. Key pair authentication is required (JWT/RSA)
4. The StreamingIngestClient requires: url, account, user, private_key

SDK API Reference:
- StreamingIngestClient(client_name, db_name, schema_name, pipe_name, properties)
- client.open_channel(channel_name) -> (channel, status)
- channel.append_rows(rows, start_offset_token, end_offset_token)
- channel.initiate_flush() / wait_for_flush(timeout_seconds)
- channel.close(drop, wait_for_flush, timeout_seconds)
- client.close(wait_for_flush, timeout_seconds)

Author: Flux Data Forge Team
Date: January 2026
"""

import os
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import threading

logger = logging.getLogger(__name__)


@dataclass
class SnowpipeStreamingConfig:
    """Configuration for Snowpipe Streaming connections"""
    
    # Snowflake connection
    account: str = ""
    user: str = ""
    role: str = "SYSADMIN"
    private_key: str = ""
    private_key_path: str = "/usr/local/creds/secret_string"
    
    # Target location
    database: str = "SI_DEMOS"
    schema: str = "PRODUCTION"
    table: str = "AMI_STREAMING_READINGS"
    
    # PIPE object name (REQUIRED for Snowpipe Streaming)
    pipe_name: str = "AMI_STREAMING_PIPE"
    
    # Client settings
    client_name: str = "flux_data_forge"
    channel_name: str = "flux_channel"
    
    def __post_init__(self):
        """Load environment variables if not provided"""
        if not self.account:
            self.account = os.environ.get('SNOWFLAKE_ACCOUNT', '')
        if not self.user:
            self.user = os.environ.get('SNOWFLAKE_USER', '')
        if not self.role:
            self.role = os.environ.get('SNOWFLAKE_ROLE', 'SYSADMIN')
        if not self.database:
            self.database = os.environ.get('SNOWFLAKE_DATABASE', 'SI_DEMOS')
        if not self.schema:
            self.schema = os.environ.get('SNOWFLAKE_SCHEMA', 'PRODUCTION')
    
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
            
        raise ValueError(f"Private key not found at {key_path} or in SNOWFLAKE_PRIVATE_KEY env var")


class SnowpipeStreamingClient:
    """
    Snowpipe Streaming client wrapper.
    
    Uses the snowpipe-streaming SDK (snowflake.ingest.streaming module).
    ALWAYS requires a PIPE object with DATA_SOURCE(TYPE => 'STREAMING').
    """
    
    def __init__(self, config: SnowpipeStreamingConfig):
        self.config = config
        self._client = None
        self._channel = None
        self._initialized = False
        self._lock = threading.Lock()
        self._rows_written = 0
        
    def initialize(self) -> bool:
        """Initialize the Snowpipe Streaming client and open channel"""
        try:
            from snowflake.ingest.streaming import StreamingIngestClient
            
            private_key = self.config.load_private_key()
            
            logger.info(f"Initializing Snowpipe Streaming as user {self.config.user}")
            logger.info(f"Account URL: {self.config.get_account_url()}")
            logger.info(f"Target PIPE: {self.config.database}.{self.config.schema}.{self.config.pipe_name}")
            
            # Connection properties per SDK documentation
            properties = {
                'url': self.config.get_account_url(),
                'account': self.config.account,
                'user': self.config.user,
                'private_key': private_key,
                'role': self.config.role,
                'authorization_type': 'JWT',
            }
            
            # Create client - PIPE object is required
            self._client = StreamingIngestClient(
                client_name=self.config.client_name,
                db_name=self.config.database,
                schema_name=self.config.schema,
                pipe_name=self.config.pipe_name,
                properties=properties,
            )
            
            # Open channel
            self._channel, status = self._client.open_channel(
                channel_name=self.config.channel_name
            )
            
            logger.info(f"Channel '{self.config.channel_name}' opened")
            logger.info(f"Status: {status.status_code}")
            
            self._initialized = True
            return True
            
        except ImportError as e:
            logger.error(f"snowpipe-streaming package not installed: {e}")
            logger.error("Install with: pip install snowpipe-streaming")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize Snowpipe Streaming client: {e}")
            return False
    
    @property
    def is_initialized(self) -> bool:
        return self._initialized
    
    def write_rows(self, rows: List[Dict[str, Any]], offset_token: Optional[str] = None) -> int:
        """
        Write rows to the streaming channel.
        
        Args:
            rows: List of dictionaries representing rows to write
            offset_token: Optional offset token for exactly-once semantics
            
        Returns:
            Number of rows written
        """
        if not self._initialized:
            raise RuntimeError("Snowpipe Streaming client not initialized")
        
        if not rows:
            return 0
        
        with self._lock:
            try:
                if offset_token is None:
                    offset_token = f"{datetime.utcnow().isoformat()}_{len(rows)}"
                
                self._channel.append_rows(
                    rows=rows,
                    start_offset_token=offset_token,
                    end_offset_token=offset_token,
                )
                
                self._rows_written += len(rows)
                logger.debug(f"Wrote {len(rows)} rows (total: {self._rows_written})")
                return len(rows)
                
            except Exception as e:
                logger.error(f"Failed to write rows: {e}")
                raise
    
    def flush(self, timeout_seconds: int = 30) -> bool:
        """
        Flush any buffered data and wait for completion.
        
        Args:
            timeout_seconds: Maximum time to wait for flush
            
        Returns:
            True if flush succeeded
        """
        if not self._channel:
            return False
            
        try:
            self._channel.initiate_flush()
            self._channel.wait_for_flush(timeout_seconds=timeout_seconds)
            logger.info("Flush completed")
            return True
        except Exception as e:
            logger.error(f"Flush failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current channel status"""
        if not self._channel:
            return {'initialized': False}
        
        try:
            status = self._channel.get_channel_status()
            return {
                'initialized': self._initialized,
                'channel_name': status.channel_name,
                'status_code': status.status_code,
                'rows_inserted': status.rows_inserted_count,
                'rows_parsed': status.rows_parsed_count,
                'rows_error': status.rows_error_count,
                'last_error': status.last_error_message,
                'local_rows_written': self._rows_written,
            }
        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            return {'initialized': self._initialized, 'error': str(e)}
    
    def close(self, wait_for_flush: bool = True, timeout_seconds: int = 30):
        """Close the channel and client gracefully"""
        if self._channel:
            try:
                self._channel.close(
                    drop=False,
                    wait_for_flush=wait_for_flush,
                    timeout_seconds=timeout_seconds
                )
                logger.info("Channel closed")
            except Exception as e:
                logger.error(f"Error closing channel: {e}")
        
        if self._client:
            try:
                self._client.close(
                    wait_for_flush=wait_for_flush,
                    timeout_seconds=timeout_seconds
                )
                logger.info("Client closed")
            except Exception as e:
                logger.error(f"Error closing client: {e}")
        
        self._initialized = False


# ============================================================================
# SQL DDL GENERATORS
# ============================================================================

def generate_streaming_table_ddl(
    database: str,
    schema: str,
    table_name: str,
) -> str:
    """
    Generate DDL for Snowpipe Streaming target table.
    """
    return f"""
-- ============================================================================
-- SNOWPIPE STREAMING TABLE SETUP
-- ============================================================================

CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
    -- Core AMI fields
    METER_ID VARCHAR(50) NOT NULL COMMENT 'Unique meter identifier',
    READING_TIMESTAMP TIMESTAMP_NTZ NOT NULL COMMENT 'Meter reading timestamp',
    
    -- Measurements
    USAGE_KWH FLOAT COMMENT '15-minute interval energy usage (kWh)',
    VOLTAGE FLOAT COMMENT 'Voltage reading (V)',
    TEMPERATURE_C FLOAT COMMENT 'Ambient temperature (Celsius)',
    
    -- Context
    SERVICE_AREA VARCHAR(100) COMMENT 'Service territory/region',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer classification',
    TRANSFORMER_ID VARCHAR(50) COMMENT 'Associated transformer',
    SUBSTATION_ID VARCHAR(50) COMMENT 'Associated substation',
    
    -- Status
    IS_OUTAGE BOOLEAN DEFAULT FALSE COMMENT 'Outage indicator',
    DATA_QUALITY VARCHAR(20) DEFAULT 'VALID' COMMENT 'Quality flag: VALID, ESTIMATED, OUTAGE',
    
    -- Metadata
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Snowflake ingestion timestamp'
)
CLUSTER BY (DATE_TRUNC('DAY', READING_TIMESTAMP), METER_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = ON
COMMENT = 'Snowpipe Streaming landing table for AMI data'
;
"""


def generate_streaming_pipe_ddl(
    database: str,
    schema: str,
    table_name: str,
    pipe_name: str,
) -> str:
    """
    Generate DDL for Snowpipe Streaming PIPE object.
    
    CRITICAL: The PIPE must use DATA_SOURCE(TYPE => 'STREAMING') for streaming ingestion.
    """
    return f"""
-- ============================================================================
-- SNOWPIPE STREAMING PIPE SETUP
-- CRITICAL: Must use DATA_SOURCE(TYPE => 'STREAMING')
-- ============================================================================

CREATE OR REPLACE PIPE {database}.{schema}.{pipe_name}
AS
COPY INTO {database}.{schema}.{table_name} (
    METER_ID,
    READING_TIMESTAMP,
    USAGE_KWH,
    VOLTAGE,
    TEMPERATURE_C,
    SERVICE_AREA,
    CUSTOMER_SEGMENT,
    TRANSFORMER_ID,
    SUBSTATION_ID,
    IS_OUTAGE,
    DATA_QUALITY,
    INGESTION_TIMESTAMP
)
FROM (
    SELECT 
        $1:METER_ID::VARCHAR(50),
        $1:READING_TIMESTAMP::TIMESTAMP_NTZ,
        $1:USAGE_KWH::FLOAT,
        $1:VOLTAGE::FLOAT,
        $1:TEMPERATURE_C::FLOAT,
        $1:SERVICE_AREA::VARCHAR(100),
        $1:CUSTOMER_SEGMENT::VARCHAR(50),
        $1:TRANSFORMER_ID::VARCHAR(50),
        $1:SUBSTATION_ID::VARCHAR(50),
        $1:IS_OUTAGE::BOOLEAN,
        COALESCE($1:DATA_QUALITY::VARCHAR(20), 'VALID'),
        CURRENT_TIMESTAMP()
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

-- Verify pipe creation (should show kind=STREAMING)
SHOW PIPES LIKE '{pipe_name}' IN SCHEMA {database}.{schema};

COMMENT ON PIPE {database}.{schema}.{pipe_name} IS 
    'Snowpipe Streaming PIPE for AMI data ingestion';
"""


def generate_full_ddl(
    database: str = "SI_DEMOS",
    schema: str = "PRODUCTION",
    table_name: str = "AMI_STREAMING_READINGS",
    pipe_name: str = "AMI_STREAMING_PIPE",
) -> str:
    """Generate complete DDL for Snowpipe Streaming setup"""
    return (
        generate_streaming_table_ddl(database, schema, table_name) +
        "\n" +
        generate_streaming_pipe_ddl(database, schema, table_name, pipe_name)
    )


# ============================================================================
# ARCHITECTURE INFO
# ============================================================================

STREAMING_INFO = {
    'name': 'Snowpipe Streaming',
    'package': 'snowpipe-streaming',
    'module': 'snowflake.ingest.streaming',
    'description': 'Low-latency streaming ingestion using PIPE objects',
    'latency': '< 5 seconds',
    'throughput': 'Up to 10 GB/s per table',
    'requires_pipe': True,
    'supports_transform': True,
    'auth': 'Key pair (JWT/RSA) required',
    'pricing': 'Volume-based (credits per uncompressed GB)',
    'key_classes': [
        'StreamingIngestClient',
        'StreamingIngestChannel',
        'ChannelStatus',
    ],
    'documentation': 'https://docs.snowflake.com/en/user-guide/snowpipe-streaming',
}


def get_python_client_code(
    database: str = "SI_DEMOS",
    schema: str = "PRODUCTION",
    pipe_name: str = "AMI_STREAMING_PIPE",
) -> str:
    """Generate Python client code example"""
    return f'''#!/usr/bin/env python3
"""
Snowpipe Streaming Client Example
"""
from snowflake.ingest.streaming import StreamingIngestClient

# Configuration
ACCOUNT = 'your_account_id'
USER = 'your_username'
ROLE = 'ACCOUNTADMIN'
DATABASE = '{database}'
SCHEMA = '{schema}'
PIPE_NAME = '{pipe_name}'

# Load private key
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

# Create client
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

# Write data
rows = [
    {{'METER_ID': 'MTR-001', 'READING_TIMESTAMP': '2026-01-17T12:00:00', 'USAGE_KWH': 1.5}},
    {{'METER_ID': 'MTR-002', 'READING_TIMESTAMP': '2026-01-17T12:00:00', 'USAGE_KWH': 2.1}},
]
channel.append_rows(rows=rows, start_offset_token='batch_1', end_offset_token='batch_1')

# Flush and wait
channel.initiate_flush()
channel.wait_for_flush(timeout_seconds=30)

# Check status
status = channel.get_channel_status()
print(f"Rows inserted: {{status.rows_inserted_count}}")

# Clean up
channel.close(wait_for_flush=True, timeout_seconds=10)
client.close(wait_for_flush=True, timeout_seconds=10)
'''


# Factory function for backwards compatibility
def create_snowpipe_client(config: Optional[SnowpipeStreamingConfig] = None) -> SnowpipeStreamingClient:
    """Factory function to create Snowpipe Streaming client"""
    if config is None:
        config = SnowpipeStreamingConfig()
    return SnowpipeStreamingClient(config)
