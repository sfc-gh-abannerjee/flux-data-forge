"""
Unit Tests for Flux Data Forge

Run with: pytest tests/ -v
"""

import pytest
import os
import sys

# Add spcs_app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spcs_app'))


class TestSnowpipeStreamingImpl:
    """Tests for snowpipe_streaming_impl.py"""
    
    def test_config_dataclass(self):
        """Test SnowpipeStreamingConfig dataclass"""
        from snowpipe_streaming_impl import SnowpipeStreamingConfig
        
        config = SnowpipeStreamingConfig(
            account="test_account",
            user="test_user",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )
        
        assert config.account == "test_account"
        assert config.user == "test_user"
        assert config.database == "TEST_DB"
        assert config.schema == "TEST_SCHEMA"
        assert config.role == "SYSADMIN"  # default
    
    def test_account_url_generation(self):
        """Test account URL generation"""
        from snowpipe_streaming_impl import SnowpipeStreamingConfig
        
        config = SnowpipeStreamingConfig(account="myaccount")
        assert config.get_account_url() == "https://myaccount.snowflakecomputing.com"
    
    def test_ddl_generation(self):
        """Test DDL generation functions"""
        from snowpipe_streaming_impl import (
            generate_streaming_table_ddl,
            generate_streaming_pipe_ddl,
            generate_full_ddl,
        )
        
        # Table DDL
        table_ddl = generate_streaming_table_ddl("TEST_DB", "TEST_SCHEMA", "TEST_TABLE")
        assert "CREATE TABLE IF NOT EXISTS TEST_DB.TEST_SCHEMA.TEST_TABLE" in table_ddl
        assert "METER_ID" in table_ddl
        assert "READING_TIMESTAMP" in table_ddl
        
        # Pipe DDL
        pipe_ddl = generate_streaming_pipe_ddl("TEST_DB", "TEST_SCHEMA", "TEST_TABLE", "TEST_PIPE")
        assert "CREATE OR REPLACE PIPE TEST_DB.TEST_SCHEMA.TEST_PIPE" in pipe_ddl
        assert "DATA_SOURCE(TYPE => 'STREAMING')" in pipe_ddl
        
        # Full DDL
        full_ddl = generate_full_ddl()
        assert "CREATE TABLE" in full_ddl
        assert "CREATE OR REPLACE PIPE" in full_ddl
    
    def test_streaming_info_constants(self):
        """Test streaming info constants"""
        from snowpipe_streaming_impl import STREAMING_INFO, ARCHITECTURE_INFO
        
        # STREAMING_INFO is backward-compatible alias for ARCHITECTURE_INFO['hp']
        assert STREAMING_INFO['name'] == 'High-Performance Snowpipe Streaming'
        assert STREAMING_INFO['requires_pipe'] == True
        assert STREAMING_INFO['python_sdk_available'] == True
        
        # Verify both architectures exist
        assert 'sql_insert' in ARCHITECTURE_INFO
        assert 'hp' in ARCHITECTURE_INFO
        assert ARCHITECTURE_INFO['sql_insert']['requires_pipe'] == False
        assert ARCHITECTURE_INFO['hp']['requires_pipe'] == True
        
        # Backward-compat: 'classic' alias should still work
        assert 'classic' in ARCHITECTURE_INFO


class TestConfigurationFiles:
    """Tests for configuration file integrity"""
    
    @pytest.fixture
    def base_path(self):
        return os.path.join(os.path.dirname(__file__), '..')
    
    def test_requirements_has_dependencies(self, base_path):
        """Test requirements.txt has all needed dependencies"""
        req_path = os.path.join(base_path, 'spcs_app', 'requirements.txt')
        
        with open(req_path, 'r') as f:
            content = f.read().lower()
        
        assert 'fastapi' in content
        assert 'uvicorn' in content
        assert 'snowflake' in content
    
    def test_dockerfile_valid(self, base_path):
        """Test Dockerfile has required directives"""
        dockerfile_path = os.path.join(base_path, 'spcs_app', 'Dockerfile')
        
        with open(dockerfile_path, 'r') as f:
            content = f.read()
        
        assert 'FROM python:' in content
        assert 'COPY requirements.txt' in content
        assert 'EXPOSE' in content
        assert 'CMD' in content
    
    def test_service_spec_valid_yaml(self, base_path):
        """Test service_spec.yaml is valid"""
        import yaml
        
        spec_path = os.path.join(base_path, 'spcs_app', 'service_spec.yaml')
        
        with open(spec_path, 'r') as f:
            spec = yaml.safe_load(f)
        
        assert 'spec' in spec
        assert 'containers' in spec['spec']
        assert 'endpoints' in spec['spec']
    
    def test_env_example_exists(self, base_path):
        """Test .env.example exists and has required vars"""
        env_path = os.path.join(base_path, '.env.example')
        
        assert os.path.exists(env_path), ".env.example should exist"
        
        with open(env_path, 'r') as f:
            content = f.read()
        
        assert 'SNOWFLAKE_DATABASE' in content
        assert 'SNOWFLAKE_SCHEMA' in content
        assert 'SNOWFLAKE_WAREHOUSE' in content


class TestSecurityChecks:
    """Security-related tests"""
    
    @pytest.fixture
    def base_path(self):
        return os.path.join(os.path.dirname(__file__), '..')
    
    def test_no_hardcoded_accounts(self, base_path):
        """Ensure no hardcoded Snowflake accounts"""
        forbidden = ['GZB42423', 'SFSEHOL', '484577546576', 'abannerjee']
        
        files_to_check = [
            'spcs_app/fastapi_app.py',
            'spcs_app/service_spec.yaml',
            'spcs_app/deploy_spcs.sql',
        ]
        
        for filepath in files_to_check:
            full_path = os.path.join(base_path, filepath)
            if os.path.exists(full_path):
                with open(full_path, 'r') as f:
                    content = f.read()
                
                for pattern in forbidden:
                    assert pattern not in content, f"Found '{pattern}' in {filepath}"
    
    def test_gitignore_covers_secrets(self, base_path):
        """Test .gitignore covers common secret files"""
        gitignore_path = os.path.join(base_path, '.gitignore')
        
        with open(gitignore_path, 'r') as f:
            content = f.read()
        
        assert '.env' in content
        assert '*.pem' in content or '*.key' in content
        assert '__pycache__' in content


class TestDataGeneration:
    """Tests for data generation logic"""
    
    def test_meter_id_format(self):
        """Test meter ID generation format"""
        import uuid
        
        meter_id = f'MTR-{uuid.uuid4().hex[:8].upper()}'
        
        assert meter_id.startswith('MTR-')
        assert len(meter_id) == 12  # MTR- + 8 chars
    
    def test_usage_calculation(self):
        """Test usage calculation by segment"""
        import random
        
        segments = {
            'RESIDENTIAL': 1,
            'COMMERCIAL': 5,
            'INDUSTRIAL': 15,
        }
        
        base_usage = 2.0  # kWh
        
        for segment, multiplier in segments.items():
            usage = base_usage * multiplier
            assert usage == base_usage * segments[segment]
    
    def test_voltage_range(self):
        """Test voltage stays in realistic range"""
        import random
        
        for _ in range(100):
            voltage = round(random.uniform(118, 122), 2)
            assert 118 <= voltage <= 122
    
    def test_data_quality_distribution(self):
        """Test data quality flags distribution"""
        import random
        
        qualities = []
        for _ in range(1000):
            roll = random.randint(1, 100)
            if roll <= 1:
                qualities.append('OUTAGE')
            elif roll >= 98:
                qualities.append('ANOMALY')
            else:
                qualities.append('VALID')
        
        # Most should be VALID
        valid_count = qualities.count('VALID')
        assert valid_count > 900, f"Expected >90% VALID, got {valid_count/10}%"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# ============================================================================
# MOCK TESTS FOR STREAMING CLIENTS AND WORKERS
# ============================================================================

class TestSqlInsertClient:
    """Mock tests for SqlInsertClient (no Snowflake connection needed)"""

    def test_sql_insert_client_import(self):
        """SqlInsertClient should be importable"""
        from snowpipe_streaming_impl import SqlInsertClient
        assert SqlInsertClient is not None

    def test_backward_compat_alias(self):
        """ClassicStreamingClient should alias to SqlInsertClient"""
        from snowpipe_streaming_impl import SqlInsertClient, ClassicStreamingClient
        assert ClassicStreamingClient is SqlInsertClient

    def test_sql_insert_client_init_without_session(self):
        """SqlInsertClient.initialize() should fail without a Snowpark session"""
        from snowpipe_streaming_impl import SqlInsertClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", architecture="sql_insert",
        )
        client = SqlInsertClient(config, session=None)
        result = client.initialize()
        assert result is False
        assert client.is_initialized is False

    def test_sql_insert_client_write_rows_without_init(self):
        """write_rows should raise RuntimeError when not initialized"""
        from snowpipe_streaming_impl import SqlInsertClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", architecture="sql_insert",
        )
        client = SqlInsertClient(config, session=None)

        with pytest.raises(RuntimeError, match="SQL INSERT streaming client not initialized"):
            client.write_rows([{"METER_ID": "M1"}])

    def test_sql_insert_client_write_empty_rows(self):
        """write_rows with empty list should return 0"""
        from snowpipe_streaming_impl import SqlInsertClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", architecture="sql_insert",
        )
        client = SqlInsertClient(config, session=None)
        # Bypass init check for empty rows
        result = client.write_rows([])
        assert result == 0

    def test_sql_insert_flush_is_noop(self):
        """flush() should return True immediately for SQL INSERT"""
        from snowpipe_streaming_impl import SqlInsertClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", architecture="sql_insert",
        )
        client = SqlInsertClient(config, session=None)
        assert client.flush() is True


class TestStreamingFactory:
    """Tests for the create_streaming_client factory function"""

    def test_factory_accepts_sql_insert(self):
        """Factory should accept 'sql_insert' architecture"""
        from snowpipe_streaming_impl import create_streaming_client, SqlInsertClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", architecture="sql_insert",
        )
        client = create_streaming_client(config)
        assert isinstance(client, SqlInsertClient)

    def test_factory_accepts_classic_alias(self):
        """Factory should accept 'classic' as backward-compat alias"""
        from snowpipe_streaming_impl import create_streaming_client, SqlInsertClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", architecture="classic",
        )
        client = create_streaming_client(config)
        assert isinstance(client, SqlInsertClient)

    def test_factory_hp_architecture(self):
        """Factory should return HPStreamingClient for 'hp' architecture"""
        from snowpipe_streaming_impl import create_streaming_client, HPStreamingClient, SnowpipeStreamingConfig

        config = SnowpipeStreamingConfig(
            account="test", user="test", database="DB", schema="SCH",
            table="TBL", pipe_name="PIPE", architecture="hp",
        )
        client = create_streaming_client(config)
        assert isinstance(client, HPStreamingClient)


class TestWorkerRowMapping:
    """Tests for the 12-column row mapping used in EventHub/PubSub workers"""

    REQUIRED_COLUMNS = [
        "METER_ID", "READING_TIMESTAMP", "USAGE_KWH", "VOLTAGE",
        "TEMPERATURE_C", "SERVICE_AREA", "CUSTOMER_SEGMENT",
        "TRANSFORMER_ID", "SUBSTATION_ID", "IS_OUTAGE",
        "DATA_QUALITY", "INGESTION_TIMESTAMP",
    ]

    def _make_row(self, body, ts, prefix="TEST"):
        """Replicate the worker row mapping logic"""
        return {
            "METER_ID": str(body.get("meter_id", body.get("device_id", body.get("id", f"{prefix}-UNKNOWN")))),
            "READING_TIMESTAMP": str(body.get("reading_ts", body.get("timestamp", ts))),
            "USAGE_KWH": float(body.get("reading_value", body.get("value", body.get("usage_kwh", 0)))),
            "VOLTAGE": float(body.get("voltage", 120.0)),
            "TEMPERATURE_C": float(body.get("temperature_c", body.get("temperature", 25.0))),
            "SERVICE_AREA": str(body.get("service_area", "UNKNOWN")),
            "CUSTOMER_SEGMENT": str(body.get("customer_segment", "RESIDENTIAL")),
            "TRANSFORMER_ID": str(body.get("transformer_id", "")),
            "SUBSTATION_ID": str(body.get("substation_id", "")),
            "IS_OUTAGE": bool(body.get("is_outage", False)),
            "DATA_QUALITY": str(body.get("quality", body.get("data_quality", "VALID"))),
            "INGESTION_TIMESTAMP": ts,
        }

    def test_row_has_all_12_columns(self):
        """Row mapping should produce exactly 12 columns"""
        row = self._make_row({"meter_id": "M1", "usage_kwh": 10.5}, "2026-01-01T00:00:00Z")
        assert len(row) == 12
        for col in self.REQUIRED_COLUMNS:
            assert col in row, f"Missing column: {col}"

    def test_row_defaults(self):
        """Empty body should produce sensible defaults"""
        row = self._make_row({}, "2026-01-01T00:00:00Z", prefix="EH")
        assert row["METER_ID"] == "EH-UNKNOWN"
        assert row["USAGE_KWH"] == 0.0
        assert row["VOLTAGE"] == 120.0
        assert row["TEMPERATURE_C"] == 25.0
        assert row["SERVICE_AREA"] == "UNKNOWN"
        assert row["CUSTOMER_SEGMENT"] == "RESIDENTIAL"
        assert row["IS_OUTAGE"] is False
        assert row["DATA_QUALITY"] == "VALID"

    def test_row_field_aliases(self):
        """Row mapping should handle field name aliases"""
        body = {
            "device_id": "DEV-001",
            "timestamp": "2026-06-15T12:00:00Z",
            "value": 42.5,
            "temperature": 30.0,
            "quality": "ANOMALY",
        }
        row = self._make_row(body, "2026-06-15T12:00:00Z")
        assert row["METER_ID"] == "DEV-001"
        assert row["USAGE_KWH"] == 42.5
        assert row["TEMPERATURE_C"] == 30.0
        assert row["DATA_QUALITY"] == "ANOMALY"

    def test_row_explicit_fields_override_defaults(self):
        """Explicit fields should override defaults"""
        body = {
            "meter_id": "MTR-999",
            "voltage": 121.5,
            "service_area": "HOUSTON_SOUTH",
            "customer_segment": "COMMERCIAL",
            "transformer_id": "TX-100",
            "substation_id": "SUB-50",
            "is_outage": True,
        }
        row = self._make_row(body, "2026-01-01T00:00:00Z")
        assert row["METER_ID"] == "MTR-999"
        assert row["VOLTAGE"] == 121.5
        assert row["SERVICE_AREA"] == "HOUSTON_SOUTH"
        assert row["CUSTOMER_SEGMENT"] == "COMMERCIAL"
        assert row["TRANSFORMER_ID"] == "TX-100"
        assert row["SUBSTATION_ID"] == "SUB-50"
        assert row["IS_OUTAGE"] is True


class TestDDLGeneration:
    """Tests for DDL generation after terminology rename"""

    def test_sql_insert_ddl_function_exists(self):
        """generate_sql_insert_streaming_ddl should be importable"""
        from snowpipe_streaming_impl import generate_sql_insert_streaming_ddl
        ddl = generate_sql_insert_streaming_ddl("DB", "SCH", "TBL")
        assert "CREATE TABLE" in ddl
        assert "METER_ID" in ddl

    def test_classic_ddl_alias(self):
        """generate_classic_streaming_ddl should alias to generate_sql_insert_streaming_ddl"""
        from snowpipe_streaming_impl import (
            generate_classic_streaming_ddl,
            generate_sql_insert_streaming_ddl,
        )
        assert generate_classic_streaming_ddl is generate_sql_insert_streaming_ddl
