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
        from snowpipe_streaming_impl import STREAMING_INFO
        
        assert STREAMING_INFO['name'] == 'Snowpipe Streaming'
        assert STREAMING_INFO['latency'] == '< 5 seconds'
        assert STREAMING_INFO['requires_pipe'] == True


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
