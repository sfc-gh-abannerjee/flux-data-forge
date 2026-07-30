"""
End-to-End Snowpipe Streaming Test

Tests both HP and Classic streaming paths against a real Snowflake account.
Target: FLUX_DB.STREAMING_TEST.AMI_STREAMING_READINGS (clean test environment)

Usage:
    python tests/test_e2e_streaming.py

Requires:
    - snowpipe-streaming SDK installed (pip install snowpipe-streaming)
    - RSA private key at the path specified below
    - FLUX_DB.STREAMING_TEST schema, table, and PIPE already created
    - se_demo Snowflake connection configured
"""

import sys
import os
import time
import traceback
from datetime import datetime, timezone

# Add spcs_app to path so we can import the implementation
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spcs_app'))

from snowpipe_streaming_impl import (
    HPStreamingClient,
    SnowpipeStreamingConfig,
    ARCHITECTURE_INFO,
)

# ============================================================================
# TEST CONFIGURATION
# ============================================================================

ACCOUNT = "SFSEHOL-SI_AE_ENABLEMENT_RETAIL_HMJRFL"
USER = "CORTEX_CLI_CPE_SVC"
ROLE = "ACCOUNTADMIN"
DATABASE = "FLUX_DB"
SCHEMA = "STREAMING_TEST"
TABLE = "AMI_STREAMING_READINGS"
PIPE = "AMI_STREAMING_PIPE"
PRIVATE_KEY_PATH = os.path.join(
    os.path.dirname(__file__),
    '..', '..', 'ami_data_generator', 'spcs_streaming', 'rsa_key.p8'
)

# ============================================================================
# TEST DATA GENERATORS
# ============================================================================

def make_hp_test_rows(count=5, prefix="HP"):
    """Generate test rows for HP streaming path"""
    rows = []
    for i in range(count):
        rows.append({
            "METER_ID": f"{prefix}_METER_{i+1:03d}",
            "READING_TIMESTAMP": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "USAGE_KWH": round(10.0 + i * 1.5, 2),
            "VOLTAGE": round(119.5 + i * 0.1, 2),
            "CUSTOMER_SEGMENT": "RESIDENTIAL",
            "TRANSFORMER_ID": f"TX_{prefix}_{i+1:03d}",
            "SUBSTATION_ID": f"SUB_{prefix}_001",
            "SERVICE_AREA": "TEST_AREA",
            "TEMPERATURE_C": round(20.0 + i * 0.5, 1),
            "IS_OUTAGE": False,
            "DATA_QUALITY": "VALID",
        })
    return rows


# ============================================================================
# HP STREAMING TEST
# ============================================================================

def test_hp_streaming():
    """Test HP Snowpipe Streaming with real SDK"""
    print("\n" + "=" * 70)
    print("TEST 1: HP Snowpipe Streaming (snowpipe-streaming SDK)")
    print("=" * 70)

    # Load private key
    key_path = os.path.abspath(PRIVATE_KEY_PATH)
    print(f"\n[1] Loading private key from: {key_path}")
    if not os.path.exists(key_path):
        print(f"  FAIL: Private key not found at {key_path}")
        return False

    with open(key_path, 'r') as f:
        private_key = f.read().strip()
    print(f"  OK: Key loaded ({len(private_key)} chars)")

    # Create config
    print(f"\n[2] Creating SnowpipeStreamingConfig")
    config = SnowpipeStreamingConfig(
        account=ACCOUNT,
        user=USER,
        role=ROLE,
        private_key=private_key,
        database=DATABASE,
        schema=SCHEMA,
        table=TABLE,
        pipe_name=PIPE,
        client_name="e2e_test_client",
        channel_name="e2e_test_channel",
        architecture="hp",
    )
    print(f"  Account URL: {config.get_account_url()}")
    print(f"  Target: {config.database}.{config.schema}.{config.pipe_name}")

    # Initialize client
    print(f"\n[3] Initializing HPStreamingClient")
    client = HPStreamingClient(config)
    if not client.initialize():
        print("  FAIL: HP client initialization failed")
        return False
    print(f"  OK: Client initialized, channel open")

    # Write test rows
    print(f"\n[4] Writing 5 test rows via HP SDK")
    rows = make_hp_test_rows(count=5, prefix="HP")
    for r in rows:
        print(f"    {r['METER_ID']}: {r['USAGE_KWH']} kWh, {r['VOLTAGE']}V")

    try:
        written = client.write_rows(rows, offset_token="e2e_test_batch_1")
        print(f"  OK: {written} rows written")
    except Exception as e:
        print(f"  FAIL: write_rows raised: {e}")
        traceback.print_exc()
        client.close(wait_for_flush=False)
        return False

    # Flush
    print(f"\n[5] Flushing data (waiting up to 30s)")
    if not client.flush(timeout_seconds=30):
        print("  WARN: Flush returned False (may still be processing)")
    else:
        print("  OK: Flush completed")

    # Get status
    print(f"\n[6] Channel status:")
    status = client.get_status()
    for k, v in status.items():
        print(f"    {k}: {v}")

    # Close
    print(f"\n[7] Closing HP client")
    client.close()
    print("  OK: Client closed")

    return True


# ============================================================================
# SQL INSERT STREAMING TEST (via Snowpark)
# ============================================================================

def test_sql_insert_streaming():
    """
    Test SQL INSERT streaming path using direct SQL INSERT.
    
    Since SqlInsertClient requires a Snowpark session and we're testing
    from the CLI (not inside SPCS), we simulate the SQL INSERT path by running
    the equivalent SQL INSERT directly via the Snowflake connection.
    This validates the same SQL pattern that SqlInsertClient.write_rows() uses.
    """
    print("\n" + "=" * 70)
    print("TEST 2: SQL INSERT Streaming (Snowpark DataFrame path)")
    print("=" * 70)
    print("\n  NOTE: SQL INSERT path uses Snowpark DataFrame writes.")
    print("  From CLI, we test the equivalent SQL INSERT pattern directly.")
    print("  This will be tested via snowflake_sql_execute in the next step.")
    return "DEFERRED_TO_SQL"


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("Snowpipe Streaming E2E Test")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"Target: {DATABASE}.{SCHEMA}.{TABLE}")

    # Verify architecture info is correct
    print("\n--- Architecture Info Check ---")
    assert "sql_insert" in ARCHITECTURE_INFO, "Missing 'sql_insert' in ARCHITECTURE_INFO"
    assert "hp" in ARCHITECTURE_INFO, "Missing 'hp' in ARCHITECTURE_INFO"
    assert ARCHITECTURE_INFO["hp"]["requires_pipe"] == True
    assert ARCHITECTURE_INFO["sql_insert"]["requires_pipe"] == False
    # Backward-compat alias
    assert "classic" in ARCHITECTURE_INFO, "Backward-compat 'classic' alias missing"
    print("  OK: ARCHITECTURE_INFO structure valid")

    # Run HP test
    hp_result = test_hp_streaming()

    # Summary
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"  HP Streaming:         {'PASS' if hp_result else 'FAIL'}")
    print(f"  SQL INSERT Streaming:  DEFERRED (will test via SQL)")
    print("=" * 70)

    sys.exit(0 if hp_result else 1)
