#!/usr/bin/env python3
"""
Smoke Tests for Flux Data Forge

These tests verify basic functionality without requiring a Snowflake connection.
Run with: python tests/smoke_test.py
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spcs_app'))


def test_imports():
    """Test that all required modules can be imported"""
    print("Testing imports...")
    
    errors = []
    
    # Core Python modules
    try:
        import json
        import uuid
        import random
        import threading
        from datetime import datetime, timedelta
        print("  ✓ Standard library imports")
    except ImportError as e:
        errors.append(f"Standard library: {e}")
    
    # FastAPI
    try:
        from fastapi import FastAPI
        from fastapi.responses import HTMLResponse, JSONResponse
        print("  ✓ FastAPI imports")
    except ImportError as e:
        errors.append(f"FastAPI: {e}")
    
    # Snowflake (may not be available in CI without credentials)
    try:
        import snowflake.connector
        print("  ✓ Snowflake connector imports")
    except ImportError as e:
        print(f"  ⚠ Snowflake connector not available (OK for CI): {e}")
    
    # Optional: boto3 for S3
    try:
        import boto3
        print("  ✓ boto3 imports")
    except ImportError as e:
        print(f"  ⚠ boto3 not available (optional): {e}")
    
    if errors:
        for err in errors:
            print(f"  ✗ {err}")
        return False
    
    return True


def test_constants():
    """Test that configuration constants are properly defined"""
    print("\nTesting constants...")
    
    try:
        # Import the main app module to check constants
        # We can't fully import fastapi_app without Snowflake, so we'll parse it
        app_path = os.path.join(os.path.dirname(__file__), '..', 'spcs_app', 'fastapi_app.py')
        
        with open(app_path, 'r') as f:
            content = f.read()
        
        # Check for required constants
        required_constants = [
            'USE_CASE_TEMPLATES',
            'FLEET_PRESETS',
            'DATA_FLOWS',
            'UTILITY_PROFILES',
            'EMISSION_PATTERNS',
        ]
        
        for const in required_constants:
            if const in content:
                print(f"  ✓ {const} defined")
            else:
                print(f"  ✗ {const} missing")
                return False
        
        return True
    
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def test_data_generation_logic():
    """Test the data generation logic in isolation"""
    print("\nTesting data generation logic...")
    
    import random
    import uuid
    from datetime import datetime
    
    # Simulate generate_ami_reading logic
    def generate_test_reading():
        hour = datetime.now().hour
        
        # Time-of-day usage multiplier
        if 14 <= hour <= 19:
            base_usage = random.uniform(1.5, 3.5)
        elif 6 <= hour <= 9:
            base_usage = random.uniform(1.0, 2.5)
        else:
            base_usage = random.uniform(0.3, 1.5)
        
        segment = random.choice(['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL'])
        if segment == 'INDUSTRIAL':
            usage_multiplier = 15
        elif segment == 'COMMERCIAL':
            usage_multiplier = 5
        else:
            usage_multiplier = 1
        
        return {
            'METER_ID': f'MTR-{uuid.uuid4().hex[:8].upper()}',
            'READING_TIMESTAMP': datetime.now(),
            'USAGE_KWH': round(base_usage * usage_multiplier, 4),
            'VOLTAGE': round(random.uniform(118, 122), 2),
            'CUSTOMER_SEGMENT': segment,
        }
    
    # Generate some test readings
    readings = [generate_test_reading() for _ in range(100)]
    
    # Validate readings
    assert len(readings) == 100, "Should generate 100 readings"
    print("  ✓ Generated 100 test readings")
    
    for r in readings:
        assert r['METER_ID'].startswith('MTR-'), "Meter ID format"
        assert 0 < r['USAGE_KWH'] < 100, "Usage in reasonable range"
        assert 118 <= r['VOLTAGE'] <= 122, "Voltage in range"
        assert r['CUSTOMER_SEGMENT'] in ['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL']
    print("  ✓ All readings have valid format")
    
    # Check segment distribution (should have some of each)
    segments = [r['CUSTOMER_SEGMENT'] for r in readings]
    unique_segments = set(segments)
    print(f"  ✓ Segments generated: {unique_segments}")
    
    # Check usage varies by segment
    residential_usage = [r['USAGE_KWH'] for r in readings if r['CUSTOMER_SEGMENT'] == 'RESIDENTIAL']
    industrial_usage = [r['USAGE_KWH'] for r in readings if r['CUSTOMER_SEGMENT'] == 'INDUSTRIAL']
    
    if residential_usage and industrial_usage:
        avg_res = sum(residential_usage) / len(residential_usage)
        avg_ind = sum(industrial_usage) / len(industrial_usage)
        print(f"  ✓ Avg residential: {avg_res:.2f} kWh, Avg industrial: {avg_ind:.2f} kWh")
    
    return True


def test_dockerfile():
    """Test that Dockerfile is valid"""
    print("\nTesting Dockerfile...")
    
    dockerfile_path = os.path.join(os.path.dirname(__file__), '..', 'spcs_app', 'Dockerfile')
    
    with open(dockerfile_path, 'r') as f:
        content = f.read()
    
    # Check required directives
    checks = [
        ('FROM python:', 'Base image'),
        ('WORKDIR', 'Working directory'),
        ('COPY requirements.txt', 'Copy requirements'),
        ('RUN pip install', 'Install dependencies'),
        ('COPY fastapi_app.py', 'Copy application'),
        ('EXPOSE', 'Expose port'),
        ('CMD', 'Start command'),
    ]
    
    for check, desc in checks:
        if check in content:
            print(f"  ✓ {desc}")
        else:
            print(f"  ✗ {desc} missing")
            return False
    
    return True


def test_requirements():
    """Test that requirements.txt has all needed packages"""
    print("\nTesting requirements.txt...")
    
    req_path = os.path.join(os.path.dirname(__file__), '..', 'spcs_app', 'requirements.txt')
    
    with open(req_path, 'r') as f:
        content = f.read().lower()
    
    required_packages = [
        'fastapi',
        'uvicorn',
        'snowflake-connector-python',
        'snowflake-snowpark-python',
    ]
    
    for pkg in required_packages:
        if pkg in content:
            print(f"  ✓ {pkg}")
        else:
            print(f"  ✗ {pkg} missing")
            return False
    
    return True


def test_no_hardcoded_secrets():
    """Test that no secrets are hardcoded in the codebase"""
    print("\nTesting for hardcoded secrets...")
    
    # Patterns that should NOT appear
    forbidden_patterns = [
        'abannerjee',
        '484577546576',
        'GZB42423',
        'SFSEHOL',
        'si-ae-enablement',
        '.snowflake.app',  # Hardcoded Postgres hosts
    ]
    
    files_to_check = [
        'spcs_app/fastapi_app.py',
        'spcs_app/service_spec.yaml',
        'spcs_app/deploy_spcs.sql',
        'spcs_app/build_and_push.sh',
    ]
    
    base_path = os.path.join(os.path.dirname(__file__), '..')
    
    for filepath in files_to_check:
        full_path = os.path.join(base_path, filepath)
        if not os.path.exists(full_path):
            print(f"  ⚠ {filepath} not found")
            continue
        
        with open(full_path, 'r') as f:
            content = f.read().lower()
        
        found_secrets = []
        for pattern in forbidden_patterns:
            if pattern.lower() in content:
                found_secrets.append(pattern)
        
        if found_secrets:
            print(f"  ✗ {filepath}: Found forbidden patterns: {found_secrets}")
            return False
        else:
            print(f"  ✓ {filepath}: No hardcoded secrets")
    
    return True


def main():
    """Run all smoke tests"""
    print("=" * 60)
    print("Flux Data Forge - Smoke Tests")
    print("=" * 60)
    
    tests = [
        ("Import Check", test_imports),
        ("Constants Check", test_constants),
        ("Data Generation Logic", test_data_generation_logic),
        ("Dockerfile Validation", test_dockerfile),
        ("Requirements Check", test_requirements),
        ("No Hardcoded Secrets", test_no_hardcoded_secrets),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"\n✗ {name} failed with exception: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    passed = sum(1 for _, p in results if p)
    total = len(results)
    
    for name, p in results:
        status = "✓ PASS" if p else "✗ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ All smoke tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
