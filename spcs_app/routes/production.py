"""
Production data source endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - GET /api/production/sources      (line ~8981)
  - GET /api/production/meters       (line ~9013)
  - GET /api/production/cache-status (line ~9177)
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api/production", tags=["production"])

# TODO: Migrate production sources endpoint
# TODO: Migrate production meters endpoint (with SQL injection fix)
# TODO: Migrate cache status endpoint
