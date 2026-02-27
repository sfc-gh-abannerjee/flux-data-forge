"""
Batch data generation endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - POST /api/generate  (line ~7960) — batch generation start
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["generation"])

# TODO: Migrate /api/generate endpoint
