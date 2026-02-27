"""
Stage management endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - GET /api/stages/{database}/{schema}         (line ~8795)
  - GET /api/stages                              (line ~9919)
  - GET /api/stage/preview/{stage_name:path}     (line ~10014)
  - GET /api/external-stage/diagnostics          (line ~9610)
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["stages"])

# TODO: Migrate stage listing endpoints
# TODO: Migrate stage preview endpoint
# TODO: Migrate external stage diagnostics endpoint
