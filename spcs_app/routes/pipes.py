"""
Snowpipe management endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - GET  /api/pipes/check/{db}/{schema}/{table} (line ~8651)
  - POST /api/pipes/auto-create                 (line ~8683)
  - GET  /api/pipes/{database}/{schema}         (line ~8825)
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api/pipes", tags=["pipes"])

# TODO: Migrate pipe check endpoint
# TODO: Migrate pipe auto-create endpoint
# TODO: Migrate pipe listing endpoint
