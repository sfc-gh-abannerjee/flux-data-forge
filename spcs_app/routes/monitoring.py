"""
Monitoring and resource endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - GET /api/monitor/metrics (line ~9483)
  - GET /api/resources       (line ~8842)
  - GET /api/context         (line ~8937)
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["monitoring"])

# TODO: Migrate monitor metrics endpoint
# TODO: Migrate resources endpoint
# TODO: Migrate context endpoint
