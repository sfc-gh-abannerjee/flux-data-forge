"""
Streaming control endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - POST /api/stream                      (line ~7382) — main streaming start
  - POST /api/streaming/stop              (line ~7326)
  - GET  /api/streaming/status            (line ~7355)
  - GET  /api/streaming/emission-patterns (line ~9188)
  - GET  /api/streaming/event-frequencies (line ~9194)
  - GET  /api/streaming/calculate-metrics (line ~9200)
  - GET  /api/streaming/preview           (line ~9268)
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api/streaming", tags=["streaming"])

# TODO: Migrate /api/stream (the main streaming start endpoint, ~580 lines)
# TODO: Migrate streaming stop/status endpoints
# TODO: Migrate streaming config endpoints (emission patterns, frequencies)
# TODO: Migrate streaming preview/metrics endpoints
