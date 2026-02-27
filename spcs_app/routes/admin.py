"""
Admin and schema exploration endpoints.

Target endpoints (to migrate from fastapi_app.py):
  - POST /api/task/suspend       (line ~7299)
  - POST /api/task/resume        (line ~7313)
  - POST /api/validate           (line ~8050)
  - GET  /api/databases          (line ~8073)
  - GET  /api/schemas/{database} (line ~8093)
  - GET  /api/tables/{db}/{sch}  (line ~8113)
  - GET  /api/tables/bronze      (line ~8132)
  - GET  /api/bronze-tables      (line ~8246)
  - GET  /api/bronze-preview     (line ~8316)
  - POST /api/tables/create-bronze (line ~8492)
  - GET  /api/warehouses         (line ~8777)
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["admin"])

# TODO: Migrate task suspend/resume endpoints
# TODO: Migrate schema exploration endpoints (databases, schemas, tables)
# TODO: Migrate bronze table management endpoints
# TODO: Migrate warehouse listing endpoint
