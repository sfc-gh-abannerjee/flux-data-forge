"""
Route stubs for Flux Data Forge.

These APIRouter instances define the target route groupings for the
modular architecture. Currently they are empty stubs — endpoints will
be migrated here incrementally from fastapi_app.py.

To register all routers with the app:
    from routes import register_routers
    register_routers(app)
"""

from fastapi import FastAPI

from .health import router as health_router
from .admin import router as admin_router
from .streaming import router as streaming_router
from .generation import router as generation_router
from .pipes import router as pipes_router
from .production import router as production_router
from .monitoring import router as monitoring_router
from .stages import router as stages_router


def register_routers(app: FastAPI) -> None:
    """Register all route modules with the FastAPI app."""
    app.include_router(health_router)
    app.include_router(admin_router)
    app.include_router(streaming_router)
    app.include_router(generation_router)
    app.include_router(pipes_router)
    app.include_router(production_router)
    app.include_router(monitoring_router)
    app.include_router(stages_router)
