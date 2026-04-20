# Backward-compatibility shim. New code: use project.interfaces.api.routers
from project.interfaces.api.routers.ingest import router as ingest_router
from project.interfaces.api.routers.publications import router as publications_router

__all__ = ["ingest_router", "publications_router"]
