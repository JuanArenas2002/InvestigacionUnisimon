"""Endpoints Layer: HTTP Handlers para el pipeline."""
from fastapi import APIRouter

from .scopus_coverage import router as scopus_coverage_router
from .extraction import router as extraction_router
from .reconciliation import router as reconciliation_router
from .admin import router as admin_router

# Combinar todos los routers de endpoints
router = APIRouter(prefix="/pipeline")

# Scopus
router.include_router(scopus_coverage_router, prefix="/scopus", tags=["Scopus Coverage"])

# Extraction
router.include_router(extraction_router, prefix="/extract", tags=["Extraction"])

# Reconciliation
router.include_router(reconciliation_router, prefix="/reconcile", tags=["Reconciliation"])

# Admin
router.include_router(admin_router, prefix="", tags=["Admin"])

__all__ = ["router"]
