"""Endpoints Layer: HTTP Handlers para el pipeline."""
from fastapi import APIRouter

from .scopus_coverage import router as scopus_coverage_router
from .extraction import router as extraction_router
from .reconciliation import router as reconciliation_router
from .admin import router as admin_router

# Combinar todos los routers de endpoints
router = APIRouter(prefix="/pipeline")

# Scopus
router.include_router(scopus_coverage_router, prefix="/scopus",    tags=["Scopus Dashboard"])
router.include_router(extraction_router,      prefix="/extract",   tags=["Pipeline · Extracción"])
router.include_router(reconciliation_router,  prefix="",           tags=["Pipeline · Reconciliación"])
router.include_router(admin_router,           prefix="",           tags=["Administración"])

__all__ = ["router"]
