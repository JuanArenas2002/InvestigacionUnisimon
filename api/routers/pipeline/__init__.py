"""
Paquete pipeline: combina los sub-routers en un único ``router``
con el prefijo ``/pipeline``.

Estructura:
  extraction.py        — /extract/openalex, /extract/scopus, /load-json, /search-doi-in-sources
  coverage.py          — /scopus/journal-coverage*, /scopus/check-publications-coverage, ...
  reconciliation_ops.py — /reconcile, /reconcile-all, /reconcile/all-sources, /crossref-scopus
  admin.py             — /truncate-all, /init-db, /scopus/test-extract
"""
from fastapi import APIRouter

from .extraction       import router as _extraction_router
from .coverage         import router as _coverage_router
from .reconciliation_ops import router as _reconciliation_router
from .admin            import router as _admin_router

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])
router.include_router(_extraction_router)
router.include_router(_coverage_router)
router.include_router(_reconciliation_router)
router.include_router(_admin_router)

__all__ = ["router"]
