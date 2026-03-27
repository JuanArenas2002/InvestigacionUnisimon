"""
Routers de fuentes de datos independientes.

Cada plataforma tiene su propio router con endpoints para:
  - Buscar y descargar registros (por institución o por autor)
  - Listar registros almacenados
  - Ver detalle de un registro

La reconciliación con canonical_publications es un proceso separado
que se invoca desde /api/pipeline/reconcile o /api/pipeline/all-sources.
"""

from fastapi import APIRouter

from .openalex      import router as openalex_router
from .scopus        import router as scopus_router
from .wos           import router as wos_router
from .cvlac         import router as cvlac_router
from .datos_abiertos import router as datos_abiertos_router

# Router raíz que agrupa todos los sub-routers bajo /sources
router = APIRouter(prefix="/sources")

router.include_router(openalex_router)
router.include_router(scopus_router)
router.include_router(wos_router)
router.include_router(cvlac_router)
router.include_router(datos_abiertos_router)
