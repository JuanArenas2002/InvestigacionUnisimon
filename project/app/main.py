"""
FastAPI — Punto de entrada de la arquitectura hexagonal.

Ejecutar de forma independiente:
    uvicorn project.app.main:app --reload --port 8001

O integrado al main principal (api/main.py ya lo incluye en /api/hex/).
"""

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

from project.app.routes.ingest import router as ingest_router
from project.app.routes.publications import router as publications_router

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

_LOG_FMT = logging.Formatter(
    "%(asctime)s │ %(name)-28s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)

_HEX_LOGGERS = [
    "project.app",
    "project.application",
    "project.infrastructure",
    "project.registry",
]


def _setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_LOG_FMT)
    for name in _HEX_LOGGERS:
        lg = logging.getLogger(name)
        lg.setLevel(logging.INFO)
        lg.handlers = [handler]
        lg.propagate = False


logger = logging.getLogger("project.app")


# ──────────────────────────────────────────────
# LIFESPAN
# ──────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    logger.info("Iniciando arquitectura hexagonal...")

    # Verificar conexion a la base de datos (opcional al arrancar en modo standalone)
    try:
        from db.session import check_connection, create_all_tables
        if check_connection():
            logger.info("Conexion a PostgreSQL verificada.")
            try:
                create_all_tables()
                logger.info("Tablas verificadas/creadas.")
            except Exception as exc:
                logger.warning("No se pudieron crear tablas: %s", exc)
        else:
            logger.warning(
                "Sin conexion a PostgreSQL. "
                "Los endpoints que usan BD retornaran error 503."
            )
    except ImportError:
        logger.warning("db.session no disponible — modo standalone sin BD.")

    yield
    logger.info("Arquitectura hexagonal detenida.")


# ──────────────────────────────────────────────
# APP
# ──────────────────────────────────────────────

_is_production = os.getenv("APP_ENV", "development").lower() == "production"
_allowed_origins: list[str] = (
    [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
    if _is_production
    else ["*"]
)

app = FastAPI(
    title="Reconciliacion Bibliografica — Hex API",
    version="1.0.0",
    description=(
        "## Arquitectura Hexagonal\n\n"
        "Pipeline ETL bibliografico con separacion estricta de capas:\n\n"
        "- **`POST /ingest`** → Ejecuta pipeline completo (collect → deduplicate → normalize → match → enrich)\n"
        "- **`GET /publications`** → Lista publicaciones canonicas\n\n"
        "### Fuentes disponibles\n"
        "Descubiertas automaticamente via `SourceRegistry`: "
        "`scopus`, `openalex`, `wos`, `cvlac`, `datos_abiertos`\n\n"
        "### Agregar una nueva fuente\n"
        "Crear `infrastructure/sources/mi_fuente_adapter.py` con `SOURCE_NAME = 'mi_fuente'` "
        "e implementar `SourcePort`. El registry la detecta automaticamente."
    ),
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "Ingest",
            "description": "Pipeline ETL de ingesta: collect → deduplicate → normalize → match → enrich",
        },
        {
            "name": "Publications",
            "description": "Consulta de publicaciones canonicas reconciliadas",
        },
        {
            "name": "General",
            "description": "Estado del servicio y discovery de fuentes",
        },
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────
# ROUTERS
# ──────────────────────────────────────────────

app.include_router(ingest_router)
app.include_router(publications_router)


# ──────────────────────────────────────────────
# ROOT
# ──────────────────────────────────────────────


@app.get("/health", tags=["General"], summary="Estado del servicio")
def health() -> dict:
    return {"status": "ok", "arquitectura": "hexagonal"}


@app.get("/", tags=["General"], summary="Info del servicio y fuentes disponibles")
def root() -> dict:
    try:
        from project.config.container import build_source_registry
        registry = build_source_registry()
        sources = registry.source_names
    except Exception:
        sources = []

    return {
        "servicio": "Reconciliacion Bibliografica — Hex API",
        "version": "1.0.0",
        "estado": "activo",
        "fuentes_disponibles": sources,
        "endpoints": {
            "ingest": "POST /ingest",
            "publications": "GET /publications",
            "docs": "/docs",
            "health": "/health",
        },
    }
