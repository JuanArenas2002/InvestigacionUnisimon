"""
FastAPI — Punto de entrada principal de la API REST.
Sistema de Reconciliación Bibliográfica Institucional.

Ejecutar:
    uvicorn api.main:app --reload --port 8000

Documentación:
    http://localhost:8000/docs     → Swagger UI
    http://localhost:8000/redoc    → ReDoc
    http://localhost:8000/openapi.json → Esquema OpenAPI
"""

from dotenv import load_dotenv
load_dotenv()

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.session import create_all_tables, check_connection


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
# NO usar basicConfig aquí: uvicorn instala su propio log config con
# disable_existing_loggers=True. Se configuran los loggers explícitamente
# desde el lifespan, después de que uvicorn terminó su setup.

_LOG_FMT = logging.Formatter(
    "%(asctime)s │ %(name)-30s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)

_APP_LOGGERS = [
    "api",
    "api.sources.openalex",
    "api.sources.scopus",
    "api.sources.wos",
    "api.sources.cvlac",
    "api.sources.datos_abiertos",
    "pipeline",
    "excel",
    "extractors.serial_title",
    "extractors.scopus",
    "extractors.openalex",
    "extractors.wos",
    "extractors.cvlac",
    "reconciliation.engine",
]


def _setup_app_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(_LOG_FMT)
    for name in _APP_LOGGERS:
        lg = logging.getLogger(name)
        lg.setLevel(logging.INFO)
        lg.handlers = [handler]
        lg.propagate = False


logger = logging.getLogger("api")


# ─────────────────────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_app_logging()
    logger.info("Iniciando API de Reconciliación Bibliográfica...")
    if check_connection():
        logger.info("Conexión a PostgreSQL verificada.")
        try:
            create_all_tables()
            logger.info("Tablas de BD verificadas/creadas.")
        except Exception as e:
            logger.warning(f"No se pudieron crear tablas: {e}")
    else:
        logger.warning(
            "No se pudo conectar a PostgreSQL. "
            "La API arrancará, pero las rutas que usan BD fallarán."
        )
    yield
    logger.info("API detenida.")


# ─────────────────────────────────────────────────────────────
# OPENAPI TAGS  (definen el orden y las descripciones en /docs)
# ─────────────────────────────────────────────────────────────
#
# Flujo de dos fases:
#   FASE 1 — Fuentes independientes: cada plataforma descarga a su tabla propia
#   FASE 2 — Reconciliación: unifica todas las fuentes en canonical_publications
#

openapi_tags = [

    # ── General ───────────────────────────────────────────────
    {
        "name": "General",
        "description": "Estado del servicio, versión y enlaces de navegación rápida.",
    },

    # ── FASE 1: Fuentes independientes ────────────────────────
    {
        "name": "Fuentes · OpenAlex",
        "description": (
            "**FASE 1 · Descarga independiente desde OpenAlex.**\n\n"
            "Busca y almacena publicaciones en `openalex_records` (status=`pending`).\n\n"
            "Soporta búsqueda por **institución** (ROR ID) y por **autor** (ORCID / OpenAlex Author ID).\n\n"
            "> Los registros quedan pendientes hasta ejecutar la reconciliación global."
        ),
        "externalDocs": {
            "description": "Documentación OpenAlex",
            "url": "https://docs.openalex.org",
        },
    },
    {
        "name": "Fuentes · Scopus",
        "description": (
            "**FASE 1 · Descarga independiente desde Scopus (Elsevier).**\n\n"
            "Busca y almacena publicaciones en `scopus_records` (status=`pending`).\n\n"
            "Soporta búsqueda por **institución** (Affiliation ID) y por **autor** (ORCID / Scopus AU-ID).\n\n"
            "> Requiere `SCOPUS_API_KEY` configurada en variables de entorno."
        ),
        "externalDocs": {
            "description": "Scopus Search API",
            "url": "https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl",
        },
    },
    {
        "name": "Fuentes · Web of Science",
        "description": (
            "**FASE 1 · Descarga independiente desde Web of Science (Clarivate).**\n\n"
            "Busca y almacena publicaciones en `wos_records` (status=`pending`).\n\n"
            "Soporta búsqueda por **institución** (nombre / ROR) y por **autor** (ORCID / ResearcherID).\n\n"
            "> Requiere `WOS_API_KEY` configurada en variables de entorno."
        ),
        "externalDocs": {
            "description": "WoS Developer Portal",
            "url": "https://developer.clarivate.com/apis/wos",
        },
    },
    {
        "name": "Fuentes · CvLAC",
        "description": (
            "**FASE 1 · Scraping de CvLAC (Minciencias Colombia).**\n\n"
            "Extrae productos bibliográficos del perfil CvLAC de investigadores "
            "y los almacena en `cvlac_records` (status=`pending`).\n\n"
            "Búsqueda por **código CvLAC** (cod_rh) individual o por lote (institución).\n\n"
            "> No requiere API key — usa web scraping con delays respetuosos."
        ),
        "externalDocs": {
            "description": "CvLAC Minciencias",
            "url": "https://scienti.minciencias.gov.co/cvlac/",
        },
    },
    {
        "name": "Fuentes · Datos Abiertos",
        "description": (
            "**FASE 1 · Descarga desde Datos Abiertos Colombia (datos.gov.co).**\n\n"
            "Extrae registros del dataset de producción científica de Minciencias "
            "y los almacena en `datos_abiertos_records` (status=`pending`).\n\n"
            "Búsqueda por **institución** (nombre) o por **nombre de autor**.\n\n"
            "> Usa la SODA API pública — no requiere autenticación."
        ),
        "externalDocs": {
            "description": "Datos Abiertos Colombia",
            "url": "https://www.datos.gov.co",
        },
    },

    # ── FASE 2: Reconciliación y canónico ─────────────────────
    {
        "name": "Pipeline · Reconciliación",
        "description": (
            "**FASE 2 · Reconciliación global entre fuentes.**\n\n"
            "Unifica los registros de todas las tablas fuente (`openalex_records`, "
            "`scopus_records`, `wos_records`, `cvlac_records`, `datos_abiertos_records`) "
            "en `canonical_publications` usando una cascada: "
            "DOI exacto → fuzzy matching → nueva publicación canónica.\n\n"
            "Cada campo canónico lleva provenance indicando de qué fuente proviene."
        ),
    },
    {
        "name": "Pipeline · Extracción",
        "description": (
            "Endpoints heredados de extracción masiva por fuente "
            "(equivalente a búsqueda + reconciliación en un solo paso). "
            "Para el nuevo flujo de dos fases, usar los endpoints de **Fuentes**."
        ),
    },

    # ── Inventario canónico ───────────────────────────────────
    {
        "name": "Publicaciones",
        "description": (
            "CRUD sobre `canonical_publications` — el registro dorado unificado.\n\n"
            "Incluye gestión de estado (`Avalado` / `Revisión` / `Rechazado`), "
            "detección de duplicados, merge y cobertura de campos por fuente."
        ),
    },
    {
        "name": "Autores",
        "description": (
            "Gestión de autores normalizados con IDs multi-plataforma "
            "(ORCID, OpenAlex ID, Scopus AU-ID, WoS ResearcherID, CvLAC code).\n\n"
            "Incluye búsqueda, merge, inventario de publicaciones y cobertura de identificadores."
        ),
    },

    # ── Análisis y reportes ───────────────────────────────────
    {
        "name": "Gráficos de Investigadores",
        "description": "Métricas bibliométricas por investigador: h-index, citas por año, tendencias de publicación y distribución por cuartiles.",
    },
    {
        "name": "Scopus Dashboard",
        "description": "Vista consolidada de cobertura, métricas y contribuciones institucionales en Scopus.",
    },
    {
        "name": "Estadísticas",
        "description": "KPIs del sistema: conteos por fuente, estado de reconciliación, cobertura de campos y salud de la base de datos.",
    },
    {
        "name": "Búsqueda en Vivo",
        "description": "Consultas en tiempo real contra las APIs externas sin almacenar resultados (útil para verificación puntual).",
    },

    # ── Soporte ───────────────────────────────────────────────
    {
        "name": "Registros Externos",
        "description": "Listado y revisión manual de registros por fuente, log de reconciliación y gestión de registros en estado `manual_review`.",
    },
    {
        "name": "Catálogos",
        "description": "Tablas de referencia normalizadas: revistas (journals) e instituciones con ROR IDs.",
    },
    {
        "name": "Administración",
        "description": (
            "⚠️ Operaciones destructivas o de mantenimiento: limpieza de tablas, "
            "deduplicación forzada, validación de integridad y reset de datos."
        ),
    },
]


# ─────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────

_API_DESCRIPTION = """
# Sistema de Reconciliación Bibliográfica Institucional

Centraliza y unifica la producción científica de la institución extrayendo datos
de múltiples bases de datos bibliográficas y reconciliando duplicados.

---

### Flujo de dos fases

```
FASE 1 — Descarga independiente por fuente
─────────────────────────────────────────────────────────────────────
  POST /api/sources/openalex/search/by-institution   →  openalex_records
  POST /api/sources/scopus/search/by-institution     →  scopus_records
  POST /api/sources/wos/search/by-institution        →  wos_records
  POST /api/sources/cvlac/search/by-author           →  cvlac_records
  POST /api/sources/datos-abiertos/search/by-institution → datos_abiertos_records

  Todos los registros entran con status = pending

FASE 2 — Reconciliación global (bajo demanda)
─────────────────────────────────────────────────────────────────────
  POST /api/pipeline/reconcile-all
    ↓
    DOI exacto → fuzzy matching (título + año + autores) → nuevo canónico
    ↓
  canonical_publications  (campo field_provenance indica la fuente de cada dato)
```

---

### Fuentes soportadas

| Fuente | Tipo de acceso | Identificador de búsqueda |
|---|---|---|
| OpenAlex | REST API pública | ROR ID / ORCID / OpenAlex Author ID |
| Scopus | API con key (Elsevier) | Affiliation ID / ORCID / Scopus AU-ID |
| Web of Science | API con key (Clarivate) | Nombre institución / ORCID / ResearcherID |
| CvLAC | Web scraping | Código CvLAC (cod_rh) |
| Datos Abiertos Colombia | SODA API pública | Nombre institución / nombre autor |
"""

app = FastAPI(
    title="Reconciliación Bibliográfica API",
    description=_API_DESCRIPTION,
    version="2.0.0",
    lifespan=lifespan,
    openapi_tags=openapi_tags,
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "Sistema de Gestión Bibliográfica",
        "email": os.getenv("CONTACT_EMAIL", "biblioteca@universidad.edu"),
    },
    license_info={
        "name": "Uso interno institucional",
    },
)


# ─────────────────────────────────────────────────────────────
# CORS
# ─────────────────────────────────────────────────────────────
# dev  → permite cualquier origen (APP_ENV != "production")
# prod → define ALLOWED_ORIGINS como lista separada por comas:
#        ALLOWED_ORIGINS=https://app.universidad.edu,https://admin.universidad.edu

_is_production = os.getenv("APP_ENV", "development").lower() == "production"
_allowed_origins: list[str] = (
    [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
    if _is_production
    else ["*"]
)

if _is_production and not _allowed_origins:
    logger.warning(
        "APP_ENV=production pero ALLOWED_ORIGINS no está definido. "
        "CORS bloqueará todas las peticiones cross-origin."
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────
# ROUTERS
# ─────────────────────────────────────────────────────────────

from api.routers import (
    publications, authors, external_records,
    stats, search, pipeline, catalogs, scopus, admin, charts,
)
from api.routers.sources import router as sources_router
from project.app.routes.ingest import router as hex_ingest_router
from project.app.routes.publications import router as hex_publications_router

# ── FASE 1: fuentes independientes ───────────────────────────
app.include_router(sources_router, prefix="/api")

# ── FASE 2: reconciliación y pipeline ────────────────────────
app.include_router(pipeline.router,         prefix="/api")

# ── Inventario canónico ───────────────────────────────────────
app.include_router(publications.router,     prefix="/api")
app.include_router(authors.router,          prefix="/api")

# ── Análisis y reportes ───────────────────────────────────────
app.include_router(charts.router,           prefix="/api")
app.include_router(scopus.router,           prefix="/api")
app.include_router(stats.router,            prefix="/api")
app.include_router(search.router,           prefix="/api")

# ── Soporte ───────────────────────────────────────────────────
app.include_router(external_records.router, prefix="/api")
app.include_router(catalogs.router,         prefix="/api")
app.include_router(admin.router,            prefix="/api")

# ── Nueva arquitectura hexagonal (compatibilidad en app principal) ──
app.include_router(hex_ingest_router,       prefix="/api/hex")
app.include_router(hex_publications_router, prefix="/api/hex")


# ─────────────────────────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────────────────────────

@app.get("/", tags=["General"], summary="Estado del servicio")
def root():
    """Retorna información básica del servicio y enlaces de navegación."""
    return {
        "servicio": "Reconciliación Bibliográfica API",
        "version": "2.0.0",
        "estado": "activo",
        "enlaces": {
            "documentacion_swagger": "/docs",
            "documentacion_redoc":   "/redoc",
            "health":                "/api/stats/health",
            "estadisticas":          "/api/stats/summary",
        },
        "flujo": {
            "fase_1_fuentes": {
                "openalex":       "/api/sources/openalex/search/by-institution",
                "scopus":         "/api/sources/scopus/search/by-institution",
                "wos":            "/api/sources/wos/search/by-institution",
                "cvlac":          "/api/sources/cvlac/search/by-author",
                "datos_abiertos": "/api/sources/datos-abiertos/search/by-institution",
            },
            "fase_2_reconciliacion": "/api/pipeline/reconcile-all",
            "publicaciones_canonicas": "/api/publications",
            "hex_ingest": "/api/hex/ingest",
            "hex_publications": "/api/hex/publications",
        },
    }
