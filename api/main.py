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
from pydantic import BaseModel

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
        "name": "Fuentes · Google Scholar",
        "description": (
            "**FASE 1 · Scraping de Google Scholar.**\n\n"
            "Extrae publicaciones de perfiles de investigadores en Google Scholar "
            "y las almacena en `google_Scholar_records` (status=`pending`).\n\n"
            "Búsqueda por **Scholar Profile ID** (ej: V94aovUAAAAJ).\n\n"
            "Soporta múltiples perfiles simultáneamente con filtros de año.\n\n"
            "> Usa la librería `scholarly` con delays respetuosos para web scraping ético.\n\n"
            "> Datos disponibles: título, autores, año, citas, DOI, URL, métricas por año."
        ),
        "externalDocs": {
            "description": "Google Scholar Profiles",
            "url": "https://scholar.google.com",
        },
    },
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

    # ── Portal del Investigador ───────────────────────────────
    {
        "name": "Portal del Investigador",
        "description": (
            "Endpoints de solo lectura para investigadores autenticados. "
            "Requieren `Authorization: Bearer <token>` (JWT obtenido en `/api/auth/login`). "
            "Cada endpoint opera exclusivamente sobre los datos del investigador autenticado."
        ),
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
| Google Scholar | Web scraping | Scholar Profile ID (ej: V94aovUAAAAJ) |
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
    stats, search, pipeline, catalogs, scopus, admin, charts, auth,
    researcher_portal,
)
from api.routers.sources import router as sources_router
from project.interfaces.api.routers.ingest import router as hex_ingest_router
from project.interfaces.api.routers.publications import router as hex_publications_router
from project.interfaces.api.routers.author_profile import router as hex_author_profile_router

# ── FASE 1: fuentes independientes ───────────────────────────
app.include_router(sources_router, prefix="/api")

# ── Autenticación ────────────────────────────────────────────
app.include_router(auth.router, prefix="/api")

# ── Portal del Investigador (solo lectura, JWT requerido) ─────
app.include_router(researcher_portal.router, prefix="/api")

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
app.include_router(hex_ingest_router,           prefix="/api/hex")
app.include_router(hex_publications_router,     prefix="/api/hex")
app.include_router(hex_author_profile_router,   prefix="/api/hex")


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
                "google_scholar": "/api/scholar/test",
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


# ─────────────────────────────────────────────────────────────
# GOOGLE SCHOLAR — ENDPOINTS DE PRUEBA
# ─────────────────────────────────────────────────────────────

class GoogleScholarExtractRequest(BaseModel):
    """Modelo para request de extracción de Google Scholar"""
    scholar_ids: list[str]
    year_from: int = 2020
    year_to: int = 2024
    max_results: int = 50
    dry_run: bool = True
    
    class Config:
        json_schema_extra = {
            "example": {
                "scholar_ids": ["V94aovUAAAAJ"],
                "year_from": 2020,
                "year_to": 2024,
                "max_results": 50,
                "dry_run": True
            }
        }

@app.get("/api/scholar/test", tags=["Fuentes · Google Scholar"], summary="Test de Google Scholar")
def test_google_scholar():
    """
    🧪 **Endpoint de prueba para Google Scholar**
    
    Retorna información sobre cómo usar Google Scholar en la API.
    
    ### Ejemplo de uso:
    
    ```json
    POST /api/hex/ingest
    {
        "sources": ["google_Scholar"],
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "max_results": 50,
        "dry_run": false
    }
    ```
    
    ### Scholar Profile IDs de ejemplo:
    - V94aovUAAAAJ — Gustavo Aroca Martinez
    - jzXp-fUAAAAJ — Simón Bauer
    
    ### Respuesta esperada:
    - 5-10 publicaciones por perfil
    - Campos: título, autores, año, citas, DOI, URL
    - Estado: pending (esperando reconciliación)
    - Destino: tabla `google_Scholar_records`
    """
    return {
        "status": "ready",
        "service": "Google Scholar Extractor",
        "version": "1.0.0",
        "modo": "Web scraping con scholarly library",
        "uso": {
            "endpoint": "/api/hex/ingest",
            "metodo": "POST",
            "parametros": {
                "sources": ["google_Scholar"],
                "scholar_ids": ["ID1", "ID2", "..."],
                "year_from": 2020,
                "year_to": 2024,
                "max_results": 100,
                "dry_run": False,
            },
            "ejemplo_request": {
                "sources": ["google_Scholar"],
                "scholar_ids": ["V94aovUAAAAJ"],
                "year_from": 2020,
                "max_results": 50,
                "dry_run": False
            },
        },
        "scholar_ids_ejemplo": {
            "Gustavo_Aroca_Martinez": "V94aovUAAAAJ",
            "Simon_Bauer": "jzXp-fUAAAAJ",
        },
        "campos_extraidos": [
            "title",
            "authors",
            "publication_year",
            "citation_count",
            "doi",
            "url",
            "publication_type",
            "source_journal",
            "citations_by_year",
            "raw_data",
        ],
        "destino_bd": "google_Scholar_records",
        "status_inicial": "pending",
        "proximos_pasos": [
            "1. POST /api/hex/ingest con scholar_ids",
            "2. Verificar resultados en google_Scholar_records",
            "3. Ejecutar /api/pipeline/reconcile-all para matching",
            "4. Ver en canonical_publications",
        ],
        "documentacion": "/docs#/Fuentes%20·%20Google%20Scholar",
        "esquema_tabla": {
            "id": "SERIAL PRIMARY KEY",
            "google_Scholar_id": "VARCHAR(50) UNIQUE",
            "scholar_profile_id": "VARCHAR(50)",
            "title": "VARCHAR(1000) NOT NULL",
            "authors_json": "JSONB",
            "publication_year": "INTEGER",
            "doi": "VARCHAR(100) UNIQUE",
            "citation_count": "INTEGER",
            "citations_by_year": "JSONB",
            "status": "VARCHAR(30) DEFAULT 'pending'",
            "canonical_publication_id": "FK → canonical_publications(id)",
            "raw_data": "JSONB",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    }


@app.post("/api/scholar/extract", tags=["Fuentes · Google Scholar"], summary="Extraer de Google Scholar")
def extract_google_scholar(request: GoogleScholarExtractRequest):
    """
    📥 **Extrae publicaciones de Google Scholar**
    
    ### Request Body (JSON):
    ```json
    {
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "year_to": 2024,
        "max_results": 50,
        "dry_run": true
    }
    ```
    
    ### Parámetros:
    - `scholar_ids`: Lista de Scholar Profile IDs (requerido)
    - `year_from`: Año inicial (default: 2020)
    - `year_to`: Año final (default: 2024)
    - `max_results`: Máximo de resultados por perfil (default: 50)
    - `dry_run`: Si true, no persiste en BD (default: true para testing)
    
    ### Ejemplo de request (con cURL):
    ```bash
    curl -X POST "http://localhost:8000/api/scholar/extract" \\
      -H "Content-Type: application/json" \\
      -d '{
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "max_results": 50,
        "dry_run": true
      }'
    ```
    
    ### Retorno:
    - `dry_run=true`: Datos extraídos sin guardar
    - `dry_run=false`: Datos insertados en `google_Scholar_records`
    """
    from project.config.container import build_pipeline
    
    try:
        logger.info("Extrayendo de Google Scholar: %s", request.scholar_ids)

        if not request.scholar_ids:
            return {
                "status": "error",
                "error": "scholar_ids vacío o no proporcionado",
                "ejemplo": {"scholar_ids": ["V94aovUAAAAJ"]}
            }

        pipeline = build_pipeline(["google_scholar"])
        result = pipeline.run(
            year_from=request.year_from,
            year_to=request.year_to,
            max_results=request.max_results,
            persist=not request.dry_run,
            source_kwargs={
                "google_scholar": {
                    "scholar_ids": request.scholar_ids
                }
            }
        )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "scholar_ids": request.scholar_ids,
            "extraidos": result.collected,
            "guardados": result.source_saved if not request.dry_run else 0,
            "by_source": result.by_source,
            "errors": result.errors,
            "proximos_pasos": [
                "Ver en BD: SELECT * FROM google_scholar_records",
                "Reconciliar: POST /api/pipeline/reconcile-all",
                "Verificar: GET /api/publications?source=google_scholar",
            ]
        }
        
    except Exception as e:
        logger.error("Error en Google Scholar extraction: %s", e, exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "tipo": type(e).__name__,
        }

