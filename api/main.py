# Cargar variables de entorno desde .env
from dotenv import load_dotenv
load_dotenv()
"""
FastAPI — Punto de entrada principal de la API REST.
Reconciliación Bibliográfica Institucional.

Ejecutar:
    uvicorn api.main:app --reload --port 8000

Documentación automática:
    http://localhost:8000/docs     (Swagger UI)
    http://localhost:8000/redoc    (ReDoc)
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.session import create_all_tables, check_connection

# ── Logging ──────────────────────────────────────────────────
# NO usar basicConfig aquí: uvicorn instala su propio log config con
# disable_existing_loggers=True, lo que silencia los loggers creados antes.
# La solución es configurarlos explícitamente con handlers propios.

_LOG_FMT = logging.Formatter(
    "%(asctime)s │ %(name)-28s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)

# Loggers de la aplicación que queremos ver en consola
_APP_LOGGERS = [
    "api",
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
    """
    Adjunta un StreamHandler propio a cada logger de la aplicación.
    Se llama desde el lifespan, DESPUÉS de que uvicorn terminó su setup,
    así no puede silenciarnos con disable_existing_loggers=True.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(_LOG_FMT)
    for name in _APP_LOGGERS:
        lg = logging.getLogger(name)
        lg.setLevel(logging.INFO)
        # Reemplazar handlers previos para evitar duplicados en --reload
        lg.handlers = [handler]
        lg.propagate = False   # no pasar al root (evita doble impresión)

logger = logging.getLogger("api")


# ── Lifespan ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown de la aplicación."""
    _setup_app_logging()   # ← configurar logging AQUÍ, después de uvicorn
    logger.info("Iniciando API de Reconciliación Bibliográfica...")
    if check_connection():
        logger.info("Conexión a PostgreSQL verificada.")
        try:
            create_all_tables()
            logger.info("Tablas de BD verificadas/creadas.")
        except Exception as e:
            logger.warning(f"No se pudieron crear tablas: {e}")
    else:
        logger.warning("No se pudo conectar a PostgreSQL. "
                        "La API arrancará, pero las rutas que usan BD fallarán.")
    yield
    logger.info("API detenida.")


# ── App ──────────────────────────────────────────────────────

# Metadatos de tags para la documentación
openapi_tags = [
    {"name": "Inicio", "description": "Información general del servicio."},
    {"name": "Publicaciones", "description": "CRUD y consultas sobre publicaciones canónicas del inventario bibliográfico."},
    {"name": "Autores", "description": "Gestión de autores, coautorías y cobertura de identificadores."},
    {"name": "Gráficos de Investigadores", "description": "Generación de gráficos de publicaciones desde Scopus con visualizaciones profesionales."},
    {"name": "Registros Externos", "description": "Registros importados de fuentes externas, revisión manual y log de reconciliación."},
    {"name": "Scopus", "description": "Dashboard completo de registros, contribuciones, cobertura y métricas de Scopus."},
    {"name": "Estadísticas", "description": "KPIs del sistema, métricas de calidad, timelines y archivos JSON."},
    {"name": "Búsqueda", "description": "Búsqueda en vivo contra APIs externas."},
    {"name": "OpenAlex", "description": "Extracción, búsqueda y enriquecimiento vía OpenAlex (PyAlex)."},

    {"name": "Catálogos", "description": "Gestión de revistas e instituciones normalizadas (tablas de referencia)."},
    {"name": "Administración", "description": "Limpieza, deduplicación, reportes de calidad y validación de integridad."},
]

app = FastAPI(
    title="Reconciliación Bibliográfica API",
    description=(
        "API REST para el sistema de reconciliación bibliográfica institucional.\n\n"
        "Permite gestionar publicaciones canónicas, autores, registros externos, "
        "ejecutar extracciones desde fuentes (OpenAlex, Scopus, WoS, CvLAC) "
        "y manejar el motor de reconciliación."
    ),
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=openapi_tags,
)

# ── CORS (permitir frontends) ────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, restringir a dominios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Registrar routers ────────────────────────────────────────

from api.routers import publications, authors, external_records, stats, search, pipeline, catalogs, scopus, admin, charts

app.include_router(publications.router, prefix="/api")
app.include_router(authors.router,      prefix="/api")
app.include_router(charts.router,       prefix="/api")
app.include_router(external_records.router, prefix="/api")
app.include_router(scopus.router,       prefix="/api")
app.include_router(stats.router,        prefix="/api")
app.include_router(search.router,       prefix="/api")
app.include_router(pipeline.router,     prefix="/api")
app.include_router(catalogs.router,     prefix="/api")
app.include_router(admin.router,        prefix="/api")


# ── Root ─────────────────────────────────────────────────────

@app.get("/", tags=["Inicio"], summary="Información del servicio")
def root():
    """Retorna información básica del servicio y enlaces útiles."""
    return {
        "servicio": "Reconciliación Bibliográfica API",
        "version": "1.0.1",
        "documentacion": "/docs",
        "estado": "/api/stats/health",
    }
