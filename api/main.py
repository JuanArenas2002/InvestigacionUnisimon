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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-25s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("api")


# ── Lifespan ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown de la aplicación."""
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
    {"name": "Registros Externos", "description": "Registros importados de fuentes externas, revisión manual y log de reconciliación."},
    {"name": "Scopus", "description": "Dashboard completo de registros, contribuciones, cobertura y métricas de Scopus."},
    {"name": "Estadísticas", "description": "KPIs del sistema, métricas de calidad, timelines y archivos JSON."},
    {"name": "Búsqueda", "description": "Búsqueda en vivo contra APIs externas (OpenAlex)."},
    {"name": "Pipeline", "description": "Extracción desde fuentes, ingesta de registros, reconciliación y creación de tablas."},
    {"name": "Catálogos", "description": "Gestión de revistas e instituciones normalizadas (tablas de referencia)."},
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

from api.routers import publications, authors, external_records, stats, search, pipeline, catalogs, scopus

app.include_router(publications.router, prefix="/api")
app.include_router(authors.router,      prefix="/api")
app.include_router(external_records.router, prefix="/api")
app.include_router(scopus.router,       prefix="/api")
app.include_router(stats.router,        prefix="/api")
app.include_router(search.router,       prefix="/api")
app.include_router(pipeline.router,     prefix="/api")
app.include_router(catalogs.router,     prefix="/api")


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
