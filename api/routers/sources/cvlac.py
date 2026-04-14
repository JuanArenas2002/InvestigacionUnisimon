"""
Router: CvLAC (Minciencias Colombia)

Dos fuentes de datos:

1. Scraping HTML de Minciencias (existente):
   CvLAC no tiene API pública — se hace web scraping por cod_rh.
   POST /sources/cvlac/search/by-author
   POST /sources/cvlac/search/by-institution
   GET  /sources/cvlac/records
   GET  /sources/cvlac/records/{id}

2. API JSON de Metrik Unisimon (nueva):
   Consume el endpoint REST de Metrik por cédula del investigador.
   GET  /sources/cvlac/profile/{cc}
"""

import logging
from typing import Optional, List, Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.routers.sources._base import SearchResult, SourceRecordSummary
from db.models import CvlacRecord
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("api.sources.cvlac")
router = APIRouter(prefix="/cvlac", tags=["Fuentes · CvLAC"])


# ─────────────────────────────────────────────────────────────
# REQUEST SCHEMA ESPECÍFICO DE CvLAC
# ─────────────────────────────────────────────────────────────

class CvlacSearchByAuthorRequest(BaseModel):
    """Parámetros para búsqueda en CvLAC por cédula del investigador."""
    cc: str = Field(
        ...,
        description="Cédula de ciudadanía del investigador",
        examples=["7977197"],
    )


class CvlacBatchSearchRequest(BaseModel):
    """Búsqueda por lote de cédulas (equivalente a 'por institución')."""
    cc_investigadores: List[str] = Field(
        ...,
        description="Lista de cédulas de los investigadores de la institución",
        min_length=1,
    )


# ─────────────────────────────────────────────────────────────
# RESPONSE SCHEMA ESPECÍFICO DE CvLAC
# ─────────────────────────────────────────────────────────────

class CvlacRecordDetail(BaseModel):
    """Todos los campos de un registro CvLAC almacenado."""
    # ── Identificadores
    id:                      int
    cvlac_code:              Optional[str] = None
    cvlac_product_id:        Optional[str] = None
    doi:                     Optional[str] = None
    isbn:                    Optional[str] = None
    # ── Tipo y metadatos
    product_type:            Optional[str] = None
    title:                   Optional[str] = None
    publication_year:        Optional[int] = None
    publication_date:        Optional[str] = None
    publication_type:        Optional[str] = None
    language:                Optional[str] = None
    source_journal:          Optional[str] = None
    issn:                    Optional[str] = None
    # ── Ubicación
    volume:                  Optional[str] = None
    issue:                   Optional[str] = None
    pages:                   Optional[str] = None
    editorial:               Optional[str] = None
    # ── Contenido
    abstract:                Optional[str] = None
    keywords:                Optional[str] = None
    # ── Clasificación Minciencias
    visibility:              Optional[str] = None
    category:                Optional[str] = None
    # ── Contexto institucional
    research_group:          Optional[str] = None
    # ── Open Access
    is_open_access:          Optional[bool] = None
    # ── Métricas
    citation_count:          int = 0
    # ── Reconciliación
    status:                  str = "pending"
    match_type:              Optional[str] = None
    match_score:             Optional[float] = None
    canonical_publication_id: Optional[int] = None
    # ── Timestamps
    created_at:              Any = None
    updated_at:              Any = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────
# POST /search/by-author
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-author",
    response_model=SearchResult,
    summary="Extracción CvLAC por cédula del investigador (Metrik Unisimon)",
)
def search_by_author(
    body: CvlacSearchByAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Consulta el perfil CvLAC de un investigador por cédula usando el API JSON
    de Metrik Unisimon y almacena los productos en `cvlac_records`.

    Requiere la cédula de ciudadanía del investigador (campo `cc`).
    """
    from extractors.cvlac import CvlacExtractor

    logger.info(f"CvLAC · búsqueda por cédula: cc={body.cc}")
    try:
        extractor = CvlacExtractor()
        records = extractor.extract(cc_investigadores=[body.cc])
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"Error extrayendo CvLAC cc={body.cc}: {e}")
        raise HTTPException(502, f"Error al consultar CvLAC: {e}")

    return _ingest(records, db, "by-author")


# ─────────────────────────────────────────────────────────────
# POST /search/by-institution  (lote de cédulas)
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-institution",
    response_model=SearchResult,
    summary="Extracción CvLAC para múltiples investigadores por cédula",
)
def search_by_institution(
    body: CvlacBatchSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Itera sobre una lista de cédulas y extrae los productos CvLAC de cada
    investigador desde el API JSON de Metrik Unisimon.

    Útil para cargar el inventario completo de una institución.
    """
    from extractors.cvlac import CvlacExtractor

    logger.info(f"CvLAC · búsqueda por institución: {len(body.cc_investigadores)} investigadores")
    try:
        extractor = CvlacExtractor()
        records = extractor.extract(cc_investigadores=body.cc_investigadores)
    except Exception as e:
        logger.error(f"Error en extracción institucional CvLAC: {e}")
        raise HTTPException(502, f"Error al consultar CvLAC: {e}")

    return _ingest(records, db, "by-institution")


# ─────────────────────────────────────────────────────────────
# GET /records
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records",
    response_model=List[SourceRecordSummary],
    summary="Listar registros almacenados de CvLAC",
)
def list_records(
    status:      Optional[str] = Query(None),
    cvlac_code:  Optional[str] = Query(None, description="Filtrar por código CvLAC del investigador"),
    year_from:   Optional[int] = Query(None),
    year_to:     Optional[int] = Query(None),
    page:        int = Query(1, ge=1),
    page_size:   int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(CvlacRecord)
    if status:
        q = q.filter(CvlacRecord.status == status)
    if cvlac_code:
        q = q.filter(CvlacRecord.cvlac_code == cvlac_code)
    if year_from:
        q = q.filter(CvlacRecord.publication_year >= year_from)
    if year_to:
        q = q.filter(CvlacRecord.publication_year <= year_to)
    return q.order_by(CvlacRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()


# ─────────────────────────────────────────────────────────────
# GET /records/{id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records/{record_id}",
    response_model=CvlacRecordDetail,
    summary="Detalle de un registro CvLAC",
)
def get_record(record_id: int, db: Session = Depends(get_db)):
    record = db.get(CvlacRecord, record_id)
    if not record:
        raise HTTPException(404, "Registro CvLAC no encontrado.")
    return record


# ─────────────────────────────────────────────────────────────
# GET /profile/{cc}  — API JSON Metrik Unisimon
# ─────────────────────────────────────────────────────────────

@router.get(
    "/profile/{cc}",
    summary="Perfil CvLAC por cédula (API JSON Metrik Unisimon)",
    response_class=JSONResponse,
)
def get_profile_by_cc(cc: str):
    """
    Consulta el perfil CvLAC de un investigador usando su cédula de ciudadanía.

    Consume el endpoint REST de Metrik Unisimon:
        https://metrik.unisimon.edu.co/scienti/cvlac/{cc}

    Devuelve JSON normalizado con la estructura:
    ```json
    {
      "investigador": { "cc", "nombre", "categoria", "nacionalidad" },
      "produccion": [
        { "cc", "autor_principal", "tipo", "subtipo", "titulo",
          "revista", "anio", "doi", "editorial", "autores" }
      ]
    }
    ```

    Reglas de normalización:
    - Campos "N/A" se omiten.
    - DOI vacío → null.
    - anio → entero.
    - autores → lista (split por coma).
    """
    from extractors.cvlac.application.metrik_service import fetch_profile

    try:
        data = fetch_profile(cc_investigador=cc)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 502
        if status == 404:
            raise HTTPException(404, f"Investigador con cc={cc} no encontrado en Metrik CvLAC.")
        raise HTTPException(502, f"Error HTTP al consultar Metrik CvLAC: {e}")
    except requests.Timeout:
        raise HTTPException(504, "Timeout al consultar el API Metrik CvLAC.")
    except requests.ConnectionError as e:
        raise HTTPException(503, f"No se pudo conectar al API Metrik CvLAC: {e}")
    except ValueError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        logger.error(f"[CvLAC Metrik] Error inesperado para cc={cc}: {e}")
        raise HTTPException(500, f"Error interno al procesar perfil CvLAC: {e}")

    return data


# ─────────────────────────────────────────────────────────────
# HELPER PRIVADO
# ─────────────────────────────────────────────────────────────

def _ingest(records, db: Session, context: str) -> SearchResult:
    if not records:
        return SearchResult(
            source="cvlac",
            inserted=0, skipped=0, errors=0,
            message="No se encontraron productos para este investigador.",
        )
    try:
        engine = ReconciliationEngine(session=db)
        # reconcile_batch = ingest_records + reconcile_pending en un solo paso:
        #   1. Guarda en cvlac_records (status=pending)
        #   2. Crea/vincula canonical_publications
        #   3. Crea publication_author (asigna al investigador)
        stats = engine.reconcile_batch(records)
        logger.info(
            f"CvLAC [{context}]: {stats.new_canonical} canónicos nuevos, "
            f"{stats.doi_exact_matches + stats.fuzzy_high_matches + stats.fuzzy_combined_matches} vinculados, "
            f"{stats.errors} errores."
        )
        return SearchResult(
            source="cvlac",
            inserted=stats.new_canonical,
            skipped=max(len(records) - stats.total_processed, 0),
            errors=stats.errors,
            message=(
                f"{stats.new_canonical} publicaciones nuevas creadas, "
                f"{stats.doi_exact_matches + stats.fuzzy_high_matches + stats.fuzzy_combined_matches} "
                f"vinculadas a publicaciones existentes. "
                f"Autor asignado en publication_author."
            ),
        )
    except Exception as e:
        logger.error(f"Error en ingesta CvLAC: {e}")
        raise HTTPException(500, f"Error al almacenar registros: {e}")
