"""
Router: CvLAC (Minciencias Colombia)

CvLAC no tiene una API pública oficial — la extracción es por web scraping.
La búsqueda se hace por código CvLAC del investigador (cod_rh).

Rutas:
  POST /sources/cvlac/search/by-author
  GET  /sources/cvlac/records
  GET  /sources/cvlac/records/{id}

Nota: No existe búsqueda por institución directa en CvLAC.
      Se puede iterar sobre una lista de códigos de investigadores.
"""

import logging
from typing import Optional, List, Any

from fastapi import APIRouter, Depends, HTTPException, Query
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
    """Parámetros para búsqueda en CvLAC. Usa código CvLAC o ORCID."""
    cvlac_code: Optional[str] = Field(
        None,
        description="Código CvLAC del investigador (cod_rh en la URL del perfil)",
    )
    orcid: Optional[str] = Field(
        None,
        description="ORCID del investigador (se intenta mapear a código CvLAC)",
    )


class CvlacBatchSearchRequest(BaseModel):
    """Búsqueda por lote de códigos CvLAC (equivalente a 'por institución')."""
    cvlac_codes: List[str] = Field(
        ...,
        description="Lista de códigos CvLAC de los investigadores de la institución",
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
    summary="Scraping de publicaciones CvLAC para un investigador",
)
def search_by_author(
    body: CvlacSearchByAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Hace scraping del perfil CvLAC de un investigador y almacena los productos
    en `cvlac_records` con status='pending'.

    Requiere el código CvLAC (cod_rh) del investigador.
    """
    from extractors.cvlac import CvlacExtractor

    if not body.cvlac_code and not body.orcid:
        raise HTTPException(400, "Se requiere cvlac_code u orcid.")

    logger.info(f"CvLAC · búsqueda por autor: code={body.cvlac_code}, orcid={body.orcid}")
    try:
        extractor = CvlacExtractor()
        records = extractor.extract(cvlac_code=body.cvlac_code, orcid=body.orcid)
    except Exception as e:
        logger.error(f"Error extrayendo CvLAC: {e}")
        raise HTTPException(502, f"Error en scraping CvLAC: {e}")

    return _ingest(records, db, "by-author")


# ─────────────────────────────────────────────────────────────
# POST /search/by-institution  (lote de códigos)
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-institution",
    response_model=SearchResult,
    summary="Scraping de CvLAC para múltiples investigadores (lote por institución)",
)
def search_by_institution(
    body: CvlacBatchSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Itera sobre una lista de códigos CvLAC (investigadores de la institución)
    y extrae los productos de cada uno.

    Útil para cargar el inventario completo de una institución desde CvLAC.
    """
    from extractors.cvlac import CvlacExtractor

    logger.info(f"CvLAC · búsqueda por institución: {len(body.cvlac_codes)} investigadores")
    all_records = []
    errors = 0

    extractor = CvlacExtractor()
    for code in body.cvlac_codes:
        try:
            records = extractor.extract(cvlac_code=code)
            all_records.extend(records)
        except Exception as e:
            logger.warning(f"Error en CvLAC para código {code}: {e}")
            errors += 1

    result = _ingest(all_records, db, "by-institution")
    result.errors += errors
    return result


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
# HELPER PRIVADO
# ─────────────────────────────────────────────────────────────

def _ingest(records, db: Session, context: str) -> SearchResult:
    if not records:
        return SearchResult(
            source="cvlac",
            inserted=0, skipped=0, errors=0,
            message="El scraping no devolvió productos.",
        )
    try:
        engine   = ReconciliationEngine(session=db)
        inserted = engine.ingest_records(records)
        skipped  = len(records) - inserted
        logger.info(f"CvLAC [{context}]: {inserted} insertados, {skipped} omitidos.")
        return SearchResult(
            source="cvlac",
            inserted=inserted,
            skipped=max(skipped, 0),
            errors=0,
            message=f"{inserted} productos nuevos almacenados en cvlac_records.",
        )
    except Exception as e:
        logger.error(f"Error en ingesta CvLAC: {e}")
        raise HTTPException(500, f"Error al almacenar registros: {e}")
