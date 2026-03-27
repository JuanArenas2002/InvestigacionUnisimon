"""
Router: Datos Abiertos Colombia

Búsqueda en el dataset de producción científica de Minciencias
publicado en datos.gov.co vía SODA API.

Rutas:
  POST /sources/datos-abiertos/search/by-institution
  POST /sources/datos-abiertos/search/by-author
  GET  /sources/datos-abiertos/records
  GET  /sources/datos-abiertos/records/{id}
"""

import logging
from typing import Optional, List, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.routers.sources._base import (
    SearchByInstitutionRequest,
    SearchByAuthorRequest,
    SearchResult,
    SourceRecordSummary,
)
from db.models import DatosAbiertosRecord
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("api.sources.datos_abiertos")
router = APIRouter(prefix="/datos-abiertos", tags=["Fuentes · Datos Abiertos"])


# ─────────────────────────────────────────────────────────────
# RESPONSE SCHEMA ESPECÍFICO DE DATOS ABIERTOS
# ─────────────────────────────────────────────────────────────

class DatosAbiertosRecordDetail(BaseModel):
    """Todos los campos de un registro Datos Abiertos almacenado."""
    # ── Identificadores
    id:               int
    datos_source_id:  Optional[str] = None
    dataset_id:       Optional[str] = None
    doi:              Optional[str] = None
    isbn:             Optional[str] = None
    # ── Tipo y metadatos
    product_type:     Optional[str] = None
    title:            Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    publication_type: Optional[str] = None
    language:         Optional[str] = None
    source_journal:   Optional[str] = None
    issn:             Optional[str] = None
    # ── Ubicación
    volume:           Optional[str] = None
    issue:            Optional[str] = None
    pages:            Optional[str] = None
    editorial:        Optional[str] = None
    # ── Cobertura geográfica
    country:          Optional[str] = None
    city:             Optional[str] = None
    # ── Clasificación Minciencias
    classification:   Optional[str] = None
    visibility:       Optional[str] = None
    # ── Contexto institucional
    research_group:   Optional[str] = None
    # ── Open Access
    is_open_access:   Optional[bool] = None
    # ── Métricas
    citation_count:   int = 0
    # ── Reconciliación
    status:           str = "pending"
    match_type:       Optional[str] = None
    match_score:      Optional[float] = None
    canonical_publication_id: Optional[int] = None
    # ── Timestamps
    created_at:       Any = None
    updated_at:       Any = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────
# POST /search/by-institution
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-institution",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de Datos Abiertos por institución",
)
def search_by_institution(
    body: SearchByInstitutionRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae registros del dataset de Datos Abiertos Colombia filtrados
    por nombre de institución y los almacena en `datos_abiertos_records`.
    """
    from extractors.datos_abiertos import DatosAbiertosExtractor

    if not body.institution_name and not body.ror_id:
        raise HTTPException(
            400,
            "Se requiere institution_name para buscar en Datos Abiertos.",
        )

    logger.info(f"Datos Abiertos · búsqueda por institución: '{body.institution_name}'")
    try:
        extractor = DatosAbiertosExtractor()
        records = extractor.extract(
            institution=body.institution_name,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        )
    except Exception as e:
        logger.error(f"Error extrayendo Datos Abiertos: {e}")
        raise HTTPException(502, f"Error al contactar Datos Abiertos: {e}")

    return _ingest(records, db, "by-institution")


# ─────────────────────────────────────────────────────────────
# POST /search/by-author
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-author",
    response_model=SearchResult,
    summary="Buscar publicaciones de Datos Abiertos por nombre de autor",
)
def search_by_author(
    body: SearchByAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae registros de Datos Abiertos filtrando por nombre del investigador.
    Datos Abiertos no tiene IDs de autor — la búsqueda es por nombre.
    """
    from extractors.datos_abiertos import DatosAbiertosExtractor

    if not body.author_name:
        raise HTTPException(
            400,
            "Datos Abiertos solo soporta búsqueda por author_name (no tiene IDs de autor).",
        )

    logger.info(f"Datos Abiertos · búsqueda por autor: '{body.author_name}'")
    try:
        extractor = DatosAbiertosExtractor()
        records = extractor.extract_by_author(
            author_name=body.author_name,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        )
    except Exception as e:
        logger.error(f"Error extrayendo Datos Abiertos por autor: {e}")
        raise HTTPException(502, f"Error al contactar Datos Abiertos: {e}")

    return _ingest(records, db, "by-author")


# ─────────────────────────────────────────────────────────────
# GET /records
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records",
    response_model=List[SourceRecordSummary],
    summary="Listar registros almacenados de Datos Abiertos",
)
def list_records(
    status:         Optional[str] = Query(None),
    classification: Optional[str] = Query(None, description="Filtrar por clasificación: A1, A2, B, C, D"),
    year_from:      Optional[int] = Query(None),
    year_to:        Optional[int] = Query(None),
    page:           int = Query(1, ge=1),
    page_size:      int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(DatosAbiertosRecord)
    if status:
        q = q.filter(DatosAbiertosRecord.status == status)
    if classification:
        q = q.filter(DatosAbiertosRecord.classification == classification)
    if year_from:
        q = q.filter(DatosAbiertosRecord.publication_year >= year_from)
    if year_to:
        q = q.filter(DatosAbiertosRecord.publication_year <= year_to)
    return q.order_by(DatosAbiertosRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()


# ─────────────────────────────────────────────────────────────
# GET /records/{id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records/{record_id}",
    response_model=DatosAbiertosRecordDetail,
    summary="Detalle de un registro Datos Abiertos",
)
def get_record(record_id: int, db: Session = Depends(get_db)):
    record = db.get(DatosAbiertosRecord, record_id)
    if not record:
        raise HTTPException(404, "Registro Datos Abiertos no encontrado.")
    return record


# ─────────────────────────────────────────────────────────────
# HELPER PRIVADO
# ─────────────────────────────────────────────────────────────

def _ingest(records, db: Session, context: str) -> SearchResult:
    if not records:
        return SearchResult(
            source="datos_abiertos",
            inserted=0, skipped=0, errors=0,
            message="La búsqueda no devolvió resultados.",
        )
    try:
        engine   = ReconciliationEngine(session=db)
        inserted = engine.ingest_records(records)
        skipped  = len(records) - inserted
        logger.info(f"Datos Abiertos [{context}]: {inserted} insertados, {skipped} omitidos.")
        return SearchResult(
            source="datos_abiertos",
            inserted=inserted,
            skipped=max(skipped, 0),
            errors=0,
            message=f"{inserted} registros nuevos almacenados en datos_abiertos_records.",
        )
    except Exception as e:
        logger.error(f"Error en ingesta Datos Abiertos: {e}")
        raise HTTPException(500, f"Error al almacenar registros: {e}")
