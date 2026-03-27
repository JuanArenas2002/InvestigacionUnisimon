"""
Router: OpenAlex

Endpoints independientes para buscar y almacenar registros de OpenAlex
sin disparar la reconciliación. La reconciliación es un paso separado.

Rutas:
  POST /sources/openalex/search/by-institution
  POST /sources/openalex/search/by-author
  GET  /sources/openalex/records
  GET  /sources/openalex/records/{id}
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
from config import institution as default_institution
from db.models import OpenalexRecord
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("api.sources.openalex")
router = APIRouter(prefix="/openalex", tags=["Fuentes · OpenAlex"])


# ─────────────────────────────────────────────────────────────
# RESPONSE SCHEMA ESPECÍFICO DE OPENALEX
# ─────────────────────────────────────────────────────────────

class OpenAlexRecordDetail(BaseModel):
    """Todos los campos de un registro OpenAlex almacenado."""
    # ── Identificadores
    id:                     int
    openalex_work_id:       Optional[str] = None
    doi:                    Optional[str] = None
    pmid:                   Optional[str] = None
    pmcid:                  Optional[str] = None
    # ── Metadatos
    title:                  Optional[str] = None
    publication_year:       Optional[int] = None
    publication_date:       Optional[str] = None
    publication_type:       Optional[str] = None
    language:               Optional[str] = None
    source_journal:         Optional[str] = None
    issn:                   Optional[str] = None
    # ── Contenido
    abstract:               Optional[str] = None
    keywords:               Optional[str] = None
    # ── Clasificación temática
    concepts:               Optional[Any] = None
    topics:                 Optional[Any] = None
    mesh_terms:             Optional[Any] = None
    # ── Open Access detallado
    is_open_access:         Optional[bool] = None
    oa_status:              Optional[str] = None
    oa_url:                 Optional[str] = None
    pdf_url:                Optional[str] = None
    license:                Optional[str] = None
    # ── Métricas
    citation_count:         int = 0
    referenced_works_count: Optional[int] = None
    apc_paid_usd:           Optional[int] = None
    # ── Financiación
    grants:                 Optional[Any] = None
    # ── Reconciliación
    status:                 str = "pending"
    match_type:             Optional[str] = None
    match_score:            Optional[float] = None
    canonical_publication_id: Optional[int] = None
    # ── Timestamps
    created_at:             Any = None
    updated_at:             Any = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────
# POST /search/by-institution
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-institution",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de OpenAlex por institución",
)
def search_by_institution(
    body: SearchByInstitutionRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae publicaciones de OpenAlex para una institución (por ROR ID)
    y las almacena en `openalex_records` con status='pending'.

    La reconciliación con `canonical_publications` es un paso separado.
    """
    from extractors.openalex.extractor import OpenAlexExtractor

    ror = body.ror_id or default_institution.ror_id
    if not ror:
        raise HTTPException(400, "Se requiere ror_id o configura ROR_ID en el entorno.")

    logger.info(f"OpenAlex · búsqueda por institución: ror={ror}, años={body.year_from}-{body.year_to}")
    try:
        extractor = OpenAlexExtractor()
        records = extractor.extract(
            ror_id=ror,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        )
    except Exception as e:
        logger.error(f"Error extrayendo OpenAlex: {e}")
        raise HTTPException(502, f"Error al contactar OpenAlex: {e}")

    return _ingest(records, db, "by-institution")


# ─────────────────────────────────────────────────────────────
# POST /search/by-author
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-author",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de OpenAlex por autor",
)
def search_by_author(
    body: SearchByAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae publicaciones de OpenAlex para un autor específico.
    Acepta ORCID o OpenAlex Author ID (source_author_id).
    """
    from extractors.openalex.extractor import OpenAlexExtractor

    orcid      = body.orcid
    author_id  = body.source_author_id  # ej: A123456789 o https://openalex.org/A123456789

    if not orcid and not author_id:
        raise HTTPException(400, "Se requiere orcid o source_author_id (OpenAlex Author ID).")

    logger.info(f"OpenAlex · búsqueda por autor: orcid={orcid}, author_id={author_id}")
    try:
        extractor = OpenAlexExtractor()
        records = extractor.extract_by_author(
            orcid=orcid,
            author_id=author_id,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        )
    except Exception as e:
        logger.error(f"Error extrayendo OpenAlex por autor: {e}")
        raise HTTPException(502, f"Error al contactar OpenAlex: {e}")

    return _ingest(records, db, "by-author")


# ─────────────────────────────────────────────────────────────
# GET /records
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records",
    response_model=List[SourceRecordSummary],
    summary="Listar registros almacenados de OpenAlex",
)
def list_records(
    status:       Optional[str] = Query(None, description="Filtrar por estado: pending, matched, new_canonical, manual_review, rejected"),
    year_from:    Optional[int] = Query(None),
    year_to:      Optional[int] = Query(None),
    page:         int           = Query(1, ge=1),
    page_size:    int           = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista los registros de OpenAlex almacenados, con filtros y paginación."""
    q = db.query(OpenalexRecord)
    if status:
        q = q.filter(OpenalexRecord.status == status)
    if year_from:
        q = q.filter(OpenalexRecord.publication_year >= year_from)
    if year_to:
        q = q.filter(OpenalexRecord.publication_year <= year_to)
    q = q.order_by(OpenalexRecord.id.desc())
    return q.offset((page - 1) * page_size).limit(page_size).all()


# ─────────────────────────────────────────────────────────────
# GET /records/{id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records/{record_id}",
    response_model=OpenAlexRecordDetail,
    summary="Detalle de un registro OpenAlex",
)
def get_record(record_id: int, db: Session = Depends(get_db)):
    """Devuelve todos los campos de un registro específico de OpenAlex."""
    record = db.get(OpenalexRecord, record_id)
    if not record:
        raise HTTPException(404, "Registro OpenAlex no encontrado.")
    return record


# ─────────────────────────────────────────────────────────────
# HELPER PRIVADO
# ─────────────────────────────────────────────────────────────

def _ingest(records, db: Session, context: str) -> SearchResult:
    """Ingesta los StandardRecords en la tabla openalex_records (sin reconciliar)."""
    if not records:
        return SearchResult(
            source="openalex",
            inserted=0, skipped=0, errors=0,
            message="La búsqueda no devolvió resultados.",
        )
    try:
        engine   = ReconciliationEngine(session=db)
        inserted = engine.ingest_records(records)
        skipped  = len(records) - inserted
        logger.info(f"OpenAlex [{context}]: {inserted} insertados, {skipped} omitidos.")
        return SearchResult(
            source="openalex",
            inserted=inserted,
            skipped=max(skipped, 0),
            errors=0,
            message=f"{inserted} registros nuevos almacenados en openalex_records.",
        )
    except Exception as e:
        logger.error(f"Error en ingesta OpenAlex: {e}")
        raise HTTPException(500, f"Error al almacenar registros: {e}")
