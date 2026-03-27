"""
Router: Web of Science (WoS)

Rutas:
  POST /sources/wos/search/by-institution
  POST /sources/wos/search/by-author
  GET  /sources/wos/records
  GET  /sources/wos/records/{id}
"""

import logging
from typing import Optional, List, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.routers.sources._base import (
    SearchByInstitutionRequest,
    SearchByAuthorRequest,
    SearchResult,
    SourceRecordSummary,
)
from db.models import WosRecord
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("api.sources.wos")
router = APIRouter(prefix="/wos", tags=["Fuentes · Web of Science"])


# ─────────────────────────────────────────────────────────────
# RESPONSE SCHEMA ESPECÍFICO DE WoS
# ─────────────────────────────────────────────────────────────

class WosRecordDetail(BaseModel):
    """Todos los campos de un registro Web of Science almacenado."""
    # ── Identificadores
    id:                        int
    wos_uid:                   Optional[str] = None
    accession_number:          Optional[str] = None
    doi:                       Optional[str] = None
    pmid:                      Optional[str] = None
    # ── Metadatos
    title:                     Optional[str] = None
    publication_year:          Optional[int] = None
    publication_date:          Optional[str] = None
    early_access_date:         Optional[str] = None
    publication_type:          Optional[str] = None
    language:                  Optional[str] = None
    source_journal:            Optional[str] = None
    issn:                      Optional[str] = None
    issn_electronic:           Optional[str] = None
    publisher:                 Optional[str] = None
    # ── Ubicación
    volume:                    Optional[str] = None
    issue:                     Optional[str] = None
    page_range:                Optional[str] = None
    # ── Contenido
    abstract:                  Optional[str] = None
    author_keywords:           Optional[str] = None
    # ── Clasificación WoS
    wos_categories:            Optional[str] = None
    research_areas:            Optional[str] = None
    # ── Evento
    conference_title:          Optional[str] = None
    # ── Open Access
    is_open_access:            Optional[bool] = None
    oa_status:                 Optional[str] = None
    # ── Métricas
    citation_count:            int = 0
    times_cited_all_databases: Optional[int] = None
    citing_patents_count:      Optional[int] = None
    # ── Financiación
    funding_orgs:              Optional[Any] = None
    # ── Reconciliación
    status:                    str = "pending"
    match_type:                Optional[str] = None
    match_score:               Optional[float] = None
    canonical_publication_id:  Optional[int] = None
    # ── Timestamps
    created_at:                Any = None
    updated_at:                Any = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────
# POST /search/by-institution
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-institution",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de WoS por institución",
)
def search_by_institution(
    body: SearchByInstitutionRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae publicaciones de Web of Science por nombre de institución o ROR ID
    y las almacena en `wos_records` con status='pending'.
    """
    from extractors.wos import WosExtractor

    institution_query = body.institution_name or body.ror_id
    if not institution_query:
        raise HTTPException(
            400,
            "Se requiere institution_name o ror_id para buscar en WoS.",
        )

    logger.info(f"WoS · búsqueda por institución: query='{institution_query}'")
    try:
        extractor = WosExtractor()
        records = extractor.extract(
            institution=institution_query,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        )
    except Exception as e:
        logger.error(f"Error extrayendo WoS: {e}")
        raise HTTPException(502, f"Error al contactar Web of Science: {e}")

    return _ingest(records, db, "by-institution")


# ─────────────────────────────────────────────────────────────
# POST /search/by-author
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-author",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de WoS por autor",
)
def search_by_author(
    body: SearchByAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae publicaciones de Web of Science para un autor.
    Acepta ORCID o WoS ResearcherID (source_author_id).
    """
    from extractors.wos import WosExtractor

    if not body.orcid and not body.source_author_id and not body.author_name:
        raise HTTPException(
            400,
            "Se requiere orcid, source_author_id (WoS ResearcherID) o author_name.",
        )

    logger.info(f"WoS · búsqueda por autor: orcid={body.orcid}, rid={body.source_author_id}")
    try:
        extractor = WosExtractor()
        records = extractor.extract_by_author(
            orcid=body.orcid,
            researcher_id=body.source_author_id,
            author_name=body.author_name,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        )
    except Exception as e:
        logger.error(f"Error extrayendo WoS por autor: {e}")
        raise HTTPException(502, f"Error al contactar Web of Science: {e}")

    return _ingest(records, db, "by-author")


# ─────────────────────────────────────────────────────────────
# GET /records
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records",
    response_model=List[SourceRecordSummary],
    summary="Listar registros almacenados de WoS",
)
def list_records(
    status:    Optional[str] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to:   Optional[int] = Query(None),
    page:      int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(WosRecord)
    if status:
        q = q.filter(WosRecord.status == status)
    if year_from:
        q = q.filter(WosRecord.publication_year >= year_from)
    if year_to:
        q = q.filter(WosRecord.publication_year <= year_to)
    return q.order_by(WosRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()


# ─────────────────────────────────────────────────────────────
# GET /records/{id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records/{record_id}",
    response_model=WosRecordDetail,
    summary="Detalle de un registro Web of Science",
)
def get_record(record_id: int, db: Session = Depends(get_db)):
    record = db.get(WosRecord, record_id)
    if not record:
        raise HTTPException(404, "Registro WoS no encontrado.")
    return record


# ─────────────────────────────────────────────────────────────
# HELPER PRIVADO
# ─────────────────────────────────────────────────────────────

def _ingest(records, db: Session, context: str) -> SearchResult:
    if not records:
        return SearchResult(
            source="wos",
            inserted=0, skipped=0, errors=0,
            message="La búsqueda no devolvió resultados.",
        )
    try:
        engine   = ReconciliationEngine(session=db)
        inserted = engine.ingest_records(records)
        skipped  = len(records) - inserted
        logger.info(f"WoS [{context}]: {inserted} insertados, {skipped} omitidos.")
        return SearchResult(
            source="wos",
            inserted=inserted,
            skipped=max(skipped, 0),
            errors=0,
            message=f"{inserted} registros nuevos almacenados en wos_records.",
        )
    except Exception as e:
        logger.error(f"Error en ingesta WoS: {e}")
        raise HTTPException(500, f"Error al almacenar registros: {e}")
