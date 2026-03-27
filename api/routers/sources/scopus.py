"""
Router: Scopus

Endpoints independientes para buscar y almacenar registros de Scopus
sin disparar la reconciliación.

Rutas:
  POST /sources/scopus/search/by-institution
  POST /sources/scopus/search/by-author
  GET  /sources/scopus/records
  GET  /sources/scopus/records/{id}
"""

import logging
from typing import Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.routers.sources._base import (
    SearchByInstitutionRequest,
    SearchByAuthorRequest,
    SearchResult,
)
from api.schemas.common import PaginatedResponse
from config import scopus_config
from db.models import ScopusRecord
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("api.sources.scopus")
router = APIRouter(prefix="/scopus", tags=["Fuentes · Scopus"])


# ─────────────────────────────────────────────────────────────
# RESPONSE SCHEMA ESPECÍFICO DE SCOPUS
# ─────────────────────────────────────────────────────────────

class ScopusRecordDetail(BaseModel):
    """Todos los campos de un registro Scopus almacenado."""
    # ── Identificadores
    id:                  int
    scopus_doc_id:       Optional[str] = None
    eid:                 Optional[str] = None
    doi:                 Optional[str] = None
    pmid:                Optional[str] = None
    isbn:                Optional[str] = None
    # ── Metadatos
    title:               Optional[str] = None
    publication_year:    Optional[int] = None
    publication_date:    Optional[str] = None
    publication_type:    Optional[str] = None
    subtype_description: Optional[str] = None
    language:            Optional[str] = None
    source_journal:      Optional[str] = None
    issn:                Optional[str] = None
    # ── Ubicación
    volume:              Optional[str] = None
    issue:               Optional[str] = None
    page_range:          Optional[str] = None
    # ── Contenido
    abstract:            Optional[str] = None
    author_keywords:     Optional[str] = None
    index_keywords:      Optional[str] = None
    # ── Evento
    conference_name:     Optional[str] = None
    # ── Open Access
    is_open_access:      Optional[bool] = None
    oa_status:           Optional[str] = None
    # ── Métricas
    citation_count:      int = 0
    # ── Financiación
    funding_agency:      Optional[str] = None
    funding_number:      Optional[str] = None
    # ── Reconciliación
    status:              str = "pending"
    match_type:          Optional[str] = None
    match_score:         Optional[float] = None
    canonical_publication_id: Optional[int] = None
    # ── Timestamps
    created_at:          Any = None
    updated_at:          Any = None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────
# POST /search/by-institution
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-institution",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de Scopus por institución",
)
def search_by_institution(
    body: SearchByInstitutionRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae publicaciones de Scopus por ID de afiliación (o nombre de institución)
    y las almacena en `scopus_records` con status='pending'.
    """
    from extractors.scopus import ScopusExtractor

    # Resolver IDs de afiliación: del body o del config
    raw_ids = body.affiliation_id or ",".join(scopus_config.affiliation_ids or [])
    if not raw_ids:
        raise HTTPException(
            400,
            "Se requiere affiliation_id o configura SCOPUS_AFFILIATION_IDS en el entorno.",
        )

    # Construir query Scopus: AF-ID(x) OR AF-ID(y) + filtro de año
    id_parts = [f"AF-ID({aid.strip()})" for aid in raw_ids.split(",") if aid.strip()]
    query = " OR ".join(id_parts)
    if body.year_from:
        query += f" AND PUBYEAR > {body.year_from - 1}"
    if body.year_to:
        query += f" AND PUBYEAR < {body.year_to + 1}"

    logger.info(f"Scopus · búsqueda por institución: query={query!r}")
    try:
        extractor = ScopusExtractor()
        records = extractor.extract(query=query, max_results=body.max_results)
    except Exception as e:
        logger.error(f"Error extrayendo Scopus: {e}")
        raise HTTPException(502, f"Error al contactar Scopus: {e}")

    return _ingest(records, db, "by-institution")


# ─────────────────────────────────────────────────────────────
# POST /search/by-author
# ─────────────────────────────────────────────────────────────

@router.post(
    "/search/by-author",
    response_model=SearchResult,
    summary="Buscar y descargar publicaciones de Scopus por autor",
)
def search_by_author(
    body: SearchByAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Extrae publicaciones de Scopus para un autor específico.
    Acepta ORCID o Scopus Author ID (source_author_id = AU-ID).
    """
    from extractors.scopus import ScopusExtractor

    if not body.orcid and not body.source_author_id:
        raise HTTPException(400, "Se requiere orcid o source_author_id (Scopus AU-ID).")

    # Construir query Scopus con filtro de autor + rango de año
    if body.orcid:
        query = f"ORCID({body.orcid})"
        label = f"orcid={body.orcid}"
    else:
        query = f"AU-ID({body.source_author_id})"
        label = f"au_id={body.source_author_id}"

    if body.year_from:
        query += f" AND PUBYEAR > {body.year_from - 1}"
    if body.year_to:
        query += f" AND PUBYEAR < {body.year_to + 1}"

    logger.info(f"Scopus · búsqueda por autor ({label}): query={query!r}")
    try:
        extractor = ScopusExtractor()
        records = extractor.extract(query=query, max_results=body.max_results)
    except Exception as e:
        logger.error(f"Error extrayendo Scopus por autor: {e}")
        raise HTTPException(502, f"Error al contactar Scopus: {e}")

    return _ingest(records, db, "by-author")


# ─────────────────────────────────────────────────────────────
# GET /records
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records",
    response_model=PaginatedResponse[ScopusRecordDetail],
    summary="Listar registros almacenados de Scopus",
)
def list_records(
    status:     Optional[str] = Query(None, description="Filtrar por estado (pending, matched, new_canonical…)"),
    search:     Optional[str] = Query(None, description="Buscar en título o DOI"),
    year_from:  Optional[int] = Query(None),
    year_to:    Optional[int] = Query(None),
    found_only: bool          = Query(False, description="Excluir placeholders (not-found-*)"),
    page:       int           = Query(1, ge=1),
    page_size:  int           = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(ScopusRecord)
    if status:
        q = q.filter(ScopusRecord.status == status)
    if search:
        term = f"%{search}%"
        q = q.filter(or_(ScopusRecord.title.ilike(term), ScopusRecord.doi.ilike(term)))
    if year_from:
        q = q.filter(ScopusRecord.publication_year >= year_from)
    if year_to:
        q = q.filter(ScopusRecord.publication_year <= year_to)
    if found_only:
        q = q.filter(~ScopusRecord.scopus_doc_id.like("not-found-%"))

    total = q.count()
    items = q.order_by(ScopusRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return PaginatedResponse.create(items=items, total=total, page=page, page_size=page_size)


# ─────────────────────────────────────────────────────────────
# GET /records/{id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/records/{record_id}",
    response_model=ScopusRecordDetail,
    summary="Detalle de un registro Scopus",
)
def get_record(record_id: int, db: Session = Depends(get_db)):
    record = db.get(ScopusRecord, record_id)
    if not record:
        raise HTTPException(404, "Registro Scopus no encontrado.")
    return record


# ─────────────────────────────────────────────────────────────
# HELPER PRIVADO
# ─────────────────────────────────────────────────────────────

def _ingest(records, db: Session, context: str) -> SearchResult:
    if not records:
        return SearchResult(
            source="scopus",
            inserted=0, skipped=0, errors=0,
            message="La búsqueda no devolvió resultados.",
        )
    try:
        engine   = ReconciliationEngine(session=db)
        inserted = engine.ingest_records(records)
        skipped  = len(records) - inserted
        logger.info(f"Scopus [{context}]: {inserted} insertados, {skipped} omitidos.")
        return SearchResult(
            source="scopus",
            inserted=inserted,
            skipped=max(skipped, 0),
            errors=0,
            message=f"{inserted} registros nuevos almacenados en scopus_records.",
        )
    except Exception as e:
        logger.error(f"Error en ingesta Scopus: {e}")
        raise HTTPException(500, f"Error al almacenar registros: {e}")
