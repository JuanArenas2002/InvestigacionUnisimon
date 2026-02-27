"""
Router de Scopus Insights.
Dashboard completo sobre registros, contribuciones y cobertura de Scopus.
Ahora usa directamente la tabla scopus_records.
"""

import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, case, or_, and_, text
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.scopus import (
    ScopusInsightsResponse,
    ScopusRecordSummary,
    ScopusCoverageVsTotal,
    ScopusFieldContribution,
    ScopusAuthorStats,
    ScopusCitationStats,
    ScopusTopJournal,
    ScopusYearDistribution,
    ScopusEnrichedPublicationSample,
)
from api.schemas.common import PaginatedResponse
from api.schemas.external_records import ExternalRecordRead, ExternalRecordDetail
from api.utils import build_source_url
from db.models import (
    CanonicalPublication,
    ScopusRecord,
    Author,
    PublicationAuthor,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/scopus", tags=["Scopus"])


def _scopus_to_read(er: ScopusRecord) -> ExternalRecordRead:
    """Convierte ScopusRecord a ExternalRecordRead."""
    return ExternalRecordRead(
        id=er.id,
        source_name=er.source_name,
        source_id=er.source_id,
        doi=er.doi,
        title=er.title,
        publication_year=er.publication_year,
        authors_text=er.authors_text,
        status=er.status,
        canonical_publication_id=er.canonical_publication_id,
        match_type=er.match_type,
        match_score=er.match_score,
        reconciled_at=er.reconciled_at,
        created_at=er.created_at,
        updated_at=er.updated_at,
        source_url=build_source_url("scopus", er.scopus_doc_id, er.doi),
    )


def _scopus_to_detail(er: ScopusRecord) -> ExternalRecordDetail:
    """Convierte ScopusRecord a ExternalRecordDetail."""
    return ExternalRecordDetail(
        id=er.id,
        source_name=er.source_name,
        source_id=er.source_id,
        doi=er.doi,
        title=er.title,
        publication_year=er.publication_year,
        authors_text=er.authors_text,
        status=er.status,
        canonical_publication_id=er.canonical_publication_id,
        match_type=er.match_type,
        match_score=er.match_score,
        reconciled_at=er.reconciled_at,
        created_at=er.created_at,
        updated_at=er.updated_at,
        source_url=build_source_url("scopus", er.scopus_doc_id, er.doi),
        raw_data=er.raw_data,
        normalized_title=er.normalized_title,
        normalized_authors=er.normalized_authors,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/insights — Dashboard completo
# ══════════════════════════════════════════════════════════════

@router.get(
    "/insights",
    response_model=ScopusInsightsResponse,
    summary="Dashboard completo de Scopus",
)
def scopus_insights(db: Session = Depends(get_db)):
    """
    Retorna **todo** lo que tiene que ver con Scopus en un solo endpoint.
    """
    records = _build_record_summary(db)
    coverage = _build_coverage(db)
    field_contributions = _build_field_contributions(db)
    authors = _build_author_stats(db)
    citations = _build_citation_stats(db)
    top_journals = _build_top_journals(db)
    year_dist = _build_year_distribution(db)
    samples = _build_enrichment_samples(db)

    return ScopusInsightsResponse(
        records=records,
        coverage=coverage,
        field_contributions=field_contributions,
        authors=authors,
        citations=citations,
        top_journals=top_journals,
        year_distribution=year_dist,
        enrichment_samples=samples,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/records — Registros Scopus paginados
# ══════════════════════════════════════════════════════════════

@router.get(
    "/records",
    response_model=PaginatedResponse[ExternalRecordRead],
    summary="Listar registros de Scopus",
)
def list_scopus_records(
    status: Optional[str] = Query(None, description="Filtrar por estado"),
    search: Optional[str] = Query(None, description="Buscar en título o DOI"),
    year: Optional[int] = Query(None, description="Filtrar por año"),
    found_only: bool = Query(False, description="Excluir placeholders (no encontrados)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista paginada de registros de Scopus con filtros."""
    q = db.query(ScopusRecord)

    if status:
        q = q.filter(ScopusRecord.status == status)
    if search:
        term = f"%{search}%"
        q = q.filter(
            or_(
                ScopusRecord.title.ilike(term),
                ScopusRecord.doi.ilike(term),
            )
        )
    if year:
        q = q.filter(ScopusRecord.publication_year == year)
    if found_only:
        q = q.filter(
            ~ScopusRecord.scopus_doc_id.like("not-found-%")
        )

    total = q.count()
    items = (
        q.order_by(ScopusRecord.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[_scopus_to_read(er) for er in items],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/records/{id} — Detalle de un registro Scopus
# ══════════════════════════════════════════════════════════════

@router.get(
    "/records/{record_id}",
    response_model=ExternalRecordDetail,
    summary="Detalle de un registro Scopus",
)
def get_scopus_record(record_id: int, db: Session = Depends(get_db)):
    """Detalle completo de un registro Scopus (incluye raw_data)."""
    er = db.query(ScopusRecord).get(record_id)
    if not er:
        raise HTTPException(404, "Registro Scopus no encontrado")
    return _scopus_to_detail(er)


# ══════════════════════════════════════════════════════════════
# GET /scopus/not-found — DOIs no encontrados en Scopus
# ══════════════════════════════════════════════════════════════

@router.get(
    "/not-found",
    response_model=PaginatedResponse[ExternalRecordRead],
    summary="DOIs no encontrados en Scopus",
)
def scopus_not_found(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Lista de DOIs que se buscaron en Scopus pero **no se encontraron**.
    Son los registros placeholder con scopus_doc_id='not-found-{doi}'.
    """
    q = db.query(ScopusRecord).filter(
        ScopusRecord.scopus_doc_id.like("not-found-%"),
    )

    total = q.count()
    items = (
        q.order_by(ScopusRecord.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[_scopus_to_read(er) for er in items],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/enriched-fields
# ══════════════════════════════════════════════════════════════

@router.get(
    "/enriched-fields",
    summary="Publicaciones donde Scopus aportó un campo específico",
)
def scopus_enriched_by_field(
    field: str = Query(
        ...,
        description="Campo a consultar (doi, source_journal, issn, citation_count, publication_type, is_open_access, publication_date)",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Retorna las publicaciones canónicas cuyo campo indicado
    fue aportado por Scopus según `field_provenance`.
    """
    q = (
        db.query(CanonicalPublication)
        .filter(
            CanonicalPublication.field_provenance.isnot(None),
            text(f"field_provenance->>'{field}' = 'scopus'"),
        )
    )

    total = q.count()
    items = (
        q.order_by(CanonicalPublication.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[
            {
                "id": p.id,
                "doi": p.doi,
                "title": p.title,
                "publication_year": p.publication_year,
                field: getattr(p, field, None),
                "field_provenance": p.field_provenance,
            }
            for p in items
        ],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/authors — Autores con Scopus ID
# ══════════════════════════════════════════════════════════════

@router.get(
    "/authors",
    summary="Autores con Scopus Author ID",
)
def scopus_authors(
    only_scopus: bool = Query(False, description="Solo autores cuyo único ID externo es Scopus"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista autores que tienen Scopus Author ID."""
    q = db.query(Author).filter(
        Author.scopus_id.isnot(None),
        Author.scopus_id != "",
    )

    if only_scopus:
        q = q.filter(
            or_(Author.orcid.is_(None), Author.orcid == ""),
            or_(Author.openalex_id.is_(None), Author.openalex_id == ""),
        )

    total = q.count()
    items = (
        q.order_by(Author.name.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[
            {
                "id": a.id,
                "name": a.name,
                "scopus_id": a.scopus_id,
                "orcid": a.orcid,
                "openalex_id": a.openalex_id,
                "is_institutional": a.is_institutional,
                "scopus_profile_url": f"https://www.scopus.com/authid/detail.uri?authorId={a.scopus_id}",
            }
            for a in items
        ],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# FUNCIONES INTERNAS (helpers para /insights)
# ══════════════════════════════════════════════════════════════

def _build_record_summary(db: Session) -> ScopusRecordSummary:
    """Conteos de registros Scopus por estado."""
    total = db.query(func.count(ScopusRecord.id)).scalar() or 0

    statuses = (
        db.query(ScopusRecord.status, func.count(ScopusRecord.id))
        .group_by(ScopusRecord.status)
        .all()
    )
    status_map = {s: c for s, c in statuses}

    not_found = db.query(func.count(ScopusRecord.id)).filter(
        ScopusRecord.scopus_doc_id.like("not-found-%"),
    ).scalar() or 0

    return ScopusRecordSummary(
        total=total,
        matched=status_map.get("matched", 0),
        new_canonical=status_map.get("new_canonical", 0),
        pending=status_map.get("pending", 0),
        manual_review=status_map.get("manual_review", 0),
        rejected=status_map.get("rejected", 0),
        not_found_placeholders=not_found,
    )


def _build_coverage(db: Session) -> ScopusCoverageVsTotal:
    """Cobertura de Scopus respecto al inventario."""
    total_canon = db.query(func.count(CanonicalPublication.id)).scalar() or 0

    with_scopus = (
        db.query(func.count(func.distinct(ScopusRecord.canonical_publication_id)))
        .filter(
            ScopusRecord.canonical_publication_id.isnot(None),
            ~ScopusRecord.scopus_doc_id.like("not-found-%"),
        )
        .scalar() or 0
    )

    pct = round(with_scopus / total_canon * 100, 1) if total_canon else 0.0

    only_scopus = (
        db.query(func.count(CanonicalPublication.id))
        .filter(
            CanonicalPublication.sources_count == 1,
            CanonicalPublication.id.in_(
                db.query(ScopusRecord.canonical_publication_id)
                .filter(
                    ScopusRecord.canonical_publication_id.isnot(None),
                    ~ScopusRecord.scopus_doc_id.like("not-found-%"),
                )
            ),
        )
        .scalar() or 0
    )

    multi = with_scopus - only_scopus if with_scopus > only_scopus else 0

    return ScopusCoverageVsTotal(
        total_canonical=total_canon,
        with_scopus_record=with_scopus,
        pct_coverage=pct,
        only_in_scopus=only_scopus,
        multi_source_with_scopus=multi,
    )


def _build_field_contributions(db: Session) -> List[ScopusFieldContribution]:
    """Cuántos campos de las canónicas fueron aportados por Scopus."""
    fields_to_check = [
        "doi", "title", "publication_year", "source_journal", "issn",
        "publication_type", "is_open_access", "citation_count",
        "publication_date",
    ]

    total_canon = db.query(func.count(CanonicalPublication.id)).scalar() or 0
    results = []

    for field in fields_to_check:
        count = (
            db.query(func.count(CanonicalPublication.id))
            .filter(
                CanonicalPublication.field_provenance.isnot(None),
                text(f"field_provenance->>'{field}' = 'scopus'"),
            )
            .scalar() or 0
        )
        pct = round(count / total_canon * 100, 1) if total_canon else 0.0
        results.append(ScopusFieldContribution(
            field=field, count=count, percentage=pct,
        ))

    results.sort(key=lambda x: x.count, reverse=True)
    return results


def _build_author_stats(db: Session) -> ScopusAuthorStats:
    """Estadísticas de autores con Scopus ID."""
    total = db.query(func.count(Author.id)).scalar() or 0

    with_sid = db.query(func.count(Author.id)).filter(
        Author.scopus_id.isnot(None), Author.scopus_id != ""
    ).scalar() or 0

    only_scopus = db.query(func.count(Author.id)).filter(
        Author.scopus_id.isnot(None), Author.scopus_id != "",
        or_(Author.orcid.is_(None), Author.orcid == ""),
        or_(Author.openalex_id.is_(None), Author.openalex_id == ""),
    ).scalar() or 0

    pct = round(with_sid / total * 100, 1) if total else 0.0

    return ScopusAuthorStats(
        total_authors=total,
        with_scopus_id=with_sid,
        pct_with_scopus_id=pct,
        only_scopus=only_scopus,
    )


def _build_citation_stats(db: Session) -> ScopusCitationStats:
    """Métricas de citas donde Scopus es la fuente."""
    q = (
        db.query(CanonicalPublication)
        .filter(
            CanonicalPublication.field_provenance.isnot(None),
            text("field_provenance->>'citation_count' = 'scopus'"),
            CanonicalPublication.citation_count > 0,
        )
    )

    pubs = q.all()
    count = len(pubs)

    if count == 0:
        return ScopusCitationStats()

    total_cites = sum(p.citation_count for p in pubs)
    top = max(pubs, key=lambda p: p.citation_count)

    return ScopusCitationStats(
        publications_with_citations_from_scopus=count,
        total_citations_from_scopus=total_cites,
        max_citation_count=top.citation_count,
        max_citation_doi=top.doi,
        max_citation_title=top.title[:200] if top.title else None,
        avg_citations=round(total_cites / count, 1),
    )


def _build_top_journals(db: Session, limit: int = 20) -> List[ScopusTopJournal]:
    """Revistas más frecuentes en registros Scopus (columna tipada)."""
    rows = (
        db.query(
            ScopusRecord.source_journal,
            func.count(ScopusRecord.id),
        )
        .filter(
            ~ScopusRecord.scopus_doc_id.like("not-found-%"),
            ScopusRecord.source_journal.isnot(None),
            ScopusRecord.source_journal != "",
        )
        .group_by(ScopusRecord.source_journal)
        .order_by(func.count(ScopusRecord.id).desc())
        .limit(limit)
        .all()
    )
    return [ScopusTopJournal(journal_name=r[0], count=r[1]) for r in rows if r[0]]


def _build_year_distribution(db: Session) -> List[ScopusYearDistribution]:
    """Registros Scopus por año de publicación."""
    rows = (
        db.query(
            ScopusRecord.publication_year,
            func.count(ScopusRecord.id),
        )
        .filter(
            ScopusRecord.publication_year.isnot(None),
            ~ScopusRecord.scopus_doc_id.like("not-found-%"),
        )
        .group_by(ScopusRecord.publication_year)
        .order_by(ScopusRecord.publication_year)
        .all()
    )
    return [ScopusYearDistribution(year=r[0], count=r[1]) for r in rows]


def _build_enrichment_samples(db: Session, limit: int = 10) -> List[ScopusEnrichedPublicationSample]:
    """Ejemplos de publicaciones donde Scopus aportó campos."""
    pubs = (
        db.query(CanonicalPublication)
        .filter(
            CanonicalPublication.field_provenance.isnot(None),
            text("field_provenance::text LIKE '%scopus%'"),
        )
        .limit(limit)
        .all()
    )

    samples = []
    for p in pubs:
        prov = p.field_provenance or {}
        fields = [k for k, v in prov.items() if v == "scopus"]
        if fields:
            samples.append(ScopusEnrichedPublicationSample(
                canonical_id=p.id,
                doi=p.doi,
                title=p.title[:200] if p.title else "",
                fields_from_scopus=fields,
            ))
    return samples
