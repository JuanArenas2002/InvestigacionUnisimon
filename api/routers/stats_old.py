"""
Router de Estadísticas y datos para Dashboards.
Provee KPIs, métricas de calidad, timelines.
"""

import logging
from typing import Optional, List
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, or_, and_, text, case
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.stats import (
    SystemStats,
    OverviewStats,
    ReconciliationTimelineItem,
    YearSourceItem,
    QualityProblemsOverview,
    QualityProblemDetail,
    HealthResponse,
    JsonFileInfo,
)
from config import DATA_DIR
from db.models import (
    CanonicalPublication,
    ExternalRecord,
    Author,
    PublicationAuthor,
    ReconciliationLog,
    Journal,
    Institution,
)
from db.session import check_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stats", tags=["Estadísticas"])


# ── GET /health ──────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse, summary="Estado de salud")
def health_check():
    """Verifica conexión a la base de datos."""
    try:
        ok = check_connection()
        return HealthResponse(
            status="ok" if ok else "error",
            database=ok,
            message="Conexión exitosa" if ok else "No se pudo conectar a PostgreSQL",
        )
    except Exception as e:
        return HealthResponse(status="error", database=False, message=str(e))


# ── GET /stats/system ────────────────────────────────────────

@router.get("/system", response_model=SystemStats, summary="KPIs del sistema")
def system_stats(db: Session = Depends(get_db)):
    """KPIs rápidos del sistema (conteos de tablas + estados)."""
    canon = db.query(func.count(CanonicalPublication.id)).scalar() or 0
    ext = db.query(func.count(ExternalRecord.id)).scalar() or 0
    authors = db.query(func.count(Author.id)).scalar() or 0
    journals = db.query(func.count(Journal.id)).scalar() or 0
    institutions = db.query(func.count(Institution.id)).scalar() or 0
    logs = db.query(func.count(ReconciliationLog.id)).scalar() or 0

    pending = db.query(func.count(ExternalRecord.id)).filter(
        ExternalRecord.status == "pending"
    ).scalar() or 0
    review = db.query(func.count(ExternalRecord.id)).filter(
        ExternalRecord.status == "manual_review"
    ).scalar() or 0
    matched = db.query(func.count(ExternalRecord.id)).filter(
        ExternalRecord.status.in_(["matched", "new_canonical"])
    ).scalar() or 0

    return SystemStats(
        canonical_publications=canon,
        external_records=ext,
        authors=authors,
        journals=journals,
        institutions=institutions,
        reconciliation_log=logs,
        pending=pending,
        manual_review=review,
        matched=matched,
    )


# ── GET /stats/overview ─────────────────────────────────────

@router.get("/overview", response_model=OverviewStats, summary="Estadísticas generales")
def overview_stats(db: Session = Depends(get_db)):
    """Estadísticas panorámicas del inventario."""
    total_canon = db.query(func.count(CanonicalPublication.id)).scalar() or 0
    total_ext = db.query(func.count(ExternalRecord.id)).scalar() or 0
    total_authors = db.query(func.count(Author.id)).scalar() or 0
    inst_authors = db.query(func.count(Author.id)).filter(
        Author.is_institutional == True
    ).scalar() or 0

    # Status counts
    status_rows = (
        db.query(ExternalRecord.status, func.count(ExternalRecord.id))
        .group_by(ExternalRecord.status)
        .all()
    )
    status_counts = {r[0]: r[1] for r in status_rows}

    # Source counts
    source_rows = (
        db.query(ExternalRecord.source_name, func.count(ExternalRecord.id))
        .group_by(ExternalRecord.source_name)
        .all()
    )
    source_counts = {r[0]: r[1] for r in source_rows}

    # Multi-source: publicaciones con registros de más de 1 fuente
    multi_source = (
        db.query(func.count())
        .select_from(
            db.query(ExternalRecord.canonical_publication_id)
            .filter(ExternalRecord.canonical_publication_id.isnot(None))
            .group_by(ExternalRecord.canonical_publication_id)
            .having(func.count(func.distinct(ExternalRecord.source_name)) > 1)
            .subquery()
        )
    ).scalar() or 0

    cross_pct = round(multi_source / total_canon * 100, 1) if total_canon > 0 else 0.0

    return OverviewStats(
        total_canonical=total_canon,
        total_external=total_ext,
        total_authors=total_authors,
        institutional_authors=inst_authors,
        status_counts=status_counts,
        source_counts=source_counts,
        multi_source_count=multi_source,
        cross_source_pct=cross_pct,
    )


# ── GET /stats/reconciliation-timeline ───────────────────────

@router.get("/reconciliation-timeline", response_model=List[ReconciliationTimelineItem], summary="Línea temporal de reconciliación")
def reconciliation_timeline(db: Session = Depends(get_db)):
    """Reconciliaciones por fecha y estado."""
    rows = (
        db.query(
            func.date(ExternalRecord.reconciled_at).label("date"),
            ExternalRecord.status,
            func.count(ExternalRecord.id),
        )
        .filter(ExternalRecord.reconciled_at.isnot(None))
        .group_by(func.date(ExternalRecord.reconciled_at), ExternalRecord.status)
        .order_by(func.date(ExternalRecord.reconciled_at))
        .all()
    )
    return [
        ReconciliationTimelineItem(
            date=str(r[0]) if r[0] else "",
            status=r[1],
            count=r[2],
        )
        for r in rows
    ]


# ── GET /stats/year-source-matrix ────────────────────────────

@router.get("/year-source-matrix", response_model=List[YearSourceItem], summary="Matriz año-fuente")
def year_source_matrix(db: Session = Depends(get_db)):
    """Registros por año y fuente para heatmap."""
    rows = (
        db.query(
            ExternalRecord.source_name,
            ExternalRecord.publication_year,
            func.count(ExternalRecord.id),
        )
        .filter(ExternalRecord.publication_year.isnot(None))
        .group_by(ExternalRecord.source_name, ExternalRecord.publication_year)
        .order_by(ExternalRecord.publication_year, ExternalRecord.source_name)
        .all()
    )
    return [
        YearSourceItem(source_name=r[0], year=r[1], count=r[2])
        for r in rows
    ]


# ── GET /stats/quality ───────────────────────────────────────

@router.get("/quality", response_model=QualityProblemsOverview, summary="Resumen de calidad de datos")
def quality_overview(db: Session = Depends(get_db)):
    """Resumen de problemas de calidad de datos."""
    total = db.query(func.count(CanonicalPublication.id)).scalar() or 0

    missing_doi = db.query(func.count(CanonicalPublication.id)).filter(
        or_(CanonicalPublication.doi.is_(None), CanonicalPublication.doi == "")
    ).scalar() or 0

    missing_year = db.query(func.count(CanonicalPublication.id)).filter(
        CanonicalPublication.publication_year.is_(None)
    ).scalar() or 0

    missing_title = db.query(func.count(CanonicalPublication.id)).filter(
        or_(CanonicalPublication.title.is_(None), CanonicalPublication.title == "")
    ).scalar() or 0

    # Publicaciones sin autores vinculados
    pubs_with_authors = (
        db.query(PublicationAuthor.publication_id).distinct().subquery()
    )
    missing_authors = db.query(func.count(CanonicalPublication.id)).filter(
        ~CanonicalPublication.id.in_(db.query(pubs_with_authors))
    ).scalar() or 0

    pending = db.query(func.count(ExternalRecord.id)).filter(
        ExternalRecord.status == "pending"
    ).scalar() or 0

    review = db.query(func.count(ExternalRecord.id)).filter(
        ExternalRecord.status == "manual_review"
    ).scalar() or 0

    ext_no_title = db.query(func.count(ExternalRecord.id)).filter(
        or_(ExternalRecord.title.is_(None), ExternalRecord.title == "")
    ).scalar() or 0

    missing_orcid = db.query(func.count(Author.id)).filter(
        Author.is_institutional == True,
        or_(Author.orcid.is_(None), Author.orcid == ""),
    ).scalar() or 0

    return QualityProblemsOverview(
        total_canonical=total,
        missing_doi_count=missing_doi,
        missing_year_count=missing_year,
        missing_title_count=missing_title,
        missing_authors_count=missing_authors,
        pending_count=pending,
        manual_review_count=review,
        external_no_title_count=ext_no_title,
        missing_orcid_count=missing_orcid,
    )


# ── GET /stats/quality/{category} ────────────────────────────

@router.get("/quality/{category}", response_model=List[QualityProblemDetail], summary="Detalle de problemas por categoría")
def quality_problem_detail(
    category: str,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Registros de una categoría de problema de calidad."""
    if category == "missing-doi":
        rows = (
            db.query(CanonicalPublication)
            .filter(or_(CanonicalPublication.doi.is_(None), CanonicalPublication.doi == ""))
            .limit(limit).all()
        )
        return [QualityProblemDetail(
            id=r.id, title=r.title, doi=r.doi,
            publication_year=r.publication_year,
            source_journal=r.source_journal, category="missing_doi",
        ) for r in rows]

    elif category == "missing-year":
        rows = (
            db.query(CanonicalPublication)
            .filter(CanonicalPublication.publication_year.is_(None))
            .limit(limit).all()
        )
        return [QualityProblemDetail(
            id=r.id, title=r.title, doi=r.doi,
            publication_year=None,
            source_journal=r.source_journal, category="missing_year",
        ) for r in rows]

    elif category == "missing-title":
        rows = (
            db.query(CanonicalPublication)
            .filter(or_(CanonicalPublication.title.is_(None), CanonicalPublication.title == ""))
            .limit(limit).all()
        )
        return [QualityProblemDetail(
            id=r.id, title=r.title, doi=r.doi,
            publication_year=r.publication_year,
            source_journal=r.source_journal, category="missing_title",
        ) for r in rows]

    else:
        return []


# ── GET /stats/json-files ────────────────────────────────────

@router.get("/json-files", response_model=List[JsonFileInfo], summary="Archivos JSON disponibles")
def list_json_files():
    """Lista archivos JSON disponibles en el directorio de datos."""
    data_dir = Path(DATA_DIR)
    if not data_dir.exists():
        return []

    files = sorted(data_dir.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
    return [
        JsonFileInfo(
            filename=f.name,
            size_mb=round(f.stat().st_size / (1024 * 1024), 2),
            modified=datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
        )
        for f in files
    ]
