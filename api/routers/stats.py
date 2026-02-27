"""
Router de Estadísticas y datos para Dashboards.
Provee KPIs, métricas de calidad, timelines.
Consulta las 5 tablas de fuente de forma unificada.
"""

import logging
from typing import Optional, List
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, or_, text
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
    Author,
    PublicationAuthor,
    ReconciliationLog,
    Journal,
    Institution,
    SOURCE_MODELS,
    count_all_source_records,
    count_source_records_by_status,
    count_source_records_by_source,
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
    ext = count_all_source_records(db)
    authors = db.query(func.count(Author.id)).scalar() or 0
    journals = db.query(func.count(Journal.id)).scalar() or 0
    institutions = db.query(func.count(Institution.id)).scalar() or 0
    logs = db.query(func.count(ReconciliationLog.id)).scalar() or 0

    status_counts = count_source_records_by_status(db)
    pending = status_counts.get("pending", 0)
    review = status_counts.get("manual_review", 0)
    matched = status_counts.get("matched", 0) + status_counts.get("new_canonical", 0)

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
    total_ext = count_all_source_records(db)
    total_authors = db.query(func.count(Author.id)).scalar() or 0
    inst_authors = db.query(func.count(Author.id)).filter(
        Author.is_institutional == True
    ).scalar() or 0

    status_counts = count_source_records_by_status(db)
    source_counts = count_source_records_by_source(db)

    # Multi-source: publicaciones con registros de más de 1 fuente
    # Contar cuántas canonicals tienen registros en > 1 tabla de fuente
    canon_sources = {}  # canonical_id → set of source_names
    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(model_cls.canonical_publication_id)
            .filter(model_cls.canonical_publication_id.isnot(None))
            .distinct()
            .all()
        )
        for (cpid,) in rows:
            if cpid not in canon_sources:
                canon_sources[cpid] = set()
            canon_sources[cpid].add(source_name)

    multi_source = sum(1 for sources in canon_sources.values() if len(sources) > 1)
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

@router.get(
    "/reconciliation-timeline",
    response_model=List[ReconciliationTimelineItem],
    summary="Línea temporal de reconciliación",
)
def reconciliation_timeline(db: Session = Depends(get_db)):
    """Reconciliaciones por fecha y estado (cross-source)."""
    from collections import Counter

    # date_status → count
    agg = Counter()

    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(
                func.date(model_cls.reconciled_at).label("date"),
                model_cls.status,
                func.count(model_cls.id),
            )
            .filter(model_cls.reconciled_at.isnot(None))
            .group_by(func.date(model_cls.reconciled_at), model_cls.status)
            .all()
        )
        for date_val, status_val, cnt in rows:
            agg[(str(date_val) if date_val else "", status_val)] += cnt

    result = [
        ReconciliationTimelineItem(date=d, status=s, count=c)
        for (d, s), c in sorted(agg.items())
    ]
    return result


# ── GET /stats/year-source-matrix ────────────────────────────

@router.get(
    "/year-source-matrix",
    response_model=List[YearSourceItem],
    summary="Matriz año-fuente",
)
def year_source_matrix(db: Session = Depends(get_db)):
    """Registros por año y fuente para heatmap."""
    result = []
    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(
                model_cls.publication_year,
                func.count(model_cls.id),
            )
            .filter(model_cls.publication_year.isnot(None))
            .group_by(model_cls.publication_year)
            .order_by(model_cls.publication_year)
            .all()
        )
        for year_val, cnt in rows:
            result.append(YearSourceItem(
                source_name=source_name, year=year_val, count=cnt,
            ))

    result.sort(key=lambda x: (x.year or 0, x.source_name))
    return result


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

    # Conteos cross-source
    status_counts = count_source_records_by_status(db)
    pending = status_counts.get("pending", 0)
    review = status_counts.get("manual_review", 0)

    # Registros sin título (cross-source)
    ext_no_title = 0
    for model_cls in SOURCE_MODELS.values():
        ext_no_title += (
            db.query(func.count(model_cls.id))
            .filter(or_(model_cls.title.is_(None), model_cls.title == ""))
            .scalar() or 0
        )

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

@router.get(
    "/quality/{category}",
    response_model=List[QualityProblemDetail],
    summary="Detalle de problemas por categoría",
)
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
