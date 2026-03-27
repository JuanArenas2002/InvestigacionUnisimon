"""
Router de Registros de Fuentes y Reconciliación.
Consultas unificadas cross-source, logs, revisión manual.

Con la nueva arquitectura de tablas por fuente, este router
consulta las 5 tablas (openalex_records, scopus_records, etc.)
y presenta resultados unificados.
"""

import logging
from typing import Optional, List
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.common import PaginatedResponse, MessageResponse
from api.schemas.external_records import (
    ExternalRecordRead,
    ExternalRecordDetail,
    SourceStatusCount,
    MatchTypeDistribution,
    ManualReviewItem,
    ResolveReviewRequest,
    ReconciliationLogRead,
)
from db.models import (
    CanonicalPublication,
    ReconciliationLog,
    SOURCE_MODELS,
    get_source_model,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/external-records", tags=["Registros Externos"])


# ── helpers ──────────────────────────────────────────────────

def _record_to_read(record) -> ExternalRecordRead:
    """Convierte cualquier modelo de fuente a ExternalRecordRead."""
    return ExternalRecordRead(
        id=record.id,
        source_name=record.source_name,
        source_id=record.source_id,
        doi=record.doi,
        title=record.title,
        publication_year=record.publication_year,
        authors_text=record.authors_text,
        status=record.status,
        canonical_publication_id=record.canonical_publication_id,
        match_type=record.match_type,
        match_score=record.match_score,
        reconciled_at=record.reconciled_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _record_to_detail(record) -> ExternalRecordDetail:
    """Convierte cualquier modelo de fuente a ExternalRecordDetail."""
    return ExternalRecordDetail(
        id=record.id,
        source_name=record.source_name,
        source_id=record.source_id,
        doi=record.doi,
        title=record.title,
        publication_year=record.publication_year,
        authors_text=record.authors_text,
        status=record.status,
        canonical_publication_id=record.canonical_publication_id,
        match_type=record.match_type,
        match_score=record.match_score,
        reconciled_at=record.reconciled_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
        raw_data=record.raw_data,
        normalized_title=record.normalized_title,
        normalized_authors=record.normalized_authors,
    )


def _find_record_by_source_and_id(db: Session, source_name: str, record_id: int):
    """Busca un registro en la tabla de fuente correspondiente."""
    model_cls = SOURCE_MODELS.get(source_name)
    if not model_cls:
        return None
    return db.get(model_cls, record_id)


def _find_record_across_sources(db: Session, record_id: int, source_name: str = None):
    """
    Busca un registro por ID. Si source_name se da, busca solo en esa tabla.
    Si no, busca en todas las tablas (menos eficiente).
    """
    if source_name:
        return _find_record_by_source_and_id(db, source_name, record_id)

    for model_cls in SOURCE_MODELS.values():
        record = db.get(model_cls, record_id)
        if record:
            return record
    return None


# ── GET /external-records ────────────────────────────────────

@router.get("", response_model=PaginatedResponse[ExternalRecordRead], summary="Listar registros de fuentes")
def list_external_records(
    source: Optional[str] = Query(None, description="Filtrar por fuente (openalex, scopus, etc.)"),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Lista paginada de registros de todas las fuentes.
    Si se especifica `source`, consulta directamente esa tabla.
    """
    offset = (page - 1) * page_size

    if source:
        # Consulta directa a la tabla de esa fuente
        model_cls = SOURCE_MODELS.get(source)
        if not model_cls:
            raise HTTPException(400, f"Fuente inválida: {source}. Válidas: {list(SOURCE_MODELS.keys())}")

        q = db.query(model_cls)
        if status:
            q = q.filter(model_cls.status == status)
        if search:
            q = q.filter(model_cls.title.ilike(f"%{search}%"))

        total = q.count()
        items = q.order_by(model_cls.created_at.desc()).offset(offset).limit(page_size).all()
        result = [_record_to_read(r) for r in items]

    else:
        # Consulta cross-source: recoger de todas las tablas
        all_records = []
        total = 0
        for model_cls in SOURCE_MODELS.values():
            q = db.query(model_cls)
            if status:
                q = q.filter(model_cls.status == status)
            if search:
                q = q.filter(model_cls.title.ilike(f"%{search}%"))
            total += q.count()
            all_records.extend(q.all())

        # Ordenar por created_at desc
        all_records.sort(key=lambda r: r.created_at, reverse=True)
        page_items = all_records[offset:offset + page_size]
        result = [_record_to_read(r) for r in page_items]

    return PaginatedResponse.create(
        items=result, total=total, page=page, page_size=page_size,
    )


# ── GET /external-records/by-source-status ───────────────────

@router.get("/by-source-status", response_model=List[SourceStatusCount], summary="Conteo por fuente y estado")
def source_status_counts(db: Session = Depends(get_db)):
    """Conteos agrupados por fuente × estado."""
    result = []
    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(model_cls.status, func.count(model_cls.id))
            .group_by(model_cls.status)
            .all()
        )
        for status_val, count_val in rows:
            result.append(SourceStatusCount(
                source_name=source_name, status=status_val, count=count_val,
            ))
    return result


# ── GET /external-records/match-types ────────────────────────

@router.get("/match-types", response_model=List[MatchTypeDistribution], summary="Distribución de tipos de coincidencia")
def match_type_distribution(db: Session = Depends(get_db)):
    """Distribución de tipos de match con score promedio."""
    from collections import defaultdict

    agg = defaultdict(lambda: {"count": 0, "score_sum": 0.0, "score_cnt": 0})

    for model_cls in SOURCE_MODELS.values():
        rows = (
            db.query(
                model_cls.match_type,
                func.count(model_cls.id),
                func.avg(model_cls.match_score),
            )
            .filter(model_cls.match_type.isnot(None))
            .group_by(model_cls.match_type)
            .all()
        )
        for mt, cnt, avg_sc in rows:
            agg[mt]["count"] += cnt
            if avg_sc is not None:
                agg[mt]["score_sum"] += avg_sc * cnt
                agg[mt]["score_cnt"] += cnt

    return [
        MatchTypeDistribution(
            match_type=mt,
            count=vals["count"],
            avg_score=(
                round(vals["score_sum"] / vals["score_cnt"], 2)
                if vals["score_cnt"] > 0 else None
            ),
        )
        for mt, vals in agg.items()
    ]


# ── GET /external-records/manual-review ──────────────────────

@router.get("/manual-review", response_model=List[ManualReviewItem], summary="Registros en revisión manual")
def manual_review_records(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Registros pendientes de revisión manual con candidato canónico."""
    all_review = []

    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(model_cls)
            .filter(model_cls.status == "manual_review")
            .order_by(model_cls.match_score.desc().nullslast())
            .limit(limit)
            .all()
        )
        all_review.extend(rows)

    # Ordenar por score desc
    all_review.sort(key=lambda r: r.match_score or 0.0, reverse=True)
    all_review = all_review[:limit]

    result = []
    for er in all_review:
        candidate_title = None
        candidate_id = None

        # Buscar candidato en reconciliation_log
        log = (
            db.query(ReconciliationLog)
            .filter(
                ReconciliationLog.source_name == er.source_name,
                ReconciliationLog.source_record_id == er.id,
            )
            .order_by(ReconciliationLog.created_at.desc())
            .first()
        )
        if log and log.canonical_publication_id:
            candidate_id = log.canonical_publication_id
            canon = db.get(CanonicalPublication, log.canonical_publication_id)
            if canon:
                candidate_title = canon.title

        result.append(ManualReviewItem(
            id=er.id,
            source_name=er.source_name,
            title=er.title,
            doi=er.doi,
            publication_year=er.publication_year,
            match_score=er.match_score,
            candidate_title=candidate_title,
            candidate_id=candidate_id,
        ))

    return result


# ── GET /reconciliation-log ──────────────────────────────────

@router.get("/reconciliation-log", response_model=List[ReconciliationLogRead], summary="Log de reconciliación")
def recent_reconciliation_logs(
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Últimas N decisiones del motor de reconciliación."""
    rows = (
        db.query(ReconciliationLog)
        .order_by(ReconciliationLog.created_at.desc())
        .limit(limit)
        .all()
    )
    return [ReconciliationLogRead.model_validate(r) for r in rows]


# ── Rutas con parámetro al final ─────────────────────────────

# ── GET /external-records/{source}/{record_id} ───────────────

@router.get(
    "/{source}/{record_id}",
    response_model=ExternalRecordDetail,
    summary="Detalle de registro de fuente",
)
def get_external_record(
    source: str,
    record_id: int,
    db: Session = Depends(get_db),
):
    """Detalle completo de un registro (incluye raw_data). Requiere source + id."""
    record = _find_record_by_source_and_id(db, source, record_id)
    if not record:
        raise HTTPException(404, f"Registro {source}:{record_id} no encontrado")
    return _record_to_detail(record)


# ── PATCH /external-records/{source}/{record_id}/resolve ─────

@router.patch(
    "/{source}/{record_id}/resolve",
    response_model=MessageResponse,
    summary="Resolver revisión manual",
)
def resolve_manual_review(
    source: str,
    record_id: int,
    body: ResolveReviewRequest,
    db: Session = Depends(get_db),
):
    """Resolver un registro en revisión manual (vincular a canónico o rechazar)."""
    er = _find_record_by_source_and_id(db, source, record_id)
    if not er:
        raise HTTPException(404, f"Registro {source}:{record_id} no encontrado")
    if er.status != "manual_review":
        raise HTTPException(400, f"El registro tiene status '{er.status}', no 'manual_review'")

    if body.action == "link":
        if not body.canonical_id:
            raise HTTPException(400, "Se requiere canonical_id para la acción 'link'")
        canon = db.get(CanonicalPublication, body.canonical_id)
        if not canon:
            raise HTTPException(404, f"Publicación canónica {body.canonical_id} no encontrada")

        er.status = "matched"
        er.canonical_publication_id = body.canonical_id
        er.match_type = "manual_resolved"
        er.reconciled_at = datetime.now(timezone.utc)

        log = ReconciliationLog(
            source_name=source,
            source_record_id=er.id,
            canonical_publication_id=body.canonical_id,
            match_type="manual_resolved",
            match_score=er.match_score,
            action="linked_existing",
        )
        db.add(log)
        db.commit()
        return MessageResponse(message=f"Registro {source}:{record_id} vinculado a canónico {body.canonical_id}")

    elif body.action == "reject":
        er.status = "rejected"
        er.reconciled_at = datetime.now(timezone.utc)

        log = ReconciliationLog(
            source_name=source,
            source_record_id=er.id,
            match_type="manual_resolved",
            match_score=er.match_score,
            action="rejected",
        )
        db.add(log)
        db.commit()
        return MessageResponse(message=f"Registro {source}:{record_id} rechazado")

    raise HTTPException(400, f"Acción desconocida: {body.action}")
