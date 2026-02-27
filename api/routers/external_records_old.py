"""
Router de Registros Externos y Reconciliación.
Gestión de external_records, logs, revisión manual.
"""

import logging
from typing import Optional, List
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.utils import build_source_url
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
    ExternalRecord,
    ReconciliationLog,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/external-records", tags=["Registros Externos"])


# ── GET /external-records ────────────────────────────────────

@router.get("", response_model=PaginatedResponse[ExternalRecordRead], summary="Listar registros externos")
def list_external_records(
    source: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista paginada de registros externos con filtros."""
    q = db.query(ExternalRecord)

    if source:
        q = q.filter(ExternalRecord.source_name == source)
    if status:
        q = q.filter(ExternalRecord.status == status)
    if search:
        term = f"%{search}%"
        q = q.filter(ExternalRecord.title.ilike(term))

    total = q.count()
    items = (
        q.order_by(ExternalRecord.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    result = []
    for er in items:
        r = ExternalRecordRead.model_validate(er)
        r.source_url = build_source_url(er.source_name, er.source_id, er.doi)
        result.append(r)

    return PaginatedResponse.create(
        items=result, total=total, page=page, page_size=page_size,
    )


# ── GET /external-records/by-source-status ───────────────────

@router.get("/by-source-status", response_model=List[SourceStatusCount], summary="Conteo por fuente y estado")
def source_status_counts(db: Session = Depends(get_db)):
    """Conteos agrupados por fuente × estado."""
    rows = (
        db.query(
            ExternalRecord.source_name,
            ExternalRecord.status,
            func.count(ExternalRecord.id),
        )
        .group_by(ExternalRecord.source_name, ExternalRecord.status)
        .all()
    )
    return [
        SourceStatusCount(source_name=r[0], status=r[1], count=r[2])
        for r in rows
    ]


# ── GET /external-records/match-types ────────────────────────

@router.get("/match-types", response_model=List[MatchTypeDistribution], summary="Distribución de tipos de coincidencia")
def match_type_distribution(db: Session = Depends(get_db)):
    """Distribución de tipos de match con score promedio."""
    rows = (
        db.query(
            ExternalRecord.match_type,
            func.count(ExternalRecord.id),
            func.avg(ExternalRecord.match_score),
        )
        .filter(ExternalRecord.match_type.isnot(None))
        .group_by(ExternalRecord.match_type)
        .all()
    )
    return [
        MatchTypeDistribution(
            match_type=r[0],
            count=r[1],
            avg_score=round(r[2], 2) if r[2] else None,
        )
        for r in rows
    ]


# ── GET /external-records/manual-review ──────────────────────

@router.get("/manual-review", response_model=List[ManualReviewItem], summary="Registros en revisión manual")
def manual_review_records(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Registros pendientes de revisión manual con candidato canónico."""
    rows = (
        db.query(ExternalRecord)
        .filter(ExternalRecord.status == "manual_review")
        .order_by(ExternalRecord.match_score.desc().nullslast())
        .limit(limit)
        .all()
    )

    result = []
    for er in rows:
        candidate_title = None
        candidate_id = None

        # Buscar el candidato en reconciliation_log
        log = (
            db.query(ReconciliationLog)
            .filter(ReconciliationLog.external_record_id == er.id)
            .order_by(ReconciliationLog.created_at.desc())
            .first()
        )
        if log and log.canonical_publication_id:
            candidate_id = log.canonical_publication_id
            canon = db.query(CanonicalPublication).get(log.canonical_publication_id)
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


# ── Rutas con parámetro {record_id} al final ─────────────────

# ── GET /external-records/{id} ───────────────────────────────

@router.get("/{record_id}", response_model=ExternalRecordDetail, summary="Detalle de registro externo")
def get_external_record(record_id: int, db: Session = Depends(get_db)):
    """Detalle completo de un registro externo (incluye raw_data)."""
    er = db.query(ExternalRecord).get(record_id)
    if not er:
        raise HTTPException(404, "Registro externo no encontrado")
    r = ExternalRecordDetail.model_validate(er)
    r.source_url = build_source_url(er.source_name, er.source_id, er.doi)
    return r


# ── PATCH /external-records/{id}/resolve ─────────────────────

@router.patch("/{record_id}/resolve", response_model=MessageResponse, summary="Resolver revisión manual")
def resolve_manual_review(
    record_id: int,
    body: ResolveReviewRequest,
    db: Session = Depends(get_db),
):
    """Resolver un registro en revisión manual (vincular a canónico o rechazar)."""
    er = db.query(ExternalRecord).get(record_id)
    if not er:
        raise HTTPException(404, "Registro externo no encontrado")
    if er.status != "manual_review":
        raise HTTPException(400, f"El registro tiene status '{er.status}', no 'manual_review'")

    if body.action == "link":
        if not body.canonical_id:
            raise HTTPException(400, "Se requiere canonical_id para la acción 'link'")
        canon = db.query(CanonicalPublication).get(body.canonical_id)
        if not canon:
            raise HTTPException(404, f"Publicación canónica {body.canonical_id} no encontrada")

        er.status = "matched"
        er.canonical_publication_id = body.canonical_id
        er.match_type = "manual_resolved"
        er.reconciled_at = datetime.now(timezone.utc)

        # Log
        log = ReconciliationLog(
            external_record_id=er.id,
            canonical_publication_id=body.canonical_id,
            match_type="manual_resolved",
            match_score=er.match_score,
            action="linked_existing",
        )
        db.add(log)
        db.commit()
        return MessageResponse(message=f"Registro {record_id} vinculado a canónico {body.canonical_id}")

    elif body.action == "reject":
        er.status = "rejected"
        er.reconciled_at = datetime.now(timezone.utc)

        log = ReconciliationLog(
            external_record_id=er.id,
            match_type="manual_resolved",
            match_score=er.match_score,
            action="rejected",
        )
        db.add(log)
        db.commit()
        return MessageResponse(message=f"Registro {record_id} rechazado")
