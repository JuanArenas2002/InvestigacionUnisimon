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
    StatusSummaryItem,
    ResetRejectedRequest,
    ResetRejectedResponse,
    ActionQueueResponse,
    ActionGroup,
    ActionItem,
    BulkResolveRequest,
    BulkResolveResponse,
    PromoteToCanonicalResponse,
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


# ── GET /external-records/status-summary ─────────────────────

@router.get(
    "/status-summary",
    response_model=List[StatusSummaryItem],
    summary="Resumen de estados de reconciliación",
)
def status_summary(db: Session = Depends(get_db)):
    """
    Devuelve el total de registros por estado (pending, matched,
    new_canonical, manual_review, rejected) desglosado por fuente.
    """
    # Acumular conteos: {status → {source → count}}
    agg: dict = {}

    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(model_cls.status, func.count(model_cls.id))
            .group_by(model_cls.status)
            .all()
        )
        for status_val, cnt in rows:
            if status_val not in agg:
                agg[status_val] = {}
            agg[status_val][source_name] = cnt

    # Ordenar estados de más accionable a menos
    order = ["pending", "manual_review", "rejected", "new_canonical", "matched"]
    all_statuses = sorted(agg.keys(), key=lambda s: order.index(s) if s in order else 99)

    return [
        StatusSummaryItem(
            status=status_val,
            total=sum(agg[status_val].values()),
            by_source=agg[status_val],
        )
        for status_val in all_statuses
    ]


# ── POST /external-records/reset-rejected ────────────────────

@router.post(
    "/reset-rejected",
    response_model=ResetRejectedResponse,
    summary="Resetear registros rechazados a pending",
)
def reset_rejected(body: ResetRejectedRequest, db: Session = Depends(get_db)):
    """
    Mueve registros con `status='rejected'` de vuelta a `pending` para
    que se reprocesen en la próxima reconciliación.

    Casos de uso:
    - Después de actualizar la blacklist: resetear `match_type='invalid_title_blacklisted'`
    - Después de ampliar `min_title_length`: resetear `match_type='invalid_title_too_short'`
    - Sin filtro: resetear todos los rechazados (úsalo con cuidado)

    Parámetros:
    - `match_type`: tipo de rechazo a resetear (opcional)
    - `source`: limitar a una fuente (opcional)
    - `dry_run`: si True, solo cuenta sin modificar
    """
    sources_to_process = (
        {body.source: SOURCE_MODELS[body.source]}
        if body.source and body.source in SOURCE_MODELS
        else SOURCE_MODELS
    )
    if body.source and body.source not in SOURCE_MODELS:
        from fastapi import HTTPException
        raise HTTPException(400, f"Fuente inválida: {body.source}. Válidas: {list(SOURCE_MODELS)}")

    reset_counts: dict = {}

    for source_name, model_cls in sources_to_process.items():
        q = db.query(model_cls).filter(model_cls.status == "rejected")
        if body.match_type:
            q = q.filter(model_cls.match_type == body.match_type)

        records = q.all()
        reset_counts[source_name] = len(records)

        if not body.dry_run:
            for r in records:
                r.status = "pending"
                r.match_type = None
                r.match_score = None
                r.reconciled_at = None

    if not body.dry_run:
        db.commit()

    total = sum(reset_counts.values())
    if not body.dry_run and total > 0:
        logger.info(
            f"reset-rejected: {total} registros → pending "
            f"(match_type={body.match_type!r}, source={body.source!r})"
        )

    return ResetRejectedResponse(
        dry_run=body.dry_run,
        match_type_filter=body.match_type,
        source_filter=body.source,
        reset=reset_counts,
        total_reset=total,
    )


# ── GET /external-records/action-queue ───────────────────────

@router.get(
    "/action-queue",
    response_model=ActionQueueResponse,
    summary="Cola de acciones pendientes de reconciliación",
)
def action_queue(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Lista todos los registros que requieren intervención humana,
    agrupados por estado y con instrucciones claras de qué hacer.

    **Grupos devueltos (en orden de prioridad):**

    - `manual_review`: el motor encontró un candidato pero no estaba seguro.
      Acción: revisar y confirmar el vínculo o rechazar.
    - `pending`: no procesados aún.
      Acción: ejecutar `POST /api/pipeline/reconcile`.
    - `rejected`: descartados por título inválido.
      Acción: si fue un falso positivo, usar `POST /api/external-records/reset-rejected`.
    """
    # ── Cargar candidatos sugeridos desde reconciliation_log (bulk) ──
    # Una sola query para todos los logs de manual_review
    log_by_record: dict = {}  # (source_name, source_record_id) → log
    logs = (
        db.query(ReconciliationLog)
        .filter(ReconciliationLog.action == "flagged_review")  # engine guarda "flagged_review"
        .order_by(ReconciliationLog.created_at.desc())
        .all()
    )
    for log in logs:
        key = (log.source_name, log.source_record_id)
        if key not in log_by_record:
            log_by_record[key] = log

    # Cargar canónicos referenciados en bulk
    canon_ids = {lg.canonical_publication_id for lg in log_by_record.values() if lg.canonical_publication_id}
    canons_by_id: dict = {}
    if canon_ids:
        canons_by_id = {
            c.id: c
            for c in db.query(CanonicalPublication).filter(CanonicalPublication.id.in_(canon_ids)).all()
        }

    groups: List[ActionGroup] = []

    # ── Grupo 1: manual_review ────────────────────────────────
    review_items: List[ActionItem] = []
    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(model_cls)
            .filter(model_cls.status == "manual_review")
            .order_by(model_cls.match_score.desc().nullslast())
            .limit(limit)
            .all()
        )
        for r in rows:
            log = log_by_record.get((source_name, r.id))
            canon = canons_by_id.get(log.canonical_publication_id) if log else None

            if canon:
                recommended = "link_suggested"
                hint = (
                    f"Vincular a canónico #{canon.id} '{(canon.title or '')[:80]}' "
                    f"(score {(r.match_score or 0)*100:.0f}%). "
                    f"PATCH /api/external-records/{source_name}/{r.id}/resolve "
                    f'con {{"action":"link","canonical_id":{canon.id}}}'
                )
            else:
                recommended = "create_new_canonical"
                hint = (
                    f"Sin candidato claro. Resetear a pending para re-reconciliar: "
                    f"POST /api/external-records/bulk-resolve "
                    f'con {{"action":"reset_pending","source":"{source_name}"}}'
                )

            review_items.append(ActionItem(
                id=r.id,
                source_name=source_name,
                title=r.title,
                doi=r.doi,
                publication_year=r.publication_year,
                match_score=r.match_score,
                match_type=r.match_type,
                suggested_canonical_id=canon.id if canon else None,
                suggested_canonical_title=canon.title if canon else None,
                suggested_canonical_doi=canon.doi if canon else None,
                recommended_action=recommended,
                resolve_hint=hint,
            ))

    review_items.sort(key=lambda x: x.match_score or 0.0, reverse=True)
    if review_items:
        groups.append(ActionGroup(
            status="manual_review",
            count=len(review_items),
            description=(
                "El motor encontró un canónico candidato pero el score está en zona gris (85-95%). "
                "Revisa cada par y usa bulk-resolve para aceptar en masa los de score alto."
            ),
            bulk_action_available=True,
            items=review_items[:limit],
        ))

    # ── Grupo 2: pending ──────────────────────────────────────
    pending_count = sum(
        db.query(model_cls).filter(model_cls.status == "pending").count()
        for model_cls in SOURCE_MODELS.values()
    )
    if pending_count:
        groups.append(ActionGroup(
            status="pending",
            count=pending_count,
            description=(
                "Registros sin procesar. "
                "Ejecuta POST /api/pipeline/reconcile para procesarlos."
            ),
            bulk_action_available=False,
            items=[],  # no listamos pending uno a uno (pueden ser miles)
        ))

    # ── Grupo 3: rejected ─────────────────────────────────────
    rejected_items: List[ActionItem] = []
    for source_name, model_cls in SOURCE_MODELS.items():
        rows = (
            db.query(model_cls)
            .filter(model_cls.status == "rejected")
            .limit(limit)
            .all()
        )
        for r in rows:
            hint = (
                f"Si fue un falso positivo: POST /api/external-records/reset-rejected "
                f'con {{"match_type":"{r.match_type}","source":"{source_name}"}}'
            )
            rejected_items.append(ActionItem(
                id=r.id,
                source_name=source_name,
                title=r.title,
                doi=r.doi,
                publication_year=r.publication_year,
                match_score=r.match_score,
                match_type=r.match_type,
                recommended_action="review_rejection",
                resolve_hint=hint,
            ))

    if rejected_items:
        groups.append(ActionGroup(
            status="rejected",
            count=len(rejected_items),
            description=(
                "Registros descartados por título inválido o en lista negra. "
                "Si fueron falsos positivos, usa reset-rejected para reprocesarlos."
            ),
            bulk_action_available=True,
            items=rejected_items,
        ))

    total = sum(g.count for g in groups)
    return ActionQueueResponse(total_pending_action=total, groups=groups)


# ── POST /external-records/bulk-resolve ──────────────────────

@router.post(
    "/bulk-resolve",
    response_model=BulkResolveResponse,
    summary="Resolver en masa registros en revisión manual",
)
def bulk_resolve(body: BulkResolveRequest, db: Session = Depends(get_db)):
    """
    Resuelve en masa los registros en `manual_review`.

    **Acciones disponibles:**

    - `link_suggested`: vincula cada registro al canónico que el motor sugirió,
      si el score ≥ `min_score` (default 90%). Los que no superen el umbral se saltan.
    - `reject_all`: marca todos como rechazados.
    - `reset_pending`: devuelve a `pending` para que la próxima reconciliación
      los procese de nuevo (útil tras mejoras al motor).

    Usa `dry_run: true` primero para ver cuántos afecta sin modificar nada.
    """
    from datetime import timezone as _tz

    sources_to_process = (
        {body.source: SOURCE_MODELS[body.source]}
        if body.source and body.source in SOURCE_MODELS
        else SOURCE_MODELS
    )
    if body.source and body.source not in SOURCE_MODELS:
        from fastapi import HTTPException as _HTTPException
        raise _HTTPException(400, f"Fuente inválida: {body.source}. Válidas: {list(SOURCE_MODELS)}")

    # Pre-cargar los logs de manual_review para saber el candidato sugerido
    logs = (
        db.query(ReconciliationLog)
        .filter(ReconciliationLog.action == "flagged_review")  # engine guarda "flagged_review"
        .order_by(ReconciliationLog.created_at.desc())
        .all()
    )
    # Último log por (source, record_id)
    log_by_record: dict = {}
    for lg in logs:
        key = (lg.source_name, lg.source_record_id)
        if key not in log_by_record:
            log_by_record[key] = lg

    linked = rejected = reset = skipped = 0
    now = datetime.now(_tz.utc)

    for source_name, model_cls in sources_to_process.items():
        records = (
            db.query(model_cls)
            .filter(model_cls.status == "manual_review")
            .all()
        )

        for r in records:
            if body.action == "link_suggested":
                log = log_by_record.get((source_name, r.id))
                score = r.match_score or 0.0
                if not log or not log.canonical_publication_id or score < body.min_score:
                    skipped += 1
                    continue
                if not body.dry_run:
                    r.status = "matched"
                    r.canonical_publication_id = log.canonical_publication_id
                    r.match_type = "bulk_resolved"
                    r.reconciled_at = now
                    db.add(ReconciliationLog(
                        source_name=source_name,
                        source_record_id=r.id,
                        canonical_publication_id=log.canonical_publication_id,
                        match_type="bulk_resolved",
                        match_score=score,
                        action="linked_existing",
                    ))
                linked += 1

            elif body.action == "reject_all":
                if not body.dry_run:
                    r.status = "rejected"
                    r.reconciled_at = now
                    db.add(ReconciliationLog(
                        source_name=source_name,
                        source_record_id=r.id,
                        match_type="bulk_resolved",
                        match_score=r.match_score,
                        action="rejected",
                    ))
                rejected += 1

            elif body.action == "reset_pending":
                if not body.dry_run:
                    r.status = "pending"
                    r.match_type = None
                    r.match_score = None
                    r.reconciled_at = None
                reset += 1

    if not body.dry_run:
        db.commit()
        logger.info(
            f"bulk-resolve action={body.action!r} source={body.source!r}: "
            f"linked={linked} rejected={rejected} reset={reset} skipped={skipped}"
        )

    total = linked + rejected + reset
    return BulkResolveResponse(
        dry_run=body.dry_run,
        action=body.action,
        source_filter=body.source,
        min_score=body.min_score if body.action == "link_suggested" else None,
        linked=linked,
        rejected=rejected,
        reset=reset,
        skipped=skipped,
        total_affected=total,
    )


# ── POST /external-records/{source}/{record_id}/promote ──────

@router.post(
    "/{source}/{record_id}/promote",
    response_model=PromoteToCanonicalResponse,
    summary="Promover registro a nueva publicación canónica",
)
def promote_to_canonical(
    source: str,
    record_id: int,
    db: Session = Depends(get_db),
):
    """
    Crea una **nueva publicación canónica** a partir de un registro de fuente
    en estado `manual_review` o `pending`, y lo vincula a ella.

    Útil cuando:
    - El motor sugirió un candidato incorrecto (conflicto de DOI).
    - El registro es realmente una publicación nueva sin canónico aún.

    El canónico se construye con todos los campos disponibles en el registro
    de fuente. Si ya existe un canónico con el mismo DOI, retorna error 409
    para evitar duplicados — en ese caso usa el endpoint `/resolve` para
    vincularlo al existente.
    """
    from extractors.base import normalize_text, normalize_doi
    from sqlalchemy.exc import IntegrityError as _IntegrityError
    from datetime import timezone as _tz

    er = _find_record_by_source_and_id(db, source, record_id)
    if not er:
        raise HTTPException(404, f"Registro {source}:{record_id} no encontrado")
    if er.status not in ("manual_review", "pending"):
        raise HTTPException(
            400,
            f"Solo se pueden promover registros en 'manual_review' o 'pending'. "
            f"Estado actual: '{er.status}'"
        )

    title = (er.title or "").strip()
    if not title:
        raise HTTPException(422, "El registro no tiene título — no se puede crear un canónico.")

    ndoi = normalize_doi(er.doi) if er.doi else None

    # Verificar que no exista ya un canónico con ese DOI
    if ndoi:
        existing = db.query(CanonicalPublication).filter_by(doi=ndoi).first()
        if existing:
            raise HTTPException(
                409,
                f"Ya existe el canónico #{existing.id} con DOI {ndoi}. "
                f"Usa PATCH /api/external-records/{source}/{record_id}/resolve "
                f'con {{"action":"link","canonical_id":{existing.id}}} para vincularlo.'
            )

    # Construir el canónico con todos los campos disponibles del registro
    prov = {f: source for f in ("title", "publication_year", "publication_type",
                                 "source_journal", "issn", "language", "is_open_access",
                                 "citation_count", "publication_date")
            if getattr(er, f, None) not in (None, "", 0)}
    if ndoi:
        prov["doi"] = source

    canon = CanonicalPublication(
        doi=ndoi,
        title=title,
        normalized_title=normalize_text(title),
        publication_year=getattr(er, "publication_year", None),
        publication_date=getattr(er, "publication_date", None),
        publication_type=getattr(er, "publication_type", None),
        source_journal=getattr(er, "source_journal", None),
        issn=getattr(er, "issn", None),
        language=getattr(er, "language", None),
        is_open_access=getattr(er, "is_open_access", None),
        oa_status=getattr(er, "oa_status", None),
        citation_count=getattr(er, "citation_count", 0) or 0,
        sources_count=1,
        field_provenance=prov,
        field_conflicts={},
    )

    try:
        db.add(canon)
        db.flush()
    except _IntegrityError:
        db.rollback()
        raise HTTPException(409, f"Conflicto al crear el canónico (posible DOI duplicado: {ndoi})")

    # Vincular el registro de fuente al nuevo canónico
    er.canonical_publication_id = canon.id
    er.status = "new_canonical"
    er.match_type = "promoted"
    er.match_score = 100.0
    er.reconciled_at = datetime.now(_tz.utc)

    db.add(ReconciliationLog(
        source_name=source,
        source_record_id=er.id,
        canonical_publication_id=canon.id,
        match_type="promoted",
        match_score=100.0,
        match_details={"method": "manual_promote"},
        action="created_new",
    ))

    db.commit()
    logger.info(f"promote: {source}:{record_id} → nuevo canónico #{canon.id} '{title[:80]}'")

    return PromoteToCanonicalResponse(
        source_name=source,
        source_record_id=record_id,
        canonical_id=canon.id,
        title=title,
        doi=ndoi,
        message=f"Canónico #{canon.id} creado y vinculado correctamente.",
    )


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
