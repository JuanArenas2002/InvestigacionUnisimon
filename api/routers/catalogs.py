"""
Router de Catálogos: Revistas e Instituciones.
Gestión de las tablas normalizadas de referencia.
"""

import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.common import PaginatedResponse
from api.schemas.stats import (
    JournalRead,
    JournalCreate,
    InstitutionRead,
    InstitutionCreate,
)
from db.models import Journal, Institution

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/catalogs", tags=["Catálogos"])


# ── REVISTAS ─────────────────────────────────────────────────

@router.get("/journals", response_model=PaginatedResponse[JournalRead], summary="Listar revistas")
def list_journals(
    search: Optional[str] = Query(None, description="Buscar por nombre o ISSN"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista paginada de revistas registradas."""
    q = db.query(Journal)
    if search:
        term = f"%{search}%"
        q = q.filter(
            Journal.name.ilike(term) | Journal.issn.ilike(term)
        )
    total = q.count()
    items = (
        q.order_by(Journal.name)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return PaginatedResponse.create(
        items=[JournalRead.model_validate(j) for j in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/journals/{journal_id}", response_model=JournalRead, summary="Detalle de revista")
def get_journal(journal_id: int, db: Session = Depends(get_db)):
    """Obtiene una revista por su ID."""
    j = db.query(Journal).get(journal_id)
    if not j:
        raise HTTPException(404, "Revista no encontrada")
    return JournalRead.model_validate(j)


@router.post("/journals", response_model=JournalRead, summary="Crear revista", status_code=201)
def create_journal(body: JournalCreate, db: Session = Depends(get_db)):
    """Crea una nueva revista. Si ya existe una con el mismo ISSN, devuelve error."""
    if body.issn:
        existing = db.query(Journal).filter(Journal.issn == body.issn).first()
        if existing:
            raise HTTPException(409, f"Ya existe revista con ISSN {body.issn} (id={existing.id})")
    j = Journal(**body.model_dump())
    db.add(j)
    db.commit()
    db.refresh(j)
    return JournalRead.model_validate(j)


# ── INSTITUCIONES ────────────────────────────────────────────

@router.get("/institutions", response_model=PaginatedResponse[InstitutionRead], summary="Listar instituciones")
def list_institutions(
    search: Optional[str] = Query(None, description="Buscar por nombre o ROR"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista paginada de instituciones registradas."""
    q = db.query(Institution)
    if search:
        term = f"%{search}%"
        q = q.filter(
            Institution.name.ilike(term) | Institution.ror_id.ilike(term)
        )
    total = q.count()
    items = (
        q.order_by(Institution.name)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return PaginatedResponse.create(
        items=[InstitutionRead.model_validate(i) for i in items],
        total=total, page=page, page_size=page_size,
    )


@router.get("/institutions/{institution_id}", response_model=InstitutionRead, summary="Detalle de institución")
def get_institution(institution_id: int, db: Session = Depends(get_db)):
    """Obtiene una institución por su ID."""
    i = db.query(Institution).get(institution_id)
    if not i:
        raise HTTPException(404, "Institución no encontrada")
    return InstitutionRead.model_validate(i)


@router.post("/institutions", response_model=InstitutionRead, summary="Crear institución", status_code=201)
def create_institution(body: InstitutionCreate, db: Session = Depends(get_db)):
    """Crea una nueva institución. Si ya existe una con el mismo ROR, devuelve error."""
    if body.ror_id:
        existing = db.query(Institution).filter(Institution.ror_id == body.ror_id).first()
        if existing:
            raise HTTPException(409, f"Ya existe institución con ROR {body.ror_id} (id={existing.id})")
    i = Institution(**body.model_dump())
    db.add(i)
    db.commit()
    db.refresh(i)
    return InstitutionRead.model_validate(i)
