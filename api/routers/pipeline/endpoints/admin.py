"""
Endpoints: Administration

endpoints/admin.py - Operaciones de administración (truncate, init, etc.)
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.common import MessageResponse


logger = logging.getLogger("pipeline")
router = APIRouter(tags=["Administration"])


# ── DELETE /pipeline/truncate-all ──────────────────────────────────────────

@router.delete(
    "/truncate-all",
    response_model=MessageResponse,
    summary="Eliminar todos los registros",
)
def truncate_all(db: Session = Depends(get_db)):
    """
    Vacía TODAS las tablas de la base de datos.
    ⚠️ OPERACIÓN DESTRUCTIVA
    """
    from sqlalchemy import text

    tables = [
        "reconciliation_log",
        "publication_authors",
        "author_institutions",
        "openalex_records",
        "scopus_records",
        "wos_records",
        "cvlac_records",
        "datos_abiertos_records",
        "canonical_publications",
        "authors",
        "journals",
        "institutions",
    ]

    try:
        for table in tables:
            db.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        db.commit()

        total = 0
        for table in tables:
            count = db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            total += count

        return MessageResponse(
            message=f"Tablas vaciadas. Registros restantes: {total}"
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Error: {e}")
        raise HTTPException(500, f"Error vaciando tablas: {e}")


# ── POST /pipeline/init-db ────────────────────────────────────────────────

@router.post(
    "/init-db",
    response_model=MessageResponse,
    summary="Inicializar base de datos",
)
def init_database():
    """Inicializa las tablas de la base de datos."""
    try:
        from db.session import create_all_tables
        create_all_tables()
        return MessageResponse(message="Tablas creadas exitosamente")
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")


# ── GET /pipeline/scopus/test-extract ──────────────────────────────────────

@router.get(
    "/scopus/test-extract",
    summary="Test extracción Scopus",
)
def scopus_test_extract():
    """Extrae 10 registros de Scopus y los guarda."""
    from config import institution
    from extractors.scopus import ScopusExtractor
    from db.session import get_engine
    from sqlalchemy.orm import sessionmaker
    from db.models import ScopusRecord

    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=10)

    engine = get_engine()
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    inserted = 0
    
    for r in records:
        if not r.source_id:
            continue
        exists = session.query(ScopusRecord).filter_by(scopus_doc_id=r.source_id).first()
        if exists:
            continue
        rec = ScopusRecord(
            scopus_doc_id=r.source_id,
            doi=r.doi,
            title=r.title,
            publication_year=r.publication_year,
            publication_date=r.publication_date,
            publication_type=r.publication_type,
            source_journal=r.source_journal,
            issn=r.issn,
            is_open_access=r.is_open_access,
            citation_count=r.citation_count,
            status="pending",
        )
        session.add(rec)
        inserted += 1
    
    session.commit()
    session.close()
    
    return {"inserted": inserted, "total": len(records)}
