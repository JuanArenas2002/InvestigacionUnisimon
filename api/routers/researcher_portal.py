"""
Portal del Investigador — /api/me

Todos los endpoints son de solo lectura y operan exclusivamente sobre
el autor autenticado. Ningún endpoint acepta author_id como parámetro
externo; el ID se deriva del JWT.

Requiere: Authorization: Bearer <token>
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.routers.auth import get_current_researcher, get_token_jti
from api.security.token_blocklist import blocklist
from api.schemas.auth import ChangePasswordRequest
from db.models import ResearcherCredential
from api.routers.authors import _batch_source_records
from api.schemas.common import PaginatedResponse
from api.schemas.researcher_portal import (
    ResearcherAffiliation,
    ResearcherProfile,
    ResearcherPublicationRead,
)
from api.schemas.authors import CoauthorRead, AuthorInventoryResponse, InventoryProductRead, InventorySummary, InventoryTypeSummary, InventorySourceSummary, InventoryYearSummary
from api.schemas.author_metrics import AuthorGeneralMetricsResponse
from api.services.author_metrics_service import AuthorMetricsService
from shared.normalizers import normalize_publication_type
from db.models import (
    Author,
    AuthorInstitution,
    CanonicalPublication,
    Institution,
    PublicationAuthor,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/me", tags=["Portal del Investigador"])


# ─────────────────────────────────────────────────────────────────────────────
# GET /me/profile
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/profile",
    response_model=ResearcherProfile,
    summary="Perfil del investigador autenticado",
    description=(
        "Devuelve el perfil completo del investigador autenticado: identificadores "
        "en todas las fuentes (ORCID, OpenAlex, Scopus, WoS, CvLAC, Google Scholar), "
        "estado de verificación, procedencia de campos y afiliaciones institucionales."
    ),
)
def get_my_profile(
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    """
    Perfil propio del investigador.

    - Identificadores multi-fuente desde `external_ids` JSONB
    - Afiliaciones institucionales con historial (fechas de inicio/fin)
    - `field_provenance`: qué fuente aportó cada campo del perfil
    - `verification_status`: estado de verificación del perfil
    """
    # Conteo de publicaciones
    pub_count = (
        db.query(func.count(PublicationAuthor.id))
        .filter(PublicationAuthor.author_id == researcher.id)
        .scalar()
        or 0
    )

    # Afiliaciones institucionales con datos de la institución
    affiliations_rows = (
        db.query(AuthorInstitution, Institution)
        .join(Institution, AuthorInstitution.institution_id == Institution.id)
        .filter(AuthorInstitution.author_id == researcher.id)
        .order_by(AuthorInstitution.is_current.desc(), AuthorInstitution.start_year.desc())
        .all()
    )

    affiliations = [
        ResearcherAffiliation(
            institution_id=ai.institution_id,
            institution_name=inst.name,
            ror_id=inst.ror_id,
            country=inst.country,
            start_year=ai.start_year,
            end_year=ai.end_year,
            is_current=ai.is_current,
        )
        for ai, inst in affiliations_rows
    ]

    return ResearcherProfile(
        id=researcher.id,
        name=researcher.name,
        normalized_name=researcher.normalized_name,
        cedula=researcher.cedula,
        orcid=researcher.orcid,
        external_ids=researcher.external_ids,
        is_institutional=researcher.is_institutional,
        verification_status=researcher.verification_status,
        field_provenance=researcher.field_provenance,
        affiliations=affiliations,
        pub_count=pub_count,
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /me/publications
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/publications",
    response_model=PaginatedResponse[ResearcherPublicationRead],
    summary="Publicaciones propias (paginadas)",
    description=(
        "Lista paginada de las publicaciones del investigador autenticado. "
        "Filtrable por año, tipo, fuente y búsqueda de texto. "
        "Incluye el estado de cada publicación (Avalado / Revisión / Rechazado) "
        "y las fuentes que la reportan."
    ),
)
def get_my_publications(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(20, ge=1, le=100, description="Registros por página"),
    year: Optional[int] = Query(None, description="Filtrar por año de publicación"),
    pub_type: Optional[str] = Query(None, description="Filtrar por tipo (Article, Review, etc.)"),
    source: Optional[str] = Query(None, description="Filtrar por fuente (openalex, scopus, wos, cvlac, datos_abiertos)"),
    search: Optional[str] = Query(None, min_length=2, description="Búsqueda en título o DOI"),
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    """
    Publicaciones del investigador autenticado.

    **Filtros disponibles:**
    - `year`: año exacto de publicación
    - `pub_type`: tipo de publicación (Article, Review, Book Chapter, etc.)
    - `source`: fuente que reporta la publicación (openalex, scopus, wos, cvlac, datos_abiertos)
    - `search`: texto libre sobre título o DOI

    **Orden:** por año descendente (más reciente primero), sin año al final.
    """
    q = (
        db.query(CanonicalPublication)
        .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.publication_id)
        .filter(PublicationAuthor.author_id == researcher.id)
    )

    if year:
        q = q.filter(CanonicalPublication.publication_year == year)
    if pub_type:
        normalized_type = normalize_publication_type(pub_type)
        q = q.filter(CanonicalPublication.publication_type == normalized_type)
    if search:
        term = f"%{search.strip()}%"
        q = q.filter(
            CanonicalPublication.title.ilike(term)
            | CanonicalPublication.doi.ilike(term)
        )

    # Total antes de paginación
    total = q.with_entities(func.count(CanonicalPublication.id)).scalar() or 0

    pubs = (
        q.order_by(CanonicalPublication.publication_year.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    pub_ids = [p.id for p in pubs]
    sources_map, sources_list_map = _batch_source_records(db, pub_ids)

    # Filtrar por fuente (post-query, sobre la página actual)
    if source:
        pubs = [p for p in pubs if source in sources_list_map.get(p.id, [])]

    items = [
        ResearcherPublicationRead(
            id=p.id,
            title=p.title,
            doi=p.doi,
            publication_year=p.publication_year,
            publication_type=p.publication_type,
            source_journal=p.source_journal,
            issn=p.issn,
            citation_count=p.citation_count or 0,
            is_open_access=p.is_open_access,
            oa_status=p.oa_status,
            estado_publicacion=p.estado_publicacion,
            sources=sorted(set(sources_list_map.get(p.id, []))),
            source_links=sources_map.get(p.id, {}),
        )
        for p in pubs
    ]

    import math
    return PaginatedResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=math.ceil(total / page_size) if page_size > 0 else 0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /me/metrics
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/metrics",
    response_model=AuthorGeneralMetricsResponse,
    summary="Métricas bibliométricas propias",
    description=(
        "Métricas bibliométricas completas del investigador autenticado: "
        "h-index, citas por publicación (CPP), distribución temporal, "
        "top revistas, distribución por tipo, acceso abierto e idiomas. "
        "No conecta APIs externas — usa el inventario local."
    ),
)
def get_my_metrics(
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    """
    Métricas bibliométricas del investigador autenticado.

    **Incluye:**
    - `general_metrics`: total publicaciones, citas, h-index, CPP, años activo
    - `publications_by_year`: serie temporal de producción e impacto
    - `publication_types`: distribución por tipo de publicación
    - `top_journals`: 10 revistas con más publicaciones
    - `open_access`: análisis detallado de acceso abierto
    - `languages`: distribución de idiomas
    - `institutional_publications`: publicaciones con afiliación institucional
    """
    try:
        return AuthorMetricsService.get_author_metrics(researcher.id, db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[Portal/metrics] Error para autor {researcher.id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error calculando métricas: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# GET /me/coauthors
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/coauthors",
    response_model=List[CoauthorRead],
    summary="Red de co-autores del investigador",
    description=(
        "Devuelve la lista de co-autores del investigador autenticado, "
        "ordenados por número de publicaciones compartidas (descendente)."
    ),
)
def get_my_coauthors(
    limit: int = Query(30, ge=1, le=100, description="Máximo de co-autores a retornar"),
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    """
    Co-autores con quienes el investigador ha publicado.

    Ordenados por `shared_pubs` (publicaciones compartidas) de mayor a menor.
    El flag `is_institutional` indica si el co-autor pertenece a la institución.
    """
    from sqlalchemy import select

    author_pub_ids = select(PublicationAuthor.publication_id).filter(
        PublicationAuthor.author_id == researcher.id
    )

    rows = (
        db.query(
            Author.id,
            Author.name,
            Author.is_institutional,
            func.count(PublicationAuthor.publication_id).label("shared"),
        )
        .join(PublicationAuthor, Author.id == PublicationAuthor.author_id)
        .filter(
            PublicationAuthor.publication_id.in_(author_pub_ids),
            Author.id != researcher.id,
        )
        .group_by(Author.id, Author.name, Author.is_institutional)
        .order_by(func.count(PublicationAuthor.publication_id).desc())
        .limit(limit)
        .all()
    )

    return [
        CoauthorRead(id=r[0], name=r[1], is_institutional=r[2], shared_pubs=r[3])
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────────────
# GET /me/inventory
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/inventory",
    response_model=AuthorInventoryResponse,
    summary="Inventario completo de producción",
    description=(
        "Inventario consolidado de la producción bibliográfica del investigador: "
        "todos los productos con metadatos y un resumen estadístico desglosado "
        "por tipo, fuente y año."
    ),
)
def get_my_inventory(
    year: Optional[int] = Query(None, description="Filtrar por año"),
    pub_type: Optional[str] = Query(None, description="Filtrar por tipo de publicación"),
    source: Optional[str] = Query(None, description="Filtrar por fuente (openalex, scopus, wos, cvlac, datos_abiertos)"),
    institutional_only: bool = Query(False, description="Solo publicaciones con coautores institucionales"),
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    """
    Inventario completo del investigador autenticado.

    **`summary`** contiene:
    - `total_products`, `total_citations`
    - `by_type[]`: desglose por tipo de publicación
    - `by_source[]`: desglose por fuente
    - `by_year[]`: desglose por año
    - `sources_coverage`: cuántos productos tiene cada fuente

    **`products[]`**: lista detallada con cada publicación, sus fuentes
    y el `field_provenance` que indica qué fuente aportó cada campo.
    """
    from api.schemas.authors import AuthorRead

    q = (
        db.query(CanonicalPublication)
        .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.publication_id)
        .filter(PublicationAuthor.author_id == researcher.id)
    )
    if year:
        q = q.filter(CanonicalPublication.publication_year == year)
    if pub_type:
        normalized_type = normalize_publication_type(pub_type)
        q = q.filter(CanonicalPublication.publication_type == normalized_type)
    if institutional_only:
        q = q.filter(CanonicalPublication.institutional_authors_count > 0)

    pubs = q.order_by(CanonicalPublication.publication_year.desc().nullslast()).all()
    pub_ids = [p.id for p in pubs]

    sources_map, sources_list_map = _batch_source_records(db, pub_ids)

    if source:
        pubs = [p for p in pubs if source in sources_list_map.get(p.id, [])]

    # Construir productos y contadores para el resumen
    products = []
    total_citations = 0
    type_counter: dict = {}
    year_counter: dict = {}
    source_counter: dict = {}

    for p in pubs:
        p_sources = sorted(set(sources_list_map.get(p.id, [])))
        products.append(InventoryProductRead(
            id=p.id,
            title=p.title,
            doi=p.doi,
            publication_year=p.publication_year,
            publication_date=getattr(p, "publication_date", None),
            publication_type=p.publication_type,
            source_journal=p.source_journal,
            issn=p.issn,
            citation_count=p.citation_count or 0,
            is_open_access=p.is_open_access,
            field_provenance=p.field_provenance,
            sources=p_sources,
            source_links=sources_map.get(p.id, {}),
        ))
        total_citations += p.citation_count or 0
        pt = p.publication_type or "Sin tipo"
        type_counter[pt] = type_counter.get(pt, 0) + 1
        yr = p.publication_year
        year_counter[yr] = year_counter.get(yr, 0) + 1
        for s in p_sources:
            source_counter[s] = source_counter.get(s, 0) + 1

    summary = InventorySummary(
        total_products=len(products),
        total_citations=total_citations,
        by_type=[
            InventoryTypeSummary(publication_type=k, count=v)
            for k, v in sorted(type_counter.items(), key=lambda x: -x[1])
        ],
        by_source=[
            InventorySourceSummary(source=k, count=v)
            for k, v in sorted(source_counter.items(), key=lambda x: -x[1])
        ],
        by_year=[
            InventoryYearSummary(year=k, count=v)
            for k, v in sorted(year_counter.items(), key=lambda x: (x[0] is None, -(x[0] or 0)))
        ],
        sources_coverage=source_counter,
    )

    author_schema = AuthorRead.model_validate(researcher)
    author_schema.pub_count = len(products)

    return AuthorInventoryResponse(author=author_schema, summary=summary, products=products)


# ─────────────────────────────────────────────────────────────────────────────
# POST /me/change-password
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/change-password",
    summary="Cambiar contraseña",
    description="Permite al investigador autenticado actualizar su contraseña.",
)
def change_my_password(
    request: ChangePasswordRequest,
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    """
    Cambia la contraseña del investigador autenticado.

    - Verifica que `old_password` coincida con la credencial activa actual.
    - La nueva contraseña debe tener al menos 8 caracteres.
    """
    credential = (
        db.query(ResearcherCredential)
        .filter(
            ResearcherCredential.author_id == researcher.id,
            ResearcherCredential.is_active == True,
        )
        .first()
    )
    if not credential:
        raise HTTPException(status_code=401, detail="No hay credencial activa")

    if not credential.verify_password(request.old_password):
        raise HTTPException(status_code=400, detail="Contraseña actual incorrecta")

    credential.password_hash = ResearcherCredential.hash_password(request.new_password)
    db.commit()
    logger.info(f"Contraseña actualizada para investigador ID {researcher.id}")
    return {"message": "Contraseña actualizada exitosamente"}


# ─────────────────────────────────────────────────────────────────────────────
# POST /me/logout
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/logout",
    summary="Cerrar sesión",
    description="Invalida el token JWT actual. Cualquier uso posterior del token retornará 401.",
)
def logout(
    token_info: tuple = Depends(get_token_jti),
    researcher: Author = Depends(get_current_researcher),
    db: Session = Depends(get_db),
):
    _token_raw, jti, expires_at = token_info
    blocklist.revoke(jti, expires_at)

    credential = (
        db.query(ResearcherCredential)
        .filter(ResearcherCredential.author_id == researcher.id, ResearcherCredential.is_active == True)
        .first()
    )
    if credential:
        credential.last_login = datetime.now(timezone.utc)
        db.commit()

    logger.info("Logout — investigador ID %s, token revocado", researcher.id)
    return {"message": "Sesión cerrada. Token invalidado."}
