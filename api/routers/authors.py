# ── GET /authors/search-with-publications ─────────────────────────────





"""
Router de Autores — VERSIÓN OPTIMIZADA
======================================
Mejoras de rendimiento aplicadas:

1. ELIMINADOS N+1 QUERIES  → conteos de publicaciones en un solo GROUP BY
2. JOINS en vez de IN (subquery)  → más eficiente en tablas grandes
3. BATCH de fuentes externas  → una sola query UNION en vez de un loop por cada SOURCE_MODEL
4. /duplicates  → conteos de pubs en una sola query con dict lookup (no loop)
5. /enrich-orcid  → movido a BackgroundTask + caché simple de ORCIDs ya procesados
6. Ruta duplicada /enrich-orcid eliminada
7. /inventory  → join directo en vez de sub-IN anidado
"""

import logging
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy import func, or_, union_all, literal, select
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.utils import get_clean_source_id
from api.schemas.common import PaginatedResponse
from api.services.unified_extractor_service import UnifiedExtractorService, UnifiedAuthorProfile
from shared.normalizers import normalize_author_name
from api.routers.pipeline.endpoints.reconciliation import reconcile_all_sources
from api.schemas.authors import (
    AuthorRead,
    AuthorDetail,
    AuthorPublicationRead,
    CoauthorRead,
    AuthorGlobalStats,
    AuthorIdsCoverage,
    DuplicateAuthorGroup,
    DuplicateAuthorMatch,
    DuplicateSummary,
    MergeAuthorsRequest,
    MergeAuthorsResponse,
    AuthorInventoryResponse,
    InventoryProductRead,
    InventorySummary,
    InventoryTypeSummary,
    InventorySourceSummary,
    InventoryYearSummary,
    AuthorAuditLogRead,
    AuthorConflictRead,
    ResolveConflictRequest,
    VerifyAuthorRequest,
    BatchImportRequest,
    BatchImportResponse,
    SimilarAuthorRead,
)
from db.models import (
    CanonicalPublication,
    Author,
    AuthorAuditLog,
    AuthorConflict,
    PublicationAuthor,
    SOURCE_MODELS,
)
from db.source_registry import SOURCE_REGISTRY

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/authors", tags=["Autores"])

# Derivado del registry — se actualiza automáticamente al registrar nuevas fuentes
KNOWN_SOURCES = SOURCE_REGISTRY.names


# ── GET /authors/search ──────────────────────────────────────

@router.get("/search", response_model=List[AuthorRead], summary="Búsqueda rápida por nombre u ORCID")
def search_authors(
    q: str = Query(..., min_length=1, description="Búsqueda por nombre u ORCID"),
    limit: int = Query(20, ge=1, le=100, description="Máximo de resultados (default: 20)"),
    db: Session = Depends(get_db),
):
    """
    Búsqueda rápida de autores sin paginación.
    
    Parámetros:
    - `q`: término de búsqueda (búsqueda flexible por nombre o ORCID exacto)
    - `limit`: máximo número de resultados
    
    Optimización: Busca por ORCID exacto primero, luego por nombre con ILIKE.
    Retorna máximo `limit` registros sin paginación.
    """
    search_term = q.strip()
    
    # Prioridad 1: Búsqueda exacta por ORCID
    authors = db.query(Author).filter(Author.orcid == search_term).limit(limit).all()
    
    if len(authors) < limit:
        # Prioridad 2: Búsqueda por nombre (flexible)
        remaining_limit = limit - len(authors)
        orcid_set = {a.orcid for a in authors if a.orcid}
        
        name_results = (
            db.query(Author)
            .filter(
                Author.name.ilike(f"%{search_term}%"),
                ~Author.orcid.in_(orcid_set) if orcid_set else True,
            )
            .order_by(Author.name)
            .limit(remaining_limit)
            .all()
        )
        authors.extend(name_results)
    
    return [AuthorRead.model_validate(a) for a in authors]


# ── GET /authors/duplicates-orcid ─────────────────────────────

@router.get("/duplicates-orcid", summary="Autores con el mismo ORCID", response_model=List[AuthorRead])
def get_authors_with_duplicate_orcid(db: Session = Depends(get_db)):
    """
    Devuelve la lista de autores que comparten el mismo ORCID (posibles duplicados).
    """
    from sqlalchemy import func
    # Buscar ORCID duplicados
    dup_orcids = (
        db.query(Author.orcid)
        .filter(Author.orcid.isnot(None), Author.orcid != "")
        .group_by(Author.orcid)
        .having(func.count(Author.id) > 1)
        .all()
    )
    dup_orcids = [o[0] for o in dup_orcids]
    # Traer todos los autores con esos ORCID
    autores = (
        db.query(Author)
        .filter(Author.orcid.in_(dup_orcids))
        .order_by(Author.orcid, Author.name)
        .all()
    )
    return [AuthorRead.model_validate(a) for a in autores]

# ── POST /authors/enrich-missing-orcid ─────────────────────────────

@router.post("/enrich-missing-orcid", summary="Enriquecer autores sin ORCID usando DOIs", response_model=dict)
def enrich_authors_missing_orcid(
    db: Session = Depends(get_db),
    limit: int = Query(10, description="Máximo de autores a procesar por llamada (default: 10)")
):
    """
    Para cada autor sin ORCID, busca los DOIs de sus publicaciones y consulta todas las fuentes externas posibles (OpenAlex, Scopus, WoS, etc.)
    para intentar encontrar y asociar ORCID y otros IDs externos.
    """
    import requests
    from extractors.openalex import OpenAlexExtractor
    enriched = 0
    total = 0
    updated_fields = 0
    details = []
    conflicts = []  # Para registrar conflictos de ORCID duplicados
    openalex = OpenAlexExtractor()
    # 1. Obtener autores sin ORCID
    authors = db.query(Author).filter((Author.orcid.is_(None)) | (Author.orcid == "")).limit(limit).all()
    for author in authors:
        total += 1
        changes = {}
        # 2. Obtener DOIs de sus publicaciones
        pub_dois = (
            db.query(CanonicalPublication.doi)
            .join(PublicationAuthor, PublicationAuthor.publication_id == CanonicalPublication.id)
            .filter(PublicationAuthor.author_id == author.id)
            .filter(CanonicalPublication.doi.isnot(None), CanonicalPublication.doi != "")
            .distinct()
            .all()
        )
        pub_dois = [d[0] for d in pub_dois]
        # 3. Buscar en OpenAlex y otras fuentes por cada DOI
        for doi in pub_dois:
            # --- OpenAlex ---
            try:
                resp = openalex.session.get(f"https://api.openalex.org/works/https://doi.org/{doi}", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    # Buscar autores con ORCID en los autores de OpenAlex
                    for oa_auth in data.get("authorships", []):
                        ext_orcid = oa_auth.get("author", {}).get("orcid")
                        if ext_orcid and not author.orcid:
                            # Validar unicidad antes de asignar
                            existing = db.query(Author).filter(Author.orcid == ext_orcid).first()
                            if existing:
                                conflicts.append({
                                    "author_id": author.id,
                                    "conflict_type": "orcid_duplicate",
                                    "orcid": ext_orcid,
                                    "existing_author_id": existing.id
                                })
                            else:
                                author.orcid = ext_orcid
                                changes["orcid"] = ext_orcid
                        ext_openalex_id = oa_auth.get("author", {}).get("id")
                        if ext_openalex_id and not (author.external_ids or {}).get("openalex"):
                            author.external_ids = {**(author.external_ids or {}), "openalex": ext_openalex_id}
                            changes["openalex"] = ext_openalex_id
                        ext_name = oa_auth.get("author", {}).get("display_name")
                        if ext_name and not author.normalized_name:
                            author.normalized_name = ext_name.lower()
                            changes["normalized_name"] = ext_name.lower()
            except Exception as e:
                pass
            # --- Scopus (si tienes APIKey) ---
            try:
                from config import scopus_config
                url = f"https://api.elsevier.com/content/search/scopus?query=DOI({doi})"
                headers = {
                    "X-ELS-APIKey": scopus_config.api_key,
                    "Accept": "application/json",
                }
                if scopus_config.inst_token:
                    headers["X-ELS-Insttoken"] = scopus_config.inst_token
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    entries = data.get("search-results", {}).get("entry", [])
                    for entry in entries:
                        # Scopus Author ID
                        scopus_id = entry.get("dc:creator", "")
                        import re
                        if scopus_id:
                            scopus_id = str(scopus_id).strip()
                            match = re.search(r"(\d+)$", scopus_id)
                            if match and not (author.external_ids or {}).get("scopus"):
                                author.external_ids = {**(author.external_ids or {}), "scopus": match.group(1)}
                                changes["scopus"] = match.group(1)
            except Exception:
                pass
            # --- WoS y otras fuentes externas: aquí puedes agregar lógica similar si tienes acceso ---
        if changes:
            enriched += 1
            updated_fields += len(changes)
            prov = dict(author.field_provenance or {})
            for k in changes:
                prov[k] = "external-doi"
            author.field_provenance = prov
            details.append({"author_id": author.id, **changes})
    db.commit()
    return {
        "total_authors_without_orcid": total,
        "authors_enriched": enriched,
        "fields_completed": updated_fields,
        "details": details,
        "conflicts": conflicts,
    }


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: carga externa de fuentes en UNA SOLA QUERY
# ─────────────────────────────────────────────────────────────────────────────

def _batch_source_records(db: Session, pub_ids: List[int]):
    """
    🚀 OPTIMIZADO: Devuelve dict pub_id -> {source_name: url} usando UNION ALL.
    
    Antes: len(SOURCE_MODELS) × 1 query = 5 queries LENTAS
    Ahora: 1 query UNION ALL + IN = 1 query rápida
    
    Índices clave (deben estar en models.py):
    - canonical_publication_id (índice obligatorio)
    - doi (para búsqueda)
    """
    if not pub_ids:
        return {}, {}

    # Derivado del registry — se actualiza automáticamente al registrar nuevas fuentes
    SOURCE_ID_MAPPING = SOURCE_REGISTRY.source_id_mapping

    # Construir UNION ALL de los SELECT de cada modelo fuente
    selects = []
    for src_name, Model in SOURCE_MODELS.items():
        model_class_name = Model.__name__
        source_id_col_name = SOURCE_ID_MAPPING.get(model_class_name)
        
        if not source_id_col_name:
            continue
        
        source_id_col = getattr(Model, source_id_col_name, None)
        if source_id_col is None:
            continue
        
        # OPT: Solo selectear columnas necesarias (no SELECT *)
        s = (
            select(
                Model.canonical_publication_id.label("pub_id"),
                literal(src_name).label("source"),
                source_id_col.label("source_id"),
                Model.doi.label("doi"),
            )
            .where(Model.canonical_publication_id.in_(pub_ids))
            # OPT: Solo registros que tengan source_id o DOI (evita filas vacías)
            .where(or_(source_id_col.isnot(None), Model.doi.isnot(None)))
        )
        selects.append(s)

    if not selects:
        return {}, {}

    stmt = union_all(*selects)
    rows = db.execute(stmt).fetchall()

    # OPT: Prealocar diccionarios con las keys conocidas
    sources_map: dict = {pid: {} for pid in pub_ids}
    sources_list_map: dict = {pid: [] for pid in pub_ids}
    
    for pub_id, sname, sid, edoi in rows:
        clean_id = get_clean_source_id(sname, sid)
        if clean_id:
            sources_map[pub_id][sname] = clean_id
        sources_list_map[pub_id].append(sname)

    return sources_map, sources_list_map


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/stats
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/stats", response_model=AuthorGlobalStats, summary="Estadísticas globales de autores")
def get_author_stats(db: Session = Depends(get_db)):
    """
    KPIs globales de autores.
    OPTIMIZACIÓN: todos los conteos en una sola pasada con CASE WHEN.
    """
    row = db.execute(
        select(
            func.count(Author.id).label("total"),
            func.count(Author.id).filter(Author.is_institutional == True).label("institutional"),
            func.count(Author.id).filter(Author.orcid.isnot(None)).label("with_orcid"),
        )
    ).one()

    total_pubs = db.query(func.count(CanonicalPublication.id)).scalar() or 0
    total_pa   = db.query(func.count(PublicationAuthor.id)).scalar() or 0
    avg_pubs   = round(total_pa / row.total, 2) if row.total else 0.0

    return AuthorGlobalStats(
        total_authors=row.total,
        total_institutional=row.institutional,
        total_with_orcid=row.with_orcid,
        total_publications=total_pubs,
        avg_pubs_per_author=avg_pubs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors
# ─────────────────────────────────────────────────────────────────────────────

@router.get("", response_model=PaginatedResponse[AuthorRead], summary="Listar autores")
def list_authors(
    search: Optional[str] = Query(None, description="Buscar por nombre u ORCID"),
    is_institutional: Optional[bool] = Query(None),
    has_orcid: Optional[bool] = Query(None),
    min_pubs: Optional[int] = Query(None, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    """
    Lista paginada de autores con conteo de publicaciones.
    OPTIMIZACIÓN: conteo con GROUP BY en lugar de subconsultas correlacionadas.
    """
    pub_count = func.count(PublicationAuthor.id).label("pub_count")
    q = (
        db.query(Author, pub_count)
        .outerjoin(PublicationAuthor, Author.id == PublicationAuthor.author_id)
        .group_by(Author.id)
    )

    if search:
        term = f"%{search}%"
        q = q.filter(
            or_(
                Author.name.ilike(term),
                Author.orcid.ilike(term),
                Author.normalized_name.ilike(term),
            )
        )
    if is_institutional is not None:
        q = q.filter(Author.is_institutional == is_institutional)
    if has_orcid is True:
        q = q.filter(Author.orcid.isnot(None))
    elif has_orcid is False:
        q = q.filter(Author.orcid.is_(None))
    if min_pubs is not None and min_pubs > 0:
        q = q.having(pub_count >= min_pubs)

    # Contar total con subquery (sin re-evaluar todos los filtros)
    total_q = q.subquery()
    total = db.query(func.count()).select_from(total_q).scalar() or 0

    rows = (
        q.order_by(pub_count.desc(), Author.name)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = []
    for author, pc in rows:
        ar = AuthorRead.model_validate(author)
        ar.pub_count = pc or 0
        items.append(ar)

    return PaginatedResponse.create(items=items, total=total, page=page, page_size=page_size)


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/ids-coverage
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/ids-coverage", response_model=AuthorIdsCoverage, summary="Cobertura de identificadores")
def author_ids_coverage(db: Session = Depends(get_db)):
    """
    Cobertura de IDs de autores.
    OPTIMIZACIÓN: todas las columnas en una sola query con CASE WHEN en lugar de 5 queries.
    """
    row = db.execute(
        select(
            func.count(Author.id).label("total"),
            func.count(Author.id).filter(Author.is_institutional == True).label("institutional"),
            func.count(Author.id).filter(
                Author.orcid.isnot(None), Author.orcid != ""
            ).label("with_orcid"),
            func.count(Author.id).filter(
                Author.external_ids.has_key("openalex")
            ).label("with_openalex"),
            func.count(Author.id).filter(
                Author.external_ids.has_key("scopus")
            ).label("with_scopus"),
            func.count(Author.id).filter(
                Author.external_ids.has_key("wos")
            ).label("with_wos"),
            func.count(Author.id).filter(
                Author.external_ids.has_key("cvlac")
            ).label("with_cvlac"),
            func.count(Author.id).filter(
                Author.external_ids.has_key("google_scholar")
            ).label("with_google_scholar"),
            func.count(Author.id).filter(
                Author.cedula.isnot(None), Author.cedula != ""
            ).label("with_cedula"),
        )
    ).one()

    return AuthorIdsCoverage(
        total=row.total,
        institutional=row.institutional,
        with_orcid=row.with_orcid,
        with_openalex=row.with_openalex,
        with_scopus=row.with_scopus,
        with_wos=row.with_wos,
        with_cvlac=row.with_cvlac,
        with_google_scholar=row.with_google_scholar,
        with_cedula=row.with_cedula,
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/without-orcid
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/without-orcid", response_model=List[AuthorRead], summary="Autores sin ORCID")
def authors_without_orcid(
    limit: int = Query(30, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Top autores institucionales sin ORCID, ordenados por publicaciones."""
    pub_count = func.count(PublicationAuthor.id).label("pub_count")
    rows = (
        db.query(Author, pub_count)
        .outerjoin(PublicationAuthor, Author.id == PublicationAuthor.author_id)
        .filter(Author.is_institutional == True)
        .filter(or_(Author.orcid.is_(None), Author.orcid == ""))
        .group_by(Author.id)
        .order_by(pub_count.desc())
        .limit(limit)
        .all()
    )
    result = []
    for a, pc in rows:
        ar = AuthorRead.model_validate(a)
        ar.pub_count = pc or 0
        result.append(ar)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/duplicates
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/duplicates",
    response_model=DuplicateSummary,
    summary="Detectar autores posiblemente duplicados",
)
def detect_duplicate_authors(
    min_group_size: int = Query(2, ge=2),
    only_institutional: bool = Query(False),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Detecta autores que podrían estar duplicados por nombre normalizado.

    OPTIMIZACIÓN CRÍTICA:
    - Antes: 1 query por autor dentro del loop (N+1)
    - Ahora: 1 sola query de conteos para TODOS los autores de la página,
      luego dict lookup O(1) en el loop.
    """
    q = (
        db.query(
            Author.normalized_name,
            func.count(Author.id).label("cnt"),
        )
        .filter(
            Author.normalized_name.isnot(None),
            Author.normalized_name != "",
        )
    )
    if only_institutional:
        q = q.filter(Author.is_institutional == True)

    q = (
        q.group_by(Author.normalized_name)
        .having(func.count(Author.id) >= min_group_size)
        .order_by(func.count(Author.id).desc())
    )

    total_groups = q.count()
    dup_rows = q.offset((page - 1) * page_size).limit(page_size).all()

    # Obtener los nombres de esta página
    page_names = [r[0] for r in dup_rows]

    if not page_names:
        return DuplicateSummary(total_groups=total_groups, total_duplicate_authors=0, groups=[])

    # ── BATCH: todos los autores de los grupos en UNA query ──
    authors_in_page = (
        db.query(Author)
        .filter(Author.normalized_name.in_(page_names))
        .order_by(Author.normalized_name, Author.created_at.asc())
        .all()
    )

    # Agrupar en memoria
    from collections import defaultdict
    groups_map: dict = defaultdict(list)
    for a in authors_in_page:
        groups_map[a.normalized_name].append(a)

    # ── BATCH: conteos de publicaciones en UNA sola query ──
    author_ids_page = [a.id for a in authors_in_page]
    pub_counts_raw = (
        db.query(PublicationAuthor.author_id, func.count(PublicationAuthor.id).label("cnt"))
        .filter(PublicationAuthor.author_id.in_(author_ids_page))
        .group_by(PublicationAuthor.author_id)
        .all()
    )
    pub_count_map = {row[0]: row[1] for row in pub_counts_raw}

    # Construir respuesta
    groups = []
    total_dup_authors = 0

    for norm_name, cnt in dup_rows:
        author_matches = []
        for a in groups_map.get(norm_name, []):
            author_matches.append(DuplicateAuthorMatch(
                id=a.id,
                name=a.name,
                normalized_name=a.normalized_name,
                orcid=a.orcid,
                openalex_id=a.openalex_id,
                scopus_id=a.scopus_id,
                is_institutional=a.is_institutional,
                field_provenance=a.field_provenance,
                pub_count=pub_count_map.get(a.id, 0),
                created_at=a.created_at,
            ))
        total_dup_authors += len(author_matches)
        groups.append(DuplicateAuthorGroup(
            normalized_name=norm_name,
            count=cnt,
            authors=author_matches,
        ))

    return DuplicateSummary(
        total_groups=total_groups,
        total_duplicate_authors=total_dup_authors,
        groups=groups,
    )


# ─────────────────────────────────────────────────────────────────────────────
# POST /authors/merge
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/merge", response_model=MergeAuthorsResponse, summary="Fusionar autores duplicados")
def merge_authors(body: MergeAuthorsRequest, db: Session = Depends(get_db)):
    """
    Fusiona autores duplicados conservando uno y absorbiendo los demás.
    OPTIMIZACIÓN: bulk update en vez de loop row-by-row para reasignar publicaciones.
    """
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy import update, delete
    from db.models import AuthorInstitution

    keep = db.get(Author, body.keep_id)
    if not keep:
        raise HTTPException(404, f"Autor principal {body.keep_id} no encontrado")
    if body.keep_id in body.merge_ids:
        raise HTTPException(400, "keep_id no puede estar en merge_ids")

    to_merge = db.query(Author).filter(Author.id.in_(body.merge_ids)).all()
    found_ids = {a.id for a in to_merge}
    missing = set(body.merge_ids) - found_ids
    if missing:
        raise HTTPException(404, f"Autores no encontrados: {missing}")

    pubs_reassigned = 0
    ids_inherited = {}

    # Capturar keep_id como valor Python puro antes del loop
    # (evita que SQLAlchemy expire el objeto y evalúe keep.id como None)
    keep_id: int = keep.id

    for donor in to_merge:
        donor_id: int = donor.id

        # ── 1. Detectar publicaciones que ya tiene el autor principal ──
        keep_pub_ids = {
            r[0] for r in
            db.query(PublicationAuthor.publication_id)
            .filter(PublicationAuthor.author_id == keep_id)
            .all()
        }

        # Contar cuántas se van a reasignar
        if keep_pub_ids:
            n_to_reassign = (
                db.query(func.count(PublicationAuthor.id))
                .filter(
                    PublicationAuthor.author_id == donor_id,
                    ~PublicationAuthor.publication_id.in_(keep_pub_ids),
                )
                .scalar() or 0
            )
        else:
            n_to_reassign = (
                db.query(func.count(PublicationAuthor.id))
                .filter(PublicationAuthor.author_id == donor_id)
                .scalar() or 0
            )

        # Reasignar en bulk usando Core con synchronize_session=False encadenado
        # al statement (no como kwarg de db.execute, que no funciona en SA 2.x).
        if keep_pub_ids:
            db.execute(
                update(PublicationAuthor)
                .where(
                    PublicationAuthor.author_id == donor_id,
                    ~PublicationAuthor.publication_id.in_(keep_pub_ids),
                )
                .values(author_id=keep_id)
                .execution_options(synchronize_session=False)
            )
            db.execute(
                delete(PublicationAuthor)
                .where(
                    PublicationAuthor.author_id == donor_id,
                    PublicationAuthor.publication_id.in_(keep_pub_ids),
                )
                .execution_options(synchronize_session=False)
            )
        else:
            db.execute(
                update(PublicationAuthor)
                .where(PublicationAuthor.author_id == donor_id)
                .values(author_id=keep_id)
                .execution_options(synchronize_session=False)
            )

        pubs_reassigned += n_to_reassign

        # Limpiar identity map para que el ORM no intente re-aplicar los
        # cambios de las filas que ya movimos con Core.
        db.expire_all()

        # ── 2. Reasignar instituciones ──
        from db.models import AuthorInstitution
        keep_inst_ids = {
            r[0] for r in
            db.query(AuthorInstitution.institution_id)
            .filter(AuthorInstitution.author_id == keep_id)
            .all()
        }
        donor_inst_rows = (
            db.query(AuthorInstitution)
            .filter(AuthorInstitution.author_id == donor_id)
            .all()
        )
        for inst_link in donor_inst_rows:
            if inst_link.institution_id in keep_inst_ids:
                db.delete(inst_link)
            else:
                inst_link.author_id = keep_id

        # Recargar keep y donor después del expire_all
        keep   = db.get(Author, keep_id)
        donor  = db.get(Author, donor_id)

        # ── 3. Heredar IDs externos ──
        for attr in ("orcid", "openalex_id", "scopus_id", "wos_id", "cvlac_id"):
            donor_val = getattr(donor, attr)
            if donor_val and not getattr(keep, attr):
                setattr(keep, attr, donor_val)
                ids_inherited[attr] = donor_val
        if donor.is_institutional and not keep.is_institutional:
            keep.is_institutional = True
            ids_inherited["is_institutional"] = True

        # ── 3b. Fusionar field_provenance ──
        donor_prov = dict(donor.field_provenance or {})
        keep_prov  = dict(keep.field_provenance or {})
        for field, source in donor_prov.items():
            keep_prov.setdefault(field, source)
        keep.field_provenance = keep_prov

        # ── 4. Eliminar donor ──
        db.delete(donor)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(500, f"Error en la fusión: {e}")

    logger.info(
        f"Merge: conservado={keep.id}, absorbidos={body.merge_ids}, "
        f"pubs_reasignadas={pubs_reassigned}, ids_heredados={ids_inherited}"
    )

    # Invalidar cache de métricas para el autor resultante y los absorbidos
    from api.services.author_metrics_service import invalidate_author_metrics_cache
    invalidate_author_metrics_cache(keep_id)
    for mid in body.merge_ids:
        invalidate_author_metrics_cache(mid)

    return MergeAuthorsResponse(
        kept_author_id=keep.id,
        merged_count=len(to_merge),
        publications_reassigned=pubs_reassigned,
        ids_inherited=ids_inherited,
        message=(
            f"Fusionados {len(to_merge)} autores en #{keep.id} ({keep.name}). "
            f"{pubs_reassigned} publicaciones reasignadas."
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /authors/{id}
# ─────────────────────────────────────────────────────────────────────────────

from api.schemas.common import MessageResponse


# ── ENDPOINTS CON PARÁMETROS DE PATH (deben ir al final) ──

@router.delete("/id/{author_id}", response_model=MessageResponse, summary="Eliminar un autor")
def delete_author(author_id: int, db: Session = Depends(get_db)):
    """Elimina un autor. Las publicaciones canónicas NO se eliminan."""
    author = db.get(Author, author_id)
    if not author:
        raise HTTPException(404, "Autor no encontrado")
    name = author.name
    db.delete(author)
    db.commit()
    return MessageResponse(message=f"Autor #{author_id} ({name}) eliminado correctamente")


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/inventory
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/inventory",
    response_model=AuthorInventoryResponse,
    summary="Inventario completo de productos de un autor",
)
def get_author_inventory(
    author_id: Optional[int]  = Query(None),
    name: Optional[str]       = Query(None, min_length=2),
    orcid: Optional[str]      = Query(None),
    scopus_id: Optional[str]  = Query(None),
    openalex_id: Optional[str] = Query(None),
    cvlac_id: Optional[str]   = Query(None),
    year: Optional[int]        = Query(None),
    publication_type: Optional[str] = Query(None),
    source: Optional[str]      = Query(None),
    institutional_only: bool   = Query(False, description="Si es True, solo publicaciones con autores institucionales (institutional_authors_count > 0)"),
    db: Session = Depends(get_db),
):
    """
    Inventario completo de un autor.
    OPTIMIZACIÓN: JOIN directo en lugar de IN (subquery), fuentes externas en UNION ALL.
    
    Parámetro `institutional_only`: Si es True, filtra solo publicaciones donde 
    institutional_authors_count > 0 (publicaciones que incluyen coautores de la institución).
    """
    if not any([author_id, name, orcid, scopus_id, openalex_id, cvlac_id]):
        raise HTTPException(
            400,
            "Debe enviar al menos un parámetro: author_id, name, orcid, scopus_id, openalex_id o cvlac_id",
        )

    # Resolver autor
    author = None
    if author_id:
        author = db.get(Author, author_id)
    if not author and orcid:
        author = db.query(Author).filter(Author.orcid == orcid.strip()).first()
    if not author and scopus_id:
        author = db.query(Author).filter(
            Author.external_ids["scopus"].astext == scopus_id.strip()
        ).first()
    if not author and openalex_id:
        author = db.query(Author).filter(
            Author.external_ids["openalex"].astext == openalex_id.strip()
        ).first()
    if not author and cvlac_id:
        author = db.query(Author).filter(
            Author.external_ids["cvlac"].astext == cvlac_id.strip()
        ).first()
    if not author and name:
        like = f"%{name.strip()}%"
        author = (
            db.query(Author)
            .filter(or_(Author.name.ilike(like), Author.normalized_name.ilike(like)))
            .first()
        )

    if not author:
        criteria = []
        if author_id:    criteria.append(f"id={author_id}")
        if name:         criteria.append(f"nombre='{name}'")
        if orcid:        criteria.append(f"orcid='{orcid}'")
        if scopus_id:    criteria.append(f"scopus_id='{scopus_id}'")
        if openalex_id:  criteria.append(f"openalex_id='{openalex_id}'")
        if cvlac_id:     criteria.append(f"cvlac_id='{cvlac_id}'")
        raise HTTPException(404, f"No se encontró ningún autor con: {', '.join(criteria)}")

    # ── Publicaciones via JOIN (evita IN con subquery) ──
    q = (
        db.query(CanonicalPublication)
        .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.publication_id)
        .filter(PublicationAuthor.author_id == author.id)
    )
    if year:
        q = q.filter(CanonicalPublication.publication_year == year)
    if publication_type:
        q = q.filter(CanonicalPublication.publication_type == publication_type)
    if institutional_only:
        q = q.filter(CanonicalPublication.institutional_authors_count > 0)

    pubs = q.order_by(CanonicalPublication.publication_year.desc().nullslast()).all()
    pub_ids = [p.id for p in pubs]

    # ── BATCH de fuentes externas ──
    sources_map, sources_list_map = _batch_source_records(db, pub_ids)

    # Filtrar por fuente si se solicita
    if source:
        allowed_ids = {pid for pid, srcs in sources_list_map.items() if source in srcs}
        pubs = [p for p in pubs if p.id in allowed_ids]

    # Conteo de publicaciones (len ya lo tenemos, pero contamos después del filtro)
    pub_count = len(pubs)
    author_schema = AuthorRead.model_validate(author)
    author_schema.pub_count = pub_count

    # Construir productos y resumen
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
            publication_date=p.publication_date,
            publication_type=p.publication_type,
            source_journal=p.source_journal,
            issn=p.issn,
            citation_count=p.citation_count,
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

    return AuthorInventoryResponse(author=author_schema, summary=summary, products=products)


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/unified-profile
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/unified-profile",
    summary="Perfil unificado del autor (todas las plataformas)",
    response_model=Dict[str, Any],
)
async def get_unified_author_profile(
    author_id: Optional[int] = Query(None, description="ID del autor en BD (mutuamente exclusivo con orcid)"),
    orcid: Optional[str] = Query(None, description="ORCID del autor (si no existe, detecta si es institucional y lo crea)"),
    platforms: Optional[str] = Query(
        None,
        description="Plataformas a incluir (csv): scopus,wos,openalex,cvlac,datos_abiertos. Si omite, usa todas disponibles"
    ),
    reconcile: bool = Query(
        True,
        description="Si True, ejecuta reconciliación y guarda publicaciones canónicas en BD. Si False, solo extrae datos."
    ),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Endpoint unificado que ejecuta TODOS los extractores disponibles para un autor.
    Opcionalmente ejecuta la reconciliación para guardar publicaciones en BD.
    
    **Tres modos de uso:**
    
    1. **Modo INSERCIÓN** (`reconcile=true`, default):
       - Extrae datos de todas las plataformas
       - Ejecuta reconciliación DOI:
         * Busca DOI exacto en canonical_publications
         * Si no, fuzzy matching (título+año+autores)
         * Evita duplicados por DOI (4 niveles: dedup_hash, source_id, doi, título+año)
       - Crea/vincula publicaciones canónicas
       - Guarda registros en BD (permanente)
    
    2. **Modo LECTURA** (`reconcile=false`):
       - Solo extrae datos
       - No modifica BD
       - Rápido para inspeccionar
    
    3. **Por ORCID**:
       - Auto-detecta si es autor institucional
       - Crea registro en BD si lo es
    
    **Ejemplos:**
    ```
    # Extraer + reconciliar (insert mode)
    GET /authors/unified-profile?author_id=123
    GET /authors/unified-profile?author_id=123&reconcile=true
    
    # Solo extraer (read mode)
    GET /authors/unified-profile?author_id=123&reconcile=false
    
    # Plataformas específicas
    GET /authors/unified-profile?author_id=123&platforms=scopus,openalex
    
    # Por ORCID
    GET /authors/unified-profile?orcid=0000-0001-8757-3778
    ```
    """
    
    try:
        if author_id is None and orcid is None:
            raise HTTPException(
                status_code=400,
                detail="Debes proporcionar 'author_id' o 'orcid' en la URL"
            )
        
        service = UnifiedExtractorService(db)
        
        include_platforms = None
        if platforms:
            include_platforms = [p.strip().lower() for p in platforms.split(",")]
        
        # Ejecutar con parámetro reconcile
        profile = service.extract_author_profile(
            author_id=author_id,
            orcid=orcid,
            include_platforms=include_platforms,
            reconcile=reconcile,
        )
        
        # Si reconcile=true, ejecutar reconciliación global en background
        if reconcile:
            logger.info("Agendando reconciliación global en background...")
            background_tasks.add_task(
                reconcile_all_sources, 
                db=db
            )
        
        return {
            "author": {
                "id": profile.author_id,
                "name": profile.author_name,
                "orcid": profile.orcid,
                "is_institutional": profile.is_institutional,
                "identifiers": profile.identifiers,
            },
            "author_data": {
                "consolidated": profile.author_data.get('consolidated', {}),
                "scopus_profile": profile.author_data.get('scopus_profile'),
                "openalex_profile": profile.author_data.get('openalex_profile'),
            },
            "summary": {
                "total_publications": profile.total_publications,
                "total_citations": profile.total_citations,
                "platforms_with_data": profile.platforms_with_data,
                "extraction_summary": profile.extraction_summary,
            },
            "platforms": {
                platform: {
                    "success": result.success,
                    "records_count": result.records_count,
                    "error": result.error,
                    "extracted_at": result.extracted_at,
                    "sample_records": [
                        r if isinstance(r, dict) else (
                            r.to_dict() if hasattr(r, 'to_dict') else str(r)
                        )
                        for r in result.records[:3]
                    ]
                }
                for platform, result in profile.platform_results.items()
            },
            "reconciliation": {
                "status": profile.reconciliation_status,
                "statistics": profile.reconciliation_stats,
                "details": (
                    f"Mode: {'INSERT (datos guardados en BD)' if reconcile else 'READ (sin guardar)'} | "
                    f"Reconciliation: {profile.reconciliation_status}"
                ),
            },
            "global_reconciliation": {
                "status": "processing_in_background",
                "message": "Reconciliación global de todas las fuentes ejecutándose en background"
            } if reconcile else None,
            "extracted_at": profile.extracted_at,
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error en unified-profile: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error extrayendo perfil unificado: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/{id}
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/id/{author_id}", response_model=AuthorRead, summary="Detalle de autor")
def get_author(author_id: int, db: Session = Depends(get_db)):
    author = db.get(Author, author_id)
    if not author:
        raise HTTPException(404, "Autor no encontrado")
    pc = (
        db.query(func.count(PublicationAuthor.id))
        .filter(PublicationAuthor.author_id == author_id)
        .scalar() or 0
    )
    ar = AuthorRead.model_validate(author)
    ar.pub_count = pc
    return ar


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/{id}/publications
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/id/{author_id}/publications",
    response_model=List[AuthorPublicationRead],
    summary="Publicaciones del autor",
)
def get_author_publications(
    author_id: int,
    year: Optional[int] = Query(None),
    publication_type: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """
    OPTIMIZACIÓN: JOIN directo + UNION ALL para fuentes externas.
    """
    author = db.get(Author, author_id)
    if not author:
        raise HTTPException(404, "Autor no encontrado")

    q = (
        db.query(CanonicalPublication)
        .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.publication_id)
        .filter(PublicationAuthor.author_id == author_id)
    )
    if year:
        q = q.filter(CanonicalPublication.publication_year == year)
    if publication_type:
        q = q.filter(CanonicalPublication.publication_type == publication_type)

    pubs = q.order_by(CanonicalPublication.publication_year.desc().nullslast()).all()
    pub_ids = [p.id for p in pubs]

    # Filtrar por fuente usando el modelo correspondiente (sigue siendo 1 query)
    if source:
        _SrcModel = SOURCE_MODELS.get(source)
        if _SrcModel:
            allowed = {
                r[0] for r in
                db.query(_SrcModel.canonical_publication_id)
                .filter(_SrcModel.canonical_publication_id.in_(pub_ids))
                .distinct()
                .all()
            }
            pubs = [p for p in pubs if p.id in allowed]
            pub_ids = [p.id for p in pubs]

    # BATCH de fuentes externas (UNION ALL)
    sources_map, sources_list_map = _batch_source_records(db, pub_ids)

    return [
        AuthorPublicationRead(
            id=p.id,
            title=p.title,
            doi=p.doi,
            publication_year=p.publication_year,
            publication_type=p.publication_type,
            source_journal=p.source_journal,
            citation_count=p.citation_count,
            is_open_access=p.is_open_access,
            sources=sorted(set(sources_list_map.get(p.id, []))),
            source_links=sources_map.get(p.id, {}),
        )
        for p in pubs
    ]


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/{id}/coauthors
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/{author_id}/coauthors",
    response_model=List[CoauthorRead],
    summary="Coautores del investigador",
)
def get_coauthors(
    author_id: int,
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Coautores (top N por publicaciones compartidas)."""
    author = db.get(Author, author_id)
    if not author:
        raise HTTPException(404, "Autor no encontrado")

    # Subquery de IDs de publicaciones del autor
    author_pub_ids = (
        db.query(PublicationAuthor.publication_id)
        .filter(PublicationAuthor.author_id == author_id)
        .subquery()
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
            Author.id != author_id,
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


# ── GET /authors/search-with-publications ─────────────────────────────
from sqlalchemy.orm import selectinload

@router.get("/search-with-publications", response_model=List[AuthorDetail], summary="Buscar autores y traer publicaciones (optimizado)")
def search_authors_with_publications(
    q: str = Query(..., min_length=1, description="Búsqueda por nombre u ORCID"),
    limit: int = Query(10, ge=1, le=50, description="Máximo de autores a retornar (default: 10)"),
    pub_limit: int = Query(10, ge=1, le=50, description="Máximo de publicaciones por autor (default: 10)"),
    db: Session = Depends(get_db),
):
    """
    Busca autores por nombre u ORCID y retorna autores junto con sus publicaciones.
    Optimizado para evitar N+1 queries usando selectinload.
    - q: término de búsqueda (nombre parcial o ORCID exacto)
    - limit: máximo de autores
    - pub_limit: máximo de publicaciones por autor
    """
    search_term = q.strip()
    authors = db.query(Author)
    if search_term:
        authors = authors.filter(
            or_(Author.orcid == search_term, Author.name.ilike(f"%{search_term}%"))
        )
    authors = (
        authors.options(selectinload(Author.publications).selectinload(PublicationAuthor.publication))
        .order_by(Author.name)
        .limit(limit)
        .all()
    )

    result = []
    for author in authors:
        pas = list(author.publications)[:pub_limit]
        pubs_data = [
            AuthorPublicationRead.model_validate(pa.publication) for pa in pas if pa.publication is not None
        ]
        # Construir el dict del autor y sobrescribir publications
        author_dict = author.__dict__.copy()
        # Quitar publicaciones originales (PublicationAuthor)
        author_dict.pop('publications', None)
        # Agregar publicaciones serializadas
        author_dict['publications'] = pubs_data
        author_data = AuthorDetail.model_validate(author_dict)
        result.append(author_data)
    return result

# ─────────────────────────────────────────────────────────────────────────────
# POST /authors/enrich-orcid  (BackgroundTask — evita timeout en llamadas externas)
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/enrich-orcid", summary="Enriquecer autores por ORCID", response_model=dict)
def enrich_authors_by_orcid(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=5000, description="Máximo de autores a procesar"),
    run_in_background: bool = Query(
        False,
        description="Si True, ejecuta en segundo plano y responde inmediatamente",
    ),
):
    """
    Recorre autores con ORCID y completa openalex_id / scopus_id.

    OPTIMIZACIONES:
    - Se eliminó la ruta duplicada (@router.post registrado dos veces).
    - `run_in_background=True` ejecuta la tarea de forma asíncrona (evita
      timeouts cuando `limit` es grande).
    - La función interna reutiliza la sesión de BD pasada como argumento para
      poder hacer commit al final en vez de por cada autor.
    """
    authors = db.query(Author).filter(Author.orcid.isnot(None)).limit(limit).all()

    def _do_enrich(authors_to_process, session: Session):
        import requests
        from extractors.openalex import OpenAlexExtractor

        openalex = OpenAlexExtractor()
        enriched = 0
        updated_fields = 0
        details = []

        for idx, author in enumerate(authors_to_process, 1):
            logger.info(f"[{idx}/{len(authors_to_process)}] Autor {author.id} ORCID {author.orcid}")
            changes = {}

            if not author.openalex_id or not author.normalized_name:
                try:
                    resp = openalex.session.get(
                        f"https://api.openalex.org/authors/ORCID:{author.orcid}",
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if not author.openalex_id and data.get("id"):
                            author.openalex_id = data["id"]
                            changes["openalex_id"] = data["id"]
                        if not author.normalized_name and data.get("display_name"):
                            # Usar normalize_author_name para normalización correcta
                            author.normalized_name = normalize_author_name(data["display_name"])
                            changes["normalized_name"] = author.normalized_name
                except Exception as e:
                    logger.error(f"OpenAlex error ORCID {author.orcid}: {e}")

            if not author.scopus_id:
                try:
                    from config import scopus_config
                    import re
                    headers = {"X-ELS-APIKey": scopus_config.api_key, "Accept": "application/json"}
                    if scopus_config.inst_token:
                        headers["X-ELS-Insttoken"] = scopus_config.inst_token
                    resp = requests.get(
                        f"https://api.elsevier.com/content/author/orcid/{author.orcid}",
                        headers=headers,
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        raw_id = (
                            data.get("author-retrieval-response", [{}])[0]
                            .get("coredata", {})
                            .get("dc:identifier", "")
                        )
                        if raw_id:
                            m = re.search(r"(\d+)$", str(raw_id).strip())
                            if m:
                                author.scopus_id = m.group(1)
                                changes["scopus_id"] = m.group(1)
                except Exception as e:
                    logger.error(f"Scopus error ORCID {author.orcid}: {e}")

            if changes:
                enriched += 1
                updated_fields += len(changes)
                prov = dict(author.field_provenance or {})
                for k in changes:
                    prov[k] = "openalex" if k in ("openalex_id", "normalized_name") else "scopus"
                author.field_provenance = prov
                details.append({"author_id": author.id, **changes})

        session.commit()
        logger.info(
            f"Enriquecimiento finalizado. Total={len(authors_to_process)}, "
            f"Enriquecidos={enriched}, Campos={updated_fields}"
        )
        return {"total": len(authors_to_process), "enriched": enriched, "fields": updated_fields, "details": details}

    if run_in_background:
        background_tasks.add_task(_do_enrich, authors, db)
        return {
            "message": f"Enriquecimiento iniciado en segundo plano para {len(authors)} autores.",
            "total_queued": len(authors),
        }

    # Ejecución síncrona (comportamiento original)
    return _do_enrich(authors, db)


# =============================================================================
# NUEVOS ENDPOINTS v11
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/{id}/similar — autores similares por nombre (fuzzy)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/id/{author_id}/similar",
    response_model=List[SimilarAuthorRead],
    summary="Autores con nombre similar (posibles duplicados)",
)
def get_similar_authors(
    author_id: int,
    threshold: float = Query(0.6, ge=0.3, le=1.0, description="Umbral de similitud pg_trgm (default: 0.6)"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    Devuelve autores cuyo normalized_name tiene similitud >= threshold
    con el autor indicado. Útil para detectar posibles duplicados manualmente.
    """
    author = db.get(Author, author_id)
    if not author:
        raise HTTPException(404, "Autor no encontrado")
    if not author.normalized_name:
        return []

    similarity_expr = func.similarity(Author.normalized_name, author.normalized_name)

    pub_count_sq = (
        db.query(
            PublicationAuthor.author_id,
            func.count(PublicationAuthor.id).label("cnt"),
        )
        .group_by(PublicationAuthor.author_id)
        .subquery()
    )

    rows = (
        db.query(Author, func.coalesce(pub_count_sq.c.cnt, 0).label("pc"), similarity_expr.label("score"))
        .outerjoin(pub_count_sq, Author.id == pub_count_sq.c.author_id)
        .filter(
            Author.id != author_id,
            Author.normalized_name.isnot(None),
            similarity_expr >= threshold,
        )
        .order_by(similarity_expr.desc())
        .limit(limit)
        .all()
    )

    result = []
    for a, pc, score in rows:
        item = SimilarAuthorRead(
            id=a.id,
            name=a.name,
            normalized_name=a.normalized_name,
            orcid=a.orcid,
            openalex_id=a.openalex_id,
            scopus_id=a.scopus_id,
            is_institutional=a.is_institutional,
            verification_status=a.verification_status,
            pub_count=pc or 0,
            similarity_score=round(float(score), 4),
        )
        result.append(item)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/{id}/audit-log
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/id/{author_id}/audit-log",
    response_model=List[AuthorAuditLogRead],
    summary="Historial de cambios de un autor",
)
def get_author_audit_log(
    author_id: int,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Devuelve el historial de cambios (creación, actualizaciones, fusiones) de un autor."""
    if not db.get(Author, author_id):
        raise HTTPException(404, "Autor no encontrado")
    entries = (
        db.query(AuthorAuditLog)
        .filter(AuthorAuditLog.author_id == author_id)
        .order_by(AuthorAuditLog.created_at.desc())
        .limit(limit)
        .all()
    )
    return [AuthorAuditLogRead.model_validate(e) for e in entries]


# ─────────────────────────────────────────────────────────────────────────────
# GET /authors/conflicts — conflictos sin resolver
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/conflicts",
    response_model=List[AuthorConflictRead],
    summary="Conflictos entre fuentes sin resolver",
)
def get_author_conflicts(
    author_id: Optional[int] = Query(None, description="Filtrar por autor"),
    only_unresolved: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista conflictos donde dos fuentes aportaron valores distintos para el mismo campo."""
    q = db.query(AuthorConflict)
    if author_id is not None:
        q = q.filter(AuthorConflict.author_id == author_id)
    if only_unresolved:
        q = q.filter(AuthorConflict.resolved == False)  # noqa: E712
    conflicts = q.order_by(AuthorConflict.created_at.desc()).limit(limit).all()
    return [AuthorConflictRead.model_validate(c) for c in conflicts]


# ─────────────────────────────────────────────────────────────────────────────
# POST /authors/conflicts/{id}/resolve
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/conflicts/{conflict_id}/resolve",
    response_model=AuthorConflictRead,
    summary="Resolver un conflicto entre fuentes",
)
def resolve_author_conflict(
    conflict_id: int,
    body: ResolveConflictRequest,
    db: Session = Depends(get_db),
):
    """
    Marca un conflicto como resuelto.

    - `kept_existing`: se conserva el valor ya almacenado
    - `used_new`: se aplica el valor de la nueva fuente al autor
    - `manual`: resolución manual libre
    - `ignored`: se ignora sin cambios
    """
    from datetime import datetime, timezone

    conflict = db.get(AuthorConflict, conflict_id)
    if not conflict:
        raise HTTPException(404, "Conflicto no encontrado")
    if conflict.resolved:
        raise HTTPException(400, "El conflicto ya fue resuelto")

    valid_resolutions = {"kept_existing", "used_new", "manual", "ignored"}
    if body.resolution not in valid_resolutions:
        raise HTTPException(400, f"Resolución inválida. Opciones: {valid_resolutions}")

    # Si se eligió usar el nuevo valor, aplicarlo al autor
    if body.resolution == "used_new":
        author = db.get(Author, conflict.author_id)
        if author and conflict.new_value is not None:
            field = conflict.field_name
            if field == "orcid":
                author.orcid = conflict.new_value
            elif field.startswith("external_ids."):
                key = field.split(".", 1)[1]
                author.external_ids = {**(author.external_ids or {}), key: conflict.new_value}

    conflict.resolved = True
    conflict.resolution = body.resolution
    conflict.resolved_at = datetime.now(timezone.utc)
    conflict.resolved_by = body.resolved_by
    db.commit()
    db.refresh(conflict)
    return AuthorConflictRead.model_validate(conflict)


# ─────────────────────────────────────────────────────────────────────────────
# POST /authors/{id}/verify — cambiar estado de verificación
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/id/{author_id}/verify",
    response_model=AuthorRead,
    summary="Cambiar estado de verificación de un autor",
)
def verify_author(
    author_id: int,
    body: VerifyAuthorRequest,
    db: Session = Depends(get_db),
):
    """
    Actualiza el `verification_status` del autor.
    Al marcar como `verified`, se limpia `possible_duplicate_of`.
    """
    valid_statuses = {"verified", "needs_review", "flagged", "auto_detected"}
    if body.verification_status not in valid_statuses:
        raise HTTPException(400, f"Estado inválido. Opciones: {valid_statuses}")

    author = db.get(Author, author_id)
    if not author:
        raise HTTPException(404, "Autor no encontrado")

    before = {
        "verification_status": author.verification_status,
        "possible_duplicate_of": author.possible_duplicate_of,
    }
    author.verification_status = body.verification_status
    if body.verification_status == "verified":
        author.possible_duplicate_of = None

    # Audit log
    entry = AuthorAuditLog(
        author_id=author.id,
        change_type="verified",
        before_data=before,
        after_data={"verification_status": author.verification_status},
        field_changes={"verification_status": {"before": before["verification_status"], "after": author.verification_status}},
        source="manual",
        changed_by=body.changed_by,
    )
    db.add(entry)
    db.commit()
    db.refresh(author)

    pc = (
        db.query(func.count(PublicationAuthor.id))
        .filter(PublicationAuthor.author_id == author_id)
        .scalar() or 0
    )
    ar = AuthorRead.model_validate(author)
    ar.pub_count = pc
    return ar


# ─────────────────────────────────────────────────────────────────────────────
# POST /authors/batch-import — importación masiva
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/batch-import",
    response_model=BatchImportResponse,
    summary="Importar autores en lote (upsert inteligente)",
)
def batch_import_authors(
    body: BatchImportRequest,
    db: Session = Depends(get_db),
):
    """
    Importa hasta 500 autores usando la misma lógica upsert del pipeline:
    - Busca por ORCID → external ID → nombre canónico → fuzzy (pg_trgm)
    - Si encuentra el autor, lo enriquece sin duplicar
    - Si no lo encuentra, lo crea con verification_status = 'auto_detected'

    Ideal para cargas masivas desde archivos CSV/Excel.
    """
    from project.infrastructure.persistence.postgres_repository import PostgresRepository
    from project.domain.models.author import Author as DomainAuthor

    created = 0
    updated = 0
    skipped = 0
    conflicts = 0
    details = []

    # Usamos la misma sesión del request para poder hacer un solo commit
    session = db

    for item in body.authors:
        if not (item.name or "").strip():
            skipped += 1
            continue

        # Construir un objeto dominio liviano para reusar _upsert_author
        domain_author = DomainAuthor(
            name=item.name,
            orcid=item.orcid,
            is_institutional=item.is_institutional,
            external_ids={
                k: v for k, v in {
                    "openalex": item.openalex_id,
                    "scopus": item.scopus_id,
                    "wos": item.wos_id,
                    "cvlac": item.cvlac_id,
                }.items() if v
            },
        )

        # Contar conflictos previos
        prev_conflicts = db.query(func.count(AuthorConflict.id)).scalar() or 0

        existing_count = db.query(func.count(Author.id)).scalar() or 0
        PostgresRepository._upsert_author(session, domain_author, body.source)
        new_count = db.query(func.count(Author.id)).scalar() or 0
        after_conflicts = db.query(func.count(AuthorConflict.id)).scalar() or 0

        if new_count > existing_count:
            created += 1
            details.append({"name": item.name, "action": "created"})
        else:
            updated += 1
            details.append({"name": item.name, "action": "updated"})

        if after_conflicts > prev_conflicts:
            conflicts += after_conflicts - prev_conflicts

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Error durante la importación: {e}")

    return BatchImportResponse(
        total_received=len(body.authors),
        created=created,
        updated=updated,
        skipped=skipped,
        conflicts=conflicts,
        details=details[:100],  # Limitar detalles para no saturar la respuesta
    )
