"""
Router de Publicaciones Canónicas.
CRUD + consultas especializadas para el inventario bibliográfico.
"""

import math
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.utils import build_source_url
from api.schemas.common import PaginatedResponse
from api.schemas.publications import (
    PublicationRead,
    PublicationDetail,
    PublicationExistsResponse,
    FieldCoverageResponse,
    YearDistribution,
    ExternalRecordBrief,
    PublicationAuthorRead,
    DuplicatePublicationPair,
    DuplicatePublicationsSummary,
)
from db.models import (
    CanonicalPublication,
    Author,
    PublicationAuthor,
    SOURCE_MODELS,
    get_all_source_records_for_canonical,
    find_record_by_doi_across_sources,
)
from extractors.base import normalize_text, normalize_doi

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/publications", tags=["Publicaciones"])


# ── GET /publications ────────────────────────────────────────

@router.get("", response_model=PaginatedResponse[PublicationRead], summary="Listar publicaciones")
def list_publications(
    search: Optional[str] = Query(None, description="Buscar en título o DOI"),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    publication_type: Optional[str] = Query(None),
    source: Optional[str] = Query(None, description="Filtrar por fuente (openalex, scopus, etc.)"),
    is_open_access: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista paginada de publicaciones canónicas con filtros."""
    q = db.query(CanonicalPublication)

    if search:
        term = f"%{search}%"
        q = q.filter(
            or_(
                CanonicalPublication.title.ilike(term),
                CanonicalPublication.doi.ilike(term),
                CanonicalPublication.source_journal.ilike(term),
            )
        )
    if year_from:
        q = q.filter(CanonicalPublication.publication_year >= year_from)
    if year_to:
        q = q.filter(CanonicalPublication.publication_year <= year_to)
    if publication_type:
        q = q.filter(CanonicalPublication.publication_type == publication_type)
    if is_open_access is not None:
        q = q.filter(CanonicalPublication.is_open_access == is_open_access)
    if source:
        model_cls = SOURCE_MODELS.get(source)
        if model_cls:
            pub_ids_with_source = (
                db.query(model_cls.canonical_publication_id)
                .filter(model_cls.canonical_publication_id.isnot(None))
                .distinct()
            )
            q = q.filter(CanonicalPublication.id.in_(pub_ids_with_source))

    total = q.count()
    items = (
        q.order_by(CanonicalPublication.publication_year.desc().nullslast(), CanonicalPublication.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[PublicationRead.model_validate(p) for p in items],
        total=total,
        page=page,
        page_size=page_size,
    )


# ── GET /publications/exists ─────────────────────────────────

@router.get("/exists", response_model=PublicationExistsResponse, summary="Verificar si existe una publicación")
def check_publication_exists(
    doi: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """Verifica si un artículo ya existe en la BD (por DOI o título+año)."""
    if doi:
        ndoi = normalize_doi(doi)
        if ndoi:
            # Buscar en canonical_publications
            canon = (
                db.query(CanonicalPublication)
                .filter(CanonicalPublication.doi == ndoi)
                .first()
            )
            if canon:
                return PublicationExistsResponse(
                    exists=True, canonical_id=canon.id,
                    source="canonical", match_method="doi_exact",
                )
            # Buscar en tablas de fuente
            ext = find_record_by_doi_across_sources(db, ndoi)
            if ext and ext.canonical_publication_id:
                return PublicationExistsResponse(
                    exists=True, canonical_id=ext.canonical_publication_id,
                    source=ext.source_name, match_method="doi_external",
                )

    if title:
        ntitle = normalize_text(title)
        q = db.query(CanonicalPublication).filter(
            CanonicalPublication.normalized_title == ntitle
        )
        if year:
            q = q.filter(CanonicalPublication.publication_year == year)
        canon = q.first()
        if canon:
            return PublicationExistsResponse(
                exists=True, canonical_id=canon.id,
                source="canonical", match_method="title_year",
            )

    return PublicationExistsResponse(exists=False)


# ── GET /publications/by-year ────────────────────────────────

@router.get("/by-year", response_model=List[YearDistribution], summary="Publicaciones por año")
def publications_by_year(db: Session = Depends(get_db)):
    """Distribución de publicaciones por año."""
    rows = (
        db.query(
            CanonicalPublication.publication_year,
            func.count(CanonicalPublication.id),
        )
        .filter(CanonicalPublication.publication_year.isnot(None))
        .group_by(CanonicalPublication.publication_year)
        .order_by(CanonicalPublication.publication_year)
        .all()
    )
    return [YearDistribution(year=r[0], count=r[1]) for r in rows]


# ── GET /publications/field-coverage ─────────────────────────

@router.get("/field-coverage", response_model=FieldCoverageResponse, summary="Cobertura de campos")
def field_coverage(db: Session = Depends(get_db)):
    """Cobertura de campos en publicaciones canónicas."""
    total = db.query(func.count(CanonicalPublication.id)).scalar() or 0

    def count_non_null(col):
        return db.query(func.count(col)).filter(col.isnot(None), col != "").scalar() or 0

    return FieldCoverageResponse(
        total=total,
        with_doi=count_non_null(CanonicalPublication.doi),
        with_journal=count_non_null(CanonicalPublication.source_journal),
        with_issn=count_non_null(CanonicalPublication.issn),
        with_year=db.query(func.count(CanonicalPublication.id)).filter(
            CanonicalPublication.publication_year.isnot(None)
        ).scalar() or 0,
        with_type=count_non_null(CanonicalPublication.publication_type),
        with_language=count_non_null(CanonicalPublication.language),
        with_oa_info=db.query(func.count(CanonicalPublication.id)).filter(
            CanonicalPublication.is_open_access.isnot(None)
        ).scalar() or 0,
    )


# ── GET /publications/types ─────────────────────────────────

@router.get("/types", response_model=List[str], summary="Tipos de publicación")
def get_publication_types(db: Session = Depends(get_db)):
    """Lista de tipos de publicación distintos."""
    rows = (
        db.query(CanonicalPublication.publication_type)
        .filter(CanonicalPublication.publication_type.isnot(None))
        .distinct()
        .all()
    )
    return sorted([r[0] for r in rows])


# ── GET /publications/duplicates ─────────────────────────────────

@router.get(
    "/duplicates",
    response_model=DuplicatePublicationsSummary,
    summary="Detectar publicaciones canónicas posiblemente duplicadas",
)
def detect_duplicate_publications(
    min_similarity: float = Query(0.8, ge=0.5, le=1.0, description="Similitud mínima del título (0-1)"),
    year: Optional[int] = Query(None, description="Filtrar solo publicaciones de este año"),
    publication_type: Optional[str] = Query(None, description="Filtrar por tipo de publicación (ej. journal-article, book-chapter)"),
    limit: int = Query(100, ge=1, le=1000, description="Máximo de pares a retornar"),
    db: Session = Depends(get_db),
):
    """
    Detecta publicaciones canónicas que podrían ser duplicadas usando
    **rapidfuzz** en Python (sin restricción de año).

    Estrategia:

    1. Carga todas las publicaciones con título normalizado > 10 chars.
    2. Usa `rapidfuzz.process.extract` con `token_sort_ratio` (C optimizado)
       para comparar cada título contra todos los demás con `score_cutoff`.
    3. Detecta pares con mismo DOI normalizado (prioridad máxima).

    Clasifica cada par:
    - **Alta** (≥95 % o mismo DOI): Recomendación = merge
    - **Media** (85-95 %): Recomendación = review
    - **Baja** (< 85 %): Recomendación = keep_both
    """
    import time
    from collections import defaultdict
    from rapidfuzz import fuzz as rfuzz, process as rprocess

    t0 = time.perf_counter()
    min_score = min_similarity * 100  # rapidfuzz usa 0-100

    # --- 1. Cargar publicaciones candidatas ---
    q = db.query(
        CanonicalPublication.id,
        CanonicalPublication.doi,
        CanonicalPublication.title,
        CanonicalPublication.normalized_title,
        CanonicalPublication.publication_type,
        CanonicalPublication.publication_year,
    ).filter(
        CanonicalPublication.normalized_title.isnot(None),
        func.length(CanonicalPublication.normalized_title) > 10,
    )
    if year:
        q = q.filter(CanonicalPublication.publication_year == year)
    if publication_type:
        q = q.filter(func.lower(CanonicalPublication.publication_type) == publication_type.lower())

    all_pubs = q.all()
    n = len(all_pubs)
    logger.info(f"Duplicados: analizando {n} publicaciones")

    if n == 0:
        return DuplicatePublicationsSummary(
            total_pairs=0, high_confidence=0, medium_confidence=0,
            low_confidence=0, same_doi_different_id=0, pairs=[],
        )

    # Pre-construir lista de títulos normalizados (orden = all_pubs)
    titles = [p.normalized_title for p in all_pubs]

    # --- 2. Comparación fuzzy: cada pub vs todas las subsiguientes ---
    seen_pairs: set = set()
    raw_pairs: list = []  # (pub1, pub2, score)

    for i in range(n):
        # extract compara contra toda la lista; filtramos j > i después
        matches = rprocess.extract(
            titles[i],
            titles,
            scorer=rfuzz.token_sort_ratio,
            score_cutoff=min_score,
            limit=50,  # máximo vecinos cercanos por pub
        )
        for _match_str, match_score, j in matches:
            if j <= i:
                continue  # evitar self-match y pares ya vistos (j < i)
            pair_key = (all_pubs[i].id, all_pubs[j].id)
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            raw_pairs.append((all_pubs[i], all_pubs[j], match_score))

    # --- 3. Añadir pares de DOI duplicado que no hayan sido capturados ---
    doi_map: dict = defaultdict(list)
    for idx, pub in enumerate(all_pubs):
        if pub.doi:
            d = normalize_doi(pub.doi)
            if d:
                doi_map[d].append(pub)

    for doi_norm, pubs_with_doi in doi_map.items():
        if len(pubs_with_doi) < 2:
            continue
        for i_d in range(len(pubs_with_doi)):
            for j_d in range(i_d + 1, len(pubs_with_doi)):
                p1, p2 = pubs_with_doi[i_d], pubs_with_doi[j_d]
                pair_key = (min(p1.id, p2.id), max(p1.id, p2.id))
                if pair_key in seen_pairs:
                    # Ya existe; actualizar score si DOI coincide
                    for k, (rp1, rp2, sc) in enumerate(raw_pairs):
                        rkey = (min(rp1.id, rp2.id), max(rp1.id, rp2.id))
                        if rkey == pair_key:
                            raw_pairs[k] = (rp1, rp2, max(sc, 100.0))
                            break
                    continue
                seen_pairs.add(pair_key)
                score = rfuzz.token_sort_ratio(
                    p1.normalized_title, p2.normalized_title
                )
                raw_pairs.append((p1, p2, max(score, 100.0)))

    # --- 4. Ordenar por score desc y limitar ---
    raw_pairs.sort(key=lambda x: -x[2])
    raw_pairs = raw_pairs[:limit]

    # --- 5. Cargar fuentes de las publicaciones involucradas ---
    pub_ids = set()
    for p1, p2, _ in raw_pairs:
        pub_ids.add(p1.id)
        pub_ids.add(p2.id)

    sources_map: dict = {}
    if pub_ids:
        for source_name, model_cls in SOURCE_MODELS.items():
            ext_rows = (
                db.query(model_cls.canonical_publication_id)
                .filter(model_cls.canonical_publication_id.in_(pub_ids))
                .distinct()
                .all()
            )
            for (cpid,) in ext_rows:
                if cpid not in sources_map:
                    sources_map[cpid] = []
                if source_name not in sources_map[cpid]:
                    sources_map[cpid].append(source_name)

    # --- 6. Clasificar y construir respuesta ---
    pairs = []
    high = 0
    medium = 0
    low = 0
    same_doi_count = 0

    for p1, p2, score in raw_pairs:
        sim = round(score / 100.0, 4)
        same_doi = bool(
            p1.doi and p2.doi
            and normalize_doi(p1.doi) == normalize_doi(p2.doi)
        )
        same_year = bool(
            p1.publication_year and p2.publication_year
            and p1.publication_year == p2.publication_year
        )

        if same_doi:
            same_doi_count += 1

        if sim >= 0.95 or same_doi:
            recommendation = "merge"
            high += 1
        elif sim >= 0.85:
            recommendation = "review"
            medium += 1
        else:
            recommendation = "keep_both"
            low += 1

        # Calcular similitud de autores y diferencias
        autores1 = set(a.id for pa, a in db.query(PublicationAuthor, Author)
            .join(Author, PublicationAuthor.author_id == Author.id)
            .filter(PublicationAuthor.publication_id == p1.id)
            .all())
        autores2 = set(a.id for pa, a in db.query(PublicationAuthor, Author)
            .join(Author, PublicationAuthor.author_id == Author.id)
            .filter(PublicationAuthor.publication_id == p2.id)
            .all())
        if autores1 or autores2:
            if autores1 and autores2:
                inter = autores1 & autores2
                union = autores1 | autores2
                author_similarity = round(len(inter) / len(union), 3) if union else 0.0
                author_diff_1 = list(autores1 - autores2)
                author_diff_2 = list(autores2 - autores1)
            else:
                author_similarity = 0.0
                author_diff_1 = list(autores1)
                author_diff_2 = list(autores2)
        else:
            author_similarity = None
            author_diff_1 = []
            author_diff_2 = []
        pairs.append(DuplicatePublicationPair(
            canonical_id_1=p1.id,
            canonical_id_2=p2.id,
            doi_1=p1.doi,
            doi_2=p2.doi,
            title_1=p1.title,
            title_2=p2.title,
            type_1=p1.publication_type,
            type_2=p2.publication_type,
            year_1=p1.publication_year,
            year_2=p2.publication_year,
            sources_1=sorted(sources_map.get(p1.id, [])),
            sources_2=sorted(sources_map.get(p2.id, [])),
            similarity_score=sim,
            same_doi=same_doi,
            same_year=same_year,
            recommendation=recommendation,
            author_similarity=author_similarity,
            author_diff_1=author_diff_1,
            author_diff_2=author_diff_2,
            authors_1=[
                PublicationAuthorRead(
                    author_id=a.id,
                    author_name=a.name,
                    is_institutional=pa.is_institutional,
                    author_position=pa.author_position,
                    orcid=a.orcid,
                )
                for pa, a in db.query(PublicationAuthor, Author)
                    .join(Author, PublicationAuthor.author_id == Author.id)
                    .filter(PublicationAuthor.publication_id == p1.id)
                    .order_by(PublicationAuthor.author_position.asc().nullslast())
                    .all()
            ],
            authors_2=[
                PublicationAuthorRead(
                    author_id=a.id,
                    author_name=a.name,
                    is_institutional=pa.is_institutional,
                    author_position=pa.author_position,
                    orcid=a.orcid,
                )
                for pa, a in db.query(PublicationAuthor, Author)
                    .join(Author, PublicationAuthor.author_id == Author.id)
                    .filter(PublicationAuthor.publication_id == p2.id)
                    .order_by(PublicationAuthor.author_position.asc().nullslast())
                    .all()
            ],
        ))

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        f"Duplicados: {len(pairs)} pares en {elapsed}s "
        f"(alta={high}, media={medium}, baja={low}, doi_dup={same_doi_count})"
    )

    return DuplicatePublicationsSummary(
        total_pairs=len(pairs),
        high_confidence=high,
        medium_confidence=medium,
        low_confidence=low,
        same_doi_different_id=same_doi_count,
        pairs=pairs,
    )


# ── GET /publications/{id} (DEBE ir al final para no capturar rutas fijas) ──

@router.get("/{pub_id}", response_model=PublicationDetail, summary="Detalle de publicación")
def get_publication(pub_id: int, db: Session = Depends(get_db)):
    """Detalle de una publicación con registros externos y autores."""
    pub = db.query(CanonicalPublication).get(pub_id)
    if not pub:
        raise HTTPException(404, "Publicación no encontrada")

    # Registros de todas las fuentes
    ext_records = get_all_source_records_for_canonical(db, pub_id)
    ext_briefs = []
    source_links = {}
    for er in ext_records:
        url = build_source_url(er.source_name, er.source_id, er.doi)
        ext_briefs.append(ExternalRecordBrief(
            id=er.id,
            source_name=er.source_name,
            source_id=er.source_id,
            doi=er.doi,
            status=er.status,
            match_type=er.match_type,
            match_score=er.match_score,
            source_url=url,
        ))
        if url:
            source_links[er.source_name] = url

    # Autores
    pub_authors = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id == pub_id)
        .order_by(PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    authors_out = [
        PublicationAuthorRead(
            author_id=a.id,
            author_name=a.name,
            is_institutional=pa.is_institutional,
            author_position=pa.author_position,
            orcid=a.orcid,
        )
        for pa, a in pub_authors
    ]

    pub_data = PublicationRead.model_validate(pub)
    return PublicationDetail(
        **pub_data.model_dump(),
        external_records=ext_briefs,
        authors=authors_out,
        source_links=source_links,
    )

@router.get("/{pub_id}/authors", response_model=List[PublicationAuthorRead], summary="Listar autores de una publicación")
def list_publication_authors(pub_id: int, db: Session = Depends(get_db)):
    """
    Devuelve la lista de autores (con orden y metadatos) de una publicación específica.
    """
    from db.models import PublicationAuthor, Author
    pub_authors = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id == pub_id)
        .order_by(PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    return [
        PublicationAuthorRead(
            author_id=a.id,
            author_name=a.name,
            is_institutional=pa.is_institutional,
            author_position=pa.author_position,
            orcid=a.orcid,
        )
        for pa, a in pub_authors
    ]


@router.get("/author/{author_id}/possible-duplicates", response_model=DuplicatePublicationsSummary, 
            summary="Detectar posibles publicaciones duplicadas de un autor")
def get_author_possible_duplicates(
    author_id: int,
    min_similarity: float = Query(0.80, ge=0.0, le=1.0, description="Umbral mínimo de similitud (0-1)"),
    include_low: bool = Query(False, description="Incluir pares con similitud baja (0.80-0.85)"),
    db: Session = Depends(get_db),
):
    """
    Detecta posibles publicaciones duplicadas para un autor específico.
    
    Compara todas las publicaciones canónicas del autor usando:
    - **Similitud de títulos** (40% del peso)
    - **Mismo DOI** (40% del peso)  
    - **Mismo año de publicación** (10% del peso)
    - **Similitud de autores** (10% del peso)
    
    **Niveles de confianza:**
    - 🔴 **Alta (≥0.95 o mismo DOI)**: Recomendación "merge" - Fusionar publicaciones
    - 🟠 **Media (0.85-0.95)**: Recomendación "review" - Revisar manualmente (posible fusión)
    - 🟡 **Baja (0.80-0.85)**: Recomendación "keep_both" - Mantener separadas (similar pero distinto)
    
    **Casos especiales:**
    - Mismo DOI pero IDs canónicos diferentes = Error de reconciliación crítico
    - Mismo año + similitud alta + autores compartidos = Probable duplicado
    
    **Headers de respuesta:**
    - `total_pairs`: Número total de pares candidatos encontrados
    - `high_confidence`: Pares muy similares (fusión recomendada)
    - `medium_confidence`: Pares moderadamente similares (revisar)
    - `low_confidence`: Pares levemente similares (mantener separados)
    - `same_doi_different_id`: Pares con mismo DOI pero diferente ID canónico
    """
    from api.routers.publications_duplicates import find_possible_duplicates
    
    # Verificar que el autor existe
    author = db.query(Author).filter(Author.id == author_id).first()
    if not author:
        raise HTTPException(status_code=404, detail=f"Autor con ID {author_id} no encontrado")
    
    # Detectar duplicados
    result = find_possible_duplicates(
        db,
        author_id=author_id,
        min_title_similarity=min_similarity,
        include_low_confidence=include_low,
    )
    
    return result

