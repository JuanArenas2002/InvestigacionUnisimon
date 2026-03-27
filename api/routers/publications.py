"""
Router de Publicaciones Canónicas.
CRUD + consultas especializadas para el inventario bibliográfico.
"""

import logging
import time
from collections import defaultdict
from typing import Optional, List

from fastapi import APIRouter, Depends, Query, HTTPException, Path, Body
from sqlalchemy import func, or_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.utils import get_clean_source_id
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
    MergePublicationsRequest,
    MergePublicationsResponse,
    EstadoPublicacion,
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
from api.routers.publications_duplicates import find_possible_duplicates

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/publications", tags=["Publicaciones"])


@router.get("/estados", summary="Listar estados posibles de publicación", tags=["Publicaciones"])
def listar_estados_publicacion(db: Session = Depends(get_db)):
    """Devuelve la lista de estados posibles para publicaciones canónicas."""
    estados = db.execute(text("SELECT id, nombre, descripcion FROM publicacion_estados ORDER BY id")).fetchall()
    return [
        {"id": row[0], "nombre": row[1], "descripcion": row[2]} for row in estados
    ]


@router.patch("/{publicacion_id}/estado", summary="Cambiar estado de publicación", tags=["Publicaciones"])
def cambiar_estado_publicacion(
    publicacion_id: int = Path(..., description="ID de la publicación canónica"),
    estado_id: int = Body(..., embed=True, description="ID del nuevo estado (ver /publications/estados)"),
    db: Session = Depends(get_db)
):
    """Cambia el estado de una publicación canónica por su ID."""
    publicacion = db.query(CanonicalPublication).filter(CanonicalPublication.id == publicacion_id).first()
    if not publicacion:
        raise HTTPException(status_code=404, detail="Publicación no encontrada")
    estado_row = db.execute(text("SELECT id, nombre FROM publicacion_estados WHERE id = :id"), {"id": estado_id}).fetchone()
    if not estado_row:
        raise HTTPException(status_code=400, detail="Estado no válido")
    publicacion.estado_publicacion = estado_row[1]
    db.commit()
    return {"ok": True, "publicacion_id": publicacion_id, "nuevo_estado_id": estado_id}


# ── GET /publications ────────────────────────────────────────

@router.get("", summary="Listar publicaciones canónicas con estado", tags=["Publicaciones"])
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
    institutional_only: Optional[bool] = Query(None, description="Si es True, solo publicaciones institucionales (institutional_authors_count > 0). Si es False, solo no institucionales. Si es None, todas."),
):
    """Lista paginada de publicaciones canónicas con filtros y estado (id y nombre)."""
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
            # EXISTS es más eficiente que IN (subquery) en PostgreSQL para sets grandes
            exists_subq = (
                db.query(model_cls.canonical_publication_id)
                .filter(model_cls.canonical_publication_id == CanonicalPublication.id)
                .exists()
            )
            q = q.filter(exists_subq)

    # Filtro institucional
    if institutional_only is not None:
        if institutional_only:
            q = q.filter(CanonicalPublication.institutional_authors_count > 1)
        else:
            q = q.filter(CanonicalPublication.institutional_authors_count <= 1)

    total = q.count()
    items = (
        q.order_by(CanonicalPublication.publication_year.desc().nullslast(), CanonicalPublication.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )


    result_items = []
    for p in items:
        estado_nombre = getattr(p, "estado_publicacion", None)
        pub_dict = PublicationRead.model_validate(p).model_dump()
        pub_dict["estado"] = EstadoPublicacion(nombre=estado_nombre).model_dump() if estado_nombre else None
        result_items.append(pub_dict)

    return PaginatedResponse.create(
        items=result_items,
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
    """Cobertura de campos en publicaciones canónicas. Una sola query con COUNT FILTER."""
    from sqlalchemy import case

    cp = CanonicalPublication
    row = db.query(
        func.count(cp.id),
        func.count(case((cp.doi.isnot(None) & (cp.doi != ""), 1))),
        func.count(case((cp.source_journal.isnot(None) & (cp.source_journal != ""), 1))),
        func.count(case((cp.issn.isnot(None) & (cp.issn != ""), 1))),
        func.count(case((cp.publication_year.isnot(None), 1))),
        func.count(case((cp.publication_type.isnot(None) & (cp.publication_type != ""), 1))),
        func.count(case((cp.language.isnot(None) & (cp.language != ""), 1))),
        func.count(case((cp.is_open_access.isnot(None), 1))),
    ).one()

    return FieldCoverageResponse(
        total=row[0] or 0,
        with_doi=row[1] or 0,
        with_journal=row[2] or 0,
        with_issn=row[3] or 0,
        with_year=row[4] or 0,
        with_type=row[5] or 0,
        with_language=row[6] or 0,
        with_oa_info=row[7] or 0,
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

    # --- 2. Comparación fuzzy agrupada por año (F5 optimization) ---
    # Sin filtro de año: comparamos solo dentro del mismo año ±1.
    # Esto reduce comparaciones de O(n²) global a O(k²) × n_años
    # donde k = publicaciones por año (~100-200 en este dataset).
    # Publicaciones sin año se comparan entre sí en un grupo separado.
    seen_pairs: set = set()
    raw_pairs: list = []  # (pub1, pub2, score)

    # Agrupar por año; None va a su propio grupo
    year_buckets: dict = defaultdict(list)
    for pub in all_pubs:
        year_buckets[pub.publication_year].append(pub)

    def _compare_bucket(bucket: list) -> None:
        """Compara todos los títulos dentro de un bucket con rapidfuzz."""
        titles = [p.normalized_title for p in bucket]
        for i in range(len(bucket)):
            matches = rprocess.extract(
                titles[i],
                titles,
                scorer=rfuzz.token_sort_ratio,
                score_cutoff=min_score,
                limit=50,
            )
            for _, match_score, j in matches:
                if j <= i:
                    continue
                pair_key = (bucket[i].id, bucket[j].id)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                raw_pairs.append((bucket[i], bucket[j], match_score))

    if year:
        # Ya filtrado en la query — comparar todo el conjunto
        _compare_bucket(all_pubs)
    else:
        # Comparar dentro de cada año y también entre años adyacentes (±1)
        distinct_years = sorted(y for y in year_buckets if y is not None)
        for idx, yr in enumerate(distinct_years):
            current = year_buckets[yr]
            _compare_bucket(current)
            # Cruzar con el año siguiente (cubre duplicados con año reportado diferente)
            if idx + 1 < len(distinct_years) and distinct_years[idx + 1] == yr + 1:
                adjacent = year_buckets[distinct_years[idx + 1]]
                # Solo comparar current contra adjacent (no re-comparar current×current)
                titles_adj = [p.normalized_title for p in adjacent]
                for pub in current:
                    matches = rprocess.extract(
                        pub.normalized_title,
                        titles_adj,
                        scorer=rfuzz.token_sort_ratio,
                        score_cutoff=min_score,
                        limit=50,
                    )
                    for _, match_score, j in matches:
                        pair_key = (min(pub.id, adjacent[j].id), max(pub.id, adjacent[j].id))
                        if pair_key in seen_pairs:
                            continue
                        seen_pairs.add(pair_key)
                        raw_pairs.append((pub, adjacent[j], match_score))
        # Publicaciones sin año: comparar entre sí
        if year_buckets[None]:
            _compare_bucket(year_buckets[None])

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

    # --- 6. Cargar autores de TODAS las publicaciones involucradas en batch (evita N+1) ---
    pub_ids_for_authors = set()
    for p1, p2, _ in raw_pairs:
        pub_ids_for_authors.add(p1.id)
        pub_ids_for_authors.add(p2.id)

    # Una sola query trae todos los autores necesarios
    authors_batch = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id.in_(pub_ids_for_authors))
        .order_by(PublicationAuthor.publication_id, PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    # Indexar por publication_id para lookup O(1)
    authors_by_pub: dict = defaultdict(list)
    for pa, a in authors_batch:
        authors_by_pub[pa.publication_id].append((pa, a))

    # --- 7. Clasificar y construir respuesta ---
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

        # Calcular similitud de autores usando datos ya cargados (sin queries adicionales)
        autores1 = {a.id for pa, a in authors_by_pub.get(p1.id, [])}
        autores2 = {a.id for pa, a in authors_by_pub.get(p2.id, [])}
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
                PublicationAuthorRead.from_pa_author(pa, a)
                for pa, a in authors_by_pub.get(p1.id, [])
            ],
            authors_2=[
                PublicationAuthorRead.from_pa_author(pa, a)
                for pa, a in authors_by_pub.get(p2.id, [])
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
    pub = db.get(CanonicalPublication, pub_id)
    if not pub:
        raise HTTPException(404, "Publicación no encontrada")

    # Registros de todas las fuentes
    ext_records = get_all_source_records_for_canonical(db, pub_id)
    ext_briefs = []
    source_links = {}
    for er in ext_records:
        ext_briefs.append(ExternalRecordBrief(
            id=er.id,
            source_name=er.source_name,
            source_id=er.source_id,
            doi=er.doi,
            status=er.status,
            match_type=er.match_type,
            match_score=er.match_score,
        ))
        clean_id = get_clean_source_id(er.source_name, er.source_id)
        if clean_id:
            source_links[er.source_name] = clean_id

    # Autores
    pub_authors = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id == pub_id)
        .order_by(PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    authors_out = [PublicationAuthorRead.from_pa_author(pa, a) for pa, a in pub_authors]

    pub_data = PublicationRead.model_validate(pub)
    return PublicationDetail(
        **pub_data.model_dump(),
        external_records=ext_briefs,
        authors=authors_out,
        source_links=source_links,
        field_conflicts=pub.field_conflicts or {},
        citations_by_source=pub.citations_by_source or {},
    )

@router.get("/{pub_id}/authors", response_model=List[PublicationAuthorRead], summary="Listar autores de una publicación")
def list_publication_authors(pub_id: int, db: Session = Depends(get_db)):
    """
    Devuelve la lista de autores (con orden y metadatos) de una publicación específica.
    """
    pub_authors = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id == pub_id)
        .order_by(PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    return [PublicationAuthorRead.from_pa_author(pa, a) for pa, a in pub_authors]


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


@router.post("/merge", response_model=MergePublicationsResponse, summary="Fusionar dos publicaciones canónicas")
def merge_publications(body: MergePublicationsRequest, db: Session = Depends(get_db)):
    """
    Fusiona dos publicaciones canónicas: conserva la de keep_id y absorbe la de merge_id.
    Reasigna autores y registros fuente, y elimina la publicación merge_id.
    """
    if body.keep_id == body.merge_id:
        raise HTTPException(400, "keep_id y merge_id deben ser diferentes")
    keeper = db.get(CanonicalPublication, body.keep_id)
    removable = db.get(CanonicalPublication, body.merge_id)
    if not keeper or not removable:
        raise HTTPException(404, "No se encontró alguna de las publicaciones")

    # --- Reasignar autores ---
    authors_to_update = db.query(PublicationAuthor).filter(
        PublicationAuthor.publication_id == removable.id
    ).all()
    keeper_authors = db.query(PublicationAuthor.author_id).filter(
        PublicationAuthor.publication_id == keeper.id
    ).all()
    keeper_author_ids = {row[0] for row in keeper_authors}
    for author_link in authors_to_update:
        if author_link.author_id in keeper_author_ids:
            db.delete(author_link)
        else:
            author_link.publication_id = keeper.id
            db.add(author_link)

    # --- Reasignar registros fuente ---
    for source_name, model_cls in SOURCE_MODELS.items():
        records_to_update = db.query(model_cls).filter(
            model_cls.canonical_publication_id == removable.id
        ).all()
        for record in records_to_update:
            record.canonical_publication_id = keeper.id
            db.add(record)

    # --- Unir field_provenance ---
    if removable.field_provenance:
        if keeper.field_provenance is None:
            keeper.field_provenance = {}
        for field, source in removable.field_provenance.items():
            if field not in keeper.field_provenance:
                keeper.field_provenance[field] = source

    # --- Actualizar sources_count ---
    keeper.sources_count = (keeper.sources_count or 1) + (removable.sources_count or 1)

    # --- Eliminar la publicación fusionada ---
    db.delete(removable)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(500, f"Error en la fusión: {e}")

    return MergePublicationsResponse(
        kept_publication_id=keeper.id,
        merged_publication_id=removable.id,
        message=f"Publicación #{removable.id} fusionada en #{keeper.id} correctamente."
    )

