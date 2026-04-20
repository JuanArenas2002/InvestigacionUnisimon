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
    AutoMergeDuplicatesRequest,
    AutoMergeDuplicatesResponse,
    EstadoPublicacion,
)
from db.models import (
    CanonicalPublication,
    Author,
    PublicationAuthor,
    PossibleDuplicatePair,
    SOURCE_MODELS,
    get_all_source_records_for_canonical,
    find_record_by_doi_across_sources,
)
from extractors.base import normalize_text, normalize_doi
from shared.normalizers import normalize_publication_type
from api.routers.publications_duplicates import find_possible_duplicates
from project.application.use_cases.publications.merge import (
    MERGE_FIELDS,
    compute_field_inheritance,
    pick_keeper as _domain_pick_keeper,
    should_skip_pair,
    validate_merge_command,
)
from project.application.schemas.publication_schemas import (
    AutoMergeFilters,
    MergePublicationsCommand,
    PublicationSnapshot,
)

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
    needs_review: Optional[bool] = Query(
        None,
        description=(
            "True → solo canónicos sin registros de fuente vinculados "
            "(source records en manual_review o sin vincular). "
            "False → solo los que sí tienen al menos un source record vinculado."
        ),
    ),
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
        normalized_type = normalize_publication_type(publication_type)
        q = q.filter(CanonicalPublication.publication_type == normalized_type)
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

    # Filtro needs_review: canónicos sin source records vinculados.
    # Ocurre cuando Phase 1 crea el canónico por DOI pero el reconciliador
    # dejó el source record en manual_review (canonical_publication_id = NULL).
    if needs_review is not None:
        # Subquery: IDs de canónicos que SÍ tienen al menos un source record vinculado
        linked_ids_subqueries = [
            db.query(model_cls.canonical_publication_id)
            .filter(model_cls.canonical_publication_id.isnot(None))
            .subquery()
            for model_cls in SOURCE_MODELS.values()
        ]
        # UNION de todos los canonical_publication_id vinculados
        from sqlalchemy import union
        linked_union = union(*[sq.select() for sq in linked_ids_subqueries]).subquery()
        has_source = CanonicalPublication.id.in_(
            db.query(linked_union.c[0])
        )
        if needs_review:
            q = q.filter(~has_source)   # sin source records → necesita revisión
        else:
            q = q.filter(has_source)    # tiene source records → OK

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
    detect_translated: bool = Query(True, description="Detectar duplicados cross-idioma traduciendo títulos al inglés"),
    db: Session = Depends(get_db),
):
    """
    Detecta publicaciones canónicas que podrían ser duplicadas usando
    **rapidfuzz** en Python.

    **Fases de detección:**

    1. Carga publicaciones con título normalizado > 10 chars.
    2. Fuzzy `token_sort_ratio` agrupado por año ±1 (títulos en mismo idioma).
    3. DOI duplicados con distinto ID canónico (prioridad máxima).
    4. **Cross-language**: traduce títulos no-ingleses al inglés y compara
       contra publicaciones en inglés del mismo año ±2. Detecta el caso
       CvLAC/DatosAbiertos (español) ≡ Scopus/WoS/OpenAlex (inglés).
       Se puede desactivar con `detect_translated=false`.

    **Clasificación por par:**
    - **Alta** (≥95 % o mismo DOI): `merge`
    - **Media** (85-95 %): `review`
    - **Baja** (< 85 %): `keep_both`

    Los pares cross-language tienen `match_method = "translated_title"` e
    incluyen `translated_title_1`/`translated_title_2` para auditoría.
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
        CanonicalPublication.language,
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

    # --- 1.5. Pre-cargar autores de TODAS las publicaciones (una sola query) ---
    # Se usa tanto para cross-language (FASE 3.5) como para la respuesta (FASE 7).
    all_pub_ids = [p.id for p in all_pubs]
    _all_pa_rows = (
        db.query(PublicationAuthor.publication_id, PublicationAuthor.author_id)
        .filter(PublicationAuthor.publication_id.in_(all_pub_ids))
        .all()
    )
    # pub_id → frozenset de author_ids
    _authors_sets: dict = defaultdict(set)
    for pub_id, author_id in _all_pa_rows:
        _authors_sets[pub_id].add(author_id)
    authors_sets: dict = {pid: frozenset(s) for pid, s in _authors_sets.items()}

    if n == 0:
        return DuplicatePublicationsSummary(
            total_pairs=0, high_confidence=0, medium_confidence=0,
            low_confidence=0, same_doi_different_id=0, translation_matches=0, pairs=[],
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

    # --- 3.5. Cross-language: detectar duplicados por solapamiento de autores ---
    # Estrategia: sin llamadas a APIs externas.
    # Si dos publicaciones en idiomas distintos (español vs inglés) comparten
    # autores y tienen año cercano, son casi con certeza el mismo paper.
    #
    # Señal: Jaccard de author_ids >= 0.25 (al menos 1 de cada 4 autores compartido).
    # Velocidad: O(k²) por bucket de año — todo en memoria con los datos ya cargados.
    translated_pair_keys: set = set()

    if detect_translated:
        _ENGLISH_CODES = {"en", "eng"}

        def _is_english(pub) -> bool:
            lang = (pub.language or "").lower().strip()
            if lang in _ENGLISH_CODES:
                return True
            if lang and lang not in ("", "unknown"):
                return False
            try:
                from langdetect import detect
                return detect(pub.normalized_title or "") == "en"
            except Exception:
                return True  # asumir inglés si no se puede detectar

        non_english = [p for p in all_pubs if not _is_english(p)]
        english_pubs  = [p for p in all_pubs if _is_english(p)]

        if non_english and english_pubs:
            # Índice de pubs en inglés por año para lookup O(1)
            en_by_year: dict = defaultdict(list)
            for ep in english_pubs:
                en_by_year[ep.publication_year].append(ep)

            cross_found = 0
            for ne_pub in non_english:
                yr = ne_pub.publication_year
                if yr is None:
                    continue

                ne_authors = authors_sets.get(ne_pub.id, frozenset())
                if not ne_authors:
                    continue

                # Candidatos en inglés: año ±2
                en_candidates = []
                for delta in range(-2, 3):
                    en_candidates.extend(en_by_year.get(yr + delta, []))

                for en_pub in en_candidates:
                    pair_key = (min(ne_pub.id, en_pub.id), max(ne_pub.id, en_pub.id))
                    if pair_key in seen_pairs:
                        continue

                    en_authors = authors_sets.get(en_pub.id, frozenset())
                    if not en_authors:
                        continue

                    shared = len(ne_authors & en_authors)
                    if shared == 0:
                        continue

                    union = len(ne_authors | en_authors)
                    jaccard = shared / union if union else 0.0
                    if jaccard < 0.25:
                        continue

                    # Score: combinar jaccard de autores con similitud de título
                    # (aunque sean idiomas distintos, pueden compartir palabras
                    # técnicas, acrónimos y nombres propios)
                    title_sim = rfuzz.token_sort_ratio(
                        ne_pub.normalized_title or "",
                        en_pub.normalized_title or "",
                    )
                    # Score final: 60% autores + 40% título (los títulos son distintos
                    # por definición, pero el solapamiento léxico ayuda a calibrar)
                    score = jaccard * 60 + title_sim * 0.40
                    if score < min_score:
                        continue

                    seen_pairs.add(pair_key)
                    translated_pair_keys.add(pair_key)
                    raw_pairs.append((ne_pub, en_pub, score))
                    cross_found += 1

            logger.info(f"Duplicados cross-language (por autores): {cross_found} pares")

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
    translation_match_count = 0

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
        pair_key = (min(p1.id, p2.id), max(p1.id, p2.id))
        is_translated = pair_key in translated_pair_keys

        if same_doi:
            same_doi_count += 1
            match_method = "doi"
        elif is_translated:
            match_method = "cross_language"
            translation_match_count += 1
        else:
            match_method = "title"

        # Pares cross-language detectados por solapamiento de autores:
        # recomendación "review" siempre (requieren confirmación humana
        # ya que los títulos son en idiomas distintos).
        if same_doi:
            recommendation = "merge"
            high += 1
        elif sim >= 0.95 and not is_translated:
            recommendation = "merge"
            high += 1
        elif sim >= 0.85 or (is_translated and sim >= 0.80):
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
            match_method=match_method,
            translated_title_1=None,
            translated_title_2=None,
        ))

    elapsed = round(time.perf_counter() - t0, 2)

    # --- 8. Persistir pares encontrados en possible_duplicate_pairs ---
    # Upsert: insertar solo si no existe ya (status != merged/dismissed se respeta).
    seen_stored_keys: set = set()
    for pair in pairs:
        id1 = min(pair.canonical_id_1, pair.canonical_id_2)
        id2 = max(pair.canonical_id_1, pair.canonical_id_2)
        key = (id1, id2)
        if key in seen_stored_keys:
            continue
        seen_stored_keys.add(key)
        existing = (
            db.query(PossibleDuplicatePair)
            .filter_by(canonical_id_1=id1, canonical_id_2=id2)
            .first()
        )
        if not existing:
            try:
                db.add(PossibleDuplicatePair(
                    canonical_id_1=id1,
                    canonical_id_2=id2,
                    similarity_score=round(pair.similarity_score * 100, 2),
                    match_method=pair.match_method,
                    status="pending",
                ))
            except Exception:
                pass  # no romper la respuesta si falla el upsert
    try:
        db.commit()
    except Exception:
        db.rollback()

    # --- 9. Incluir pares almacenados del motor que no aparecieron on-the-fly ---
    stored_pairs = (
        db.query(PossibleDuplicatePair)
        .filter(PossibleDuplicatePair.status == "pending")
        .all()
    )
    fly_keys = {(min(p.canonical_id_1, p.canonical_id_2), max(p.canonical_id_1, p.canonical_id_2)) for p in pairs}
    engine_only_ids: set = set()
    for sp in stored_pairs:
        key = (sp.canonical_id_1, sp.canonical_id_2)
        if key not in fly_keys:
            engine_only_ids.add(sp.canonical_id_1)
            engine_only_ids.add(sp.canonical_id_2)

    if engine_only_ids:
        # Cargar datos de esos canónicos y construir pares sintéticos
        engine_pubs = {
            p.id: p
            for p in db.query(CanonicalPublication).filter(CanonicalPublication.id.in_(engine_only_ids)).all()
        }
        # Cargar autores en batch
        engine_pa_rows = (
            db.query(PublicationAuthor, Author)
            .join(Author, PublicationAuthor.author_id == Author.id)
            .filter(PublicationAuthor.publication_id.in_(engine_only_ids))
            .order_by(PublicationAuthor.publication_id, PublicationAuthor.author_position.asc().nullslast())
            .all()
        )
        engine_authors_by_pub: dict = defaultdict(list)
        for pa, a in engine_pa_rows:
            engine_authors_by_pub[pa.publication_id].append((pa, a))
        # Cargar sources
        engine_sources_map: dict = {}
        for source_name, model_cls in SOURCE_MODELS.items():
            rows = (
                db.query(model_cls.canonical_publication_id)
                .filter(model_cls.canonical_publication_id.in_(engine_only_ids))
                .distinct().all()
            )
            for (cpid,) in rows:
                if cpid not in engine_sources_map:
                    engine_sources_map[cpid] = []
                if source_name not in engine_sources_map[cpid]:
                    engine_sources_map[cpid].append(source_name)

        for sp in stored_pairs:
            key = (sp.canonical_id_1, sp.canonical_id_2)
            if key in fly_keys:
                continue
            p1 = engine_pubs.get(sp.canonical_id_1)
            p2 = engine_pubs.get(sp.canonical_id_2)
            if not p1 or not p2:
                continue
            sim = round(sp.similarity_score / 100.0, 4) if sp.similarity_score > 1.0 else round(sp.similarity_score, 4)
            same_doi = bool(p1.doi and p2.doi and normalize_doi(p1.doi) == normalize_doi(p2.doi))
            same_year = bool(p1.publication_year and p2.publication_year and p1.publication_year == p2.publication_year)
            if same_doi or sim >= 0.95:
                recommendation = "merge"
                high += 1
            elif sim >= 0.85:
                recommendation = "review"
                medium += 1
            else:
                recommendation = "keep_both"
                low += 1

            a1 = {a.id for _, a in engine_authors_by_pub.get(p1.id, [])}
            a2 = {a.id for _, a in engine_authors_by_pub.get(p2.id, [])}
            if a1 or a2:
                union_a = a1 | a2
                author_similarity = round(len(a1 & a2) / len(union_a), 3) if union_a else 0.0
                author_diff_1 = list(a1 - a2)
                author_diff_2 = list(a2 - a1)
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
                sources_1=sorted(engine_sources_map.get(p1.id, [])),
                sources_2=sorted(engine_sources_map.get(p2.id, [])),
                similarity_score=sim,
                same_doi=same_doi,
                same_year=same_year,
                recommendation=recommendation,
                author_similarity=author_similarity,
                author_diff_1=author_diff_1,
                author_diff_2=author_diff_2,
                authors_1=[PublicationAuthorRead.from_pa_author(pa, a) for pa, a in engine_authors_by_pub.get(p1.id, [])],
                authors_2=[PublicationAuthorRead.from_pa_author(pa, a) for pa, a in engine_authors_by_pub.get(p2.id, [])],
                match_method=sp.match_method,
                translated_title_1=None,
                translated_title_2=None,
            ))

    logger.info(
        f"Duplicados: {len(pairs)} pares en {elapsed}s "
        f"(alta={high}, media={medium}, baja={low}, "
        f"doi_dup={same_doi_count}, traduccion={translation_match_count}, "
        f"motor={len(engine_only_ids) // 2 if engine_only_ids else 0})"
    )

    return DuplicatePublicationsSummary(
        total_pairs=len(pairs),
        high_confidence=high,
        medium_confidence=medium,
        low_confidence=low,
        same_doi_different_id=same_doi_count,
        translation_matches=translation_match_count,
        pairs=pairs,
    )


# ── POST /publications/enrich-by-doi ─────────────────────────────────────────

@router.post(
    "/enrich-by-doi",
    response_model=dict,
    summary="Alimentar una publicación canónica desde todos los extractores usando su DOI",
    tags=["Publicaciones"],
)
async def enrich_publication_by_doi(
    doi: str = Body(..., embed=True, description="DOI de la publicación (ej: 10.1016/j.xxx.2020.01.001)"),
    db: Session = Depends(get_db),
):
    """
    Dado un DOI, consulta **todos los extractores** que soportan búsqueda por DOI
    (OpenAlex, Scopus, Web of Science), ingesta los registros encontrados,
    los reconcilia contra el canónico correspondiente y actualiza `citations_by_source`.

    Flujo:
    1. Normaliza el DOI.
    2. Busca el canónico existente por DOI (o lo crea si la reconciliación genera uno nuevo).
    3. Consulta en paralelo: OpenAlex → Scopus → WoS.
    4. Ingesta los registros nuevos (omite duplicados ya existentes).
    5. Reconcilia los pendientes contra el canónico.
    6. Fuerza enriquecimiento del canónico: completa campos vacíos y actualiza
       `citations_by_source` con los valores reportados por cada plataforma.
    7. Retorna el canónico actualizado con el desglose de citas por fuente.

    Útil para alimentar una publicación específica sin tener que correr el
    pipeline completo de todas las fuentes.
    """
    from starlette.concurrency import run_in_threadpool
    from extractors.scopus import ScopusExtractor, ScopusAPIError
    from extractors.wos import WosExtractor
    from extractors.openalex.extractor import OpenAlexExtractor
    from reconciliation.engine import ReconciliationEngine
    from extractors.base import normalize_doi as _norm_doi

    ndoi = _norm_doi(doi)
    if not ndoi:
        raise HTTPException(400, f"DOI inválido: '{doi}'")

    def _run():
        platform_results = {}
        records_to_ingest = []

        # ── OpenAlex (sin cuota, primero) ──
        try:
            rec = OpenAlexExtractor().search_by_doi(ndoi)
            if rec:
                records_to_ingest.append(rec)
                platform_results["openalex"] = {
                    "found": True,
                    "source_id": rec.source_id,
                    "citations": rec.citation_count,
                }
            else:
                platform_results["openalex"] = {"found": False}
        except Exception as e:
            platform_results["openalex"] = {"found": False, "error": str(e)}

        # ── Scopus ──
        try:
            rec = ScopusExtractor().search_by_doi(ndoi)
            if rec:
                records_to_ingest.append(rec)
                platform_results["scopus"] = {
                    "found": True,
                    "source_id": rec.source_id,
                    "citations": rec.citation_count,
                }
            else:
                platform_results["scopus"] = {"found": False}
        except ScopusAPIError as e:
            platform_results["scopus"] = {"found": False, "error": str(e)}
        except Exception as e:
            platform_results["scopus"] = {"found": False, "error": str(e)}

        # ── WoS ──
        try:
            rec = WosExtractor().search_by_doi(ndoi)
            if rec:
                records_to_ingest.append(rec)
                platform_results["wos"] = {
                    "found": True,
                    "source_id": rec.source_id,
                    "citations": rec.citation_count,
                }
            else:
                platform_results["wos"] = {"found": False}
        except Exception as e:
            platform_results["wos"] = {"found": False, "error": str(e)}

        engine = ReconciliationEngine(session=db)

        # Ingestar + reconciliar
        ingested = engine.ingest_records(records_to_ingest)
        recon_stats = None
        if ingested > 0:
            recon_stats = engine.reconcile_pending(batch_size=ingested + 10)

        # Encontrar el canónico resultante (por DOI normalizado)
        canonical = (
            db.query(CanonicalPublication)
            .filter(CanonicalPublication.doi == ndoi)
            .first()
        )

        enrich_result = {}
        if canonical:
            enrich_result = engine.enrich_canonical(canonical.id)
            db.commit()
            db.refresh(canonical)

        pub_data = None
        if canonical:
            from api.schemas.publications import PublicationDetail, ExternalRecordBrief
            from db.models import get_all_source_records_for_canonical
            from api.utils import get_clean_source_id

            ext_records = get_all_source_records_for_canonical(db, canonical.id)
            ext_briefs = [
                ExternalRecordBrief(
                    id=er.id, source_name=er.source_name, source_id=er.source_id,
                    doi=er.doi, status=er.status, match_type=er.match_type,
                    match_score=er.match_score,
                ).model_dump()
                for er in ext_records
            ]
            source_links = {
                er.source_name: get_clean_source_id(er.source_name, er.source_id)
                for er in ext_records
                if get_clean_source_id(er.source_name, er.source_id)
            }
            pub_data = {
                "id": canonical.id,
                "doi": canonical.doi,
                "title": canonical.title,
                "publication_year": canonical.publication_year,
                "publication_type": canonical.publication_type,
                "language": canonical.language,
                "source_journal": canonical.source_journal,
                "issn": canonical.issn,
                "abstract": canonical.abstract,
                "keywords": canonical.keywords,
                "page_range": canonical.page_range,
                "publisher": canonical.publisher,
                "is_open_access": canonical.is_open_access,
                "oa_status": canonical.oa_status,
                "citation_count": canonical.citation_count,
                "citations_by_source": canonical.citations_by_source or {},
                "sources_count": canonical.sources_count,
                "field_provenance": canonical.field_provenance or {},
                "external_records": ext_briefs,
                "source_links": source_links,
            }

        return {
            "doi": ndoi,
            "canonical_id": canonical.id if canonical else None,
            "platforms": platform_results,
            "records_ingested": ingested,
            "reconciliation": recon_stats.to_dict() if recon_stats else None,
            "enrichment": enrich_result,
            "publication": pub_data,
        }

    return await run_in_threadpool(_run)


# ── POST /publications/{id}/enrich-all ───────────────────────────────────────

@router.post(
    "/{pub_id}/enrich-all",
    response_model=dict,
    summary="Pipeline completo: buscar en todas las fuentes, reconciliar y enriquecer",
    tags=["Publicaciones"],
)
async def enrich_publication_all_sources(
    pub_id: int = Path(..., description="ID de la publicación canónica"),
    db: Session = Depends(get_db),
):
    """
    Pipeline completo para una publicación específica:

    1. Carga el canónico por `pub_id` y obtiene su DOI.
    2. Busca en **todas las fuentes** que soportan búsqueda por DOI
       (OpenAlex, Scopus, Web of Science).
    3. Ingesta los registros nuevos encontrados (omite duplicados).
    4. Reconcilia los pendientes y los vincula al canónico.
    5. Enriquece el canónico: completa campos vacíos y actualiza
       `citations_by_source` con los conteos de cada plataforma.
    6. Retorna el canónico actualizado con registros externos y autores.

    Diferencia con `/{pub_id}/fetch-all`: este endpoint además ejecuta
    el paso de enriquecimiento (`enrich_canonical`) y devuelve la
    publicación completa con todos sus datos actualizados.
    """
    from starlette.concurrency import run_in_threadpool
    from extractors.scopus import ScopusExtractor, ScopusAPIError
    from extractors.wos import WosExtractor
    from extractors.openalex.extractor import OpenAlexExtractor
    from reconciliation.engine import ReconciliationEngine
    from extractors.base import normalize_doi as _norm_doi

    pub = db.get(CanonicalPublication, pub_id)
    if not pub:
        raise HTTPException(404, "Publicación no encontrada")

    if not pub.doi:
        raise HTTPException(
            400,
            "La publicación no tiene DOI; no es posible buscarla en fuentes externas.",
        )

    ndoi = _norm_doi(pub.doi) or pub.doi

    def _run():
        platform_results = {}
        records_to_ingest = []

        # ── OpenAlex (sin cuota, siempre primero) ──────────────────────────
        try:
            rec = OpenAlexExtractor().search_by_doi(ndoi)
            if rec:
                records_to_ingest.append(rec)
                platform_results["openalex"] = {
                    "found": True,
                    "source_id": rec.source_id,
                    "citations": rec.citation_count,
                }
            else:
                platform_results["openalex"] = {"found": False}
        except Exception as e:
            platform_results["openalex"] = {"found": False, "error": str(e)}

        # ── Scopus ─────────────────────────────────────────────────────────
        try:
            rec = ScopusExtractor().search_by_doi(ndoi)
            if rec:
                records_to_ingest.append(rec)
                platform_results["scopus"] = {
                    "found": True,
                    "source_id": rec.source_id,
                    "citations": rec.citation_count,
                }
            else:
                platform_results["scopus"] = {"found": False}
        except ScopusAPIError as e:
            platform_results["scopus"] = {"found": False, "error": str(e)}
        except Exception as e:
            platform_results["scopus"] = {"found": False, "error": str(e)}

        # ── Web of Science ─────────────────────────────────────────────────
        try:
            rec = WosExtractor().search_by_doi(ndoi)
            if rec:
                records_to_ingest.append(rec)
                platform_results["wos"] = {
                    "found": True,
                    "source_id": rec.source_id,
                    "citations": rec.citation_count,
                }
            else:
                platform_results["wos"] = {"found": False}
        except Exception as e:
            platform_results["wos"] = {"found": False, "error": str(e)}

        engine = ReconciliationEngine(session=db)

        # ── Ingesta + reconciliación ───────────────────────────────────────
        # Always reconcile: pre-existing pending records (ingested=0 on dedup)
        # still need to be linked to the canonical before enrichment.
        # We first reconcile any pending records that match this DOI across ALL
        # source tables (they may have been ingested in a previous call), then
        # run a general pass for anything newly ingested.
        ingested = engine.ingest_records(records_to_ingest)

        from db.models import SOURCE_MODELS
        from config import RecordStatus
        pending_for_doi = []
        for model_cls in SOURCE_MODELS.values():
            pending_for_doi.extend(
                db.query(model_cls)
                .filter(
                    model_cls.doi == ndoi,
                    model_cls.status == RecordStatus.PENDING,
                )
                .all()
            )
        engine._cache = engine._build_cache()
        for rec in pending_for_doi:
            try:
                engine._reconcile_one(rec)
            except Exception:
                pass
        if pending_for_doi or ingested > 0:
            db.flush()

        recon_stats = engine.reconcile_pending(batch_size=max(ingested + 20, 50))

        # ── Enriquecimiento del canónico ───────────────────────────────────
        enrich_result = engine.enrich_canonical(pub_id)
        db.commit()
        db.refresh(pub)

        # ── Construir respuesta completa ───────────────────────────────────
        from api.schemas.publications import PublicationDetail, ExternalRecordBrief
        from db.models import get_all_source_records_for_canonical
        from api.utils import get_clean_source_id

        ext_records = get_all_source_records_for_canonical(db, pub_id)
        ext_briefs = [
            ExternalRecordBrief(
                id=er.id, source_name=er.source_name, source_id=er.source_id,
                doi=er.doi, status=er.status, match_type=er.match_type,
                match_score=er.match_score,
            ).model_dump()
            for er in ext_records
        ]
        source_links = {
            er.source_name: get_clean_source_id(er.source_name, er.source_id)
            for er in ext_records
            if get_clean_source_id(er.source_name, er.source_id)
        }

        pub_authors = (
            db.query(PublicationAuthor, Author)
            .join(Author, PublicationAuthor.author_id == Author.id)
            .filter(PublicationAuthor.publication_id == pub_id)
            .order_by(PublicationAuthor.author_position.asc().nullslast())
            .all()
        )
        authors_out = [
            {"id": a.id, "name": a.name, "orcid": a.orcid,
             "position": pa.author_position}
            for pa, a in pub_authors
        ]

        return {
            "pub_id": pub_id,
            "doi": ndoi,
            "platforms": platform_results,
            "records_ingested": ingested,
            "reconciliation": recon_stats.to_dict() if recon_stats else None,
            "enrichment": enrich_result,
            "publication": {
                "id": pub.id,
                "doi": pub.doi,
                "title": pub.title,
                "publication_year": pub.publication_year,
                "publication_type": pub.publication_type,
                "language": pub.language,
                "source_journal": pub.source_journal,
                "issn": pub.issn,
                "abstract": pub.abstract,
                "keywords": pub.keywords,
                "publisher": pub.publisher,
                "is_open_access": pub.is_open_access,
                "oa_status": pub.oa_status,
                "citation_count": pub.citation_count,
                "citations_by_source": pub.citations_by_source or {},
                "sources_count": pub.sources_count,
                "field_provenance": pub.field_provenance or {},
                "external_records": ext_briefs,
                "source_links": source_links,
                "authors": authors_out,
            },
        }

    return await run_in_threadpool(_run)


# ── POST /publications/{id}/fetch-all ────────────────────────────────────────

@router.post(
    "/{pub_id}/fetch-all",
    response_model=dict,
    summary="Buscar e ingestar publicación en todas las plataformas",
    tags=["Publicaciones"],
)
async def fetch_publication_all_sources(
    pub_id: int = Path(..., description="ID de la publicación canónica"),
    db: Session = Depends(get_db),
):
    """
    Busca la publicación en **todas las plataformas externas** (Scopus, WoS, OpenAlex)
    usando su DOI, ingesta los registros encontrados y los reconcilia contra el canónico.

    Flujo:
    1. Obtiene la publicación canónica por ID.
    2. Usa el DOI para buscar en Scopus, WoS y OpenAlex.
    3. Ingesta los registros nuevos (deduplicación automática).
    4. Ejecuta reconciliación sobre los pendientes.

    Respuesta: resultados por plataforma + estadísticas de reconciliación.
    """
    from starlette.concurrency import run_in_threadpool
    from extractors.scopus import ScopusExtractor, ScopusAPIError
    from extractors.wos import WosExtractor
    from extractors.openalex.extractor import OpenAlexExtractor
    from reconciliation.engine import ReconciliationEngine

    pub = db.get(CanonicalPublication, pub_id)
    if not pub:
        raise HTTPException(404, "Publicación no encontrada")

    doi = pub.doi
    if not doi:
        raise HTTPException(400, "La publicación no tiene DOI; no se puede buscar en fuentes externas")

    def _fetch_and_ingest():
        platform_results = {}
        records_to_ingest = []

        # ── Scopus ──
        try:
            scopus = ScopusExtractor()
            record = scopus.search_by_doi(doi)
            if record:
                records_to_ingest.append(record)
                platform_results["scopus"] = {"found": True, "source_id": record.source_id}
            else:
                platform_results["scopus"] = {"found": False}
        except ScopusAPIError as e:
            platform_results["scopus"] = {"found": False, "error": str(e)}
        except Exception as e:
            platform_results["scopus"] = {"found": False, "error": str(e)}

        # ── WoS ──
        try:
            wos = WosExtractor()
            record = wos.search_by_doi(doi)
            if record:
                records_to_ingest.append(record)
                platform_results["wos"] = {"found": True, "source_id": record.source_id}
            else:
                platform_results["wos"] = {"found": False}
        except Exception as e:
            platform_results["wos"] = {"found": False, "error": str(e)}

        # ── OpenAlex ──
        try:
            openalex = OpenAlexExtractor()
            record = openalex.search_by_doi(doi)
            if record:
                records_to_ingest.append(record)
                platform_results["openalex"] = {"found": True, "source_id": record.source_id}
            else:
                platform_results["openalex"] = {"found": False}
        except Exception as e:
            platform_results["openalex"] = {"found": False, "error": str(e)}

        # ── Ingesta + reconciliación ──
        engine = ReconciliationEngine(session=db)
        ingested = engine.ingest_records(records_to_ingest)
        recon_stats = engine.reconcile_pending(batch_size=len(records_to_ingest) + 10) if ingested > 0 else None

        return {
            "pub_id": pub_id,
            "doi": doi,
            "platforms": platform_results,
            "records_ingested": ingested,
            "reconciliation": recon_stats.to_dict() if recon_stats else None,
        }

    return await run_in_threadpool(_fetch_and_ingest)


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
    )

@router.get("/{pub_id}/source-links", summary="Links de acceso por fuente", tags=["Publicaciones"])
def get_publication_source_links(pub_id: int, db: Session = Depends(get_db)):
    """
    Devuelve, para cada fuente donde está registrada la publicación, todos los
    links de acceso disponibles: URL directa almacenada, URL canónica de la
    plataforma (construida a partir del ID), link DOI, y para OpenAlex también
    la URL open-access y el PDF si existen.

    Útil para mostrar un panel de \"Ver en...\" en el frontend.
    """
    pub = db.get(CanonicalPublication, pub_id)
    if not pub:
        raise HTTPException(404, "Publicación no encontrada")

    from project.infrastructure.persistence.source_registry import SOURCE_REGISTRY

    def _openalex_profile_url(work_id: str) -> str:
        if not work_id:
            return None
        wid = work_id.strip()
        if wid.startswith("https://openalex.org/"):
            return wid
        return f"https://openalex.org/{wid}"

    def _scopus_profile_url(record) -> str:
        eid = getattr(record, "eid", None) or getattr(record, "scopus_doc_id", None)
        if not eid:
            return None
        if str(eid).startswith("2-s2.0-"):
            return f"https://www.scopus.com/record/display.uri?eid={eid}"
        return f"https://www.scopus.com/inward/record.uri?partnerID=HzOxMe3b&scp={eid}"

    def _wos_profile_url(wos_uid: str) -> str:
        if not wos_uid:
            return None
        uid = wos_uid.strip()
        if uid.startswith("WOS:"):
            return f"https://www.webofscience.com/wos/woscc/full-record/{uid}"
        return f"https://www.webofscience.com/wos/woscc/full-record/WOS:{uid}"

    _BUILDERS = {
        "openalex":      lambda r: _openalex_profile_url(r.source_id),
        "scopus":        lambda r: _scopus_profile_url(r),
        "wos":           lambda r: _wos_profile_url(r.source_id),
        "cvlac":         lambda r: getattr(r, "url", None),
        "datos_abiertos": lambda r: getattr(r, "url", None),
        "gruplac":       lambda r: getattr(r, "url", None),
    }

    doi_url = f"https://doi.org/{pub.doi}" if pub.doi else None

    sources_out = []
    for src_def in SOURCE_REGISTRY.all():
        model_cls = src_def.model_class
        record = (
            db.query(model_cls)
            .filter_by(canonical_publication_id=pub_id)
            .first()
        )
        if record is None:
            continue

        build_url = _BUILDERS.get(src_def.name, lambda r: getattr(r, "url", None))
        profile_url = build_url(record)

        entry = {
            "source":      src_def.name,
            "source_id":   record.source_id,
            "profile_url": profile_url,
            "stored_url":  getattr(record, "url", None),
            "doi":         record.doi,
            "status":      record.status,
        }
        if src_def.name == "openalex":
            entry["oa_url"]  = getattr(record, "oa_url", None)
            entry["pdf_url"] = getattr(record, "pdf_url", None)

        sources_out.append(entry)

    return {
        "publication_id": pub_id,
        "title":          pub.title,
        "doi_url":        doi_url,
        "sources_count":  len(sources_out),
        "sources":        sources_out,
    }


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

    # --- Enriquecer campos nulos del keeper con datos del removable ---
    keeper_data = {f: getattr(keeper, f, None) for f in MERGE_FIELDS}
    removable_data = {f: getattr(removable, f, None) for f in MERGE_FIELDS}
    updates, _ = compute_field_inheritance(
        keeper_data, removable_data,
        dict(keeper.field_provenance or {}),
        dict(removable.field_provenance or {}),
        merge_label="merged",
    )
    for attr, val in updates.items():
        setattr(keeper, attr, val)

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


# ── POST /publications/auto-merge-duplicates ─────────────────────────────────


def _do_merge(db: Session, keeper: CanonicalPublication, removable: CanonicalPublication) -> None:
    """Fusiona removable en keeper: autores, source records, campos vacíos, sources_count."""
    keeper_author_ids = {
        row[0] for row in
        db.query(PublicationAuthor.author_id)
        .filter(PublicationAuthor.publication_id == keeper.id).all()
    }
    for link in db.query(PublicationAuthor).filter(PublicationAuthor.publication_id == removable.id).all():
        if link.author_id in keeper_author_ids:
            db.delete(link)
        else:
            link.publication_id = keeper.id
            db.add(link)

    for model_cls in SOURCE_MODELS.values():
        for rec in db.query(model_cls).filter(model_cls.canonical_publication_id == removable.id).all():
            rec.canonical_publication_id = keeper.id
            db.add(rec)

    keeper_data = {f: getattr(keeper, f, None) for f in MERGE_FIELDS}
    removable_data = {f: getattr(removable, f, None) for f in MERGE_FIELDS}
    updates, _ = compute_field_inheritance(
        keeper_data, removable_data,
        dict(keeper.field_provenance or {}),
        dict(removable.field_provenance or {}),
        merge_label="auto_merged",
    )
    for attr, val in updates.items():
        setattr(keeper, attr, val)
    keeper.sources_count = (keeper.sources_count or 1) + (removable.sources_count or 1)
    db.delete(removable)


def _pick_keeper(p1: CanonicalPublication, p2: CanonicalPublication) -> tuple:
    """Delegates to domain use case."""
    snap1 = PublicationSnapshot(
        id=p1.id, title=p1.title, doi=p1.doi,
        publication_year=p1.publication_year,
        publication_type=p1.publication_type,
        sources_count=p1.sources_count or 0,
        field_provenance=dict(p1.field_provenance or {}),
    )
    snap2 = PublicationSnapshot(
        id=p2.id, title=p2.title, doi=p2.doi,
        publication_year=p2.publication_year,
        publication_type=p2.publication_type,
        sources_count=p2.sources_count or 0,
        field_provenance=dict(p2.field_provenance or {}),
    )
    keeper_snap, removable_snap = _domain_pick_keeper(snap1, snap2)
    return (p1, p2) if keeper_snap.id == p1.id else (p2, p1)


@router.post(
    "/auto-merge-duplicates",
    response_model=AutoMergeDuplicatesResponse,
    summary="Fusionar automáticamente pares duplicados con alta similitud",
)
def auto_merge_duplicates(
    body: AutoMergeDuplicatesRequest,
    db: Session = Depends(get_db),
):
    """
    Recorre la tabla `possible_duplicate_pairs` con `status='pending'`,
    aplica los filtros del body y fusiona los pares que cumplan las condiciones.

    **Criterio de selección del keeper:**
    1. Más `sources_count` (más fuentes vinculadas).
    2. Más campos rellenos.
    3. ID menor (más antiguo) como tie-break.

    **Herencia de campos:** todos los campos nulos del keeper se completan
    con los valores del removable (título, DOI, abstract, keywords, etc.).
    Los autores y registros fuente del removable se reasignan al keeper.

    **dry_run=true:** reporta qué se haría sin modificar la BD.
    """
    # Umbral en escala 0-100 (como está guardado en la tabla)
    min_score_db = body.min_similarity * 100.0

    pending_pairs = (
        db.query(PossibleDuplicatePair)
        .filter(
            PossibleDuplicatePair.status == "pending",
            PossibleDuplicatePair.similarity_score >= min_score_db,
        )
        .order_by(PossibleDuplicatePair.similarity_score.desc())
        .all()
    )

    merged_pairs = []
    skipped_pairs = []

    for sp in pending_pairs:
        p1 = db.get(CanonicalPublication, sp.canonical_id_1)
        p2 = db.get(CanonicalPublication, sp.canonical_id_2)

        if not p1 or not p2:
            skipped_pairs.append({
                "canonical_id_1": sp.canonical_id_1,
                "canonical_id_2": sp.canonical_id_2,
                "reason": "uno o ambos canónicos ya no existen",
            })
            if not body.dry_run:
                # El canónico ya no existe → par huérfano, borrar directo
                db.delete(sp)
            continue

        skip_reason = should_skip_pair(
            p1_doi=p1.doi, p2_doi=p2.doi,
            p1_type=p1.publication_type, p2_type=p2.publication_type,
            p1_year=p1.publication_year, p2_year=p2.publication_year,
            filters=AutoMergeFilters(
                only_same_year=body.only_same_year,
                skip_doi_conflicts=body.skip_doi_conflicts,
                skip_type_conflicts=body.skip_type_conflicts,
            ),
        )
        if skip_reason:
            skipped_pairs.append({
                "canonical_id_1": p1.id, "canonical_id_2": p2.id,
                "title_1": p1.title, "title_2": p2.title,
                "reason": skip_reason,
                "similarity_score": sp.similarity_score,
            })
            continue

        # Guard: al menos 1 autor institucional compartido
        if body.require_shared_author:
            authors1 = {
                row[0] for row in
                db.query(PublicationAuthor.author_id)
                .filter(PublicationAuthor.publication_id == p1.id).all()
            }
            authors2 = {
                row[0] for row in
                db.query(PublicationAuthor.author_id)
                .filter(PublicationAuthor.publication_id == p2.id).all()
            }
            if not (authors1 & authors2):
                skipped_pairs.append({
                    "canonical_id_1": p1.id, "canonical_id_2": p2.id,
                    "title_1": p1.title, "title_2": p2.title,
                    "reason": "ningún autor institucional compartido",
                    "similarity_score": sp.similarity_score,
                })
                continue

        keeper, removable = _pick_keeper(p1, p2)

        merged_pairs.append({
            "kept_id": keeper.id,
            "merged_id": removable.id,
            "kept_title": keeper.title,
            "merged_title": removable.title,
            "similarity_score": sp.similarity_score,
            "match_method": sp.match_method,
            "keeper_sources": keeper.sources_count,
            "removable_sources": removable.sources_count,
        })

        if not body.dry_run:
            try:
                # Guardar datos del par antes de expunge (para log de error si falla)
                sp_id1, sp_id2 = sp.canonical_id_1, sp.canonical_id_2
                # Detach del par de la sesión ANTES del merge:
                # _do_merge borra removable → ON DELETE CASCADE borra el par en la BD.
                # Si sp sigue trackeado, SQLAlchemy intenta UPDATE sobre la fila ya
                # eliminada → StaleDataError. Expunge lo evita.
                db.expunge(sp)
                _do_merge(db, keeper, removable)
            except Exception as exc:
                db.rollback()
                merged_pairs[-1]["error"] = str(exc)
                logger.error(f"Error fusionando {p1.id}↔{p2.id}: {exc}")

    if not body.dry_run:
        try:
            db.commit()
        except IntegrityError as e:
            db.rollback()
            raise HTTPException(500, f"Error en auto-merge: {e}")

    return AutoMergeDuplicatesResponse(
        dry_run=body.dry_run,
        pairs_evaluated=len(pending_pairs),
        pairs_merged=len(merged_pairs),
        pairs_skipped=len(skipped_pairs),
        merged_pairs=merged_pairs,
        skipped_pairs=skipped_pairs,
    )

