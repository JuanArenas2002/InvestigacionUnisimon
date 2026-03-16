"""
Utility module para detectar posibles duplicados en publicaciones.
🚀 OPTIMIZADO PARA RENDIMIENTO CON BATCH QUERIES Y FILTROS TEMPRANOS

Usado por:
- GET /publications/author/{author_id}/possible-duplicates
"""

from typing import List, Tuple, Dict, Set
from sqlalchemy.orm import Session, selectinload, joinedload
from sqlalchemy import and_, or_, func, select

from db.models import CanonicalPublication, PublicationAuthor, Author, SOURCE_MODELS
from reconciliation.fuzzy_matcher import compare_titles, compare_authors
from api.schemas.publications import (
    DuplicatePublicationPair,
    DuplicatePublicationsSummary,
    PublicationAuthorRead,
)


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZACIÓN 1: Eager Loading de Relaciones
# ─────────────────────────────────────────────────────────────────────────────

def get_author_publications_optimized(db: Session, author_id: int) -> List[CanonicalPublication]:
    """
    Obtiene todas las publicaciones canónicas de un autor con EAGER LOADING.
    
    🚀 Reduce N+1 queries a 1 main query + bulk loading de relations.
    """
    # Query 1: Obtener todas las publicaciones del autor
    publications = (
        db.query(CanonicalPublication)
        .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.publication_id)
        .filter(PublicationAuthor.author_id == author_id)
        .distinct()
        .all()
    )
    
    # Query 2: Cargar todos los autores de esas publicaciones (bulk load)
    if publications:
        pub_ids = [p.id for p in publications]
        pub_authors = (
            db.query(PublicationAuthor, Author)
            .join(Author)
            .filter(PublicationAuthor.publication_id.in_(pub_ids))
            .all()
        )
        
        # Mapear autores a publicaciones en memoria para acceso rápido
        author_map = {}
        for pa, author in pub_authors:
            if pa.publication_id not in author_map:
                author_map[pa.publication_id] = []
            author_map[pa.publication_id].append((pa, author))
        
        # Asignar directamente a cada publicación (evita lazy load)
        for pub in publications:
            pub._authors_cache = author_map.get(pub.id, [])
    
    return publications


def get_publication_authors_cached(
    db: Session, 
    pub_id: int,
    authors_cache: Dict[int, List[PublicationAuthorRead]] = None
) -> List[PublicationAuthorRead]:
    """
    Obtiene autores con caché para evitar queries repetidas.
    """
    if authors_cache is None:
        authors_cache = {}
    
    if pub_id in authors_cache:
        return authors_cache[pub_id]
    
    pub_authors = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id == pub_id)
        .order_by(PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    
    result = [
        PublicationAuthorRead(
            author_id=a.id,
            author_name=a.name,
            is_institutional=pa.is_institutional,
            author_position=pa.author_position,
            orcid=a.orcid,
        )
        for pa, a in pub_authors
    ]
    
    authors_cache[pub_id] = result
    return result


def get_all_publication_authors_batch(
    db: Session,
    pub_ids: List[int]
) -> Dict[int, List[PublicationAuthorRead]]:
    """
    Carga TODOS los autores para múltiples publicaciones en UNA SOLA QUERY.
    
    🚀 En lugar de N queries, hace 1 query.
    """
    if not pub_ids:
        return {}
    
    pub_authors = (
        db.query(PublicationAuthor, Author)
        .join(Author, PublicationAuthor.author_id == Author.id)
        .filter(PublicationAuthor.publication_id.in_(pub_ids))
        .order_by(PublicationAuthor.publication_id, PublicationAuthor.author_position.asc().nullslast())
        .all()
    )
    
    result: Dict[int, List[PublicationAuthorRead]] = {pid: [] for pid in pub_ids}
    
    for pa, a in pub_authors:
        result[pa.publication_id].append(
            PublicationAuthorRead(
                author_id=a.id,
                author_name=a.name,
                is_institutional=pa.is_institutional,
                author_position=pa.author_position,
                orcid=a.orcid,
            )
        )
    
    return result


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZACIÓN 2: Batch Query de Asociaciones Fuente-Publicación
# ─────────────────────────────────────────────────────────────────────────────

def get_source_names_batch(db: Session, pub_ids: List[int]) -> Dict[int, List[str]]:
    """
    Obtiene TODAS las asociaciones fuente-publicación en UNA query.
    
    🚀 En lugar de N queries (una por publicación), hace 1 query por fuente.
    """
    result: Dict[int, List[str]] = {pid: [] for pid in pub_ids}
    
    for src_name, Model in SOURCE_MODELS.items():
        records = (
            db.query(Model.canonical_publication_id)
            .filter(Model.canonical_publication_id.in_(pub_ids))
            .distinct()
            .all()
        )
        
        for (pub_id,) in records:
            result[pub_id].append(src_name)
    
    return result


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZACIÓN 3: Filtros de Pre-screening para Descartar Trivialmente
# ─────────────────────────────────────────────────────────────────────────────

def should_skip_pair(pub1: CanonicalPublication, pub2: CanonicalPublication) -> bool:
    """
    Descarta pares que obviamente no son duplicados ANTES de hacer cálculos costosos.
    
    🚀 Evita comparaciones innecesarias de título/autores.
    """
    
    # Si ambas tienen DOI diferentes, probablemente no son duplicados
    if pub1.doi and pub2.doi:
        doi1_norm = normalize_doi(pub1.doi)
        doi2_norm = normalize_doi(pub2.doi)
        if doi1_norm and doi2_norm and doi1_norm != doi2_norm:
            # DOIs diferentes = No duplicados (a menos que sea error de reconciliación)
            return False  # Continuar (podría ser error, pero poco probable)
    
    # Si los años están más de 3 años alejados, descartamos
    if pub1.publication_year and pub2.publication_year:
        year_diff = abs(pub1.publication_year - pub2.publication_year)
        if year_diff > 3:
            return True  # Saltar este par
    
    # Si los títulos son MUY diferentes en longitud, descartamos
    len1 = len(pub1.title or "")
    len2 = len(pub2.title or "")
    if len1 > 0 and len2 > 0:
        length_ratio = min(len1, len2) / max(len1, len2)
        if length_ratio < 0.6:  # Menos del 60% de similitud en longitud
            return True
    
    return False


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZACIÓN 4: Caché de Similitudes
# ─────────────────────────────────────────────────────────────────────────────

def calculate_duplicate_pair_optimized(
    db: Session,
    pub1: CanonicalPublication,
    pub2: CanonicalPublication,
    authors_cache: Dict[int, List[PublicationAuthorRead]],
    sources_cache: Dict[int, List[str]],
    title_sim_cache: Dict[Tuple[str, str], float] = None,
    min_title_similarity: float = 0.80
) -> Tuple[DuplicatePublicationPair, float]:
    """
    Calcula similitud entre dos publicaciones CON CACHÉ de cálculos.
    
    🚀 Evita recalcular similitudes de títulos ya comparados.
    """
    
    if title_sim_cache is None:
        title_sim_cache = {}
    
    # Similitud de títulos (con caché)
    title_key = tuple(sorted([pub1.title or "", pub2.title or ""]))
    if title_key in title_sim_cache:
        title_sim = title_sim_cache[title_key]
    else:
        title_sim = compare_titles(pub1.title or "", pub2.title or "")
        title_sim_cache[title_key] = title_sim
    
    # Mismo DOI
    same_doi = False
    if pub1.doi and pub2.doi and normalize_doi(pub1.doi) == normalize_doi(pub2.doi):
        same_doi = True
    
    # Mismo año
    same_year = pub1.publication_year == pub2.publication_year
    
    # Similitud de autores (desde caché)
    pub1_authors = authors_cache.get(pub1.id, [])
    pub2_authors = authors_cache.get(pub2.id, [])
    
    pub1_author_ids = {a.author_id for a in pub1_authors}
    pub2_author_ids = {a.author_id for a in pub2_authors}
    
    intersection = len(pub1_author_ids & pub2_author_ids)
    union = len(pub1_author_ids | pub2_author_ids)
    author_sim = intersection / union if union > 0 else 0.0
    
    author_diff_1 = list(pub1_author_ids - pub2_author_ids)
    author_diff_2 = list(pub2_author_ids - pub1_author_ids)
    
    # Calcular puntuación ponderada (genera valores en rango 0-40.6 aprox)
    overall_score = (
        title_sim * 0.40 +
        (1.0 if same_doi else 0.0) * 0.40 +
        (1.0 if same_year else 0.0) * 0.10 +
        author_sim * 0.10
    )
    
    # Normalizar a rango 0-100 para consistencia
    # El máximo posible es: 100*0.40 + 1*0.40 + 1*0.10 + 1*0.10 = 40.6
    # Se de normaliza multiplicando por 100/40.6 ≈ 2.46
    overall_score_normalized = (overall_score / 40.6) * 100
    
    # Determinar recomendación
    if same_doi or (title_sim >= 0.95 and (same_year or author_sim >= 0.7)):
        recommendation = "merge"
    elif title_sim >= 0.85 and author_sim >= 0.5:
        recommendation = "review"
    else:
        recommendation = "keep_both"
    
    # Obtener fuentes desde caché
    sources_1 = sources_cache.get(pub1.id, [])
    sources_2 = sources_cache.get(pub2.id, [])
    
    pair = DuplicatePublicationPair(
        canonical_id_1=pub1.id,
        canonical_id_2=pub2.id,
        doi_1=pub1.doi,
        doi_2=pub2.doi,
        title_1=pub1.title or "",
        title_2=pub2.title or "",
        type_1=pub1.publication_type,
        type_2=pub2.publication_type,
        year_1=pub1.publication_year,
        year_2=pub2.publication_year,
        sources_1=sources_1,
        sources_2=sources_2,
        similarity_score=overall_score_normalized,
        same_doi=same_doi,
        same_year=same_year,
        recommendation=recommendation,
        author_similarity=author_sim,
        author_diff_1=author_diff_1,
        author_diff_2=author_diff_2,
        authors_1=pub1_authors,
        authors_2=pub2_authors,
    )
    
    return pair, overall_score_normalized


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZACIÓN 5: Algoritmo Principal Optimizado
# ─────────────────────────────────────────────────────────────────────────────

def find_possible_duplicates_optimized(
    db: Session,
    author_id: int,
    min_title_similarity: float = 0.80,
    include_low_confidence: bool = False,
    max_pairs: int = 100  # Limitar outputs para evitar sobrecarga
) -> DuplicatePublicationsSummary:
    """
    🚀 VERSIÓN OPTIMIZADA - Detecta duplicados con excelente rendimiento.
    
    Optimizaciones aplicadas:
    1. ✅ Eager loading de relaciones
    2. ✅ Batch queries en lugar de N queries
    3. ✅ Pre-screening de pares obviamente no duplicados
    4. ✅ Caché de cálculos (similitud de títulos, autores)
    5. ✅ Índices en BD (debe estar configurado en models.py)
    6. ✅ Límite de outputs para evitar transferencias masivas
    
    Complejidad: O(n²) pero con constantes muy bajas por pre-screening y caché.
    
    Nota: min_title_similarity viene en escala 0-1 desde el endpoint, se convierte a 0-100 internamente.
    """
    
    # Convertir min_title_similarity de escala 0-1 a escala 0-100 (usada por score)
    min_title_similarity_100 = min_title_similarity * 100
    
    # FASE 1: Cargar publicaciones con eager loading
    publications = get_author_publications_optimized(db, author_id)
    
    if len(publications) < 2:
        return DuplicatePublicationsSummary(pairs=[])
    
    # FASE 2: Precarga de datos en batch (no N queries)
    pub_ids = [p.id for p in publications]
    
    # Precarga de autores (batch) - Una sola query para todos los autores
    authors_cache = get_all_publication_authors_batch(db, pub_ids)
    
    # Precarga de fuentes (batch)
    sources_cache = get_source_names_batch(db, pub_ids)
    
    # FASE 3: Comparar pares CON pre-screening y caché
    pairs = []
    seen_pairs: Set[Tuple[int, int]] = set()
    title_sim_cache: Dict[Tuple[str, str], float] = {}
    
    for i, pub1 in enumerate(publications):
        for pub2 in publications[i + 1:]:
            # Evitar duplicados
            pair_key = tuple(sorted([pub1.id, pub2.id]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            
            # PRE-SCREENING: Descartar temprano pares obviamente diferentes
            if should_skip_pair(pub1, pub2):
                continue
            
            # Calcular similitud (con caché)
            pair, score = calculate_duplicate_pair_optimized(
                db, pub1, pub2,
                authors_cache,
                sources_cache,
                title_sim_cache,
                min_title_similarity
            )
            
            # Filtrar por similitud mínima
            if score >= min_title_similarity_100 or pair.same_doi:
                pairs.append((pair, score))
    
    # FASE 4: Ordenar y filtrar
    pairs.sort(key=lambda x: x[1], reverse=True)
    
    # Aplicar filtro y límite
    filtered_pairs = [
        p for p, s in pairs
        if (include_low_confidence or s >= 85 or p.same_doi)
    ][:max_pairs]
    
    # Agrupar por confianza (sobre los pares filtrados que se devuelven)
    high_confidence = sum(1 for p, s in [(p, p.similarity_score) for p in filtered_pairs] if s >= 95 or p.same_doi)
    medium_confidence = sum(1 for p, s in [(p, p.similarity_score) for p in filtered_pairs] if 85 <= s < 95)
    low_confidence = sum(1 for p, s in [(p, p.similarity_score) for p in filtered_pairs] if 80 <= s < 85)
    same_doi_different_id = sum(1 for p in filtered_pairs if p.same_doi)
    
    return DuplicatePublicationsSummary(
        total_pairs=len(filtered_pairs),
        high_confidence=high_confidence,
        medium_confidence=medium_confidence,
        low_confidence=low_confidence,
        same_doi_different_id=same_doi_different_id,
        pairs=filtered_pairs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize_doi(doi: str) -> str:
    """Normaliza un DOI para comparación."""
    if not doi:
        return ""
    return doi.lower().strip()


# ─────────────────────────────────────────────────────────────────────────────
# LEGACY: Mantener para compatibilidad backward
# ─────────────────────────────────────────────────────────────────────────────

def get_author_publications(db: Session, author_id: int) -> List[CanonicalPublication]:
    """Compatibilidad backward."""
    return get_author_publications_optimized(db, author_id)


def find_possible_duplicates(
    db: Session,
    author_id: int,
    min_title_similarity: float = 0.80,
    include_low_confidence: bool = False
) -> DuplicatePublicationsSummary:
    """Compatibilidad backward - Usa versión optimizada."""
    return find_possible_duplicates_optimized(
        db, author_id, min_title_similarity, include_low_confidence
    )

