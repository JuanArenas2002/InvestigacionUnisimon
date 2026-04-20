"""
Motor de Reconciliación Bibliográfica.

Orquesta el proceso de vincular registros de múltiples fuentes
a publicaciones canónicas únicas.

Arquitectura de tablas por fuente:
  Cada fuente (openalex, scopus, wos, cvlac, datos_abiertos) tiene
  su propia tabla con columnas tipadas. La reconciliación busca
  coincidencias y vincula/crea registros canónicos.

Flujo en cascada:
  ┌─────────────────────────────────────────────────────┐
  │  Por cada registro fuente con status='pending':     │
  │                                                     │
  │  PASO 1: ¿Tiene DOI?                               │
  │    SÍ → Buscar en canonical_publications            │
  │       Encontrado → VINCULAR (doi_exact)             │
  │       No encontrado → Buscar en TODAS las tablas    │
  │         de fuente con mismo DOI ya reconciliado     │
  │           Encontrado → VINCULAR al mismo canon      │
  │           No encontrado → ir a PASO 3               │
  │    NO → ir a PASO 2                                 │
  │                                                     │
  │  PASO 2: Fuzzy matching                             │
  │    Comparar título+año+autores contra TODOS         │
  │    los canonical_publications                       │
  │      Score >= combined_threshold → VINCULAR         │
  │      Score >= manual_review → MARCAR REVISIÓN       │
  │      Score < manual_review → ir a PASO 2.3          │
  │                                                     │
  │  PASO 2.3: Fuzzy con título traducido al inglés     │
  │    Si el título entrante no está en inglés,         │
  │    traducirlo (deep-translator, caché in-memory)    │
  │    y repetir fuzzy. Requiere author_score >= 40     │
  │    como guard contra falsos positivos.              │
  │      Match → VINCULAR como FUZZY_COMBINED           │
  │      Manual review → MARCAR REVISIÓN                │
  │      Sin match → ir a PASO 2.5                      │
  │                                                     │
  │  PASO 2.5: Match por IDs externos de autores        │
  │    (solo registros sin DOI)                         │
  │      ≥ 2 IDs compartidos → VINCULAR                 │
  │      Sin match → ir a PASO 3                        │
  │                                                     │
  │  PASO 3: Crear nueva publicación canónica           │
  │    Insertar en canonical_publications               │
  │    Vincular el registro fuente                      │
  └─────────────────────────────────────────────────────┘
"""

import hashlib
import logging
import re as _re
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Any, Set

# Fijar semilla de langdetect para resultados reproducibles
try:
    from langdetect import DetectorFactory as _DetectorFactory
    _DetectorFactory.seed = 0
except ImportError:
    pass

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from config import (
    reconciliation_config as rc_config,
    criteria_config,
    MatchType,
    RecordStatus,
)
from db.models import (
    CanonicalPublication,
    Author,
    PublicationAuthor,
    ReconciliationLog,
    PossibleDuplicatePair,
    SOURCE_MODELS,
    find_record_by_doi_across_sources,
)
from db.source_registry import SOURCE_REGISTRY
from db.session import get_session
from extractors.base import StandardRecord, normalize_text, normalize_doi
from shared.normalizers import normalize_publication_type, normalize_author_name
from reconciliation.fuzzy_matcher import compare_records, FuzzyMatchResult

logger = logging.getLogger(__name__)


# Mapeo source_name → id_attr derivado del registry (no hardcodeado)
_SOURCE_ID_ATTR = SOURCE_REGISTRY.id_attrs


# =============================================================
# ESTADÍSTICAS DE RECONCILIACIÓN
# =============================================================

class ReconciliationStats:
    """Acumulador de estadísticas del proceso"""

    def __init__(self):
        self.total_processed = 0
        self.doi_exact_matches = 0
        self.fuzzy_high_matches = 0
        self.fuzzy_combined_matches = 0
        self.manual_review = 0
        self.new_canonical = 0
        self.errors = 0

    def to_dict(self) -> dict:
        return {
            "total_processed": self.total_processed,
            "doi_exact_matches": self.doi_exact_matches,
            "fuzzy_high_matches": self.fuzzy_high_matches,
            "fuzzy_combined_matches": self.fuzzy_combined_matches,
            "manual_review": self.manual_review,
            "new_canonical_created": self.new_canonical,
            "errors": self.errors,
        }

    def __repr__(self):
        return (
            f"ReconciliationStats("
            f"processed={self.total_processed}, "
            f"doi_match={self.doi_exact_matches}, "
            f"fuzzy_high={self.fuzzy_high_matches}, "
            f"fuzzy_combined={self.fuzzy_combined_matches}, "
            f"review={self.manual_review}, "
            f"new={self.new_canonical}, "
            f"errors={self.errors})"
        )


# =============================================================
# MOTOR DE RECONCILIACIÓN
# =============================================================

class ReconciliationEngine:
    """
    Motor principal que ejecuta la cascada de reconciliación.

    Uso:
        engine = ReconciliationEngine()
        stats = engine.reconcile_batch(records)
        # o
        stats = engine.reconcile_pending()  # procesa lo que esté en DB con status=pending
    """

    def __init__(self, session: Session = None):
        self.session = session or get_session()
        self.stats = ReconciliationStats()
        # Cache in-memory; se inicializa en reconcile_pending
        self._cache: Optional[Dict] = None
        # Caché de traducciones: título_original → título_en_inglés (o None si ya era inglés)
        self._translation_cache: Dict[str, Optional[str]] = {}
        # Caché de detección de idioma: texto → código ISO (ej: 'es', 'en')
        self._lang_cache: Dict[str, str] = {}

    # ---------------------------------------------------------
    # API PÚBLICA
    # ---------------------------------------------------------

    @staticmethod
    def _compute_dedup_hash(source_name: str, source_id=None, doi=None,
                            normalized_title=None, year=None) -> str:
        """
        Genera un hash determinista para deduplicación.
        Mismo registro → mismo hash, sin importar cuántas veces se ejecute.
        """
        parts = [
            str(source_name or ""),
            str(source_id or ""),
            str(doi or ""),
            str(normalized_title or "")[:200],
            str(year or ""),
        ]
        raw = "|".join(parts).lower().strip()
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    # ---------------------------------------------------------
    # CONSTRUCCIÓN DE REGISTROS POR FUENTE
    # ---------------------------------------------------------

    def _build_source_record(self, record: StandardRecord, dedup: str) -> Any:
        """
        Crea la instancia del modelo correcto según la fuente,
        poblando columnas comunes + columnas específicas.
        """
        model_cls = SOURCE_MODELS.get(record.source_name)
        if not model_cls:
            raise ValueError(f"Fuente desconocida: {record.source_name}")

        # Enriquecer raw_data con autores parseados
        enriched_raw = dict(record.raw_data) if record.raw_data else {}
        enriched_raw["_parsed_authors"] = record.authors or []
        enriched_raw["_parsed_institutional_authors"] = record.institutional_authors or []

        # --- Campos comunes (del mixin) ---
        kwargs = dict(
            dedup_hash=dedup,
            doi=record.doi,
            title=record.title,
            normalized_title=record.normalized_title,
            publication_year=record.publication_year,
            publication_date=record.publication_date,
            publication_type=normalize_publication_type(record.publication_type),
            source_journal=record.source_journal,
            issn=record.issn,
            language=record.language,
            is_open_access=record.is_open_access,
            oa_status=record.oa_status,
            citation_count=record.citation_count or 0,
            authors_text=record.authors_text,
            normalized_authors=record.normalized_authors,
            url=record.url,
            raw_data=enriched_raw,
            status=RecordStatus.PENDING,
        )

        # --- Campos específicos por fuente (delegado al registry) ---
        # Para agregar una nueva fuente: crear sources/nueva_fuente.py y listo.
        raw = record.raw_data or {}
        source_def = SOURCE_REGISTRY.get(record.source_name)
        source_def.build_specific_kwargs(record, raw, kwargs)

        return model_cls(**kwargs)

    # ---------------------------------------------------------
    # INGESTA
    # ---------------------------------------------------------

    def ingest_records(self, records: List[StandardRecord]) -> int:
        """
        PASO 0: Ingesta — inserta StandardRecords en la tabla de fuente correcta.
        Los marca como 'pending' para reconciliación posterior.

        Protección anti-duplicados en 4 niveles (ahora con pre-carga bulk):
          1. Hash determinista (dedup_hash) — pre-cargado en set
          2. source_id — pre-cargado en set por fuente
          3. DOI dentro de la tabla — pre-cargado en set por fuente
          4. título normalizado+año — pre-cargado en set por fuente

        Returns:
            Número de registros insertados (excluyendo duplicados)
        """
        if not records:
            return 0

        # ── Pre-cargar sets de dedup por fuente (bulk, 4 queries por fuente) ──
        # Agrupa por source_name para cargar solo los modelos que se usan
        source_names_used = {r.source_name for r in records}
        existing: Dict[str, Dict[str, set]] = {}

        for src_name in source_names_used:
            model_cls = SOURCE_MODELS.get(src_name)
            if not model_cls:
                continue
            sid_attr = _SOURCE_ID_ATTR.get(src_name)

            hashes = {
                row[0]
                for row in self.session.query(model_cls.dedup_hash).all()
                if row[0]
            }
            sids = (
                {
                    str(row[0])
                    for row in self.session.query(getattr(model_cls, sid_attr)).all()
                    if row[0]
                }
                if sid_attr
                else set()
            )
            dois = {
                row[0]
                for row in self.session.query(model_cls.doi).filter(
                    model_cls.doi.isnot(None)
                ).all()
                if row[0]
            }
            title_year = {
                (row[0], row[1])
                for row in self.session.query(
                    model_cls.normalized_title, model_cls.publication_year
                ).filter(
                    model_cls.normalized_title.isnot(None),
                    model_cls.publication_year.isnot(None),
                ).all()
                if row[0] and row[1]
            }
            existing[src_name] = {
                "hashes": hashes,
                "sids": sids,
                "dois": dois,
                "title_year": title_year,
            }

        inserted = 0
        skipped = 0

        for record in records:
            try:
                model_cls = SOURCE_MODELS.get(record.source_name)
                if not model_cls:
                    logger.warning(f"Fuente desconocida: {record.source_name}, omitiendo")
                    skipped += 1
                    continue

                ex = existing.get(record.source_name, {})
                sid_attr = _SOURCE_ID_ATTR.get(record.source_name)

                # --- Calcular hash de deduplicación ---
                dedup = self._compute_dedup_hash(
                    record.source_name, record.source_id,
                    record.doi, record.normalized_title, record.publication_year,
                )

                # --- Nivel 1: dedup_hash (in-memory) ---
                if dedup in ex.get("hashes", set()):
                    skipped += 1
                    continue

                # --- Nivel 2: source_id (in-memory) ---
                if record.source_id and sid_attr:
                    if str(record.source_id) in ex.get("sids", set()):
                        skipped += 1
                        continue

                # --- Nivel 3: DOI (in-memory) ---
                if record.doi:
                    ndoi = normalize_doi(record.doi)
                    if ndoi and ndoi in ex.get("dois", set()):
                        skipped += 1
                        continue

                # --- Nivel 4: título normalizado + año (in-memory) ---
                if record.normalized_title and record.publication_year:
                    if (record.normalized_title, record.publication_year) in ex.get("title_year", set()):
                        skipped += 1
                        continue

                # --- Insertar registro en la tabla de fuente ---
                source_record = self._build_source_record(record, dedup)
                try:
                    with self.session.begin_nested():
                        self.session.add(source_record)
                        self.session.flush()
                except IntegrityError:
                    skipped += 1
                    logger.debug(
                        f"Duplicado detectado por constraint DB: "
                        f"{record.source_name}:{record.source_id}"
                    )
                    continue

                # Actualizar sets para detectar duplicados dentro del mismo lote
                ex.setdefault("hashes", set()).add(dedup)
                if record.source_id and sid_attr:
                    ex.setdefault("sids", set()).add(str(record.source_id))
                if record.doi:
                    ndoi = normalize_doi(record.doi)
                    if ndoi:
                        ex.setdefault("dois", set()).add(ndoi)
                if record.normalized_title and record.publication_year:
                    ex.setdefault("title_year", set()).add(
                        (record.normalized_title, record.publication_year)
                    )

                inserted += 1

            except Exception as e:
                logger.warning(f"Error insertando registro {record.source_name}:{record.source_id}: {e}")
                continue

        self.session.commit()
        logger.info(
            f"Ingesta completada: {inserted} nuevos, {skipped} duplicados omitidos "
            f"(de {len(records)} total)."
        )
        return inserted

    # ---------------------------------------------------------
    # CACHÉ IN-MEMORY (evita N+1 queries en matching)
    # ---------------------------------------------------------

    def _build_cache(self) -> Dict:
        """
        Pre-carga en memoria los datos necesarios para la reconciliación.
        Reemplaza el patrón N+1 en _find_fuzzy_match y _find_author_id_match:
        en vez de hacer 1 query por canónico, se hacen 2 queries totales.

        Estructura:
            canonicals   : {pub_id → CanonicalPublication}
            authors_text : {pub_id → "nombre1; nombre2; ..."}
            author_ids   : {pub_id → frozenset of (source, id)}
            by_year      : {year → [pub_id, ...]}
            by_doi       : {doi_str → pub_id}
        """
        t0 = datetime.now()

        # ── Query 1: todos los canónicos ──────────────────────
        all_canonicals = self.session.query(CanonicalPublication).all()

        # ── Query 2: todas las relaciones pub→autor en bulk ──
        author_rows = (
            self.session.query(
                PublicationAuthor.publication_id,
                Author.name,
                Author.orcid,
                Author.external_ids,
            )
            .join(Author, Author.id == PublicationAuthor.author_id)
            .all()
        )

        # Agrupar autores por publicación
        _txt: Dict[int, List[str]] = defaultdict(list)
        _ids: Dict[int, Set] = defaultdict(set)

        for pub_id, name, orcid, ext_ids in author_rows:
            if name:
                _txt[pub_id].append(name)
            if orcid:
                _ids[pub_id].add(("orcid", orcid.strip()))
            for src in ("scopus", "openalex", "wos"):
                val = (ext_ids or {}).get(src)
                if val:
                    _ids[pub_id].add((src, str(val).strip()))

        # Índices auxiliares
        by_year: Dict[Optional[int], List[int]] = defaultdict(list)
        by_doi: Dict[str, int] = {}
        canonicals: Dict[int, CanonicalPublication] = {}

        for c in all_canonicals:
            canonicals[c.id] = c
            by_year[c.publication_year].append(c.id)
            if c.doi:
                by_doi[c.doi] = c.id

        cache = {
            "canonicals":   canonicals,
            "authors_text": {pid: "; ".join(names) for pid, names in _txt.items()},
            "author_ids":   {pid: frozenset(ids) for pid, ids in _ids.items()},
            "by_year":      dict(by_year),
            "by_doi":       by_doi,
        }

        elapsed = (datetime.now() - t0).total_seconds()
        logger.info(
            f"Caché construida en {elapsed:.1f}s: "
            f"{len(canonicals)} canónicos, {len(author_rows)} relaciones autor-pub"
        )
        return cache

    def _cache_add_canonical(self, canonical: CanonicalPublication):
        """Registra un canónico recién creado en la caché in-memory."""
        if self._cache is None:
            return
        self._cache["canonicals"][canonical.id] = canonical
        self._cache["by_year"].setdefault(canonical.publication_year, []).append(canonical.id)
        if canonical.doi:
            self._cache["by_doi"][canonical.doi] = canonical.id
        # sin autores aún
        self._cache["authors_text"].setdefault(canonical.id, "")
        self._cache["author_ids"].setdefault(canonical.id, frozenset())

    # ---------------------------------------------------------
    # RECONCILIACIÓN
    # ---------------------------------------------------------

    def reconcile_pending(self, batch_size: int = 500) -> ReconciliationStats:
        """
        Procesa registros pendientes de TODAS las tablas de fuente.

        Args:
            batch_size: Máximo de registros a procesar por ejecución

        Returns:
            Estadísticas del proceso
        """
        self.stats = ReconciliationStats()

        # ── Construir caché in-memory ANTES del loop ──────────
        # Esto reemplaza el patrón N+1 (1 query por canónico para cargar
        # autores) por 2 queries totales, independientemente del volumen.
        self._cache = self._build_cache()

        # Recoger pendientes de todas las tablas de fuente
        all_pending = []
        per_source_limit = max(batch_size // len(SOURCE_MODELS), 100)

        for source_name, model_cls in SOURCE_MODELS.items():
            pending = (
                self.session.query(model_cls)
                .filter_by(status=RecordStatus.PENDING)
                .limit(per_source_limit)
                .all()
            )
            all_pending.extend(pending)

        # Ordenar por created_at para consistencia
        all_pending.sort(key=lambda r: r.created_at)

        if len(all_pending) > batch_size:
            all_pending = all_pending[:batch_size]

        logger.info(f"Reconciliando {len(all_pending)} registros pendientes...")

        for source_record in all_pending:
            try:
                self._reconcile_one(source_record)
                self.stats.total_processed += 1
            except Exception as e:
                logger.error(
                    f"Error reconciliando {source_record.__class__.__name__} "
                    f"id={source_record.id}: {e}"
                )
                self.stats.errors += 1
                continue

        self.session.commit()

        # Post-reconciliación: rellenar IDs de autores desde raw_data
        self.backfill_author_ids()

        logger.info(f"Reconciliación completada: {self.stats}")
        return self.stats

    def reconcile_batch(self, records: List[StandardRecord]) -> ReconciliationStats:
        """
        Flujo completo: ingesta + reconciliación.

        Args:
            records: Lista de StandardRecord de cualquier fuente

        Returns:
            Estadísticas
        """
        logger.info(f"Iniciando reconciliación de lote: {len(records)} registros")

        # Paso 0: Ingesta
        self.ingest_records(records)

        # Paso 1-3: Reconciliación
        return self.reconcile_pending()

    # ---------------------------------------------------------
    # CASCADA DE RECONCILIACIÓN
    # ---------------------------------------------------------

    def _reconcile_one(self, ext):
        """
        Ejecuta la cascada completa para UN registro de fuente.
        ext: instancia de cualquier modelo de fuente (OpenalexRecord, ScopusRecord, etc.)
        """
        now = datetime.now(timezone.utc)

        # =====================================================
        # PASO 1: Match por DOI exacto
        # =====================================================
        if ext.doi and rc_config.doi_exact_match:
            normalized_doi = normalize_doi(ext.doi)

            if normalized_doi:
                # 1a. ¿Existe canonical_publication con este DOI?
                canonical = (
                    self.session.query(CanonicalPublication)
                    .filter_by(doi=normalized_doi)
                    .first()
                )

                if canonical:
                    self._link_to_canonical(
                        ext, canonical, MatchType.DOI_EXACT, 100.0,
                        {"method": "doi_exact_canonical"}, now
                    )
                    self.stats.doi_exact_matches += 1
                    return

                # 1b. ¿Hay otro registro (de cualquier fuente) con este DOI ya reconciliado?
                sibling = find_record_by_doi_across_sources(
                    self.session, normalized_doi,
                    exclude_source=ext.source_name,
                    exclude_id=ext.id,
                )

                if sibling and sibling.canonical_publication_id:
                    canonical = (
                        self.session.query(CanonicalPublication)
                        .get(sibling.canonical_publication_id)
                    )
                    if canonical:
                        self._link_to_canonical(
                            ext, canonical, MatchType.DOI_EXACT, 100.0,
                            {
                                "method": "doi_exact_sibling",
                                "sibling_source": sibling.source_name,
                                "sibling_id": sibling.id,
                            },
                            now,
                        )
                        self.stats.doi_exact_matches += 1
                        return

        # =====================================================
        # PASO 2: Fuzzy matching
        # =====================================================
        if rc_config.fuzzy_enabled:
            best_match, best_result = self._find_fuzzy_match(ext)

            if best_match and best_result:
                if best_result.match_type in (
                    MatchType.FUZZY_HIGH,
                    MatchType.FUZZY_COMBINED,
                ):
                    # Guard: si ambos registros tienen DOIs distintos, degradar a revisión manual.
                    # DOIs distintos casi siempre implican productos distintos.
                    if ext.doi and best_match.doi:
                        ndoi_ext = normalize_doi(ext.doi)
                        if ndoi_ext and ndoi_ext != best_match.doi:
                            details = {
                                **best_result.to_dict(),
                                "doi_conflict": {
                                    "incoming": ndoi_ext,
                                    "canonical": best_match.doi,
                                },
                            }
                            self._flag_for_review(ext, best_match, best_result.combined_score, details, now)
                            self.stats.manual_review += 1
                            logger.info(
                                f"  CONFLICTO DOI: fuzzy match degradado a revisión — "
                                f"incoming={ndoi_ext} canonical={best_match.doi}"
                            )
                            return

                    self._link_to_canonical(
                        ext, best_match,
                        best_result.match_type,
                        best_result.combined_score,
                        best_result.to_dict(),
                        now,
                    )
                    if best_result.match_type == MatchType.FUZZY_HIGH:
                        self.stats.fuzzy_high_matches += 1
                    else:
                        self.stats.fuzzy_combined_matches += 1
                    return

                elif best_result.match_type == MatchType.MANUAL_REVIEW:
                    self._flag_for_review(
                        ext, best_match,
                        best_result.combined_score,
                        best_result.to_dict(),
                        now,
                    )
                    self.stats.manual_review += 1
                    return

        # =====================================================
        # PASO 2.3: Fuzzy matching con título traducido al inglés
        # Cubre el caso de papers con título en otro idioma:
        #   CvLAC/DatosAbiertos (español) vs Scopus/WoS/OpenAlex (inglés)
        # Solo se activa si el PASO 2 no encontró ningún match.
        # =====================================================
        if rc_config.fuzzy_enabled:
            trans_match, trans_result = self._find_translated_fuzzy_match(ext)
            if trans_match and trans_result:
                if trans_result.match_type in (
                    MatchType.FUZZY_HIGH,
                    MatchType.FUZZY_COMBINED,
                ):
                    # Guard: DOIs distintos → no fusionar, revisión manual
                    if ext.doi and trans_match.doi:
                        ndoi_ext = normalize_doi(ext.doi)
                        if ndoi_ext and ndoi_ext != trans_match.doi:
                            details = {
                                **trans_result.to_dict(),
                                "method": "translated_fuzzy",
                                "translated_title": self._translation_cache.get(ext.title or ""),
                                "doi_conflict": {
                                    "incoming": ndoi_ext,
                                    "canonical": trans_match.doi,
                                },
                            }
                            self._flag_for_review(
                                ext, trans_match,
                                trans_result.combined_score, details, now
                            )
                            self.stats.manual_review += 1
                            logger.info(
                                f"  CONFLICTO DOI (traducción): match degradado a revisión — "
                                f"incoming={ndoi_ext} canonical={trans_match.doi}"
                            )
                            return

                    details = {
                        **trans_result.to_dict(),
                        "method": "translated_fuzzy",
                        "original_title": ext.title,
                        "translated_title": self._translation_cache.get(ext.title or ""),
                    }
                    self._link_to_canonical(
                        ext, trans_match,
                        MatchType.FUZZY_COMBINED,
                        trans_result.combined_score,
                        details,
                        now,
                    )
                    self.stats.fuzzy_combined_matches += 1
                    logger.info(
                        f"  MATCH TRADUCIDO: '{(ext.title or '')[:50]}' → "
                        f"canon={trans_match.id} "
                        f"(score={trans_result.combined_score:.1f}, "
                        f"authors={trans_result.author_score:.1f})"
                    )
                    return

                elif trans_result.match_type == MatchType.MANUAL_REVIEW:
                    details = {
                        **trans_result.to_dict(),
                        "method": "translated_fuzzy",
                        "original_title": ext.title,
                        "translated_title": self._translation_cache.get(ext.title or ""),
                    }
                    self._flag_for_review(
                        ext, trans_match,
                        trans_result.combined_score, details, now
                    )
                    self.stats.manual_review += 1
                    return

        # =====================================================
        # PASO 2.5: Match por solapamiento de IDs externos de autores
        # Cubre el caso de títulos en idiomas distintos (sin DOI)
        # donde el fuzzy de título falla pero los autores son los mismos.
        # =====================================================
        if not ext.doi:
            author_match, overlap_count = self._find_author_id_match(ext)
            if author_match:
                details = {
                    "method": "author_id_overlap",
                    "shared_author_ids": overlap_count,
                    "note": "Posible mismo paper en idioma distinto",
                }
                self._link_to_canonical(
                    ext, author_match, MatchType.FUZZY_COMBINED, 80.0, details, now
                )
                self.stats.fuzzy_combined_matches += 1
                logger.info(
                    f"  AUTHOR-ID MATCH: {overlap_count} IDs externos compartidos → "
                    f"canonical_id={author_match.id}"
                )
                return

        # =====================================================
        # PASO 2.8: Guard de título-solo antes de crear nuevo canónico.
        # Si el título es casi idéntico a un canónico existente (>= title_high_confidence)
        # y el año coincide, es muy probable que sea el mismo paper con DOI diferente
        # (ej: DOI de revista vs DOI de versión preprint, o error tipográfico en DOI).
        # → Flaggear para revisión manual y NO crear nuevo canónico.
        # → Guardar el par como posible duplicado para auditoría.
        # =====================================================
        title_only_match = self._find_title_only_match(ext)
        if title_only_match is not None:
            details = {
                "method": "title_only_guard",
                "title_score": rc_config.title_high_confidence,
                "note": "Título casi idéntico a canónico existente; DOI diferente o ausente",
            }
            self._flag_for_review(ext, title_only_match, rc_config.title_high_confidence, details, now)
            self.stats.manual_review += 1
            logger.info(
                f"  PASO 2.8: título idéntico a canonical_id={title_only_match.id}, "
                f"no se crea nuevo canónico"
            )
            return

        # =====================================================
        # PASO 3: Crear nueva publicación canónica
        # =====================================================
        self._create_new_canonical(ext, now)
        self.stats.new_canonical += 1

    # ---------------------------------------------------------
    # TRADUCCIÓN DE TÍTULOS (para matching multi-idioma)
    # ---------------------------------------------------------

    def _detect_language(self, text: str) -> str:
        """
        Detecta el idioma de un texto usando langdetect.
        Retorna código ISO639-1 ('es', 'en', 'fr', ...) o 'unknown'.
        Cachea resultados para no re-procesar el mismo título.
        """
        if not text or len(text.strip()) < 10:
            return "unknown"

        cache_key = text[:200]
        if cache_key in self._lang_cache:
            return self._lang_cache[cache_key]

        try:
            from langdetect import detect, LangDetectException
            lang = detect(text)
        except Exception:
            lang = "unknown"

        self._lang_cache[cache_key] = lang
        return lang

    def _translate_to_english(self, title: str) -> Optional[str]:
        """
        Traduce un título al inglés usando deep-translator (backend: Google Translate).

        - Si el título ya está en inglés, retorna None (no hace API call).
        - Cachea todas las traducciones en self._translation_cache para evitar
          llamadas repetidas dentro del mismo lote.
        - Si la traducción falla (red caída, cuota, etc.) retorna None
          sin interrumpir el flujo de reconciliación.

        Retorna la traducción en inglés, o None si no aplica / falla.
        """
        if not title or len(title.strip()) < 5:
            return None

        cache_key = title[:300]
        if cache_key in self._translation_cache:
            return self._translation_cache[cache_key]

        # 1. Detectar idioma: si ya es inglés no hay nada que hacer
        lang = self._detect_language(title)
        if lang == "en":
            self._translation_cache[cache_key] = None
            return None

        # 2. Traducir
        try:
            from deep_translator import GoogleTranslator
            translated = GoogleTranslator(source="auto", target="en").translate(title)
            if not translated:
                self._translation_cache[cache_key] = None
                return None
            # Si la traducción es idéntica al original, no aporta nada
            if translated.strip().lower() == title.strip().lower():
                self._translation_cache[cache_key] = None
                return None
            self._translation_cache[cache_key] = translated
            logger.debug(
                f"  TRADUCCIÓN [{lang}→en]: '{title[:60]}' → '{translated[:60]}'"
            )
            return translated
        except Exception as e:
            logger.debug(f"  Error de traducción (ignorado): {e}")
            self._translation_cache[cache_key] = None
            return None

    def _find_translated_fuzzy_match(
        self, ext
    ) -> Tuple[Optional[CanonicalPublication], Optional[FuzzyMatchResult]]:
        """
        Busca coincidencia fuzzy usando el título del registro entrante
        TRADUCIDO al inglés.

        Propósito: detectar el mismo paper publicado con título en dos idiomas
        distintos (ej: CvLAC/DatosAbiertos en español vs Scopus/WoS en inglés).

        Diseño:
          - Solo se activa si el título entrante NO está en inglés.
          - Traduce el título una sola vez (caché in-memory durante el lote).
          - Usa la misma lógica de candidatos por año que _find_fuzzy_match.
          - Safety guard: exige author_score >= 40 para aceptar un match por
            traducción, ya que las traducciones automáticas pueden introducir
            ruido léxico y un buen solapamiento de autores confirma que es el
            mismo paper.

        Retorna (canonical, result) o (None, None) si no hay match suficiente.
        """
        incoming_title = ext.title or ""
        if not incoming_title:
            return None, None

        translated = self._translate_to_english(incoming_title)
        if not translated:
            # Título ya en inglés o traducción fallida → el fuzzy normal ya lo cubrió
            return None, None

        cache = self._cache
        if not cache:
            return None, None

        canonicals = cache["canonicals"]

        # Filtrar candidatos por año (idéntico a _find_fuzzy_match)
        if ext.publication_year and rc_config.year_must_match:
            tol = rc_config.year_tolerance
            candidate_ids: List[int] = []
            for yr in range(ext.publication_year - tol, ext.publication_year + tol + 1):
                candidate_ids.extend(cache["by_year"].get(yr, []))
        else:
            candidate_ids = list(canonicals.keys())

        if not candidate_ids:
            return None, None

        best_canonical = None
        best_result = None
        best_score = 0.0

        for pub_id in candidate_ids:
            canonical = canonicals.get(pub_id)
            if not canonical:
                continue

            authors_b_text = cache["authors_text"].get(pub_id, "")

            result = compare_records(
                title_a=translated,
                year_a=ext.publication_year,
                authors_a=ext.authors_text or "",
                title_b=canonical.title or "",
                year_b=canonical.publication_year,
                authors_b=authors_b_text,
            )

            # Safety guard: traducción automática puede ser imprecisa.
            # Exigir solapamiento mínimo de autores como señal de confirmación.
            if result.author_score < 40.0:
                continue

            if result.combined_score > best_score:
                best_score = result.combined_score
                best_canonical = canonical
                best_result = result

        return best_canonical, best_result

    # ---------------------------------------------------------
    # FUZZY MATCHING CONTRA CANÓNICAS
    # ---------------------------------------------------------

    def _find_fuzzy_match(self, ext) -> Tuple[Optional[CanonicalPublication], Optional[FuzzyMatchResult]]:
        """
        Busca la mejor coincidencia fuzzy entre el registro de fuente
        y las publicaciones canónicas existentes.

        Usa la caché in-memory para evitar N+1 queries:
        los autores de cada canónico ya están cargados en self._cache.
        """
        best_canonical = None
        best_result = None
        best_score = 0.0

        cache = self._cache
        canonicals = cache["canonicals"] if cache else {}

        # Obtener IDs de candidatos filtrando por año desde la caché
        if ext.publication_year and rc_config.year_must_match:
            tol = rc_config.year_tolerance
            candidate_ids: List[int] = []
            for yr in range(ext.publication_year - tol, ext.publication_year + tol + 1):
                candidate_ids.extend(cache["by_year"].get(yr, []))
        else:
            candidate_ids = list(canonicals.keys())

        for pub_id in candidate_ids:
            canonical = canonicals.get(pub_id)
            if not canonical:
                continue

            authors_b_text = cache["authors_text"].get(pub_id, "") if cache else ""

            result = compare_records(
                title_a=ext.title or "",
                year_a=ext.publication_year,
                authors_a=ext.authors_text or "",
                title_b=canonical.title or "",
                year_b=canonical.publication_year,
                authors_b=authors_b_text,
            )

            if result.combined_score > best_score:
                best_score = result.combined_score
                best_canonical = canonical
                best_result = result

        return best_canonical, best_result

    def _find_title_only_match(
        self, ext
    ) -> Optional[CanonicalPublication]:
        """
        Chequeo ligero de título-solo a alta confianza (>= title_high_confidence).
        Usado en PASO 2.8 para evitar crear canonicals duplicados cuando el único
        obstáculo fue el conflicto de DOI o la baja similitud de autores.

        Retorna el canónico existente si hay match, None en caso contrario.
        Solo considera candidatos del mismo año (tolerancia ±1) para reducir falsos positivos.
        """
        from reconciliation.fuzzy_matcher import compare_titles

        incoming_title = ext.normalized_title or normalize_text(ext.title or "")
        if not incoming_title or len(incoming_title) < 15:
            return None

        yr = ext.publication_year
        year_filter = []
        if yr:
            year_filter = list(range(yr - 1, yr + 2))

        incoming_type = normalize_publication_type(ext.publication_type or "") if ext.publication_type else None

        q = self.session.query(
            CanonicalPublication.id,
            CanonicalPublication.normalized_title,
            CanonicalPublication.doi,
            CanonicalPublication.publication_type,
        )
        if year_filter:
            q = q.filter(CanonicalPublication.publication_year.in_(year_filter))
        q = q.filter(
            CanonicalPublication.normalized_title.isnot(None),
            func.length(CanonicalPublication.normalized_title) > 10,
        )

        best_id: Optional[int] = None
        best_score: float = 0.0

        for (canon_id, canon_title, _, canon_type) in q.all():
            if not canon_title:
                continue
            # Si ambos tienen tipo definido y son distintos, no son el mismo producto
            if incoming_type and canon_type:
                if normalize_publication_type(canon_type) != incoming_type:
                    continue
            score = compare_titles(incoming_title, canon_title)
            if score >= rc_config.title_high_confidence and score > best_score:
                best_score = score
                best_id = canon_id

        if best_id is None:
            return None

        return self.session.get(CanonicalPublication, best_id)

    def _find_author_id_match(
        self, ext
    ) -> Tuple[Optional[CanonicalPublication], int]:
        """
        Busca un canónico del mismo año que comparta ≥ 2 IDs externos de autores
        (orcid, scopus_id, openalex_id). Agnóstico al idioma del título.

        Usa la caché in-memory; no hace queries a la BD.
        Retorna (canonical, shared_count) o (None, 0).
        """
        # Recolectar IDs externos del registro entrante
        incoming_ids: frozenset = frozenset()
        raw_ids: set = set()
        raw_data = ext.raw_data or {}
        for author in raw_data.get("_parsed_authors") or raw_data.get("authors") or []:
            if not isinstance(author, dict):
                continue
            if author.get("orcid"):
                raw_ids.add(("orcid", author["orcid"].strip()))
            if author.get("scopus_id"):
                raw_ids.add(("scopus", str(author["scopus_id"]).strip()))
            if author.get("openalex_id"):
                val = str(author["openalex_id"]).strip().rstrip("/").split("/")[-1]
                raw_ids.add(("openalex", val))

        if not raw_ids:
            return None, 0
        incoming_ids = frozenset(raw_ids)

        cache = self._cache
        if not cache:
            return None, 0

        canonicals = cache["canonicals"]

        # Candidatos: sin DOI, mismo año (desde caché)
        if ext.publication_year and rc_config.year_must_match:
            tol = rc_config.year_tolerance
            candidate_ids = [
                pid
                for yr in range(ext.publication_year - tol, ext.publication_year + tol + 1)
                for pid in cache["by_year"].get(yr, [])
                if not canonicals.get(pid, None) or not canonicals[pid].doi
            ]
        else:
            candidate_ids = [
                pid for pid, c in canonicals.items() if not c.doi
            ]

        best_canonical = None
        best_overlap = 0

        for pub_id in candidate_ids:
            canon_ids = cache["author_ids"].get(pub_id, frozenset())
            shared = len(incoming_ids & canon_ids)
            if shared >= 2 and shared > best_overlap:
                best_overlap = shared
                best_canonical = canonicals[pub_id]

        return best_canonical, best_overlap

    # ---------------------------------------------------------
    # ACCIONES
    # ---------------------------------------------------------

    def _link_to_canonical(
        self,
        ext,
        canonical: CanonicalPublication,
        match_type: str,
        score: float,
        details: dict,
        timestamp: datetime,
    ):
        """Vincula un registro de fuente a una publicación canónica existente."""
        ext.canonical_publication_id = canonical.id
        ext.status = RecordStatus.MATCHED
        ext.match_type = match_type
        ext.match_score = score
        ext.reconciled_at = timestamp

        # Actualizar conteo de fuentes incrementalmente (evita 5 queries a BD)
        canonical.sources_count = (canonical.sources_count or 0) + 1

        # Enriquecer la canónica con datos faltantes de esta fuente
        self._enrich_canonical(canonical, ext)

        # Ingestar autores
        self._ingest_authors(canonical, ext)

        # Log de auditoría
        log = ReconciliationLog(
            source_name=ext.source_name,
            source_record_id=ext.id,
            canonical_publication_id=canonical.id,
            match_type=match_type,
            match_score=score,
            match_details=details,
            action="linked_existing",
        )
        self.session.add(log)

        logger.debug(
            f"  VINCULADO: {ext.source_name}:{ext.id} → canon={canonical.id} "
            f"({match_type}, score={score:.1f})"
        )

    @staticmethod
    def _extract_raw_field(raw: dict, source_name: str, field: str) -> Optional[str]:
        """
        Extrae abstract, page_range o publisher desde raw_data cuando no hay
        columna tipada en el modelo de fuente.

        Claves por fuente:
          openalex  → abstract: abstract_inverted_index (reconstruido)
                      publisher: primary_location.source.publisher_lineage_names[0]
          scopus    → abstract: dc:description | description
                      page_range: prism:pageRange | pageRange
                      publisher: dc:publisher | prism:publisher
          wos       → abstract: abstracts.items[0].value
                      page_range: source.pages.range | source.pages.compact
                      publisher: source.publisherName
        """
        if not raw:
            return None

        if field == "abstract":
            if source_name == "openalex":
                aii = raw.get("abstract_inverted_index") or {}
                if aii:
                    pos_word: Dict[int, str] = {}
                    for word, positions in aii.items():
                        for pos in (positions if isinstance(positions, list) else [positions]):
                            pos_word[pos] = word
                    return " ".join(pos_word[i] for i in sorted(pos_word)) or None
            elif source_name == "scopus":
                return raw.get("dc:description") or raw.get("description") or raw.get("abstract")
            elif source_name == "wos":
                items = (raw.get("abstracts") or {}).get("items") or []
                return items[0].get("value") if items else None

        elif field == "page_range":
            if source_name == "scopus":
                return raw.get("prism:pageRange") or raw.get("pageRange")
            elif source_name == "wos":
                pages = (raw.get("source") or {}).get("pages") or {}
                return pages.get("range") or pages.get("compact")

        elif field == "publisher":
            if source_name == "openalex":
                primary = raw.get("primary_location") or {}
                source = primary.get("source") or {}
                lineage = source.get("publisher_lineage_names") or []
                return lineage[0] if lineage else None
            elif source_name == "scopus":
                return raw.get("dc:publisher") or raw.get("prism:publisher")
            elif source_name == "wos":
                return (raw.get("source") or {}).get("publisherName")

        return None

    def _reject_source_record(self, ext, reason: str):
        """Marca un registro de fuente como rechazado sin crear canónica."""
        ext.status = RecordStatus.REJECTED
        ext.match_type = reason
        ext.match_score = 0.0
        ext.reconciled_at = datetime.now(timezone.utc)
        logger.warning(
            f"  RECHAZADO: {ext.source_name}:{ext.id} — {reason} "
            f"(título: '{(ext.title or '')[:80]}')"
        )

    def _create_new_canonical(self, ext, timestamp: datetime):
        """
        Crea una nueva publicación canónica a partir de un registro de fuente.
        Incluye:
          - Protección contra DOI duplicado.
          - Validación de título: rechaza registros sin título o con título
            en la lista negra (blacklist_keywords de criteria_config).
        """
        ndoi = normalize_doi(ext.doi) if ext.doi else None

        # --- Validación de título (Problema 5) ---
        raw_title = (ext.title or "").strip()
        if not raw_title or len(raw_title) < criteria_config.min_title_length:
            self._reject_source_record(ext, "invalid_title_too_short")
            return

        title_lower = normalize_text(raw_title)
        for kw in criteria_config.blacklist_keywords:
            if kw in title_lower:
                self._reject_source_record(ext, f"invalid_title_blacklisted")
                return

        # --- Verificación final anti-duplicado por DOI ---
        if ndoi:
            existing = (
                self.session.query(CanonicalPublication)
                .filter_by(doi=ndoi)
                .first()
            )
            if existing:
                logger.info(
                    f"  DOI {ndoi} ya tiene canónica {existing.id}, "
                    f"vinculando en vez de crear nueva."
                )
                self._link_to_canonical(
                    ext, existing, MatchType.DOI_EXACT, 100.0,
                    {"method": "doi_safety_check_at_creation"}, timestamp
                )
                self.stats.doi_exact_matches += 1
                self.stats.new_canonical -= 1
                return

        # Construir provenance inicial
        provenance = {}
        src = ext.source_name
        if ndoi:
            provenance["doi"] = src
        if ext.title:
            provenance["title"] = src
        if ext.publication_year:
            provenance["publication_year"] = src
        if ext.publication_type:
            provenance["publication_type"] = src
        if ext.source_journal:
            provenance["source_journal"] = src
        if ext.is_open_access is not None:
            provenance["is_open_access"] = src
        if ext.citation_count:
            provenance["citation_count"] = src
        if ext.publication_date:
            provenance["publication_date"] = src
        if ext.issn:
            provenance["issn"] = src

        # Campos enriquecidos: extraer desde raw_data si no hay columna tipada
        raw = ext.raw_data or {}
        abstract_val = getattr(ext, "abstract", None) or self._extract_raw_field(raw, src, "abstract")
        page_range_val = getattr(ext, "page_range", None) or self._extract_raw_field(raw, src, "page_range")
        publisher_val = getattr(ext, "publisher", None) or self._extract_raw_field(raw, src, "publisher")
        pmid_val = getattr(ext, "pmid", None)
        pmcid_val = getattr(ext, "pmcid", None)

        for field, val in [
            ("abstract", abstract_val), ("page_range", page_range_val),
            ("publisher", publisher_val), ("pmid", pmid_val), ("pmcid", pmcid_val),
        ]:
            if val:
                provenance[field] = src

        initial_cites = ext.citation_count or 0
        canonical = CanonicalPublication(
            doi=ndoi,
            title=raw_title,
            normalized_title=ext.normalized_title or (normalize_text(raw_title) if raw_title else None),
            publication_year=ext.publication_year,
            publication_date=ext.publication_date,
            publication_type=normalize_publication_type(ext.publication_type),
            source_journal=ext.source_journal,
            issn=ext.issn,
            language=ext.language,
            is_open_access=ext.is_open_access,
            oa_status=ext.oa_status,
            citation_count=initial_cites,
            citations_by_source={src: initial_cites} if initial_cites else {},
            sources_count=1,
            field_provenance=provenance,
            field_conflicts={},
            abstract=abstract_val,
            page_range=page_range_val,
            publisher=publisher_val,
            pmid=pmid_val,
            pmcid=pmcid_val,
        )

        try:
            self.session.add(canonical)
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            if ndoi:
                canonical = self.session.query(CanonicalPublication).filter_by(doi=ndoi).first()
                if canonical:
                    self._link_to_canonical(
                        ext, canonical, MatchType.DOI_EXACT, 100.0,
                        {"method": "doi_integrity_error_recovery"}, timestamp
                    )
                    self.stats.doi_exact_matches += 1
                    self.stats.new_canonical -= 1
                    return
            raise

        # Vincular el registro a la canónica
        ext.canonical_publication_id = canonical.id
        ext.status = RecordStatus.NEW_CANONICAL
        ext.match_type = MatchType.NO_MATCH
        ext.match_score = 0.0
        ext.reconciled_at = timestamp

        # Ingestar autores si hay datos
        self._ingest_authors(canonical, ext)

        # Log
        log = ReconciliationLog(
            source_name=ext.source_name,
            source_record_id=ext.id,
            canonical_publication_id=canonical.id,
            match_type=MatchType.NO_MATCH,
            match_score=0.0,
            match_details={"method": "new_canonical_created"},
            action="created_new",
        )
        self.session.add(log)

        logger.debug(
            f"  NUEVO: {ext.source_name}:{ext.id} → canon={canonical.id} "
            f"('{canonical.title[:50]}...')"
        )

        # Registrar en caché para que el resto del lote lo encuentre
        self._cache_add_canonical(canonical)

    def _flag_for_review(
        self,
        ext,
        candidate: CanonicalPublication,
        score: float,
        details: dict,
        timestamp: datetime,
    ):
        """Marca un registro para revisión manual."""
        ext.status = RecordStatus.REVIEW
        ext.match_type = MatchType.MANUAL_REVIEW
        ext.match_score = score
        ext.reconciled_at = timestamp

        log = ReconciliationLog(
            source_name=ext.source_name,
            source_record_id=ext.id,
            canonical_publication_id=candidate.id,
            match_type=MatchType.MANUAL_REVIEW,
            match_score=score,
            match_details=details,
            action="flagged_review",
        )
        self.session.add(log)

        logger.debug(
            f"  REVISIÓN: {ext.source_name}:{ext.id}, posible_canon={candidate.id} "
            f"(score={score:.1f})"
        )

    def _store_duplicate_pair(
        self,
        canonical_id_a: int,
        canonical_id_b: int,
        score: float,
        method: str = "title",
    ) -> None:
        """
        Persiste un par de publicaciones posiblemente duplicadas.
        Idempotente: ignora si el par ya existe (ON CONFLICT DO NOTHING).
        Ordena los IDs para cumplir el CHECK canonical_id_1 < canonical_id_2.
        """
        id1, id2 = (canonical_id_a, canonical_id_b) if canonical_id_a < canonical_id_b else (canonical_id_b, canonical_id_a)
        try:
            existing = (
                self.session.query(PossibleDuplicatePair)
                .filter_by(canonical_id_1=id1, canonical_id_2=id2)
                .first()
            )
            if existing:
                return
            pair = PossibleDuplicatePair(
                canonical_id_1=id1,
                canonical_id_2=id2,
                similarity_score=round(score, 2),
                match_method=method,
                status="pending",
            )
            self.session.add(pair)
            logger.info(
                f"  DUPLICATE_PAIR guardado: {id1}↔{id2} score={score:.1f} method={method}"
            )
        except Exception as exc:
            logger.warning(f"  No se pudo guardar duplicate pair {id1}↔{id2}: {exc}")

    # ---------------------------------------------------------
    # ENRIQUECIMIENTO
    # ---------------------------------------------------------

    def _record_conflict(
        self,
        canonical: CanonicalPublication,
        field: str,
        existing_source: str,
        existing_value,
        new_source: str,
        new_value,
    ):
        """
        Registra en field_conflicts que dos fuentes discrepan en un campo.
        No sobreescribe el valor canónico; solo documenta la discrepancia
        para revisión posterior.
        """
        conflicts = dict(canonical.field_conflicts or {})
        entry = conflicts.get(field, {})
        entry[existing_source] = str(existing_value)
        entry[new_source] = str(new_value)
        conflicts[field] = entry
        canonical.field_conflicts = conflicts
        logger.info(
            f"  CONFLICTO canon={canonical.id} campo='{field}': "
            f"{existing_source}={existing_value!r} vs {new_source}={new_value!r}"
        )

    def _enrich_canonical(self, canonical: CanonicalPublication, ext):
        """
        Completa campos vacíos de la publicación canónica
        usando las columnas tipadas del registro de fuente.

        - Solo escribe si el campo de la canónica está vacío/nulo.
        - Registra en field_provenance qué fuente aportó cada dato.
        - Detecta y registra en field_conflicts cuando fuentes discrepan
          en campos críticos (is_open_access, doi).
        - Actualiza citations_by_source por fuente; citation_count = max.
        """
        enriched_fields = []
        src = ext.source_name
        prov = dict(canonical.field_provenance or {})

        # DOI — detectar conflicto si ambos tienen DOI distintos
        if ext.doi:
            ndoi = normalize_doi(ext.doi)
            if ndoi:
                if not canonical.doi:
                    canonical.doi = ndoi
                    enriched_fields.append("doi")
                    prov["doi"] = src
                elif canonical.doi != ndoi:
                    # Ambas fuentes reportan DOIs diferentes → conflicto
                    self._record_conflict(
                        canonical, "doi",
                        prov.get("doi", "original"), canonical.doi,
                        src, ndoi,
                    )

        # Revista / fuente
        if not canonical.source_journal and ext.source_journal:
            canonical.source_journal = ext.source_journal
            enriched_fields.append("source_journal")
            prov["source_journal"] = src

        # Tipo de publicación
        if not canonical.publication_type and ext.publication_type:
            canonical.publication_type = normalize_publication_type(ext.publication_type)
            enriched_fields.append("publication_type")
            prov["publication_type"] = src

        # Open Access — detectar conflicto si ambas fuentes tienen valores opuestos
        if ext.is_open_access is not None:
            if canonical.is_open_access is None:
                canonical.is_open_access = ext.is_open_access
                enriched_fields.append("is_open_access")
                prov["is_open_access"] = src
            elif canonical.is_open_access != ext.is_open_access:
                # Fuentes en desacuerdo: registrar conflicto, no sobreescribir
                self._record_conflict(
                    canonical, "is_open_access",
                    prov.get("is_open_access", "original"), canonical.is_open_access,
                    src, ext.is_open_access,
                )

        # OA Status
        if not canonical.oa_status and ext.oa_status:
            canonical.oa_status = ext.oa_status
            enriched_fields.append("oa_status")
            prov["oa_status"] = src

        # ISSN
        if not canonical.issn and ext.issn:
            canonical.issn = ext.issn
            enriched_fields.append("issn")
            prov["issn"] = src

        # Citas: registrar por fuente y usar el máximo como valor canónico
        new_cites = ext.citation_count or 0
        cbs = dict(canonical.citations_by_source or {})
        cbs[src] = new_cites
        canonical.citations_by_source = cbs
        max_cites = max(cbs.values())
        if max_cites != (canonical.citation_count or 0):
            canonical.citation_count = max_cites
            enriched_fields.append("citation_count")
            prov["citation_count"] = max(cbs, key=cbs.get)

        # Año de publicación
        if not canonical.publication_year and ext.publication_year:
            canonical.publication_year = ext.publication_year
            enriched_fields.append("publication_year")
            prov["publication_year"] = src

        # Fecha de publicación
        if not canonical.publication_date and ext.publication_date:
            canonical.publication_date = ext.publication_date
            enriched_fields.append("publication_date")
            prov["publication_date"] = src

        # Idioma
        if not canonical.language and ext.language:
            canonical.language = ext.language
            enriched_fields.append("language")
            prov["language"] = src

        # ── Campos enriquecidos ───────────────────────────────────

        # Normalized title — computar desde title si falta
        if not canonical.normalized_title and canonical.title:
            canonical.normalized_title = normalize_text(canonical.title)
            enriched_fields.append("normalized_title")
            prov["normalized_title"] = src

        # Abstract — columna tipada si existe, si no extraer de raw_data
        if not canonical.abstract:
            raw_e = ext.raw_data or {}
            abstract_val = (
                getattr(ext, "abstract", None)
                or self._extract_raw_field(raw_e, src, "abstract")
            )
            if abstract_val:
                canonical.abstract = abstract_val
                enriched_fields.append("abstract")
                prov["abstract"] = src

        # Keywords
        if not canonical.keywords:
            kw = (
                getattr(ext, "keywords", None)
                or getattr(ext, "author_keywords", None)
            )
            if kw:
                canonical.keywords = kw
                enriched_fields.append("keywords")
                prov["keywords"] = src

        # URL fuente
        if not canonical.source_url and ext.url:
            canonical.source_url = ext.url
            enriched_fields.append("source_url")
            prov["source_url"] = src

        # Rango de páginas — columna tipada si existe, si no extraer de raw_data
        if not canonical.page_range:
            raw_e = ext.raw_data or {}
            page_range_val = (
                getattr(ext, "page_range", None)
                or self._extract_raw_field(raw_e, src, "page_range")
            )
            if page_range_val:
                canonical.page_range = page_range_val
                enriched_fields.append("page_range")
                prov["page_range"] = src

        # Editorial / publisher — columna tipada si existe, si no extraer de raw_data
        if not canonical.publisher:
            raw_e = ext.raw_data or {}
            publisher_val = (
                getattr(ext, "publisher", None)
                or self._extract_raw_field(raw_e, src, "publisher")
            )
            if publisher_val:
                canonical.publisher = publisher_val
                enriched_fields.append("publisher")
                prov["publisher"] = src

        # PMID / PMCID — solo disponible en registros de OpenAlex
        if not canonical.pmid:
            pmid_val = getattr(ext, "pmid", None)
            if pmid_val:
                canonical.pmid = pmid_val
                enriched_fields.append("pmid")
                prov["pmid"] = src
        if not canonical.pmcid:
            pmcid_val = getattr(ext, "pmcid", None)
            if pmcid_val:
                canonical.pmcid = pmcid_val
                enriched_fields.append("pmcid")
                prov["pmcid"] = src

        # Cobertura de revista
        if not canonical.journal_coverage:
            jc = getattr(ext, "journal_coverage", None) or getattr(ext, "coverage", None)
            if jc:
                canonical.journal_coverage = jc
                enriched_fields.append("journal_coverage")
                prov["journal_coverage"] = src

        # ── Campos que requieren raw_data ─────────────────────────
        raw = ext.raw_data or {}
        parsed_authors = raw.get("_parsed_authors") or []

        # Primer autor
        if not canonical.first_author and parsed_authors:
            first = parsed_authors[0]
            fa_name = first.get("name") if isinstance(first, dict) else str(first)
            if fa_name:
                canonical.first_author = fa_name[:300]
                enriched_fields.append("first_author")
                prov["first_author"] = src

        # Número de co-autores
        if not canonical.coauthorships_count and parsed_authors:
            canonical.coauthorships_count = len(parsed_authors)
            enriched_fields.append("coauthorships_count")
            prov["coauthorships_count"] = src

        # Autor de correspondencia (OpenAlex authorships)
        if not canonical.corresponding_author:
            for authorship in raw.get("authorships", []):
                if authorship.get("is_corresponding"):
                    ca = authorship.get("author", {}).get("display_name")
                    if ca:
                        canonical.corresponding_author = ca[:300]
                        enriched_fields.append("corresponding_author")
                        prov["corresponding_author"] = src
                        break

        # Área de conocimiento
        if not canonical.knowledge_area:
            # OpenAlex: usar el dominio del primer topic
            topics = raw.get("topics") or []
            if topics and isinstance(topics[0], dict):
                domain = topics[0].get("domain", {})
                area = (
                    domain.get("display_name")
                    if isinstance(domain, dict)
                    else topics[0].get("field", {}).get("display_name")
                )
                if area:
                    canonical.knowledge_area = str(area)[:300]
                    enriched_fields.append("knowledge_area")
                    prov["knowledge_area"] = src
            # Scopus: subject_areas
            if not canonical.knowledge_area:
                subject_areas = raw.get("subject_areas") or raw.get("subjectAreas") or []
                if subject_areas and isinstance(subject_areas[0], dict):
                    area = subject_areas[0].get("@abbrev") or subject_areas[0].get("$")
                    if area:
                        canonical.knowledge_area = str(area)[:300]
                        enriched_fields.append("knowledge_area")
                        prov["knowledge_area"] = src

        # Código CINE (Scopus subject area code → clasificación Minciencias)
        if not canonical.cine_code:
            subject_areas = raw.get("subject_areas") or raw.get("subjectAreas") or []
            if subject_areas and isinstance(subject_areas[0], dict):
                code = subject_areas[0].get("@code") or subject_areas[0].get("code")
                if code:
                    canonical.cine_code = str(code)[:50]
                    enriched_fields.append("cine_code")
                    prov["cine_code"] = src

        # Persistir provenance actualizado
        if enriched_fields:
            canonical.field_provenance = prov
            logger.info(
                f"  ENRIQUECIDO canon={canonical.id} con {src}: "
                f"{', '.join(enriched_fields)}"
            )

    def enrich_canonical(self, canonical_id: int) -> dict:
        """
        Enriquece UN canónico específico con los registros de fuente ya vinculados.
        Llena campos vacíos, actualiza citations_by_source y retorna diff completo.
        """
        from db.source_registry import SOURCE_REGISTRY

        _TRACKED = [
            "doi", "title", "publication_year", "publication_date",
            "publication_type", "language", "source_journal", "issn",
            "abstract", "keywords", "source_url", "page_range", "publisher",
            "journal_coverage", "knowledge_area", "cine_code",
            "first_author", "corresponding_author", "coauthorships_count",
            "is_open_access", "oa_status", "citation_count",
            "citations_by_source",
        ]

        canonical = self.session.get(CanonicalPublication, canonical_id)
        if not canonical:
            return {"error": f"Canónico {canonical_id} no encontrado"}

        # Snapshot antes
        before = {f: getattr(canonical, f, None) for f in _TRACKED}
        if isinstance(before.get("citations_by_source"), dict):
            before["citations_by_source"] = dict(before["citations_by_source"])

        sources_used: set = set()
        for src_def in SOURCE_REGISTRY.all():
            model = src_def.model_class
            linked = (
                self.session.query(model)
                .filter(model.canonical_publication_id == canonical_id)
                .all()
            )
            for ext in linked:
                self._enrich_canonical(canonical, ext)
                sources_used.add(ext.source_name)

        self.session.flush()

        # Snapshot después — detectar qué cambió
        after = {f: getattr(canonical, f, None) for f in _TRACKED}
        fields_filled   = []  # vacío → con valor
        fields_updated  = []  # valor viejo → valor nuevo

        for f in _TRACKED:
            bv, av = before[f], after[f]
            if bv == av:
                continue
            if not bv and av:
                fields_filled.append(f)
            elif bv and av and bv != av:
                fields_updated.append(f)

        return {
            "canonical_id": canonical_id,
            "sources_used": sorted(sources_used),
            "fields_filled": fields_filled,
            "fields_updated": fields_updated,
            "total_changes": len(fields_filled) + len(fields_updated),
            "citation_count": canonical.citation_count,
            "citations_by_source": dict(canonical.citations_by_source or {}),
        }

    # ---------------------------------------------------------
    # ENRIQUECIMIENTO MASIVO DE CANÓNICOS EXISTENTES
    # ---------------------------------------------------------

    def enrich_all_canonicals(self, batch_size: int = 200) -> dict:
        """
        Recorre TODOS los canónicos existentes y los enriquece con los
        registros de fuente ya vinculados (*_records con canonical_publication_id).

        No importa el status del registro de fuente — si está vinculado
        a un canónico, aporta sus campos.

        Returns:
            Resumen detallado de la ejecución: contadores globales,
            desglose por campo y por fuente, y muestra de canónicos enriquecidos.
        """
        from db.source_registry import SOURCE_REGISTRY
        from collections import defaultdict

        _ENRICHABLE_FIELDS = [
            "doi", "source_journal", "publication_type", "is_open_access",
            "oa_status", "issn", "citation_count", "publication_year",
            "publication_date", "language", "abstract", "keywords",
            "source_url", "page_range", "publisher", "journal_coverage",
            "first_author", "coauthorships_count", "corresponding_author",
            "knowledge_area", "cine_code",
        ]

        def _is_empty(v):
            return v is None or v == "" or v == 0

        # ── Contadores ────────────────────────────────────────
        total_processed = 0
        total_with_changes = 0
        total_errors = 0

        # campo → cuántos canónicos lo recibieron por primera vez
        filled_by_field: dict  = defaultdict(int)
        # campo → cuántos canónicos lo actualizaron (ya tenía valor, cambió)
        updated_by_field: dict = defaultdict(int)
        # fuente → cuántos campos aportó en total
        contrib_by_source: dict = defaultdict(int)
        # muestra de canónicos que cambiaron
        changed_sample: list = []

        offset = 0
        while True:
            batch = (
                self.session.query(CanonicalPublication)
                .order_by(CanonicalPublication.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )
            if not batch:
                break

            for canonical in batch:
                try:
                    # Snapshot antes
                    before = {f: getattr(canonical, f, None) for f in _ENRICHABLE_FIELDS}
                    prov_before = dict(canonical.field_provenance or {})

                    # Enriquecer con todos los registros vinculados de todas las fuentes
                    for src_def in SOURCE_REGISTRY.all():
                        model = src_def.model_class
                        linked = (
                            self.session.query(model)
                            .filter(model.canonical_publication_id == canonical.id)
                            .all()
                        )
                        for ext in linked:
                            self._enrich_canonical(canonical, ext)

                    # Snapshot después
                    after  = {f: getattr(canonical, f, None) for f in _ENRICHABLE_FIELDS}
                    prov_after = dict(canonical.field_provenance or {})

                    newly_filled = []   # vacío → valor
                    updated      = []   # valor → valor distinto

                    for f in _ENRICHABLE_FIELDS:
                        bv, av = before[f], after[f]
                        if bv == av:
                            continue
                        if _is_empty(bv) and not _is_empty(av):
                            newly_filled.append(f)
                            filled_by_field[f] += 1
                            contrib_by_source[prov_after.get(f, "unknown")] += 1
                        elif not _is_empty(bv) and not _is_empty(av) and bv != av:
                            updated.append(f)
                            updated_by_field[f] += 1
                            contrib_by_source[prov_after.get(f, "unknown")] += 1

                    total_processed += 1
                    if newly_filled or updated:
                        total_with_changes += 1
                        if len(changed_sample) < 50:
                            changed_sample.append({
                                "canonical_id":    canonical.id,
                                "doi":             canonical.doi,
                                "title":           (canonical.title or "")[:120],
                                "campos_nuevos":   newly_filled,
                                "campos_actualizados": updated,
                                "fuentes_usadas":  sorted({
                                    prov_after.get(f, "unknown")
                                    for f in newly_filled + updated
                                }),
                            })

                except Exception as e:
                    logger.warning(f"Error enriqueciendo canonical={canonical.id}: {e}")
                    total_errors += 1

            self.session.commit()
            offset += batch_size
            logger.info(
                f"Enriquecimiento lote {offset}: "
                f"{total_with_changes}/{total_processed} con cambios."
            )

        total_filled  = sum(filled_by_field.values())
        total_updated = sum(updated_by_field.values())

        return {
            "resumen": {
                "canonicals_procesados":      total_processed,
                "canonicals_con_cambios":     total_with_changes,
                "canonicals_sin_cambios":     total_processed - total_with_changes,
                "campos_completados_total":   total_filled,
                "campos_actualizados_total":  total_updated,
                "errores":                    total_errors,
                "promedio_cambios_por_canonico": (
                    round((total_filled + total_updated) / total_with_changes, 1)
                    if total_with_changes else 0
                ),
            },
            "campos_completados": dict(
                sorted(filled_by_field.items(), key=lambda x: x[1], reverse=True)
            ),
            "campos_actualizados": dict(
                sorted(updated_by_field.items(), key=lambda x: x[1], reverse=True)
            ),
            "aporte_por_fuente": dict(
                sorted(contrib_by_source.items(), key=lambda x: x[1], reverse=True)
            ),
            "muestra_cambios": changed_sample,
        }


    # ---------------------------------------------------------
    # INGESTA DE AUTORES
    # ---------------------------------------------------------

    def _ingest_authors(self, canonical: CanonicalPublication, ext):
        """
        Crea/vincula autores desde los datos del registro de fuente.

        Busca autores en este orden:
          1. _parsed_authors (inyectados en raw_data durante ingesta)
          2. authors / authorships del raw_data original

        La bandera is_institutional se determina cruzando con
        _parsed_institutional_authors o el campo del dict.
        """
        raw = ext.raw_data or {}

        # --- Obtener lista completa de autores ---
        all_authors = raw.get("_parsed_authors") or []

        # Fallback: formato viejo JSON
        if not all_authors:
            all_authors = raw.get("authors") or []

        # Fallback: formato crudo OpenAlex API (authorships)
        if not all_authors:
            for authorship in raw.get("authorships", []):
                author_info = authorship.get("author") or {}
                name = author_info.get("display_name") or ""
                all_authors.append({
                    "name": name,
                    "orcid": author_info.get("orcid"),
                    "openalex_id": author_info.get("id"),
                    "is_institutional": False,
                })

        if not all_authors:
            return

        # --- Set de IDs/nombres institucionales ---
        inst_set = set()
        for ia in raw.get("_parsed_institutional_authors") or raw.get("institutional_authors") or []:
            if isinstance(ia, dict):
                _n = ia.get("name", "")
                _o = ia.get("orcid", "")
                if _o:
                    inst_set.add(_o)
                if _n:
                    inst_set.add(_n)

        institutional_count = 0
        src = ext.source_name

        for idx, author_data in enumerate(all_authors):
            if isinstance(author_data, str):
                author_data = {"name": author_data}

            raw_name = author_data.get("name") or ""
            name = normalize_author_name(raw_name)
            if not name:
                continue

            # Descartar nombres excesivamente largos
            if len(name) > 200:
                logger.warning(
                    f"  Nombre de autor descartado (demasiado largo, {len(name)} chars): "
                    f"'{name[:80]}...'"
                )
                continue

            name = name[:300]

            orcid = author_data.get("orcid") or ""
            if orcid:
                orcid = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip()
                # Validar formato ORCID: XXXX-XXXX-XXXX-XXXX (último dígito puede ser X)
                if not _re.fullmatch(r'\d{4}-\d{4}-\d{4}-\d{3}[\dX]', orcid):
                    logger.debug(
                        f"  ORCID inválido descartado para '{name}': '{orcid}'"
                    )
                    orcid = ""

            # Determinar si es institucional
            is_inst = author_data.get("is_institutional", False)
            if not is_inst:
                is_inst = (orcid and orcid in inst_set) or (raw_name in inst_set) or (name in inst_set)

            # Buscar autor existente
            author = None
            cedula = author_data.get("cedula")

            # 0. Cédula (columna directa con unique index — máxima prioridad para CvLAC)
            if not author and cedula:
                author = self.session.query(Author).filter_by(cedula=str(cedula).strip()).first()

            if orcid and not author:
                author = self.session.query(Author).filter_by(orcid=orcid).first()

            scopus_id = author_data.get("scopus_id")
            if not author and scopus_id:
                author = (
                    self.session.query(Author)
                    .filter(Author.external_ids["scopus"].astext == str(scopus_id))
                    .first()
                )

            if not author:
                norm_name = normalize_text(name)
                if norm_name:
                    author = self.session.query(Author).filter_by(normalized_name=norm_name).first()

            # --- Match por posición + apellido ---
            if not author:
                from rapidfuzz import fuzz as _fuzz

                new_norm = normalize_text(name) or ""
                new_parts = new_norm.split()
                new_surname = new_parts[0] if new_parts else ""

                if new_surname and len(new_surname) > 2:
                    pub_authors = (
                        self.session.query(Author, PublicationAuthor.author_position)
                        .join(PublicationAuthor, Author.id == PublicationAuthor.author_id)
                        .filter(PublicationAuthor.publication_id == canonical.id)
                        .all()
                    )

                    best_match = None
                    best_score = 0

                    for candidate, cand_pos in pub_authors:
                        c_norm = candidate.normalized_name or ""
                        c_parts = c_norm.split()
                        c_surname = c_parts[0] if c_parts else ""

                        surname_ok = (
                            new_surname == c_surname
                            or (len(c_parts) > 1 and new_surname in c_parts)
                            or (len(new_parts) > 1 and c_surname in new_parts)
                        )
                        if not surname_ok:
                            continue

                        # Solo nombre completo normalizado — sin bonus de posición
                        # El orden de autores varía entre fuentes y no es señal fiable
                        fuzzy_score = _fuzz.token_sort_ratio(new_norm, c_norm)

                        if fuzzy_score > best_score:
                            best_score = fuzzy_score
                            best_match = candidate

                    if best_match and best_score >= 80:
                        author = best_match
                        logger.debug(
                            f"  MATCH por posición+apellido: '{name}' → "
                            f"#{best_match.id} '{best_match.name}' (score={best_score})"
                        )

            # --- Match GLOBAL por apellido + fuzzy ---
            if not author:
                from rapidfuzz import fuzz as _fuzz2

                new_norm = normalize_text(name) or ""
                new_parts = new_norm.split()
                new_surname = new_parts[0] if new_parts else ""

                if new_surname and len(new_surname) > 2:
                    candidates = (
                        self.session.query(Author)
                        .filter(Author.normalized_name.ilike(f"{new_surname}%"))
                        .limit(50)
                        .all()
                    )
                    if len(candidates) < 50:
                        candidates2 = (
                            self.session.query(Author)
                            .filter(
                                Author.normalized_name.ilike(f"% {new_surname}%"),
                                ~Author.id.in_([c.id for c in candidates]),
                            )
                            .limit(20)
                            .all()
                        )
                        candidates.extend(candidates2)

                    best_match = None
                    best_score = 0

                    for candidate in candidates:
                        c_norm = candidate.normalized_name or ""
                        c_parts = c_norm.split()
                        c_surname = c_parts[0] if c_parts else ""

                        surname_ok = (
                            new_surname == c_surname
                            or (len(c_parts) > 1 and new_surname in c_parts)
                            or (len(new_parts) > 1 and c_surname in new_parts)
                        )
                        if not surname_ok:
                            continue

                        fuzzy_score = _fuzz2.token_sort_ratio(new_norm, c_norm)

                        if fuzzy_score > best_score:
                            best_score = fuzzy_score
                            best_match = candidate

                    if best_match and best_score >= 72:
                        author = best_match
                        logger.debug(
                            f"  MATCH GLOBAL por apellido+fuzzy: '{name}' → "
                            f"#{best_match.id} '{best_match.name}' (score={best_score})"
                        )

            if not author:
                # Crear nuevo autor
                author_prov = {"name": src}
                if orcid:
                    author_prov["orcid"] = src
                if is_inst:
                    author_prov["is_institutional"] = src

                eids = {}
                openalex_id = author_data.get("openalex_id")
                if openalex_id:
                    eids["openalex"] = openalex_id
                    author_prov["openalex"] = src
                scopus_id = author_data.get("scopus_id")
                if scopus_id:
                    eids["scopus"] = str(scopus_id)
                    author_prov["scopus"] = src
                wos_id = author_data.get("wos_id")
                if wos_id:
                    eids["wos"] = str(wos_id)
                    author_prov["wos"] = src
                cvlac_id = author_data.get("cvlac_id")
                if cvlac_id:
                    eids["cvlac"] = str(cvlac_id)
                    author_prov["cvlac"] = src

                author = Author(
                    name=normalize_author_name(name) if name else name,
                    normalized_name=normalize_text(name),
                    orcid=orcid if orcid else None,
                    cedula=str(cedula).strip() if cedula else None,
                    is_institutional=is_inst,
                    external_ids=eids if eids else None,
                )

                author.field_provenance = author_prov

                try:
                    with self.session.begin_nested():
                        self.session.add(author)
                        self.session.flush()
                except IntegrityError:
                    author = None
                    if orcid:
                        author = self.session.query(Author).filter_by(orcid=orcid).first()
                    if not author:
                        norm_name = normalize_text(name)
                        author = self.session.query(Author).filter_by(normalized_name=norm_name).first()
                    if not author:
                        continue
            else:
                # Actualizar datos si tenemos info nueva
                prov = dict(author.field_provenance or {})
                changed = False
                if cedula and not author.cedula:
                    author.cedula = str(cedula).strip()
                    prov["cedula"] = src
                    changed = True
                if orcid and not author.orcid:
                    author.orcid = orcid
                    prov["orcid"] = src
                    changed = True
                if is_inst and not author.is_institutional:
                    author.is_institutional = True
                    prov["is_institutional"] = src
                    changed = True
                cur_eids = dict(author.external_ids or {})
                openalex_id = author_data.get("openalex_id")
                if openalex_id and not cur_eids.get("openalex"):
                    cur_eids["openalex"] = openalex_id
                    prov["openalex"] = src
                    changed = True
                scopus_id = author_data.get("scopus_id")
                if scopus_id and not cur_eids.get("scopus"):
                    cur_eids["scopus"] = str(scopus_id)
                    prov["scopus"] = src
                    changed = True
                wos_id = author_data.get("wos_id")
                if wos_id and not cur_eids.get("wos"):
                    cur_eids["wos"] = str(wos_id)
                    prov["wos"] = src
                    changed = True
                cvlac_id = author_data.get("cvlac_id")
                if cvlac_id and not cur_eids.get("cvlac"):
                    cur_eids["cvlac"] = str(cvlac_id)
                    prov["cvlac"] = src
                    changed = True
                if changed:
                    author.external_ids = cur_eids
                    author.field_provenance = prov

            # Vincular a publicación (evitar duplicados)
            existing_link = (
                self.session.query(PublicationAuthor)
                .filter_by(publication_id=canonical.id, author_id=author.id)
                .first()
            )
            if not existing_link:
                try:
                    with self.session.begin_nested():
                        link = PublicationAuthor(
                            publication_id=canonical.id,
                            author_id=author.id,
                            is_institutional=is_inst,
                            author_position=idx,
                        )
                        self.session.add(link)
                        self.session.flush()
                except IntegrityError:
                    pass

            if is_inst:
                institutional_count += 1

        # Actualizar conteo solo si encontramos más que antes
        if institutional_count > (canonical.institutional_authors_count or 0):
            canonical.institutional_authors_count = institutional_count

    # ---------------------------------------------------------
    # BACKFILL: autores de publicaciones canónicas existentes
    # ---------------------------------------------------------

    def backfill_publication_authors(self, batch_size: int = 500) -> dict:
        """
        Recorre TODOS los registros de fuente ya reconciliados y llama
        _ingest_authors() para cada uno, creando/vinculando todos los
        co-autores a sus publicaciones canónicas.

        Útil para poblar publication_authors en bases de datos existentes
        donde las publicaciones se crearon sin procesar autores.

        Returns:
            dict con estadísticas: canonicals_processed, authors_linked,
            authors_created, source_records_processed, errors.
        """
        processed = 0
        authors_linked = 0
        authors_created = 0
        errors = 0

        authors_before = self.session.query(Author).count()

        for source_name, model_cls in SOURCE_MODELS.items():
            offset = 0
            while True:
                batch = (
                    self.session.query(model_cls)
                    .filter(model_cls.canonical_publication_id.isnot(None))
                    .order_by(model_cls.id)
                    .offset(offset)
                    .limit(batch_size)
                    .all()
                )
                if not batch:
                    break

                for ext in batch:
                    try:
                        canonical = (
                            self.session.query(CanonicalPublication)
                            .get(ext.canonical_publication_id)
                        )
                        if not canonical:
                            continue
                        self._ingest_authors(canonical, ext)
                        processed += 1
                    except Exception as e:
                        logger.error(
                            f"Error en backfill_publication_authors "
                            f"{source_name}:{ext.id}: {e}"
                        )
                        errors += 1

                self.session.commit()
                offset += batch_size
                logger.info(
                    f"  [{source_name}] procesados {offset} registros..."
                )

        authors_after = self.session.query(Author).count()
        authors_created = authors_after - authors_before
        authors_linked = (
            self.session.query(PublicationAuthor).count()
        )

        logger.info(
            f"backfill_publication_authors completado: "
            f"{processed} source records, "
            f"{authors_created} autores nuevos, "
            f"{authors_linked} vínculos totales, "
            f"{errors} errores"
        )

        return {
            "source_records_processed": processed,
            "authors_created": authors_created,
            "total_publication_author_links": authors_linked,
            "errors": errors,
        }

    # ---------------------------------------------------------
    # BACKFILL: autores consultando fuentes externas por DOI
    # ---------------------------------------------------------

    def backfill_publication_authors_from_sources(
        self,
        batch_size: int = 100,
        use_openalex: bool = True,
        use_scopus: bool = True,
        use_wos: bool = True,
        scopus_delay: float = 0.3,
    ) -> dict:
        """
        Para cada publicación canónica, re-extrae autores consultando las
        fuentes externas en orden de prioridad:

          1. authors_json del source record en BD  (sin API call)
          2. OpenAlex search_by_doi               (gratis, sin cuota)
          3. Scopus  search_by_doi                (cuota, delay configurable)
          4. WoS     search_by_doi                (cuota)

        Solo sube a la siguiente fuente si la anterior no devuelve autores.

        Returns:
            dict: estadísticas del proceso.
        """
        from extractors.openalex.extractor import OpenAlexExtractor
        from extractors.scopus import ScopusExtractor
        from extractors.wos import WosExtractor

        # Inicializar extractores (solo si están habilitados)
        oa_ext  = OpenAlexExtractor() if use_openalex else None
        sc_ext  = ScopusExtractor()   if use_scopus  else None
        wos_ext = WosExtractor()      if use_wos     else None

        stats = {
            "canonicals_processed": 0,
            "authors_linked": 0,
            "from_db": 0,
            "from_openalex": 0,
            "from_scopus": 0,
            "from_wos": 0,
            "no_authors_found": 0,
            "errors": 0,
        }

        # Clase auxiliar para pasar un StandardRecord a _ingest_authors
        # (que espera un objeto con .raw_data y .source_name)
        class _FakeExt:
            def __init__(self, record: StandardRecord):
                raw = dict(record.raw_data or {})
                raw["_parsed_authors"] = record.authors or []
                raw["_parsed_institutional_authors"] = record.institutional_authors or []
                self.raw_data = raw
                self.source_name = record.source_name

        # Función auxiliar: intentar obtener autores desde authors_json en BD
        def _authors_from_db(canonical_id: int) -> Optional[tuple]:
            """
            Busca en los source records vinculados al canónico.
            Retorna (lista_autores, source_name) o None.
            """
            for src_name, model_cls in SOURCE_MODELS.items():
                rec = (
                    self.session.query(model_cls)
                    .filter(model_cls.canonical_publication_id == canonical_id)
                    .first()
                )
                if rec is None:
                    continue
                # Intentar _parsed_authors en raw_data primero
                raw = rec.raw_data or {}
                authors = (
                    raw.get("_parsed_authors")
                    or raw.get("authors")
                    or []
                )
                # Fallback a authorships (OpenAlex)
                if not authors:
                    for auth in raw.get("authorships", []):
                        info = auth.get("author") or {}
                        name = info.get("display_name", "")
                        if name:
                            authors.append({
                                "name": name,
                                "orcid": info.get("orcid"),
                                "openalex_id": info.get("id"),
                                "is_institutional": False,
                            })
                # Fallback a authors_json columna
                if not authors and hasattr(rec, "authors_json"):
                    authors = rec.authors_json or []
                if authors:
                    return (authors, src_name, rec)
            return None

        # Procesar en lotes
        offset = 0
        while True:
            canonicals = (
                self.session.query(CanonicalPublication)
                .order_by(CanonicalPublication.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )
            if not canonicals:
                break

            for canonical in canonicals:
                try:
                    record_used: Optional[StandardRecord] = None
                    source_tag = None

                    # ── Nivel 1: source records en BD ──────────────────────
                    db_result = _authors_from_db(canonical.id)
                    if db_result:
                        authors_list, src_name, src_rec = db_result
                        fake_raw = dict(src_rec.raw_data or {})
                        fake_raw["_parsed_authors"] = authors_list
                        fake_raw["_parsed_institutional_authors"] = (
                            fake_raw.get("_parsed_institutional_authors") or []
                        )
                        import types as _types
                        fake_ext = _types.SimpleNamespace(
                            raw_data=fake_raw, source_name=src_name
                        )
                        self._ingest_authors(canonical, fake_ext)
                        stats["from_db"] += 1
                        source_tag = "db"

                    # ── Nivel 2: OpenAlex por DOI ──────────────────────────
                    if not source_tag and oa_ext and canonical.doi:
                        try:
                            record_used = oa_ext.search_by_doi(canonical.doi)
                            if record_used and record_used.authors:
                                self._ingest_authors(canonical, _FakeExt(record_used))
                                stats["from_openalex"] += 1
                                source_tag = "openalex"
                        except Exception as e:
                            logger.debug(f"OpenAlex DOI {canonical.doi}: {e}")

                    # ── Nivel 3: Scopus por DOI ────────────────────────────
                    if not source_tag and sc_ext and canonical.doi:
                        try:
                            record_used = sc_ext.search_by_doi(canonical.doi)
                            if record_used and record_used.authors:
                                self._ingest_authors(canonical, _FakeExt(record_used))
                                stats["from_scopus"] += 1
                                source_tag = "scopus"
                            if scopus_delay > 0:
                                import time as _time
                                _time.sleep(scopus_delay)
                        except Exception as e:
                            logger.debug(f"Scopus DOI {canonical.doi}: {e}")

                    # ── Nivel 4: WoS por DOI ───────────────────────────────
                    if not source_tag and wos_ext and canonical.doi:
                        try:
                            record_used = wos_ext.search_by_doi(canonical.doi)
                            if record_used and record_used.authors:
                                self._ingest_authors(canonical, _FakeExt(record_used))
                                stats["from_wos"] += 1
                                source_tag = "wos"
                        except Exception as e:
                            logger.debug(f"WoS DOI {canonical.doi}: {e}")

                    if not source_tag:
                        stats["no_authors_found"] += 1
                        logger.debug(
                            f"Sin autores para canonical {canonical.id} "
                            f"'{(canonical.title or '')[:60]}'"
                        )

                    stats["canonicals_processed"] += 1

                except Exception as e:
                    logger.error(
                        f"Error procesando canonical {canonical.id}: {e}",
                        exc_info=True,
                    )
                    stats["errors"] += 1

            self.session.commit()
            offset += batch_size
            logger.info(
                f"[backfill_from_sources] procesados {offset} canónicos... "
                f"({stats['from_db']} BD / {stats['from_openalex']} OA / "
                f"{stats['from_scopus']} Scopus / {stats['from_wos']} WoS)"
            )

        stats["authors_linked"] = self.session.query(PublicationAuthor).count()
        logger.info(f"backfill_from_sources completado: {stats}")
        return stats

    # ---------------------------------------------------------
    # BACKFILL: completar IDs de autores desde raw_data
    # ---------------------------------------------------------

    def backfill_author_ids(self):
        """
        Recorre los registros ya reconciliados de TODAS las tablas de fuente
        y completa scopus_id / openalex_id en los autores existentes.
        Se ejecuta automáticamente al final de cada reconciliación.
        """
        from rapidfuzz import fuzz

        updated = 0

        scopus_expr = Author.external_ids["scopus"].astext
        existing_sids = {
            r[0] for r in
            self.session.query(scopus_expr)
            .filter(scopus_expr.isnot(None), scopus_expr != "")
            .all()
        }

        # Iterar todas las tablas de fuente
        for source_name, model_cls in SOURCE_MODELS.items():
            externals = (
                self.session.query(model_cls)
                .filter(
                    model_cls.canonical_publication_id.isnot(None),
                    model_cls.raw_data.isnot(None),
                )
                .all()
            )

            for ext in externals:
                raw = ext.raw_data or {}
                pub_id = ext.canonical_publication_id

                # Obtener lista de autores del raw
                authors_raw = raw.get("_parsed_authors") or []
                if not authors_raw:
                    authors_raw = raw.get("author") or []
                if not authors_raw:
                    for authorship in raw.get("authorships", []):
                        author_info = authorship.get("author") or {}
                        authors_raw.append({
                            "name": author_info.get("display_name", ""),
                            "openalex_id": author_info.get("id"),
                            "orcid":       author_info.get("orcid"),
                        })

                for auth_data in authors_raw:
                    if isinstance(auth_data, str):
                        continue

                    scopus_id = auth_data.get("scopus_id") or auth_data.get("authid")
                    openalex_id = auth_data.get("openalex_id")
                    orcid = auth_data.get("orcid") or ""
                    if orcid:
                        orcid = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip()

                    if not scopus_id and not openalex_id:
                        continue

                    if scopus_id and str(scopus_id) in existing_sids:
                        continue

                    # Estrategia 1: match exacto por IDs o nombre
                    author = None
                    if orcid:
                        author = self.session.query(Author).filter_by(orcid=orcid).first()
                    if not author and scopus_id:
                        author = (
                            self.session.query(Author)
                            .filter(Author.external_ids["scopus"].astext == str(scopus_id))
                            .first()
                        )
                    if not author and openalex_id:
                        author = (
                            self.session.query(Author)
                            .filter(Author.external_ids["openalex"].astext == openalex_id)
                            .first()
                        )
                    if not author:
                        name = auth_data.get("name") or auth_data.get("authname") or ""
                        norm = normalize_text(normalize_author_name(name))
                        if norm:
                            author = self.session.query(Author).filter_by(normalized_name=norm).first()

                    # Estrategia 2: match por publicación + fuzzy apellido
                    if not author and pub_id and scopus_id:
                        name = auth_data.get("name") or auth_data.get("authname") or ""
                        sc_norm = normalize_text(normalize_author_name(name)) or ""
                        sc_parts = sc_norm.split()
                        sc_surname = sc_parts[0] if sc_parts else ""

                        if sc_surname and len(sc_surname) > 2:
                            pub_authors = (
                                self.session.query(Author)
                                .join(PublicationAuthor, Author.id == PublicationAuthor.author_id)
                                .filter(
                                    PublicationAuthor.publication_id == pub_id,
                                    ~Author.external_ids.has_key("scopus"),
                                )
                                .all()
                            )
                            best_match = None
                            best_score = 0
                            for candidate in pub_authors:
                                c_norm = candidate.normalized_name or ""
                                c_parts = c_norm.split()
                                c_surname = c_parts[0] if c_parts else ""
                                if sc_surname == c_surname or (
                                    len(c_parts) > 1 and sc_surname in c_parts
                                ):
                                    score = fuzz.token_sort_ratio(sc_norm, c_norm)
                                    if score > best_score:
                                        best_score = score
                                        best_match = candidate
                            if best_match and best_score >= 65:
                                author = best_match

                    if not author:
                        continue

                    changed = False
                    prov = dict(author.field_provenance or {})
                    backfill_src = source_name
                    bf_eids = dict(author.external_ids or {})
                    if scopus_id and not bf_eids.get("scopus"):
                        bf_eids["scopus"] = str(scopus_id)
                        existing_sids.add(str(scopus_id))
                        prov["scopus"] = backfill_src
                        changed = True
                    if openalex_id and not bf_eids.get("openalex"):
                        bf_eids["openalex"] = openalex_id
                        prov["openalex"] = backfill_src
                        changed = True
                    if orcid and not author.orcid:
                        author.orcid = orcid
                        prov["orcid"] = backfill_src
                        changed = True
                    if changed:
                        author.external_ids = bf_eids
                        author.field_provenance = prov
                        updated += 1

        if updated > 0:
            self.session.commit()
            logger.info(f"Backfill de IDs de autores: {updated} autores actualizados")


# =============================================================
# HELPER DE MÓDULO (fuera de la clase)
# =============================================================

def _count_filled(canonical: CanonicalPublication) -> int:
    """Cuenta cuántos campos enriquecibles tienen valor en una publicación canónica."""
    fields = [
        "doi", "source_journal", "publication_type", "is_open_access",
        "oa_status", "issn", "citation_count", "publication_year",
        "publication_date", "language", "abstract", "keywords",
        "source_url", "page_range", "publisher", "journal_coverage",
        "first_author", "coauthorships_count", "corresponding_author",
        "knowledge_area", "cine_code",
    ]
    return sum(1 for f in fields if getattr(canonical, f, None) not in (None, "", 0))
