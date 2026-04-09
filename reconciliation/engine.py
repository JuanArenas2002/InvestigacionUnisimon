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
  │      Score < manual_review → ir a PASO 3            │
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
    SOURCE_MODELS,
    find_record_by_doi_across_sources,
)
from db.source_registry import SOURCE_REGISTRY
from db.session import get_session
from extractors.base import StandardRecord, normalize_text, normalize_doi, normalize_author_name
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
            publication_type=record.publication_type,
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
                self.session.add(source_record)
                self.session.flush()

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

            except IntegrityError:
                self.session.rollback()
                skipped += 1
                logger.debug(
                    f"Duplicado detectado por constraint DB: "
                    f"{record.source_name}:{record.source_id}"
                )
                continue
            except Exception as e:
                self.session.rollback()
                logger.warning(f"Error insertando registro: {e}")
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
        # PASO 3: Crear nueva publicación canónica
        # =====================================================
        self._create_new_canonical(ext, now)
        self.stats.new_canonical += 1

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

        initial_cites = ext.citation_count or 0
        canonical = CanonicalPublication(
            doi=ndoi,
            title=raw_title,
            normalized_title=ext.normalized_title,
            publication_year=ext.publication_year,
            publication_date=ext.publication_date,
            publication_type=ext.publication_type,
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
            canonical.publication_type = ext.publication_type
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

        # Abstract
        if not canonical.abstract and hasattr(ext, "abstract") and ext.abstract:
            canonical.abstract = ext.abstract
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

        # Rango de páginas
        if not canonical.page_range and hasattr(ext, "page_range") and ext.page_range:
            canonical.page_range = ext.page_range
            enriched_fields.append("page_range")
            prov["page_range"] = src

        # Editorial / publisher
        if not canonical.publisher and hasattr(ext, "publisher") and ext.publisher:
            canonical.publisher = ext.publisher
            enriched_fields.append("publisher")
            prov["publisher"] = src

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
            if orcid:
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
                    name=name,
                    normalized_name=normalize_text(name),
                    orcid=orcid if orcid else None,
                    is_institutional=is_inst,
                    external_ids=eids if eids else None,
                )

                author.field_provenance = author_prov

                try:
                    self.session.add(author)
                    self.session.flush()
                except IntegrityError:
                    self.session.rollback()
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
                    link = PublicationAuthor(
                        publication_id=canonical.id,
                        author_id=author.id,
                        is_institutional=is_inst,
                        author_position=idx,
                    )
                    self.session.add(link)
                    self.session.flush()
                except IntegrityError:
                    self.session.rollback()
                    pass

            if is_inst:
                institutional_count += 1

        # Actualizar conteo solo si encontramos más que antes
        if institutional_count > (canonical.institutional_authors_count or 0):
            canonical.institutional_authors_count = institutional_count

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
