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
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Any

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
    count_source_records_for_canonical,
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
        # Para agregar una nueva fuente: crear build_X_kwargs en db/source_builders.py
        # y registrarla en db/models.py con SOURCE_REGISTRY.register().
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

        Protección anti-duplicados en 4 niveles:
          1. Hash determinista (dedup_hash) con UNIQUE constraint en DB
          2. Consulta previa por source_id (columna específica de la tabla)
          3. Consulta por DOI dentro de la misma tabla de fuente
          4. Consulta por título normalizado + año

        Returns:
            Número de registros insertados (excluyendo duplicados)
        """
        inserted = 0
        skipped = 0

        for record in records:
            try:
                model_cls = SOURCE_MODELS.get(record.source_name)
                if not model_cls:
                    logger.warning(f"Fuente desconocida: {record.source_name}, omitiendo")
                    skipped += 1
                    continue

                sid_attr = _SOURCE_ID_ATTR.get(record.source_name)

                # --- Calcular hash de deduplicación ---
                dedup = self._compute_dedup_hash(
                    record.source_name, record.source_id,
                    record.doi, record.normalized_title, record.publication_year,
                )

                # --- Nivel 1: verificar por dedup_hash ---
                exists_hash = (
                    self.session.query(model_cls.id)
                    .filter_by(dedup_hash=dedup)
                    .first()
                )
                if exists_hash:
                    skipped += 1
                    continue

                # --- Nivel 2: verificar por source_id ---
                if record.source_id and sid_attr:
                    exists_sid = (
                        self.session.query(model_cls.id)
                        .filter(getattr(model_cls, sid_attr) == record.source_id)
                        .first()
                    )
                    if exists_sid:
                        skipped += 1
                        continue

                # --- Nivel 3: verificar por DOI ---
                if record.doi:
                    ndoi = normalize_doi(record.doi)
                    if ndoi:
                        exists_doi = (
                            self.session.query(model_cls.id)
                            .filter_by(doi=ndoi)
                            .first()
                        )
                        if exists_doi:
                            skipped += 1
                            continue

                # --- Nivel 4: verificar por título normalizado + año ---
                if record.normalized_title and record.publication_year:
                    exists_title = (
                        self.session.query(model_cls.id)
                        .filter_by(
                            normalized_title=record.normalized_title,
                            publication_year=record.publication_year,
                        )
                        .first()
                    )
                    if exists_title:
                        skipped += 1
                        continue

                # --- Insertar registro en la tabla de fuente ---
                source_record = self._build_source_record(record, dedup)
                self.session.add(source_record)
                self.session.flush()
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

        Optimización: solo compara con canónicas del mismo año
        (o año ± tolerancia si está configurado).
        """
        best_canonical = None
        best_result = None
        best_score = 0.0

        # Filtrar candidatos por año para reducir comparaciones
        candidates_query = self.session.query(CanonicalPublication)

        if ext.publication_year and rc_config.year_must_match:
            tolerance = rc_config.year_tolerance
            candidates_query = candidates_query.filter(
                CanonicalPublication.publication_year.between(
                    ext.publication_year - tolerance,
                    ext.publication_year + tolerance,
                )
            )

        candidates = candidates_query.all()

        for canonical in candidates:
            # Cargar autores de la publicación canónica
            canon_authors = (
                self.session.query(Author.name)
                .join(PublicationAuthor, Author.id == PublicationAuthor.author_id)
                .filter(PublicationAuthor.publication_id == canonical.id)
                .all()
            )
            authors_b_text = "; ".join(a[0] for a in canon_authors) if canon_authors else ""

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

        # Actualizar conteo de fuentes (cross-source)
        canonical.sources_count = (
            count_source_records_for_canonical(self.session, canonical.id) + 1
        )

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

        # Persistir provenance actualizado
        if enriched_fields:
            canonical.field_provenance = prov
            logger.info(
                f"  ENRIQUECIDO canon={canonical.id} con {src}: "
                f"{', '.join(enriched_fields)}"
            )

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
                author = self.session.query(Author).filter_by(scopus_id=str(scopus_id)).first()

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

                author = Author(
                    name=name,
                    normalized_name=normalize_text(name),
                    orcid=orcid if orcid else None,
                    is_institutional=is_inst,
                )
                openalex_id = author_data.get("openalex_id")
                if openalex_id:
                    author.openalex_id = openalex_id
                    author_prov["openalex_id"] = src
                scopus_id = author_data.get("scopus_id")
                if scopus_id:
                    author.scopus_id = str(scopus_id)
                    author_prov["scopus_id"] = src
                wos_id = author_data.get("wos_id")
                if wos_id:
                    author.wos_id = str(wos_id)
                    author_prov["wos_id"] = src
                cvlac_id = author_data.get("cvlac_id")
                if cvlac_id:
                    author.cvlac_id = str(cvlac_id)
                    author_prov["cvlac_id"] = src

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
                openalex_id = author_data.get("openalex_id")
                if openalex_id and not author.openalex_id:
                    author.openalex_id = openalex_id
                    prov["openalex_id"] = src
                    changed = True
                scopus_id = author_data.get("scopus_id")
                if scopus_id and not author.scopus_id:
                    author.scopus_id = str(scopus_id)
                    prov["scopus_id"] = src
                    changed = True
                wos_id = author_data.get("wos_id")
                if wos_id and not author.wos_id:
                    author.wos_id = str(wos_id)
                    prov["wos_id"] = src
                    changed = True
                cvlac_id = author_data.get("cvlac_id")
                if cvlac_id and not author.cvlac_id:
                    author.cvlac_id = str(cvlac_id)
                    prov["cvlac_id"] = src
                    changed = True
                if changed:
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

        existing_sids = {
            r[0] for r in
            self.session.query(Author.scopus_id)
            .filter(Author.scopus_id.isnot(None), Author.scopus_id != "")
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
                            "orcid": author_info.get("orcid"),
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
                        author = self.session.query(Author).filter_by(scopus_id=str(scopus_id)).first()
                    if not author and openalex_id:
                        author = self.session.query(Author).filter_by(openalex_id=openalex_id).first()
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
                                    (Author.scopus_id.is_(None)) | (Author.scopus_id == ""),
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
                    if scopus_id and not author.scopus_id:
                        author.scopus_id = str(scopus_id)
                        existing_sids.add(str(scopus_id))
                        prov["scopus_id"] = backfill_src
                        changed = True
                    if openalex_id and not author.openalex_id:
                        author.openalex_id = openalex_id
                        prov["openalex_id"] = backfill_src
                        changed = True
                    if orcid and not author.orcid:
                        author.orcid = orcid
                        prov["orcid"] = backfill_src
                        changed = True
                    if changed:
                        author.field_provenance = prov
                        updated += 1

        if updated > 0:
            self.session.commit()
            logger.info(f"Backfill de IDs de autores: {updated} autores actualizados")
