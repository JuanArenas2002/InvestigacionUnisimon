"""
Motor de Reconciliación Bibliográfica.

Orquesta el proceso de vincular registros de múltiples fuentes
a publicaciones canónicas únicas.

Flujo en cascada:
  ┌─────────────────────────────────────────────────┐
  │  Por cada ExternalRecord con status='pending':  │
  │                                                 │
  │  PASO 1: ¿Tiene DOI?                           │
  │    SÍ → Buscar en canonical_publications        │
  │       Encontrado → VINCULAR (doi_exact)         │
  │       No encontrado → Buscar en otros           │
  │         external_records con mismo DOI           │
  │           Encontrado → VINCULAR al mismo canon  │
  │           No encontrado → ir a PASO 3           │
  │    NO → ir a PASO 2                             │
  │                                                 │
  │  PASO 2: Fuzzy matching                         │
  │    Comparar título+año+autores contra TODOS     │
  │    los canonical_publications                   │
  │      Score >= combined_threshold → VINCULAR     │
  │      Score >= manual_review → MARCAR REVISIÓN   │
  │      Score < manual_review → ir a PASO 3        │
  │                                                 │
  │  PASO 3: Crear nueva publicación canónica       │
  │    Insertar en canonical_publications           │
  │    Vincular el external_record                  │
  └─────────────────────────────────────────────────┘
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from config import (
    reconciliation_config as rc_config,
    MatchType,
    RecordStatus,
)
from db.models import (
    CanonicalPublication,
    ExternalRecord,
    Author,
    PublicationAuthor,
    ReconciliationLog,
)
from db.session import get_session
from extractors.base import StandardRecord, normalize_text, normalize_doi, normalize_author_name
from reconciliation.fuzzy_matcher import compare_records, FuzzyMatchResult

logger = logging.getLogger(__name__)


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

    def ingest_records(self, records: List[StandardRecord]) -> int:
        """
        PASO 0: Ingesta — inserta StandardRecords en external_records.
        Los marca como 'pending' para reconciliación posterior.

        Protección anti-duplicados en 3 niveles:
          1. Hash determinista (dedup_hash) con UNIQUE constraint en DB
          2. Consulta previa por source_name+source_id, DOI, o título+año
          3. Captura de IntegrityError como último recurso

        Returns:
            Número de registros insertados (excluyendo duplicados)
        """
        inserted = 0
        skipped = 0

        for record in records:
            try:
                # --- Calcular hash de deduplicación ---
                dedup = self._compute_dedup_hash(
                    record.source_name, record.source_id,
                    record.doi, record.normalized_title, record.publication_year,
                )

                # --- Nivel 1: verificar por dedup_hash (más rápido, cubre TODO) ---
                exists_hash = (
                    self.session.query(ExternalRecord.id)
                    .filter_by(dedup_hash=dedup)
                    .first()
                )
                if exists_hash:
                    skipped += 1
                    continue

                # --- Nivel 2: verificar por source_name + source_id ---
                if record.source_id:
                    exists_sid = (
                        self.session.query(ExternalRecord.id)
                        .filter_by(
                            source_name=record.source_name,
                            source_id=record.source_id,
                        )
                        .first()
                    )
                    if exists_sid:
                        skipped += 1
                        continue

                # --- Nivel 3: verificar por source_name + DOI ---
                if record.doi:
                    ndoi = normalize_doi(record.doi)
                    if ndoi:
                        exists_doi = (
                            self.session.query(ExternalRecord.id)
                            .filter_by(source_name=record.source_name, doi=ndoi)
                            .first()
                        )
                        if exists_doi:
                            skipped += 1
                            continue

                # --- Nivel 4: verificar por source + título normalizado + año ---
                if record.normalized_title and record.publication_year:
                    exists_title = (
                        self.session.query(ExternalRecord.id)
                        .filter_by(
                            source_name=record.source_name,
                            normalized_title=record.normalized_title,
                            publication_year=record.publication_year,
                        )
                        .first()
                    )
                    if exists_title:
                        skipped += 1
                        continue

                # --- Insertar ---
                enriched_raw = dict(record.raw_data) if record.raw_data else {}
                enriched_raw["_parsed_authors"] = record.authors or []
                enriched_raw["_parsed_institutional_authors"] = record.institutional_authors or []

                ext = ExternalRecord(
                    source_name=record.source_name,
                    source_id=record.source_id,
                    dedup_hash=dedup,
                    doi=record.doi,
                    title=record.title,
                    normalized_title=record.normalized_title,
                    publication_year=record.publication_year,
                    authors_text=record.authors_text,
                    normalized_authors=record.normalized_authors,
                    raw_data=enriched_raw,
                    status=RecordStatus.PENDING,
                )
                self.session.add(ext)
                self.session.flush()  # Forzar INSERT para detectar constraint violation
                inserted += 1

            except IntegrityError:
                self.session.rollback()
                skipped += 1
                logger.debug(f"Duplicado detectado por constraint DB: {record.source_name}:{record.source_id}")
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

    def reconcile_pending(self, batch_size: int = 500) -> ReconciliationStats:
        """
        Procesa todos los external_records con status='pending'.

        Args:
            batch_size: Cuántos registros procesar por lote

        Returns:
            Estadísticas del proceso
        """
        self.stats = ReconciliationStats()

        pending = (
            self.session.query(ExternalRecord)
            .filter_by(status=RecordStatus.PENDING)
            .limit(batch_size)
            .all()
        )

        logger.info(f"Reconciliando {len(pending)} registros pendientes...")

        for ext_record in pending:
            try:
                self._reconcile_one(ext_record)
                self.stats.total_processed += 1
            except Exception as e:
                logger.error(f"Error reconciliando record {ext_record.id}: {e}")
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

    def _reconcile_one(self, ext: ExternalRecord):
        """
        Ejecuta la cascada completa para UN registro externo.
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

                # 1b. ¿Hay otro external_record con este DOI ya reconciliado?
                sibling = (
                    self.session.query(ExternalRecord)
                    .filter(
                        ExternalRecord.doi == normalized_doi,
                        ExternalRecord.id != ext.id,
                        ExternalRecord.canonical_publication_id.isnot(None),
                    )
                    .first()
                )

                if sibling and sibling.canonical_publication_id:
                    canonical = (
                        self.session.query(CanonicalPublication)
                        .get(sibling.canonical_publication_id)
                    )
                    if canonical:
                        self._link_to_canonical(
                            ext, canonical, MatchType.DOI_EXACT, 100.0,
                            {"method": "doi_exact_sibling", "sibling_id": sibling.id},
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

    def _find_fuzzy_match(
        self, ext: ExternalRecord
    ) -> Tuple[Optional[CanonicalPublication], Optional[FuzzyMatchResult]]:
        """
        Busca la mejor coincidencia fuzzy entre el registro externo
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
        ext: ExternalRecord,
        canonical: CanonicalPublication,
        match_type: str,
        score: float,
        details: dict,
        timestamp: datetime,
    ):
        """Vincula un registro externo a una publicación canónica existente"""
        ext.canonical_publication_id = canonical.id
        ext.status = RecordStatus.MATCHED
        ext.match_type = match_type
        ext.match_score = score
        ext.reconciled_at = timestamp

        # Actualizar conteo de fuentes
        canonical.sources_count = (
            self.session.query(ExternalRecord)
            .filter_by(canonical_publication_id=canonical.id)
            .count()
        ) + 1  # +1 por el que estamos vinculando ahora

        # Enriquecer la canonica con datos faltantes de esta fuente
        self._enrich_canonical(canonical, ext)

        # Ingestar autores (pueden venir de una fuente distinta)
        self._ingest_authors(canonical, ext)

        # Log de auditoría
        log = ReconciliationLog(
            external_record_id=ext.id,
            canonical_publication_id=canonical.id,
            match_type=match_type,
            match_score=score,
            match_details=details,
            action="linked_existing",
        )
        self.session.add(log)

        logger.debug(
            f"  VINCULADO: ext={ext.id} → canon={canonical.id} "
            f"({match_type}, score={score:.1f})"
        )

    def _create_new_canonical(
        self, ext: ExternalRecord, timestamp: datetime
    ):
        """Crea una nueva publicación canónica a partir de un registro externo.
        Incluye protección contra DOI duplicado."""
        ndoi = normalize_doi(ext.doi) if ext.doi else None

        # --- Verificación final anti-duplicado por DOI ---
        if ndoi:
            existing = (
                self.session.query(CanonicalPublication)
                .filter_by(doi=ndoi)
                .first()
            )
            if existing:
                # Ya existe una canónica con ese DOI → vincular en vez de crear
                logger.info(
                    f"  DOI {ndoi} ya tiene canónica {existing.id}, vinculando en vez de crear nueva."
                )
                self._link_to_canonical(
                    ext, existing, MatchType.DOI_EXACT, 100.0,
                    {"method": "doi_safety_check_at_creation"}, timestamp
                )
                self.stats.doi_exact_matches += 1
                self.stats.new_canonical -= 1  # Compensar el incremento que viene después
                return

        # Construir provenance inicial: todos los campos vienen de esta fuente
        provenance = {}
        src = ext.source_name
        if ndoi:
            provenance["doi"] = src
        if ext.title:
            provenance["title"] = src
        if ext.publication_year:
            provenance["publication_year"] = src
        ptype = self._extract_type(ext)
        if ptype:
            provenance["publication_type"] = src
        journal = self._extract_journal(ext)
        if journal:
            provenance["source_journal"] = src
        oa = self._extract_oa(ext)
        if oa is not None:
            provenance["is_open_access"] = src
        cites = self._extract_citations(ext)
        if cites:
            provenance["citation_count"] = src

        canonical = CanonicalPublication(
            doi=ndoi,
            title=ext.title or "Sin título",
            normalized_title=ext.normalized_title,
            publication_year=ext.publication_year,
            publication_type=ptype,
            source_journal=journal,
            is_open_access=oa,
            citation_count=cites,
            sources_count=1,
            field_provenance=provenance,
        )

        try:
            self.session.add(canonical)
            self.session.flush()  # Para obtener el ID
        except IntegrityError:
            # DOI duplicado detectado a nivel DB — recuperar la existente
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

        # Vincular el registro externo
        ext.canonical_publication_id = canonical.id
        ext.status = RecordStatus.NEW_CANONICAL
        ext.match_type = MatchType.NO_MATCH
        ext.match_score = 0.0
        ext.reconciled_at = timestamp

        # Ingestar autores si hay datos
        self._ingest_authors(canonical, ext)

        # Log
        log = ReconciliationLog(
            external_record_id=ext.id,
            canonical_publication_id=canonical.id,
            match_type=MatchType.NO_MATCH,
            match_score=0.0,
            match_details={"method": "new_canonical_created"},
            action="created_new",
        )
        self.session.add(log)

        logger.debug(
            f"  NUEVO: ext={ext.id} → canon={canonical.id} "
            f"('{canonical.title[:50]}...')"
        )

    def _flag_for_review(
        self,
        ext: ExternalRecord,
        candidate: CanonicalPublication,
        score: float,
        details: dict,
        timestamp: datetime,
    ):
        """Marca un registro para revisión manual"""
        ext.status = RecordStatus.REVIEW
        ext.match_type = MatchType.MANUAL_REVIEW
        ext.match_score = score
        ext.reconciled_at = timestamp

        log = ReconciliationLog(
            external_record_id=ext.id,
            canonical_publication_id=candidate.id,
            match_type=MatchType.MANUAL_REVIEW,
            match_score=score,
            match_details=details,
            action="flagged_review",
        )
        self.session.add(log)

        logger.debug(
            f"  REVISIÓN: ext={ext.id} , posible_canon={candidate.id} "
            f"(score={score:.1f})"
        )

    # ---------------------------------------------------------
    # INGESTA DE AUTORES
    # ---------------------------------------------------------

    def _ingest_authors(
        self, canonical: CanonicalPublication, ext: ExternalRecord
    ):
        """
        Crea/vincula autores desde los datos del external_record.

        Busca autores en este orden de prioridad:
          1. _parsed_authors (inyectados en ingest_records, formato homogeneo)
          2. authors / authorships del raw_data original

        Siempre usa la lista COMPLETA de autores. La bandera
        is_institutional se determina cruzando con la lista
        _parsed_institutional_authors o el campo del dict.
        """
        raw = ext.raw_data or {}

        # --- Obtener lista completa de autores ---
        all_authors = raw.get("_parsed_authors") or []

        # Fallback: formato viejo JSON que tiene "authors" directamente
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

        # --- Construir set de IDs/nombres institucionales para lookup rapido ---
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

        for idx, author_data in enumerate(all_authors):
            if isinstance(author_data, str):
                author_data = {"name": author_data}

            raw_name = author_data.get("name") or ""
            name = normalize_author_name(raw_name)
            if not name:
                continue

            # Descartar "nombres" que en realidad son biografías o textos largos
            # Un nombre real rara vez supera los 150 caracteres
            if len(name) > 200:
                logger.warning(
                    f"  Nombre de autor descartado (demasiado largo, {len(name)} chars): "
                    f"'{name[:80]}...'"
                )
                continue

            # Truncar a 300 por seguridad (límite de la columna)
            name = name[:300]

            orcid = author_data.get("orcid") or ""
            # Normalizar ORCID: quitar prefijo URL
            if orcid:
                orcid = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip()

            # Determinar si es institucional
            is_inst = author_data.get("is_institutional", False)
            if not is_inst:
                is_inst = (orcid and orcid in inst_set) or (raw_name in inst_set) or (name in inst_set)

            # Buscar autor existente (por ORCID, Scopus ID, o nombre normalizado)
            author = None
            if orcid:
                author = (
                    self.session.query(Author)
                    .filter_by(orcid=orcid)
                    .first()
                )

            scopus_id = author_data.get("scopus_id")
            if not author and scopus_id:
                author = (
                    self.session.query(Author)
                    .filter_by(scopus_id=str(scopus_id))
                    .first()
                )

            if not author:
                norm_name = normalize_text(name)
                if norm_name:
                    author = (
                        self.session.query(Author)
                        .filter_by(normalized_name=norm_name)
                        .first()
                    )

            # Fuente del registro externo
            src = ext.source_name

            # --- Paso extra: matching por posición + apellido ---
            # Si no encontramos por ORCID/ScopusID/nombre exacto, buscar
            # entre los autores que YA están vinculados a esta publicación
            # en la misma posición. Esto resuelve nombres abreviados de
            # Scopus ("Miranda Giraldo M.") vs nombres completos de OpenAlex
            # ("Michael Miranda Giraldo").
            if not author:
                from rapidfuzz import fuzz as _fuzz

                new_norm = normalize_text(name) or ""
                new_parts = new_norm.split()
                # Extraer apellido(s): para "miranda giraldo m" → ["miranda", "giraldo"]
                # Para "michael miranda giraldo" → ["michael", "miranda", "giraldo"]
                new_surname = new_parts[0] if new_parts else ""

                if new_surname and len(new_surname) > 2:
                    # Buscar autores ya vinculados a esta publicación
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

                        # El apellido debe coincidir (o estar contenido)
                        surname_ok = (
                            new_surname == c_surname
                            or (len(c_parts) > 1 and new_surname in c_parts)
                            or (len(new_parts) > 1 and c_surname in new_parts)
                        )
                        if not surname_ok:
                            continue

                        # Misma posición da un bonus fuerte
                        position_match = (cand_pos == idx)

                        # Score fuzzy del nombre completo
                        fuzzy_score = _fuzz.token_sort_ratio(new_norm, c_norm)

                        # Combinar: posición + fuzzy
                        combined = fuzzy_score + (20 if position_match else 0)

                        if combined > best_score:
                            best_score = combined
                            best_match = candidate

                    # Umbral: 60 fuzzy + 20 posición = 80 es match seguro
                    # Pero aceptamos 65+ si la posición coincide
                    if best_match and best_score >= 65:
                        author = best_match
                        logger.debug(
                            f"  MATCH por posición+apellido: '{name}' → "
                            f"#{best_match.id} '{best_match.name}' (score={best_score})"
                        )

            # --- Paso extra 2: matching GLOBAL por apellido + fuzzy ---
            # Si tampoco lo encontramos en la misma publicación, buscar
            # en TODA la tabla de autores por apellido coincidente.
            # Esto cubre el caso de publicaciones distintas con el mismo autor
            # en formatos diferentes ("Miranda Giraldo M." vs "Michael Miranda Giraldo").
            if not author:
                from rapidfuzz import fuzz as _fuzz2

                new_norm = normalize_text(name) or ""
                new_parts = new_norm.split()
                new_surname = new_parts[0] if new_parts else ""

                if new_surname and len(new_surname) > 2:
                    # Buscar autores cuyo nombre normalizado empiece con el mismo apellido
                    candidates = (
                        self.session.query(Author)
                        .filter(Author.normalized_name.ilike(f"{new_surname}%"))
                        .limit(50)
                        .all()
                    )
                    # También buscar donde el apellido aparezca en otra posición
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

                        # Apellido debe coincidir
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

                    # Umbral más alto (72) porque no tenemos posición como backup
                    # pero lo suficientemente bajo para cubrir nombres con 2 apellidos
                    if best_match and best_score >= 72:
                        author = best_match
                        logger.debug(
                            f"  MATCH GLOBAL por apellido+fuzzy: '{name}' → "
                            f"#{best_match.id} '{best_match.name}' (score={best_score})"
                        )

            if not author:
                # --- Construir provenance inicial ---
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
                # Guardar IDs de fuente si estan disponibles
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
                    # ORCID o nombre duplicado — buscar el existente
                    self.session.rollback()
                    if orcid:
                        author = self.session.query(Author).filter_by(orcid=orcid).first()
                    if not author:
                        norm_name = normalize_text(name)
                        author = self.session.query(Author).filter_by(normalized_name=norm_name).first()
                    if not author:
                        continue  # No se pudo resolver, saltar
            else:
                # Actualizar datos si tenemos info nueva (+ provenance)
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

            # Vincular a publicacion (evitar duplicados)
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
                    # Ya existe la relación, continuar
                    pass

            if is_inst:
                institutional_count += 1

        # Actualizar conteo solo si encontramos mas que antes
        if institutional_count > (canonical.institutional_authors_count or 0):
            canonical.institutional_authors_count = institutional_count

    # ---------------------------------------------------------
    # BACKFILL: completar IDs de autores desde raw_data
    # ---------------------------------------------------------

    def backfill_author_ids(self):
        """
        Recorre los external_records ya reconciliados y completa
        scopus_id / openalex_id en los autores existentes.
        Se ejecuta automaticamente al final de cada reconciliacion.

        Estrategia:
          1. Match exacto por ORCID, scopus_id, openalex_id, nombre normalizado
          2. Match por publicacion compartida + fuzzy de apellido (para nombres
             abreviados de Scopus vs nombres completos de OpenAlex/BD)
        """
        from rapidfuzz import fuzz

        updated = 0

        # Registros reconciliados que tienen raw_data
        externals = (
            self.session.query(ExternalRecord)
            .filter(
                ExternalRecord.canonical_publication_id.isnot(None),
                ExternalRecord.raw_data.isnot(None),
            )
            .all()
        )

        # Set de scopus_ids ya asignados (para no duplicar)
        existing_sids = {
            r[0] for r in
            self.session.query(Author.scopus_id)
            .filter(Author.scopus_id.isnot(None), Author.scopus_id != "")
            .all()
        }

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

                # Ya asignado?
                if scopus_id and str(scopus_id) in existing_sids:
                    continue

                # --- Estrategia 1: match exacto por IDs o nombre ---
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

                # --- Estrategia 2: match por publicacion + fuzzy apellido ---
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
                            # Apellido debe coincidir
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
                backfill_src = ext.source_name
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

    # ---------------------------------------------------------
    # ENRIQUECIMIENTO: completar campos vacios de la canonica
    # con datos de una nueva fuente
    # ---------------------------------------------------------

    def _enrich_canonical(self, canonical: CanonicalPublication, ext: ExternalRecord):
        """
        Completa los campos vacios de la publicacion canonica
        con datos que aporta el registro externo de una nueva fuente.
        Solo escribe si el campo de la canonica esta vacio/nulo.
        Registra en field_provenance qué fuente aportó cada dato.
        """
        enriched_fields = []
        src = ext.source_name

        # Inicializar provenance si no existe
        prov = dict(canonical.field_provenance or {})

        # DOI
        if not canonical.doi and ext.doi:
            canonical.doi = normalize_doi(ext.doi)
            enriched_fields.append("doi")
            prov["doi"] = src

        # Revista / fuente
        if not canonical.source_journal:
            journal = self._extract_journal(ext)
            if journal:
                canonical.source_journal = journal
                enriched_fields.append("source_journal")
                prov["source_journal"] = src

        # Tipo de publicacion
        if not canonical.publication_type:
            ptype = self._extract_type(ext)
            if ptype:
                canonical.publication_type = ptype
                enriched_fields.append("publication_type")
                prov["publication_type"] = src

        # Open Access
        if canonical.is_open_access is None:
            oa = self._extract_oa(ext)
            if oa is not None:
                canonical.is_open_access = oa
                enriched_fields.append("is_open_access")
                prov["is_open_access"] = src

        # ISSN
        if not canonical.issn:
            issn = self._extract_issn(ext)
            if issn:
                canonical.issn = issn
                enriched_fields.append("issn")
                prov["issn"] = src

        # Citas: tomar el maximo entre fuentes
        new_cites = self._extract_citations(ext)
        if new_cites > (canonical.citation_count or 0):
            canonical.citation_count = new_cites
            enriched_fields.append("citation_count")
            prov["citation_count"] = src

        # Ano de publicacion
        if not canonical.publication_year and ext.publication_year:
            canonical.publication_year = ext.publication_year
            enriched_fields.append("publication_year")
            prov["publication_year"] = src

        # Fecha de publicacion
        if not canonical.publication_date:
            pub_date = self._extract_publication_date(ext)
            if pub_date:
                canonical.publication_date = pub_date
                enriched_fields.append("publication_date")
                prov["publication_date"] = src

        # Persistir provenance actualizado
        if enriched_fields:
            canonical.field_provenance = prov
            logger.info(
                f"  ENRIQUECIDO canon={canonical.id} con {ext.source_name}: "
                f"{', '.join(enriched_fields)}"
            )

    # ---------------------------------------------------------
    # HELPERS para extraer datos del raw_data
    # Soportan formatos: OpenAlex, Scopus, WoS, CVLAC, DA
    # ---------------------------------------------------------

    @staticmethod
    def _extract_type(ext: ExternalRecord) -> Optional[str]:
        raw = ext.raw_data or {}
        return (
            raw.get("publication_type")
            or raw.get("type")
            or raw.get("subtypeDescription")      # Scopus
            or (raw.get("doctype", {}).get("label") if isinstance(raw.get("doctype"), dict) else None)
        )

    @staticmethod
    def _extract_journal(ext: ExternalRecord) -> Optional[str]:
        raw = ext.raw_data or {}
        # OpenAlex: raw_data.source.display_name
        source = raw.get("source", {})
        if isinstance(source, dict):
            name = source.get("display_name") or source.get("name")
            if name:
                return name
        # Scopus: raw_data["prism:publicationName"]
        if raw.get("prism:publicationName"):
            return raw["prism:publicationName"]
        # WoS / generico
        return raw.get("source_journal") or raw.get("sourceTitle")

    @staticmethod
    def _extract_oa(ext: ExternalRecord) -> Optional[bool]:
        raw = ext.raw_data or {}
        # OpenAlex
        oa = raw.get("open_access", {})
        if isinstance(oa, dict) and "is_oa" in oa:
            return oa.get("is_oa")
        # Scopus
        flag = raw.get("openaccessFlag")
        if flag is not None:
            if isinstance(flag, bool):
                return flag
            return str(flag).lower() == "true"
        return raw.get("is_open_access")

    @staticmethod
    def _extract_citations(ext: ExternalRecord) -> int:
        raw = ext.raw_data or {}
        # OpenAlex
        cites = raw.get("cited_by_count")
        if cites is not None:
            return int(cites)
        # Scopus
        cites = raw.get("citedby-count")
        if cites is not None:
            return int(cites)
        # Generico
        return int(raw.get("citation_count") or 0)

    @staticmethod
    def _extract_issn(ext: ExternalRecord) -> Optional[str]:
        raw = ext.raw_data or {}
        # OpenAlex
        source = raw.get("source", {})
        if isinstance(source, dict) and source.get("issn_l"):
            return source["issn_l"]
        # Scopus
        if raw.get("prism:issn"):
            return raw["prism:issn"]
        return raw.get("issn")

    @staticmethod
    def _extract_publication_date(ext: ExternalRecord) -> Optional[str]:
        raw = ext.raw_data or {}
        return (
            raw.get("publication_date")
            or raw.get("prism:coverDate")   # Scopus
            or raw.get("publishDate")       # WoS
        )
