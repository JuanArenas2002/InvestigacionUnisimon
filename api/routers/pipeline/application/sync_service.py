"""
Application Layer — FullSyncService

Orquesta el flujo completo de sincronización y enriquecimiento:
  Fase 1: Reconcilia registros de todas las fuentes contra canonical_publications.
  Fase 2: Enriquece canónicos consultando Scopus por DOI y actualiza author IDs.

Principios:
  - Recibe Session como parámetro (sin acoplar a FastAPI ni a get_db).
  - No contiene lógica HTTP.
  - Todas las dependencias externas (extractores, engine) se instancian internamente
    para mantener el contrato del endpoint sin cambios.
"""

import logging
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from db.models import (
    CanonicalPublication,
    ScopusRecord,
    OpenalexRecord,
    WosRecord,
    CvlacRecord,
    DatosAbiertosRecord,
    Author,
    PublicationAuthor,
)
from reconciliation.engine import ReconciliationEngine
from shared.normalizers import normalize_doi, normalize_author_name

logger = logging.getLogger("pipeline")

# Campos que se propagan desde fuentes hacia canónico (sólo si están vacíos)
_ENRICHABLE_FIELDS = [
    "title",
    "publication_year",
    "publication_date",
    "publication_type",
    "source_journal",
    "issn",
    "is_open_access",
    "citation_count",
]

_SOURCE_MODELS = [
    ScopusRecord,
    OpenalexRecord,
    WosRecord,
    CvlacRecord,
    DatosAbiertosRecord,
]


# ── Result dataclasses ────────────────────────────────────────────────────────

@dataclass
class Phase1Stats:
    """Resultados de la Fase 1: reconciliación por DOI entre fuentes y canónicos."""
    created: int = 0
    reconciled: int = 0
    duplicates: int = 0
    enriched: int = 0

    def to_dict(self) -> dict:
        return {
            "created": self.created,
            "reconciled": self.reconciled,
            "duplicates": self.duplicates,
            "enriched": self.enriched,
        }


@dataclass
class Phase2Stats:
    """Resultados de la Fase 2: enriquecimiento cruzando canónicos con Scopus."""
    total_canonical_with_doi: int = 0
    already_in_scopus: int = 0
    dois_consulted: int = 0
    found_in_scopus: int = 0
    not_found: int = 0
    inserted: int = 0
    enriched_publications: int = 0
    fields_filled: int = 0
    authors_enriched: int = 0
    errors: int = 0
    status: str = "completed"
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "total_canonical_with_doi": self.total_canonical_with_doi,
            "already_in_scopus": self.already_in_scopus,
            "dois_consulted": self.dois_consulted,
            "found_in_scopus": self.found_in_scopus,
            "not_found": self.not_found,
            "inserted": self.inserted,
            "enriched_publications": self.enriched_publications,
            "fields_filled": self.fields_filled,
            "authors_enriched": self.authors_enriched,
            "errors": self.errors,
            "status": self.status,
            "message": self.message,
        }


# ── Service ───────────────────────────────────────────────────────────────────

class FullSyncService:
    """
    Caso de uso: sincronización y enriquecimiento completo del inventario.

    Uso:
        service = FullSyncService()
        result = service.run(db, batch_size=50)
    """

    def run(self, db: Session, batch_size: int = 50) -> dict:
        """
        Ejecuta el flujo completo:
          1. Fase 1 — reconciliación DOI entre todas las fuentes y canónicos.
          2. Fase 2 — enriquecimiento de canónicos via Scopus API.

        Devuelve el mismo dict que devolvía el endpoint original para
        mantener el contrato de respuesta sin cambios.
        """
        logger.info("=" * 70)
        logger.info("INICIANDO FLUJO COMPLETO DE SINCRONIZACIÓN Y ENRIQUECIMIENTO")
        logger.info("=" * 70)

        phase1 = self._run_phase_1(db)
        phase2 = self._run_phase_2(db, batch_size=batch_size)

        logger.info("\n" + "=" * 70)
        logger.info("FLUJO COMPLETADO CON ÉXITO")
        logger.info("=" * 70)

        return {
            "phase_1_reconciliation": phase1.to_dict(),
            "phase_2_scopus_enrichment": phase2.to_dict(),
            "overall_message": (
                f"Flujo completado: {phase1.created + phase1.reconciled} pubs reconciliadas, "
                f"{phase2.enriched_publications} enriquecidas desde Scopus"
            ),
        }

    # ── Fase 1 ────────────────────────────────────────────────────────────────

    def _run_phase_1(self, db: Session) -> Phase1Stats:
        """
        Itera todos los registros de fuente, normaliza DOIs y:
          - Si el canónico ya existe → enriquece campos vacíos.
          - Si no existe → crea un nuevo canónico mínimo.
        """
        logger.info("FASE 1: Reconciliando registros de todas las fuentes...")
        stats = Phase1Stats()
        seen_dois: set = set()

        for SourceModel in _SOURCE_MODELS:
            for record in db.query(SourceModel).all():
                doi = normalize_doi(getattr(record, "doi", None))

                if not doi:
                    continue

                if doi in seen_dois:
                    stats.duplicates += 1
                    continue

                seen_dois.add(doi)
                source_name = getattr(record, "source_name", SourceModel.__tablename__)

                canon = (
                    db.query(CanonicalPublication)
                    .filter(CanonicalPublication.doi == doi)
                    .first()
                )

                if canon:
                    enriched_count = self._enrich_canonical(canon, record, source_name)
                    stats.enriched += enriched_count
                    stats.reconciled += 1
                else:
                    title = getattr(record, "title", None)
                    if not title:
                        logger.warning(
                            f"Registro sin título omitido al crear canónico "
                            f"(source={source_name}, doi={doi})"
                        )
                        continue
                    canon = CanonicalPublication(
                        doi=doi,
                        title=title,
                        field_provenance={"title": source_name},
                    )
                    db.add(canon)
                    db.flush()  # flush sin commit para mantener la transacción abierta
                    stats.created += 1

        db.commit()

        logger.info(
            f"✓ FASE 1 completada: creados={stats.created}, "
            f"reconciliados={stats.reconciled}, duplicados={stats.duplicates}, "
            f"campos enriquecidos={stats.enriched}"
        )
        return stats

    def _enrich_canonical(
        self,
        canon: CanonicalPublication,
        record: object,
        source_name: str,
    ) -> int:
        """
        Completa campos vacíos del canónico con valores del registro fuente.
        Retorna el número de campos actualizados.
        """
        prov = dict(canon.field_provenance or {})
        updated = 0

        for campo in _ENRICHABLE_FIELDS:
            val_canon = getattr(canon, campo, None)
            val_source = getattr(record, campo, None)
            if (val_canon is None or val_canon == "") and val_source not in (None, ""):
                setattr(canon, campo, val_source)
                prov[campo] = source_name
                updated += 1

        if updated:
            canon.field_provenance = prov

        return updated

    # ── Fase 2 ────────────────────────────────────────────────────────────────

    def _run_phase_2(self, db: Session, batch_size: int) -> Phase2Stats:
        """
        Para cada canónico con DOI no cubierto por Scopus:
          1. Consulta la API de Scopus por DOI.
          2. Inserta el registro Scopus si se encontró.
          3. Enriquece el canónico con campos de Scopus.
          4. Actualiza el Scopus Author ID de los autores del canónico.
        """
        # Import aquí para evitar circular en tests que mockean extractores
        from extractors.scopus import ScopusExtractor

        logger.info("\nFASE 2: Enriqueciendo canónicos con datos de Scopus...")
        stats = Phase2Stats()

        stats.total_canonical_with_doi = (
            db.query(CanonicalPublication.id)
            .filter(
                CanonicalPublication.doi.isnot(None),
                CanonicalPublication.doi != "",
            )
            .count()
        )

        if stats.total_canonical_with_doi == 0:
            logger.info("✗ No hay publicaciones canónicas con DOI para cruzar con Scopus.")
            stats.status = "skipped"
            stats.message = "No hay publicaciones con DOI"
            return stats

        existing_scopus_dois = {
            row[0].strip().lower()
            for row in db.query(ScopusRecord.doi)
            .filter(ScopusRecord.doi.isnot(None))
            .all()
        }
        stats.already_in_scopus = len(existing_scopus_dois)

        batch = (
            db.query(CanonicalPublication)
            .filter(
                CanonicalPublication.doi.isnot(None),
                CanonicalPublication.doi != "",
                ~CanonicalPublication.doi.in_(existing_scopus_dois),
            )
            .order_by(CanonicalPublication.id.asc())
            .limit(batch_size)
            .all()
        )

        engine = ReconciliationEngine(session=db)
        extractor = ScopusExtractor()

        for canon in batch:
            doi = canon.doi.strip().lower()
            stats.dois_consulted += 1

            try:
                record = extractor.search_by_doi(doi)
            except Exception as e:
                logger.warning(f"Error consultando Scopus para DOI {doi}: {e}")
                stats.errors += 1
                continue

            if not record:
                stats.not_found += 1
                continue

            stats.found_in_scopus += 1

            try:
                stats.inserted += engine.ingest_records([record])
            except Exception as e:
                logger.error(f"Error insertando registro Scopus (DOI={doi}): {e}")
                stats.errors += 1

            fields_updated = self._enrich_from_scopus_record(canon, record)
            stats.fields_filled += len(fields_updated)
            if fields_updated:
                stats.enriched_publications += 1

            stats.authors_enriched += self._enrich_author_scopus_ids(db, canon, record)

        db.commit()

        logger.info(
            f"✓ FASE 2 completada: consultados={stats.dois_consulted}, "
            f"encontrados={stats.found_in_scopus}, "
            f"enriquecidas={stats.enriched_publications}, errores={stats.errors}"
        )
        return stats

    def _enrich_from_scopus_record(
        self,
        canon: CanonicalPublication,
        record: object,
    ) -> list:
        """
        Completa campos vacíos del canónico con datos del registro Scopus.
        Retorna la lista de campos actualizados.
        """
        prov = dict(getattr(canon, "field_provenance", {}) or {})
        fields_updated = []

        for attr in ("issn", "publication_type", "publication_date"):
            if not getattr(canon, attr, None) and getattr(record, attr, None):
                setattr(canon, attr, getattr(record, attr))
                prov[attr] = "scopus"
                fields_updated.append(attr)

        if canon.is_open_access is None and getattr(record, "is_open_access", None) is not None:
            canon.is_open_access = record.is_open_access
            prov["is_open_access"] = "scopus"
            fields_updated.append("is_open_access")

        record_citations = getattr(record, "citation_count", None)
        if record_citations and record_citations > (canon.citation_count or 0):
            canon.citation_count = record_citations
            prov["citation_count"] = "scopus"
            fields_updated.append("citation_count")

        if fields_updated:
            canon.field_provenance = prov

        return fields_updated

    def _enrich_author_scopus_ids(
        self,
        db: Session,
        canon: CanonicalPublication,
        record: object,
    ) -> int:
        """
        Para cada autor del canónico sin scopus_id, intenta resolverlo
        comparando el nombre normalizado contra los autores del registro Scopus.
        Retorna el número de autores actualizados.
        """
        record_authors = getattr(record, "authors", None)
        if not record_authors:
            return 0

        scopus_id_by_name = {
            normalize_author_name(sa["name"]): sa["scopus_id"]
            for sa in record_authors
            if sa.get("scopus_id") and sa.get("name")
            and normalize_author_name(sa["name"])
        }
        if not scopus_id_by_name:
            return 0

        pub_authors = (
            db.query(Author)
            .join(PublicationAuthor, PublicationAuthor.author_id == Author.id)
            .filter(PublicationAuthor.publication_id == canon.id)
            .all()
        )

        updated = 0
        for author in pub_authors:
            if author.scopus_id or not author.normalized_name:
                continue
            sid = scopus_id_by_name.get(author.normalized_name)
            if sid:
                author.scopus_id = sid
                prov = dict(author.field_provenance or {})
                prov["scopus_id"] = "scopus"
                author.field_provenance = prov
                updated += 1

        return updated
