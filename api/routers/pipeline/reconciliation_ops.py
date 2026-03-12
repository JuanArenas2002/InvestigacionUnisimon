"""
Endpoints de reconciliación de publicaciones canónicas.

Rutas:
  POST /reconcile                — Reconciliar lote de pendientes.
  POST /reconcile-all            — Reconciliar todos los pendientes.
  POST /reconcile/all-sources    — Reconciliar todos contra todas las fuentes.
  POST /crossref-scopus          — Cruzar canónicos con Scopus por DOI (lotes).
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.external_records import (
    ReconciliationStatsResponse,
    CrossrefScopusResponse,
    EnrichedFieldDetail,
)
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("pipeline")
router = APIRouter(tags=["Pipeline"])


# ── POST /pipeline/reconcile ─────────────────────────────────────────────────

@router.post(
    "/reconcile",
    response_model=ReconciliationStatsResponse,
    summary="Reconciliar pendientes",
)
def reconcile_pending(batch_size: int = 500):
    """Ejecuta un lote de reconciliación sobre registros pendientes."""
    engine = ReconciliationEngine()
    try:
        stats = engine.reconcile_pending(batch_size=batch_size)
        return ReconciliationStatsResponse(**stats.to_dict())
    except Exception as e:
        raise HTTPException(500, f"Error en reconciliación: {e}")
    finally:
        engine.session.close()


# ── POST /pipeline/reconcile-all ─────────────────────────────────────────────

@router.post(
    "/reconcile-all",
    response_model=ReconciliationStatsResponse,
    summary="Reconciliar todos",
)
def reconcile_all():
    """Reconcilia TODOS los registros pendientes (puede tardar)."""
    engine = ReconciliationEngine()
    try:
        total_stats = ReconciliationStatsResponse()
        while True:
            stats = engine.reconcile_pending(batch_size=500)
            if stats.total_processed == 0:
                break
            total_stats.total_processed          += stats.total_processed
            total_stats.doi_exact_matches        += stats.doi_exact_matches
            total_stats.fuzzy_high_matches       += stats.fuzzy_high_matches
            total_stats.fuzzy_combined_matches   += stats.fuzzy_combined_matches
            total_stats.manual_review            += stats.manual_review
            total_stats.new_canonical_created    += stats.new_canonical
            total_stats.errors                   += stats.errors
        return total_stats
    except Exception as e:
        raise HTTPException(500, f"Error en reconciliación: {e}")
    finally:
        engine.session.close()


# ── POST /pipeline/reconcile/all-sources ─────────────────────────────────────

@router.post(
    "/reconcile/all-sources",
    response_model=dict,
    summary="Reconciliar todos los registros de todas las fuentes",
)
def reconcile_all_sources(db: Session = Depends(get_db)):
    """
    Recorre todos los registros de todas las fuentes, busca por DOI en las
    demás fuentes y reconcilia en publicaciones canónicas.
    """
    import re
    from unidecode import unidecode
    from db.models import (
        CanonicalPublication,
        ScopusRecord,
        OpenalexRecord,
        WosRecord,
        CvlacRecord,
        DatosAbiertosRecord,
    )
    from sqlalchemy.orm.exc import NoResultFound

    sources = [ScopusRecord, OpenalexRecord, WosRecord, CvlacRecord, DatosAbiertosRecord]
    created, reconciled, duplicates, enriched = 0, 0, 0, 0
    seen_dois: set = set()

    def normalize_doi(doi: str):
        if not doi:
            return None
        doi = doi.strip().lower()
        doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        doi = doi.split()[0]
        if not re.match(r"^10\.\d{4,9}/[-._;()/:a-z0-9]+$", doi):
            return None
        return doi

    def normalize_title(title: str):
        if not title:
            return None
        return unidecode(title.strip().lower())

    campos = [
        "title", "publication_year", "publication_date", "publication_type",
        "source_journal", "issn", "is_open_access", "citation_count",
    ]

    for SourceModel in sources:
        for r in db.query(SourceModel).all():
            doi = normalize_doi(getattr(r, "doi", None))
            if not doi:
                continue
            if doi in seen_dois:
                duplicates += 1
                continue
            seen_dois.add(doi)

            try:
                pub = db.query(CanonicalPublication).filter_by(doi=doi).one()
                enriched_this = False
                prov = dict(pub.field_provenance or {})
                for campo in campos:
                    val_canon  = getattr(pub, campo, None)
                    val_fuente = getattr(r, campo, None)
                    if campo == "title":
                        val_canon  = normalize_title(val_canon)
                        val_fuente = normalize_title(val_fuente)
                    if (val_canon is None or val_canon == "") and val_fuente not in (None, ""):
                        if campo == "title":
                            setattr(pub, campo, getattr(r, campo, None))
                        else:
                            setattr(pub, campo, val_fuente)
                        prov[campo] = getattr(r, "source_name", SourceModel.__tablename__)
                        enriched_this = True
                if enriched_this:
                    pub.field_provenance = prov
                    enriched += 1
                reconciled += 1
            except NoResultFound:
                pub = CanonicalPublication(doi=doi, title=getattr(r, "title", None))
                prov = {"title": getattr(r, "source_name", SourceModel.__tablename__)}
                pub.field_provenance = prov
                db.add(pub)
                db.commit()
                created += 1

    db.commit()
    return {
        "created":         created,
        "reconciled":      reconciled,
        "duplicates":      duplicates,
        "enriched":        enriched,
        "total_processed": len(seen_dois),
    }


# ── POST /pipeline/crossref-scopus ───────────────────────────────────────────

@router.post(
    "/crossref-scopus",
    response_model=CrossrefScopusResponse,
    summary="Cruzar inventario con Scopus por DOI (por lotes)",
)
def crossref_scopus(
    batch_size: int = 50,
    db: Session = Depends(get_db),
):
    """
    Cruza las publicaciones canónicas con Scopus y **enriquece** datos faltantes.

    Trabaja **por lotes**: cada llamada procesa hasta `batch_size` DOIs (default 50).
    Llámalo varias veces hasta que `pending` llegue a 0.

    Cada llamada:
    1. Toma los próximos N canónicos con DOI que NO tengan registro Scopus.
    2. Busca cada DOI en la API de Scopus.
    3. Si lo encuentra → rellena campos vacíos.
    4. Actualiza autores con Scopus Author ID.
    5. Ingesta el registro Scopus y reconcilia.
    """
    from db.models import CanonicalPublication, ScopusRecord, Author, PublicationAuthor
    from extractors.scopus import ScopusExtractor
    from extractors.base import normalize_author_name

    batch_size = min(max(batch_size, 1), 200)

    all_with_doi = (
        db.query(CanonicalPublication.id)
        .filter(CanonicalPublication.doi.isnot(None))
        .filter(CanonicalPublication.doi != "")
        .count()
    )

    if all_with_doi == 0:
        return CrossrefScopusResponse(
            total_canonical_with_doi=0,
            already_in_scopus=0,
            dois_consulted=0,
            found_in_scopus=0,
            not_found=0,
            inserted=0,
            enriched_publications=0,
            fields_filled=0,
            authors_enriched=0,
            errors=0,
            message="No hay publicaciones canónicas con DOI para cruzar.",
            enrichment_detail=None,
            reconciliation=None,
        )

    existing_scopus_dois = set(
        row[0].strip().lower()
        for row in db.query(ScopusRecord.doi).filter(ScopusRecord.doi.isnot(None)).all()
    )
    already_in_scopus = len(existing_scopus_dois)

    batch = (
        db.query(CanonicalPublication)
        .filter(CanonicalPublication.doi.isnot(None))
        .filter(CanonicalPublication.doi != "")
        .filter(~CanonicalPublication.doi.in_(existing_scopus_dois))
        .order_by(CanonicalPublication.id.asc())
        .limit(batch_size)
        .all()
    )

    dois_consulted        = 0
    found_in_scopus       = 0
    not_found             = 0
    inserted              = 0
    enriched_publications = 0
    fields_filled_count   = 0
    authors_enriched_count = 0
    errors                = 0
    enrichment_detail     = []
    engine    = ReconciliationEngine()
    extractor = ScopusExtractor()

    for canon in batch:
        doi = canon.doi.strip().lower()
        dois_consulted += 1
        try:
            record = extractor.search_by_doi(doi)
        except Exception as e:
            logger.error(f"Error consultando Scopus para DOI {doi}: {e}")
            errors += 1
            continue

        if record:
            found_in_scopus += 1
            try:
                inserted += engine.ingest_records([record])
            except Exception as e:
                logger.error(f"Error insertando registro Scopus: {e}")
                errors += 1

            fields_updated = []
            prov = dict(getattr(canon, "field_provenance", {}) or {})

            def _fill(attr: str, source_attr: str = None):
                nonlocal fields_filled_count, enriched_publications
                sattr = source_attr or attr
                val = getattr(record, sattr, None)
                if val is not None and not getattr(canon, attr, None):
                    old = getattr(canon, attr, None)
                    setattr(canon, attr, val)
                    fields_updated.append((attr, old, val))
                    prov[attr] = "scopus"

            _fill("issn")
            _fill("publication_type")
            _fill("publication_date")
            if canon.is_open_access is None and getattr(record, "is_open_access", None) is not None:
                old = str(canon.is_open_access)
                canon.is_open_access = record.is_open_access
                fields_updated.append(("is_open_access", old, str(record.is_open_access)))
                prov["is_open_access"] = "scopus"
            if getattr(record, "citation_count", None) and record.citation_count > (canon.citation_count or 0):
                old = str(canon.citation_count)
                canon.citation_count = record.citation_count
                fields_updated.append(("citation_count", old, str(record.citation_count)))
                prov["citation_count"] = "scopus"

            if fields_updated:
                canon.field_provenance = prov
                enriched_publications += 1
                fields_filled_count   += len(fields_updated)
                for field_name, old_val, new_val in fields_updated:
                    if len(enrichment_detail) < 100:
                        enrichment_detail.append(EnrichedFieldDetail(
                            canonical_id=canon.id,
                            doi=canon.doi,
                            field=field_name,
                            old_value=str(old_val),
                            new_value=str(new_val),
                        ))

            if getattr(record, "authors", None):
                pub_authors = (
                    db.query(Author)
                    .join(PublicationAuthor, PublicationAuthor.author_id == Author.id)
                    .filter(PublicationAuthor.publication_id == canon.id)
                    .all()
                )
                scopus_author_map = {}
                for sa in record.authors:
                    if sa.get("scopus_id") and sa.get("name"):
                        norm = normalize_author_name(sa["name"])
                        if norm:
                            scopus_author_map[norm] = sa["scopus_id"]

                for author in pub_authors:
                    if not author.scopus_id and author.normalized_name:
                        sid = scopus_author_map.get(author.normalized_name)
                        if sid:
                            author.scopus_id = sid
                            a_prov = dict(author.field_provenance or {})
                            a_prov["scopus_id"] = "scopus"
                            author.field_provenance = a_prov
                            authors_enriched_count += 1
        else:
            not_found += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error haciendo commit de enriquecimientos: {e}")

    total_stats = ReconciliationStatsResponse()
    try:
        stats = engine.reconcile_pending(batch_size=500)
        total_stats = ReconciliationStatsResponse(**stats.to_dict())
    except Exception as e:
        logger.error(f"Error en reconciliación: {e}")

    return CrossrefScopusResponse(
        total_canonical_with_doi=all_with_doi,
        already_in_scopus=already_in_scopus,
        dois_consulted=dois_consulted,
        found_in_scopus=found_in_scopus,
        not_found=not_found,
        inserted=inserted,
        enriched_publications=enriched_publications,
        fields_filled=fields_filled_count,
        authors_enriched=authors_enriched_count,
        errors=errors,
        message=(
            f"Lote de {len(batch)} procesado. "
            f"{found_in_scopus} encontrados en Scopus, "
            f"{enriched_publications} enriquecidos."
        ),
        enrichment_detail=enrichment_detail if enrichment_detail else None,
        reconciliation=total_stats if total_stats.total_processed > 0 else None,
    )
