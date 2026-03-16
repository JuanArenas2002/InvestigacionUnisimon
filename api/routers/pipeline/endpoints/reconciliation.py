"""
Endpoints: Reconciliation

endpoints/reconciliation.py - Mantiene 372 líneas de reconciliation_ops.py reducidas a ~200
"""
import logging
import re
from unidecode import unidecode

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
router = APIRouter(tags=["Reconciliation"])


# ── POST /pipeline/reconcile ──────────────────────────────────────────────

@router.post(
    "/reconcile",
    response_model=ReconciliationStatsResponse,
    summary="Reconciliar lote de pendientes",
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


# ── POST /pipeline/reconcile-all ──────────────────────────────────────────

@router.post(
    "/reconcile-all",
    response_model=ReconciliationStatsResponse,
    summary="Reconciliar todos los pendientes",
)
def reconcile_all():
    """Reconcilia TODOS los registros pendientes."""
    engine = ReconciliationEngine()
    try:
        total_stats = ReconciliationStatsResponse()
        while True:
            stats = engine.reconcile_pending(batch_size=500)
            if stats.total_processed == 0:
                break
            total_stats.total_processed += stats.total_processed
            total_stats.doi_exact_matches += stats.doi_exact_matches
            total_stats.fuzzy_high_matches += stats.fuzzy_high_matches
            total_stats.fuzzy_combined_matches += stats.fuzzy_combined_matches
            total_stats.manual_review += stats.manual_review
            total_stats.new_canonical_created += stats.new_canonical
            total_stats.errors += stats.errors
        return total_stats
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")
    finally:
        engine.session.close()


# ── POST /pipeline/reconcile/all-sources ──────────────────────────────────

@router.post(
    "/all-sources",
    response_model=dict,
    summary="Reconciliar todos los registros de todas las fuentes",
)
def reconcile_all_sources(db: Session = Depends(get_db)):
    """
    Recorre todos los registros de todas las fuentes, busca por DOI
    y reconcilia en publicaciones canónicas.
    """
    from db.models import (
        CanonicalPublication,
        ScopusRecord,
        OpenalexRecord,
        WosRecord,
        CvlacRecord,
        DatosAbiertosRecord,
    )

    sources = [ScopusRecord, OpenalexRecord, WosRecord, CvlacRecord, DatosAbiertosRecord]
    created, reconciled, duplicates, enriched = 0, 0, 0, 0
    seen_dois = set()

    def normalize_doi(doi: str):
        if not doi:
            return None
        doi = doi.strip().lower()
        doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        doi = doi.split()[0]
        if not re.match(r"^10\.\d{4,9}/[-._;()/:a-z0-9]+$", doi):
            return None
        return doi

    campos = [
        "title", "publication_year", "publication_date", "publication_type",
        "source_journal", "issn", "is_open_access", "citation_count",
    ]

    for SourceModel in sources:
        for r in db.query(SourceModel).all():
            doi = normalize_doi(getattr(r, "doi", None))
            if not doi or doi in seen_dois:
                if doi in seen_dois:
                    duplicates += 1
                continue
            
            seen_dois.add(doi)

            try:
                pub = db.query(CanonicalPublication).filter_by(doi=doi).one()
                prov = dict(pub.field_provenance or {})
                for campo in campos:
                    val_canon = getattr(pub, campo, None)
                    val_fuente = getattr(r, campo, None)
                    if (val_canon is None or val_canon == "") and val_fuente not in (None, ""):
                        setattr(pub, campo, val_fuente)
                        prov[campo] = getattr(r, "source_name", SourceModel.__tablename__)
                        enriched += 1
                pub.field_provenance = prov
                reconciled += 1
            except:
                pub = CanonicalPublication(doi=doi, title=getattr(r, "title", None))
                pub.field_provenance = {"title": getattr(r, "source_name", SourceModel.__tablename__)}
                db.add(pub)
                db.commit()
                created += 1

    db.commit()
    return {
        "created": created,
        "reconciled": reconciled,
        "duplicates": duplicates,
        "enriched": enriched,
    }


# ── POST /pipeline/crossref-scopus ────────────────────────────────────────

@router.post(
    "/crossref-scopus",
    response_model=CrossrefScopusResponse,
    summary="Cruzar inventario con Scopus por DOI",
)
def crossref_scopus(batch_size: int = 50, db: Session = Depends(get_db)):
    """
    Cruza canónicos con Scopus y enriquece datos faltantes.
    Trabaja por lotes: cada llamada procesa hasta `batch_size` DOIs.
    """
    from db.models import CanonicalPublication, ScopusRecord, Author, PublicationAuthor
    from extractors.scopus import ScopusExtractor
    from extractors.base import normalize_author_name

    batch_size = min(max(batch_size, 1), 200)

    all_with_doi = db.query(CanonicalPublication.id).filter(
        CanonicalPublication.doi.isnot(None)
    ).count()

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
            message="No hay publicaciones con DOI",
        )

    existing_scopus_dois = set(
        row[0].strip().lower() for row in db.query(ScopusRecord.doi).all()
        if row[0]
    )

    batch = db.query(CanonicalPublication).filter(
        CanonicalPublication.doi.isnot(None),
        ~CanonicalPublication.doi.in_(existing_scopus_dois),
    ).order_by(CanonicalPublication.id.asc()).limit(batch_size).all()

    dois_consulted, found, not_found, inserted = 0, 0, 0, 0
    enriched_pubs, fields_filled, authors_enriched, errors = 0, 0, 0, 0
    enrichment_detail = []
    
    engine = ReconciliationEngine()
    extractor = ScopusExtractor()

    for canon in batch:
        doi = canon.doi.strip().lower()
        dois_consulted += 1
        try:
            record = extractor.search_by_doi(doi)
        except Exception as e:
            logger.error(f"Error consultando Scopus para {doi}: {e}")
            errors += 1
            continue

        if record:
            found += 1
            try:
                inserted += engine.ingest_records([record])
            except Exception as e:
                logger.error(f"Error insertando: {e}")
                errors += 1

            fields_updated = []
            prov = dict(getattr(canon, "field_provenance", {}) or {})

            if not canon.issn and getattr(record, "issn", None):
                canon.issn = record.issn
                fields_updated.append(("issn", None, record.issn))
                prov["issn"] = "scopus"

            if canon.is_open_access is None and getattr(record, "is_open_access", None):
                canon.is_open_access = record.is_open_access
                fields_updated.append(("is_open_access", None, str(record.is_open_access)))
                prov["is_open_access"] = "scopus"

            if fields_updated:
                canon.field_provenance = prov
                enriched_pubs += 1
                fields_filled += len(fields_updated)
                for field_name, old_val, new_val in fields_updated[:100]:
                    enrichment_detail.append(EnrichedFieldDetail(
                        canonical_id=canon.id,
                        doi=canon.doi,
                        field=field_name,
                        old_value=str(old_val),
                        new_value=str(new_val),
                    ))
        else:
            not_found += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error en commit: {e}")

    total_stats = ReconciliationStatsResponse()
    try:
        stats = engine.reconcile_pending(batch_size=500)
        total_stats = ReconciliationStatsResponse(**stats.to_dict())
    except Exception as e:
        logger.error(f"Error reconciliando: {e}")

    return CrossrefScopusResponse(
        total_canonical_with_doi=all_with_doi,
        already_in_scopus=len(existing_scopus_dois),
        dois_consulted=dois_consulted,
        found_in_scopus=found,
        not_found=not_found,
        inserted=inserted,
        enriched_publications=enriched_pubs,
        fields_filled=fields_filled,
        authors_enriched=authors_enriched,
        errors=errors,
        message=f"Procesados {len(batch)}: {found} en Scopus, {enriched_pubs} enriquecidos",
        enrichment_detail=enrichment_detail if enrichment_detail else None,
        reconciliation=total_stats if total_stats.total_processed > 0 else None,
    )
