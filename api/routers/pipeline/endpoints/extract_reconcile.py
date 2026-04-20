"""
Endpoint: Extract + Reconcile (all sources in one call)

POST /pipeline/extract-and-reconcile

Extracts from one or more sources using institution or author search params,
stores the raw records, then reconciles them against canonical_publications.
Returns a per-source breakdown plus totals.
"""
import logging
from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.dependencies import get_db
from config import institution as default_institution
from reconciliation.engine import ReconciliationEngine

logger = logging.getLogger("pipeline.extract_reconcile")
router = APIRouter(tags=["Pipeline · Extracción + Reconciliación"])

ALL_SOURCES = ["openalex", "scopus", "wos", "datos_abiertos", "cvlac"]


# ── Request / Response schemas ────────────────────────────────────────────────

class ExtractAndReconcileRequest(BaseModel):
    """
    Unified extraction + reconciliation request.

    Pick `search_type` = "institution" or "author", then fill the relevant fields.
    Fields not needed by a source are silently ignored; missing required fields
    cause that source to be skipped (reported in `sources_skipped`).
    """
    search_type: Literal["institution", "author"] = Field(
        ..., description="Type of search to perform across all selected sources"
    )
    sources: Optional[List[str]] = Field(
        None,
        description=(
            "Sources to query. Defaults to all: openalex, scopus, wos, datos_abiertos, cvlac. "
            "Use this to run only a subset."
        ),
    )

    # ── Institution params ────────────────────────────────────────────────────
    ror_id: Optional[str] = Field(
        None, description="ROR ID (OpenAlex, WoS fallback). Falls back to env ROR_ID."
    )
    affiliation_id: Optional[str] = Field(
        None,
        description=(
            "Scopus affiliation ID(s), comma-separated. "
            "Falls back to env SCOPUS_AFFILIATION_IDS."
        ),
    )
    institution_name: Optional[str] = Field(
        None, description="Free-text institution name (WoS, Datos Abiertos)."
    )
    cc_investigadores: Optional[List[str]] = Field(
        None,
        description=(
            "CvLAC researcher IDs (cédulas). Required for CvLAC institution search. "
            "For CvLAC author search use `cvlac_cc` instead."
        ),
    )

    # ── Author params ─────────────────────────────────────────────────────────
    orcid: Optional[str] = Field(None, description="ORCID (accepted by all sources).")
    source_author_id: Optional[str] = Field(
        None,
        description=(
            "Platform author ID: OpenAlex Author ID, Scopus AU-ID, or WoS ResearcherID. "
            "Used for the matching source only."
        ),
    )
    author_name: Optional[str] = Field(
        None, description="Free-text author name (WoS, Datos Abiertos fallback)."
    )
    cvlac_cc: Optional[str] = Field(
        None, description="Researcher cédula for CvLAC author search."
    )

    # ── Common params ─────────────────────────────────────────────────────────
    year_from: Optional[int] = Field(None, ge=1900, le=2099)
    year_to: Optional[int] = Field(None, ge=1900, le=2099)
    max_results: int = Field(500, ge=1, le=10000)
    reconcile: bool = Field(
        True,
        description=(
            "If true (default), run ReconciliationEngine.ingest_records() immediately "
            "after extracting from each source so records are linked to canonical_publications."
        ),
    )


class SourceExtractionResult(BaseModel):
    extracted: int
    inserted: int
    skipped: int
    errors: int


class ReconciliationSummary(BaseModel):
    total_processed: int
    doi_exact_matches: int
    fuzzy_matches: int
    new_canonicals: int
    errors: int


class ExtractAndReconcileResponse(BaseModel):
    search_type: str
    sources_attempted: List[str]
    sources_succeeded: List[str]
    sources_skipped: List[str]
    sources_failed: List[str]
    results: Dict[str, SourceExtractionResult]
    totals: SourceExtractionResult
    reconciliation: Optional[ReconciliationSummary] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ingest_and_reconcile(
    records: list,
    db: Session,
    do_reconcile: bool,
) -> tuple[int, int, ReconciliationSummary | None]:
    """
    Store records and optionally reconcile them.
    Returns (inserted, skipped, reconciliation_summary).
    """
    if not records:
        return 0, 0, None

    engine = ReconciliationEngine(session=db)
    inserted = engine.ingest_records(records)
    skipped = max(len(records) - inserted, 0)

    recon_summary = None
    if do_reconcile and inserted > 0:
        stats = engine.reconcile_pending(batch_size=inserted + 50)
        recon_summary = ReconciliationSummary(
            total_processed=stats.total_processed,
            doi_exact_matches=stats.doi_exact_matches,
            fuzzy_matches=getattr(stats, "fuzzy_high_matches", 0)
                          + getattr(stats, "fuzzy_combined_matches", 0),
            new_canonicals=getattr(stats, "new_canonical", 0),
            errors=stats.errors,
        )

    return inserted, skipped, recon_summary


def _extract_institution(source: str, body: ExtractAndReconcileRequest, db: Session):
    """Extract by institution for a single source. Returns (records, skipped_reason)."""
    if source == "openalex":
        from extractors.openalex.extractor import OpenAlexExtractor
        ror = body.ror_id or default_institution.ror_id
        if not ror:
            return None, "missing ror_id (and no ROR_ID env var)"
        ext = OpenAlexExtractor()
        return ext.extract(
            ror_id=ror,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        ), None

    if source == "scopus":
        from config import scopus as scopus_config
        from extractors.scopus import ScopusExtractor
        raw_ids = body.affiliation_id or ",".join(scopus_config.affiliation_ids or [])
        if not raw_ids:
            return None, "missing affiliation_id (and no SCOPUS_AFFILIATION_IDS env var)"
        id_parts = [f"AF-ID({aid.strip()})" for aid in raw_ids.split(",") if aid.strip()]
        query = " OR ".join(id_parts)
        if body.year_from:
            query += f" AND PUBYEAR > {body.year_from - 1}"
        if body.year_to:
            query += f" AND PUBYEAR < {body.year_to + 1}"
        ext = ScopusExtractor()
        return ext.extract(query=query, max_results=body.max_results), None

    if source == "wos":
        from extractors.wos import WosExtractor
        institution_query = body.institution_name or body.ror_id
        if not institution_query:
            return None, "missing institution_name or ror_id"
        ext = WosExtractor()
        return ext.extract(
            institution=institution_query,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        ), None

    if source == "datos_abiertos":
        from extractors.datos_abiertos import DatosAbiertosExtractor
        if not body.institution_name:
            return None, "missing institution_name"
        ext = DatosAbiertosExtractor()
        return ext.extract(
            institution=body.institution_name,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        ), None

    if source == "cvlac":
        from extractors.cvlac import CvlacExtractor
        if not body.cc_investigadores:
            return None, "missing cc_investigadores (list of researcher IDs)"
        ext = CvlacExtractor()
        return ext.extract(cc_investigadores=body.cc_investigadores), None

    return None, f"unknown source '{source}'"


def _extract_author(source: str, body: ExtractAndReconcileRequest, db: Session):
    """Extract by author for a single source. Returns (records, skipped_reason)."""
    if source == "openalex":
        from extractors.openalex.extractor import OpenAlexExtractor
        if not body.orcid and not body.source_author_id:
            return None, "missing orcid or source_author_id"
        ext = OpenAlexExtractor()
        return ext.extract_by_author(
            orcid=body.orcid,
            author_id=body.source_author_id,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        ), None

    if source == "scopus":
        from extractors.scopus import ScopusExtractor
        if not body.orcid and not body.source_author_id:
            return None, "missing orcid or source_author_id (Scopus AU-ID)"
        query = (
            f"ORCID({body.orcid})" if body.orcid
            else f"AU-ID({body.source_author_id})"
        )
        ext = ScopusExtractor()
        return ext.extract(query=query, max_results=body.max_results), None

    if source == "wos":
        from extractors.wos import WosExtractor
        if not body.orcid and not body.source_author_id and not body.author_name:
            return None, "missing orcid, source_author_id (WoS ResearcherID), or author_name"
        ext = WosExtractor()
        return ext.extract_by_author(
            orcid=body.orcid,
            researcher_id=body.source_author_id,
            author_name=body.author_name,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        ), None

    if source == "datos_abiertos":
        from extractors.datos_abiertos import DatosAbiertosExtractor
        if not body.orcid and not body.source_author_id and not body.author_name:
            return None, "missing orcid, source_author_id, or author_name"
        ext = DatosAbiertosExtractor()
        return ext.extract_by_author(
            orcid=body.orcid,
            author_id=body.source_author_id,
            author_name=body.author_name,
            year_from=body.year_from,
            year_to=body.year_to,
            max_results=body.max_results,
        ), None

    if source == "cvlac":
        from extractors.cvlac import CvlacExtractor
        if not body.cvlac_cc:
            return None, "missing cvlac_cc (researcher cédula)"
        ext = CvlacExtractor()
        return ext.extract(cc_investigadores=[body.cvlac_cc]), None

    return None, f"unknown source '{source}'"


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post(
    "/extract-and-reconcile",
    response_model=ExtractAndReconcileResponse,
    summary="Extraer de todas las fuentes y reconciliar en un solo paso",
)
async def extract_and_reconcile(
    body: ExtractAndReconcileRequest,
    db: Session = Depends(get_db),
):
    """
    Pipeline completo de extracción + reconciliación en un solo call.

    1. Para cada fuente seleccionada, extrae publicaciones (by institution o by author).
    2. Almacena los registros crudos en la tabla `<source>_records` con status='pending'.
    3. Si `reconcile=true` (default), ejecuta `ReconciliationEngine` inmediatamente
       para vincular los nuevos registros a `canonical_publications`.
    4. Devuelve estadísticas por fuente y totales.

    **Nota sobre CvLAC:**
    - Institution search requiere `cc_investigadores` (lista de cédulas).
    - Author search requiere `cvlac_cc` (una cédula).

    **Fuentes omitidas (`sources_skipped`):** aquellas a las que les faltan parámetros
    requeridos — no son errores, solo se saltan silenciosamente.

    **Fuentes fallidas (`sources_failed`):** hubo un error de red / API al extraer.
    """
    from starlette.concurrency import run_in_threadpool

    requested_sources = body.sources or ALL_SOURCES
    invalid = [s for s in requested_sources if s not in ALL_SOURCES]
    if invalid:
        raise HTTPException(400, f"Unknown sources: {invalid}. Valid: {ALL_SOURCES}")

    extract_fn = _extract_institution if body.search_type == "institution" else _extract_author

    attempted: List[str] = []
    succeeded: List[str] = []
    skipped: List[str] = []
    failed: List[str] = []
    results: Dict[str, SourceExtractionResult] = {}
    combined_recon = ReconciliationSummary(
        total_processed=0, doi_exact_matches=0, fuzzy_matches=0,
        new_canonicals=0, errors=0,
    )
    has_recon = False

    def _run():
        nonlocal has_recon
        for source in requested_sources:
            attempted.append(source)
            logger.info(f"extract-and-reconcile: starting {source} ({body.search_type})")
            try:
                records, skip_reason = extract_fn(source, body, db)
            except Exception as exc:
                logger.error(f"extract-and-reconcile: {source} extraction failed: {exc}")
                failed.append(source)
                results[source] = SourceExtractionResult(
                    extracted=0, inserted=0, skipped=0, errors=1
                )
                continue

            if records is None:
                logger.info(f"extract-and-reconcile: skipping {source} — {skip_reason}")
                skipped.append(source)
                continue

            try:
                inserted, sk, recon = _ingest_and_reconcile(records, db, body.reconcile)
            except Exception as exc:
                logger.error(f"extract-and-reconcile: {source} ingest failed: {exc}")
                failed.append(source)
                results[source] = SourceExtractionResult(
                    extracted=len(records), inserted=0, skipped=0, errors=1
                )
                continue

            succeeded.append(source)
            results[source] = SourceExtractionResult(
                extracted=len(records),
                inserted=inserted,
                skipped=sk,
                errors=0,
            )
            if recon:
                has_recon = True
                combined_recon.total_processed += recon.total_processed
                combined_recon.doi_exact_matches += recon.doi_exact_matches
                combined_recon.fuzzy_matches += recon.fuzzy_matches
                combined_recon.new_canonicals += recon.new_canonicals
                combined_recon.errors += recon.errors
                logger.info(
                    f"extract-and-reconcile: {source} done — "
                    f"extracted={len(records)}, inserted={inserted}, "
                    f"reconciled={recon.total_processed}"
                )
            else:
                logger.info(
                    f"extract-and-reconcile: {source} done — "
                    f"extracted={len(records)}, inserted={inserted}"
                )

    await run_in_threadpool(_run)

    totals = SourceExtractionResult(
        extracted=sum(r.extracted for r in results.values()),
        inserted=sum(r.inserted for r in results.values()),
        skipped=sum(r.skipped for r in results.values()),
        errors=sum(r.errors for r in results.values()),
    )

    return ExtractAndReconcileResponse(
        search_type=body.search_type,
        sources_attempted=attempted,
        sources_succeeded=succeeded,
        sources_skipped=skipped,
        sources_failed=failed,
        results=results,
        totals=totals,
        reconciliation=combined_recon if has_recon else None,
    )
