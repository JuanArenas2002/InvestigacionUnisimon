"""
Endpoints: Extraction / Data Ingestion

endpoints/extraction.py - Mantiene 278 líneas de extraction.py reducidas a ~150
"""
import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Body

from api.schemas.external_records import (
    ExtractionRequest,
    ScopusExtractionRequest,
    JsonLoadRequest,
    ExtractionResponse,
    ReconciliationStatsResponse,
)
from config import DATA_DIR
from reconciliation.engine import ReconciliationEngine

from .._json_loader import (
    DoiSearchRequest,
    DoiSourceResult,
    DoiSearchResponse,
    _detect_json_source,
    _parse_json_records,
)


logger = logging.getLogger("pipeline")
router = APIRouter(tags=["Extraction"])


# ── POST /pipeline/extract/openalex ────────────────────────────────────────

@router.post(
    "/openalex",
    response_model=ExtractionResponse,
    summary="Extraer de OpenAlex por ROR",
)
def extract_openalex(body: ExtractionRequest):
    """
    Extrae publicaciones de OpenAlex usando el ROR id de la institución
    y reconcilia automáticamente.
    """
    from config import institution
    from extractors.openalex import OpenAlexExtractor
    from db.session import get_engine
    from sqlalchemy.orm import sessionmaker
    from db.models import OpenalexRecord

    ror_id = body.affiliation_id or institution.ror_id
    extractor = OpenAlexExtractor(ror_id=ror_id)
    records = extractor.extract(
        year_from=body.year_from,
        year_to=body.year_to,
        max_results=body.max_results,
    )
    
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    inserted = 0
    
    for r in records:
        if not r.source_id:
            continue
        exists = session.query(OpenalexRecord).filter_by(openalex_work_id=r.source_id).first()
        if exists:
            continue
        rec = OpenalexRecord(
            openalex_work_id=r.source_id,
            doi=r.doi,
            title=r.title,
            publication_year=r.publication_year,
            publication_date=r.publication_date,
            publication_type=r.publication_type,
            source_journal=r.source_journal,
            issn=r.issn,
            is_open_access=r.is_open_access,
            citation_count=r.citation_count,
            status="pending",
        )
        session.add(rec)
        inserted += 1
    
    session.commit()
    session.close()

    engine2 = ReconciliationEngine()
    stats = engine2.reconcile_pending(batch_size=500)
    
    return ExtractionResponse(
        extracted=len(records),
        inserted=inserted,
        message=f"Extraídos {len(records)}, insertados {inserted}",
        reconciliation=ReconciliationStatsResponse(**stats.to_dict()),
    )


# ── POST /pipeline/extract/scopus ──────────────────────────────────────────

@router.post(
    "/scopus",
    response_model=ExtractionResponse,
    summary="Extraer de Scopus",
)
def extract_scopus(body: ScopusExtractionRequest):
    """Extrae publicaciones de Scopus e ingesta."""
    from config import institution
    from extractors.scopus import ScopusExtractor

    affiliation_id = body.affiliation_id or institution.scopus_affiliation_id
    extractor = ScopusExtractor()
    records = extractor.extract(
        year_from=body.year_from,
        year_to=body.year_to,
        max_results=body.max_results,
        affiliation_id=affiliation_id,
    )
    
    rec_engine = ReconciliationEngine()
    stats = rec_engine.reconcile_batch(records)
    
    return ExtractionResponse(
        extracted=len(records),
        inserted=stats.total_processed,
        message=f"Extraídos {len(records)}, reconciliados {stats.total_processed}",
        reconciliation=ReconciliationStatsResponse(**stats.to_dict()),
    )


# ── POST /pipeline/load-json ──────────────────────────────────────────────

@router.post(
    "/load-json",
    response_model=ExtractionResponse,
    summary="Cargar archivo JSON",
)
def load_json_file(body: JsonLoadRequest):
    """
    Carga un archivo JSON, auto-detecta la fuente y reconcilia.
    
    Deduplicación en 4 niveles:
      1. Hash determinista (source + ID + DOI + título + año)
      2. source + ID
      3. source + DOI
      4. source + título + año
    """
    filepath = Path(DATA_DIR) / body.filename
    if not filepath.exists():
        raise HTTPException(404, f"Archivo no encontrado: {body.filename}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except Exception as e:
        raise HTTPException(400, f"Error leyendo JSON: {e}")

    source = body.source or _detect_json_source(raw_data)
    logger.info(f"Cargando JSON '{body.filename}' como: {source}")

    try:
        records = _parse_json_records(raw_data, source)
    except Exception as e:
        raise HTTPException(500, f"Error parseando JSON: {e}")

    if not records:
        return ExtractionResponse(
            extracted=0,
            inserted=0,
            message=f"No se encontraron registros válidos en '{body.filename}'",
        )

    rec_engine = ReconciliationEngine()
    try:
        stats = rec_engine.reconcile_batch(records)
        return ExtractionResponse(
            extracted=len(records),
            inserted=stats.total_processed,
            message=f"JSON: {len(records)} leídos, reconciliados {stats.total_processed}",
            reconciliation=ReconciliationStatsResponse(**stats.to_dict()),
        )
    except Exception as e:
        raise HTTPException(500, f"Error en ingesta: {e}")
    finally:
        rec_engine.session.close()


# ── POST /pipeline/search-doi-in-sources ──────────────────────────────────

@router.post(
    "/search-doi-in-sources",
    response_model=DoiSearchResponse,
    summary="Buscar DOI en todas las fuentes",
)
def search_doi_in_sources(body: DoiSearchRequest = Body(...)):
    """
    Busca un DOI en todas las fuentes externas (OpenAlex, Scopus, WoS,
    CvLAC, Datos Abiertos).
    """
    doi = body.doi.strip().lower()
    results = []

    # OpenAlex
    try:
        from extractors.openalex import OpenAlexExtractor
        extractor = OpenAlexExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        results.append(DoiSourceResult(
            source="openalex",
            record=record.to_dict() if record else None,
        ))
    except Exception:
        results.append(DoiSourceResult(source="openalex", record=None))

    # Scopus
    try:
        from extractors.scopus import ScopusExtractor
        extractor = ScopusExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        results.append(DoiSourceResult(
            source="scopus",
            record=record.to_dict() if record else None,
        ))
    except Exception:
        results.append(DoiSourceResult(source="scopus", record=None))

    # WoS
    try:
        from extractors.wos import WosExtractor
        extractor = WosExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        results.append(DoiSourceResult(
            source="wos",
            record=record.to_dict() if record else None,
        ))
    except Exception:
        results.append(DoiSourceResult(source="wos", record=None))

    # CvLAC
    try:
        from extractors.cvlac import CvlacExtractor
        extractor = CvlacExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        results.append(DoiSourceResult(
            source="cvlac",
            record=record.to_dict() if record else None,
        ))
    except Exception:
        results.append(DoiSourceResult(source="cvlac", record=None))

    # Datos Abiertos
    try:
        from extractors.datos_abiertos import DatosAbiertosExtractor
        extractor = DatosAbiertosExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        results.append(DoiSourceResult(
            source="datos_abiertos",
            record=record.to_dict() if record else None,
        ))
    except Exception:
        results.append(DoiSourceResult(source="datos_abiertos", record=None))

    return DoiSearchResponse(results=results)
