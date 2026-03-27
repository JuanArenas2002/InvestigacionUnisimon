"""
Endpoints: Utilidades de ingesta (Pipeline · Extracción)

Contiene operaciones que no tienen equivalente en /sources/:
  - load-json          : cargar un archivo JSON local y reconciliar
  - search-doi-in-sources : buscar un DOI en todas las fuentes externas

Para extraer publicaciones de una plataforma usa los endpoints de Fuentes:
  POST /api/sources/openalex/search/by-institution
  POST /api/sources/scopus/search/by-institution
  POST /api/sources/wos/search/by-institution
  POST /api/sources/cvlac/search/by-author
  POST /api/sources/datos-abiertos/search/by-institution
"""
import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Body

from api.schemas.external_records import (
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
router = APIRouter(tags=["Pipeline · Extracción"])


# ── POST /pipeline/extract/load-json ──────────────────────────────────────

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
