"""
Carga e ingesta de archivos JSON con publicaciones de múltiples fuentes.

Contiene:
  - _detect_json_source()  — auto-detección de la fuente por estructura del JSON.
  - _parse_json_records()  — parseo usando el extractor correcto.
  - Modelos Pydantic para el endpoint /search-doi-in-sources.
"""
import logging
from pydantic import BaseModel

logger = logging.getLogger("pipeline")


# ── Modelos Pydantic ─────────────────────────────────────────────────────────

class DoiSearchRequest(BaseModel):
    doi: str


class DoiSourceResult(BaseModel):
    source: str
    record: dict | None


class DoiSearchResponse(BaseModel):
    results: list[DoiSourceResult]


# ── Detección de fuente ───────────────────────────────────────────────────────

def _detect_json_source(data) -> str:
    """
    Auto-detecta la fuente de un JSON por su estructura.

    Returns:
        'openalex' | 'scopus' | 'wos' | 'cvlac' | 'datos_abiertos'
    """
    items = (
        data
        if isinstance(data, list)
        else data.get(
            "results",
            data.get("works", data.get("search-results", {}).get("entry", [])),
        )
    )
    if not items:
        if isinstance(data, dict) and "search-results" in data:
            return "scopus"
        return "openalex"

    sample = items[0] if items else {}

    # Scopus: tiene dc:identifier, prism:publicationName
    if "dc:identifier" in sample or "prism:publicationName" in sample or "dc:title" in sample:
        return "scopus"

    # OpenAlex: tiene 'authorships', 'primary_location'
    if "authorships" in sample or (
        isinstance(sample.get("id", ""), str) and "openalex.org" in sample.get("id", "")
    ):
        return "openalex"

    # WoS: tiene 'uid' con WOS: o title como dict
    if "uid" in sample or (
        isinstance(sample.get("title"), dict) and "value" in sample.get("title", {})
    ):
        return "wos"

    # Datos Abiertos
    if "cod_producto" in sample or "nme_tipologia_producto" in sample:
        return "datos_abiertos"

    # CvLAC
    if "cod_rh" in sample or "grupo" in sample:
        return "cvlac"

    return "openalex"


# ── Parseo según fuente ───────────────────────────────────────────────────────

def _parse_json_records(raw_data, source: str) -> list:
    """
    Parsea un JSON usando el extractor correcto según la fuente.

    Args:
        raw_data: datos cargados con json.load().
        source:   'openalex' | 'scopus' | 'wos' | 'cvlac' | 'datos_abiertos'

    Returns:
        Lista de StandardRecord.
    """
    records = []

    if source == "openalex":
        from extractors.openalex import OpenAlexExtractor
        extractor = OpenAlexExtractor()
        items = (
            raw_data
            if isinstance(raw_data, list)
            else raw_data.get("results", raw_data.get("works", []))
        )
        for item in items:
            try:
                records.append(extractor._parse_record(item))
            except Exception:
                continue
        records = extractor._post_process(records)

    elif source == "scopus":
        from extractors.scopus import ScopusExtractor
        extractor = ScopusExtractor()
        if isinstance(raw_data, dict) and "search-results" in raw_data:
            items = raw_data["search-results"].get("entry", [])
        elif isinstance(raw_data, list):
            items = raw_data
        else:
            items = raw_data.get("results", raw_data.get("entry", []))
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    elif source == "wos":
        from extractors.wos import WoSExtractor
        extractor = WoSExtractor()
        items = (
            raw_data
            if isinstance(raw_data, list)
            else raw_data.get("hits", raw_data.get("records", []))
        )
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    elif source == "datos_abiertos":
        from extractors.datos_abiertos import DatosAbiertosExtractor
        extractor = DatosAbiertosExtractor()
        items = (
            raw_data
            if isinstance(raw_data, list)
            else raw_data.get("results", [])
        )
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    elif source == "cvlac":
        from extractors.cvlac import CvLACExtractor
        extractor = CvLACExtractor()
        items = (
            raw_data
            if isinstance(raw_data, list)
            else raw_data.get("results", [])
        )
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    else:
        raise ValueError(f"Fuente no soportada: {source}")

    return records
