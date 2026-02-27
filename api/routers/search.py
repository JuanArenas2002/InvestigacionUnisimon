"""
Router de Búsqueda live en OpenAlex.
Permite buscar publicaciones en la API de OpenAlex sin ingesta.
"""

import logging
from typing import Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import APIRouter, Query, HTTPException

from config import openalex_config, institution
from extractors.base import normalize_doi

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["Búsqueda"])


def _get_session() -> requests.Session:
    """Sesión HTTP con retry."""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


# ── GET /search/openalex ─────────────────────────────────────

@router.get("/openalex", summary="Buscar en OpenAlex")
def search_openalex(
    query: Optional[str] = Query(None, description="Texto libre de búsqueda"),
    doi: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    author: Optional[str] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    max_results: int = Query(25, ge=1, le=200),
):
    """
    Búsqueda en vivo en la API de OpenAlex.
    Retorna publicaciones con metadatos y autores institucionales.
    """
    params = {
        "per_page": min(max_results, 200),
        "mailto": institution.contact_email,
    }

    # Construir filtros
    filters = []
    if doi:
        ndoi = normalize_doi(doi)
        if ndoi:
            filters.append(f"doi:https://doi.org/{ndoi}")
    if year_from and year_to:
        filters.append(f"from_publication_date:{year_from}-01-01")
        filters.append(f"to_publication_date:{year_to}-12-31")
    elif year_from:
        filters.append(f"from_publication_date:{year_from}-01-01")
    elif year_to:
        filters.append(f"to_publication_date:{year_to}-12-31")

    if filters:
        params["filter"] = ",".join(filters)

    # Búsqueda por texto
    search_parts = []
    if query:
        search_parts.append(query)
    if title:
        search_parts.append(title)
    if author:
        search_parts.append(author)
    if search_parts:
        params["search"] = " ".join(search_parts)

    if not params.get("search") and not params.get("filter"):
        raise HTTPException(400, "Debe proporcionar al menos un criterio de búsqueda")

    try:
        session = _get_session()
        resp = session.get(openalex_config.base_url, params=params, timeout=openalex_config.timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error(f"Error buscando en OpenAlex: {e}")
        raise HTTPException(502, f"Error al consultar OpenAlex: {e}")

    results = data.get("results", [])
    ror_id = institution.ror_id

    output = []
    for work in results:
        # Identificar autores institucionales
        inst_authors = []
        all_authors = []

        for authorship in work.get("authorships", []):
            author_info = authorship.get("author", {})
            author_name = author_info.get("display_name", "Desconocido")
            author_orcid = author_info.get("orcid")
            openalex_id = author_info.get("id", "")

            is_inst = False
            for inst in authorship.get("institutions", []):
                if inst.get("ror") == ror_id:
                    is_inst = True
                    break

            entry = {
                "name": author_name,
                "orcid": author_orcid,
                "openalex_id": openalex_id,
                "is_institutional": is_inst,
            }
            all_authors.append(entry)
            if is_inst:
                inst_authors.append(entry)

        # Fuente / landing page
        primary_loc = work.get("primary_location") or {}
        source_info = primary_loc.get("source") or {}
        landing_url = primary_loc.get("landing_page_url")

        oa_info = work.get("open_access", {})

        output.append({
            "openalex_id": work.get("id", ""),
            "doi": work.get("doi"),
            "title": work.get("title", ""),
            "publication_year": work.get("publication_year"),
            "publication_type": work.get("type"),
            "cited_by_count": work.get("cited_by_count", 0),
            "is_open_access": oa_info.get("is_oa", False),
            "oa_status": oa_info.get("oa_status"),
            "source_journal": source_info.get("display_name"),
            "issn": source_info.get("issn_l"),
            "landing_page_url": landing_url or work.get("doi"),
            "all_authors": all_authors,
            "institutional_authors": inst_authors,
            "institutional_authors_count": len(inst_authors),
        })

    return {
        "count": data.get("meta", {}).get("count", len(output)),
        "results": output,
    }
