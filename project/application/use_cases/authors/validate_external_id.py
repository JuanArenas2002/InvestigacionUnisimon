"""
Use case: Validate an external author ID by cross-checking publications.

Extracted from api/routers/authors.py::_validate_external_id.
No FastAPI dependency — callable from any interface.
"""
from __future__ import annotations

import logging
import re
from typing import Optional, Set
from urllib.parse import parse_qs, urlparse

from project.application.schemas.author_schemas import ExternalIdValidationResult

logger = logging.getLogger(__name__)

# ── URL → canonical ID extraction ───────────────────────────────────────────

def extract_id_from_url(source: str, url: str) -> Optional[str]:
    """
    Extracts the canonical author ID from a profile URL.

    Supported sources: orcid, openalex, scopus, wos, cvlac.
    Returns None if the pattern does not match.
    """
    url = url.strip()

    if source == "orcid":
        m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", url, re.IGNORECASE)
        return m.group(1) if m else None

    if source == "openalex":
        m = re.search(r"(A\d+)", url, re.IGNORECASE)
        if m:
            return f"https://openalex.org/{m.group(1).upper()}"
        return None

    if source == "scopus":
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        aid = qs.get("authorId") or qs.get("authorid")
        if aid:
            return aid[0]
        m = re.search(r"(\d{7,})", url)
        return m.group(1) if m else None

    if source == "wos":
        m = re.search(r"/record/([A-Z0-9\-]+)", url, re.IGNORECASE)
        if m:
            return m.group(1).upper()
        m = re.search(r"([A-Z]-\d{4}-\d{4})", url, re.IGNORECASE)
        return m.group(1).upper() if m else None

    if source == "cvlac":
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        cod = qs.get("cod_rh")
        if cod:
            return cod[0]
        m = re.search(r"(\d{7,})", url)
        return m.group(1) if m else None

    return None


# ── Cross-validation ─────────────────────────────────────────────────────────

def validate_external_id(
    source: str,
    candidate_id: str,
    author_dois: Set[str],
    max_results: int = 50,
) -> ExternalIdValidationResult:
    """
    Queries the external platform for candidate_id and computes DOI overlap
    with author_dois (normalized DOIs already in the database).

    CvLAC has no queryable API; it is accepted without cross-validation.
    """
    if source == "cvlac":
        return ExternalIdValidationResult(
            source=source,
            candidate_id=candidate_id,
            matched=0,
            total_from_source=0,
            author_pubs_in_db=len(author_dois),
            match_rate=0.0,
            validated=True,
            message="CvLAC no soporta validación automática — aceptado sin verificación",
        )

    total_from_source = 0
    matched = 0
    message = ""

    try:
        platform_dois: Set[str] = set()

        if source == "openalex":
            from extractors.openalex.extractor import OpenAlexExtractor
            records = OpenAlexExtractor().extract_by_author(
                author_id=candidate_id, max_results=max_results
            )
            platform_dois = {_norm(r.doi) for r in records if r.doi}
            total_from_source = len(records)

        elif source == "orcid":
            from extractors.openalex.extractor import OpenAlexExtractor
            records = OpenAlexExtractor().extract_by_author(
                orcid=candidate_id, max_results=max_results
            )
            platform_dois = {_norm(r.doi) for r in records if r.doi}
            total_from_source = len(records)

        elif source == "scopus":
            from extractors.scopus import ScopusExtractor
            records = ScopusExtractor().extract_by_author(
                scopus_author_id=candidate_id, max_results=max_results
            )
            platform_dois = {_norm(r.doi) for r in records if r.doi}
            total_from_source = len(records)

        elif source == "wos":
            from extractors.wos import WosExtractor
            records = WosExtractor().extract_by_author(
                wos_author_id=candidate_id, max_results=max_results
            )
            platform_dois = {_norm(r.doi) for r in records if r.doi}
            total_from_source = len(records)

        else:
            return ExternalIdValidationResult(
                source=source,
                candidate_id=candidate_id,
                validated=False,
                author_pubs_in_db=len(author_dois),
                message=f"Fuente '{source}' no soportada para validación",
            )

        matched = len(author_dois & platform_dois)

    except Exception as exc:
        message = f"Error consultando {source}: {exc}"
        logger.warning(message)
        return ExternalIdValidationResult(
            source=source,
            candidate_id=candidate_id,
            validated=False,
            author_pubs_in_db=len(author_dois),
            message=message,
        )

    n = len(author_dois)
    match_rate = min(matched / n if n > 0 else (1.0 if matched > 0 else 0.0), 1.0)

    return ExternalIdValidationResult(
        source=source,
        candidate_id=candidate_id,
        matched=matched,
        total_from_source=total_from_source,
        author_pubs_in_db=n,
        match_rate=round(match_rate, 4),
        validated=False,  # caller sets based on threshold
        message=message,
    )


def _norm(doi: Optional[str]) -> Optional[str]:
    from project.domain.value_objects.doi import DOI
    vo = DOI.parse(doi or "")
    return vo.value if vo else None
