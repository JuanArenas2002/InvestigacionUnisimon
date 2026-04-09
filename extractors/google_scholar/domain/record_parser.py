"""
Conversión de publicaciones de Google Scholar al formato StandardRecord.

Google Scholar devuelve datos mínimos por publicación:
  - title, year, citation count, venue (revista/conferencia), url
  - No incluye DOI ni autores co-autores en la lista del perfil

Los autores se infieren del perfil: el dueño del perfil es siempre
marcado como autor institucional.
"""

from typing import Optional


def parse_publication(pub: dict, scholar_id: str, profile_name: str) -> dict:
    """
    Convierte un dict de publicación de `scholarly` a campos de StandardRecord.

    Args:
        pub:          Dict de publicación devuelto por scholarly (bib + num_citations).
        scholar_id:   ID del perfil Google Scholar (para source_id).
        profile_name: Nombre del investigador dueño del perfil.

    Returns:
        Dict con campos listos para construir un StandardRecord.
    """
    bib = pub.get("bib", {})
    title = bib.get("title") or pub.get("title")
    year = _safe_int(bib.get("pub_year") or bib.get("year"))
    journal = bib.get("venue") or bib.get("journal") or bib.get("conference")
    citations = pub.get("num_citations") or 0
    url = pub.get("pub_url") or pub.get("url")
    pub_id = pub.get("author_pub_id") or pub.get("citekey")

    # El dueño del perfil es siempre el autor institucional
    author_entry = {
        "name": profile_name,
        "orcid": None,
        "is_institutional": True,
        "google_scholar_id": scholar_id,
    }

    return {
        "source_id":             pub_id or f"{scholar_id}_{title[:30] if title else 'unknown'}",
        "doi":                   _extract_doi(pub),
        "title":                 title,
        "publication_year":      year,
        "publication_type":      _infer_type(bib),
        "source_journal":        journal,
        "issn":                  None,
        "authors":               [author_entry],
        "institutional_authors": [author_entry],
        "citation_count":        citations,
        "url":                   url,
        "raw_data":              pub,
    }


def _safe_int(value) -> Optional[int]:
    try:
        return int(value) if value else None
    except (ValueError, TypeError):
        return None


def _extract_doi(pub: dict) -> Optional[str]:
    """Intenta extraer DOI del dict de publicación."""
    doi = pub.get("bib", {}).get("doi") or pub.get("doi")
    if doi:
        return doi.strip()
    # Algunos registros tienen el DOI en el URL
    url = pub.get("pub_url", "") or ""
    if "doi.org/" in url:
        return url.split("doi.org/")[-1].strip()
    return None


def _infer_type(bib: dict) -> str:
    """Infiere el tipo de publicación desde los metadatos disponibles."""
    if bib.get("conference"):
        return "conference_paper"
    if bib.get("journal"):
        return "article"
    return "publication"
