"""
Conversión de publicaciones de Google Scholar al formato StandardRecord.

Google Scholar devuelve datos mínimos por publicación:
  - title, year, citation count, venue (revista/conferencia), url
  - Co-autores disponibles en bib["author"] como string "A and B and C"

Los autores se extraen del campo bib["author"]. El dueño del perfil
siempre se marca como institucional si aparece entre los co-autores,
o se agrega al final si no está en la lista.
"""

import hashlib
from typing import List, Optional


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

    authors = _extract_authors(bib, scholar_id, profile_name)

    return {
        "source_id":             pub_id or _stable_id(scholar_id, title, url),
        "doi":                   _extract_doi(pub),
        "title":                 title,
        "publication_year":      year,
        "publication_type":      _infer_type(bib),
        "source_journal":        journal,
        "issn":                  None,
        "authors":               authors,
        "institutional_authors": [a for a in authors if a["is_institutional"]],
        "citation_count":        citations,
        "url":                   url,
        "raw_data":              pub,
    }


def _extract_authors(bib: dict, scholar_id: str, profile_name: str) -> List[dict]:
    """
    Extrae co-autores del campo bib["author"] (string "A and B and C").
    Marca al dueño del perfil como institucional.
    Si el campo no existe, retorna solo el dueño del perfil.
    """
    author_str = bib.get("author", "")
    if not author_str:
        return [{
            "name": profile_name,
            "orcid": None,
            "is_institutional": True,
            "google_scholar_id": scholar_id,
        }]

    names = [n.strip() for n in author_str.split(" and ") if n.strip()]
    profile_name_lower = profile_name.lower()

    authors = []
    profile_found = False
    for name in names:
        is_owner = profile_name_lower in name.lower() or name.lower() in profile_name_lower
        if is_owner:
            profile_found = True
        authors.append({
            "name": name,
            "orcid": None,
            "is_institutional": is_owner,
            "google_scholar_id": scholar_id if is_owner else None,
        })

    # Si el perfil no apareció en la lista de autores, agregarlo
    if not profile_found:
        authors.append({
            "name": profile_name,
            "orcid": None,
            "is_institutional": True,
            "google_scholar_id": scholar_id,
        })

    return authors


def _stable_id(scholar_id: str, title: Optional[str], url: Optional[str]) -> str:
    """Genera un ID estable y único cuando author_pub_id no está disponible."""
    content = f"{scholar_id}|{title or ''}|{url or ''}"
    return f"{scholar_id}_{hashlib.md5(content.encode()).hexdigest()[:8]}"


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
    url = pub.get("pub_url", "") or ""
    if "doi.org/" in url:
        return url.split("doi.org/")[-1].strip()
    return None


def _infer_type(bib: dict) -> str:
    """Infiere el tipo de publicación desde los metadatos disponibles."""
    venue = (bib.get("venue") or "").lower()
    if bib.get("conference") or any(
        kw in venue for kw in ("conference", "proceedings", "workshop", "symposium")
    ):
        return "conference_paper"
    if bib.get("journal") or bib.get("venue"):
        return "article"
    return "publication"
