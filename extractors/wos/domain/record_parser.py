"""
Parseo puro de registros crudos de Web of Science → campos de StandardRecord.

Este módulo no hace llamadas HTTP ni I/O de ningún tipo. Su única
responsabilidad es transformar el dict 'hit' que devuelve la WoS Starter API
en los campos que necesita StandardRecord.

Estructura del hit de WoS Starter:
  {
    "uid": "WOS:000...",
    "title": "...",
    "identifiers": {"doi": "10.xxx/..."},
    "source": {"publishYear": 2023, "sourceTitle": "...", "sourceType": [...]},
    "names": {"authors": [{"displayName": "...", "wosStandard": "..."}]},
    "citations": [{"count": 42}]
  }
"""

from typing import List, Dict, Optional


def parse_authors(names_block: dict) -> List[Dict]:
    """
    Extrae la lista de autores del bloque 'names' de un hit de WoS.

    WoS puede representar el nombre en dos formatos:
      - displayName: nombre para mostrar (ej: "García López, Juan")
      - wosStandard: nombre normalizado por WoS (ej: "Garcia Lopez, J")
    Se prefiere displayName y se cae al wosStandard si no está disponible.

    Args:
        names_block: Valor de hit.get('names', {}). Contiene la clave 'authors'.

    Returns:
        Lista de dicts con claves: name, orcid, wos_id, is_institutional.
        is_institutional siempre es False: WoS no expone afiliación a nivel
        de autor en la Starter API.
    """
    authors = []
    for entry in (names_block or {}).get("authors", []) or []:
        name = entry.get("displayName") or entry.get("wosStandard")
        if name:
            authors.append({
                "name": name,
                "orcid": None,           # WoS Starter no incluye ORCID
                "wos_id": None,          # No hay ID de autor en Starter API
                "is_institutional": False,
            })
    return authors


def parse_publication_year(source_block: dict) -> Optional[int]:
    """
    Extrae el año de publicación del bloque 'source' de un hit de WoS.

    Args:
        source_block: Valor de hit.get('source', {}). Contiene 'publishYear'.

    Returns:
        Año como entero, o None si no está disponible o no es parseable.
    """
    raw = (source_block or {}).get("publishYear")
    if raw is not None:
        try:
            return int(raw)
        except (ValueError, TypeError):
            return None
    return None


def parse_publication_type(source_block: dict) -> Optional[str]:
    """
    Extrae el tipo de documento del bloque 'source' de un hit de WoS.

    sourceType puede venir como string o como lista. Se toma el primer elemento
    si es lista.

    Args:
        source_block: Valor de hit.get('source', {}). Contiene 'sourceType'.

    Returns:
        Tipo de documento como string (ej: 'Article', 'Review'), o None.
    """
    doc_types = (source_block or {}).get("sourceType", [])
    if isinstance(doc_types, str):
        return doc_types
    if isinstance(doc_types, list) and doc_types:
        return doc_types[0]
    return None


def parse_citation_count(hit: dict) -> int:
    """
    Extrae el conteo de citas de un hit de WoS.

    WoS devuelve citas como lista de dicts: [{"count": N, "type": "..."}].
    Se toma el count del primer elemento.

    Args:
        hit: Dict completo del hit de WoS Starter API.

    Returns:
        Conteo de citas como entero. 0 si no está disponible.
    """
    citations = hit.get("citations")
    if citations and isinstance(citations, list):
        return int(citations[0].get("count", 0) or 0)
    return 0


def parse_hit(hit: dict) -> dict:
    """
    Convierte un hit completo de WoS Starter API en un dict intermedio
    con los campos necesarios para construir un StandardRecord.

    Este dict actúa como intermediario: el extractor lo recibe y construye
    el StandardRecord, manteniendo el dominio independiente de la clase base.

    Args:
        hit: Registro crudo de la respuesta JSON de WoS Starter API.

    Returns:
        Dict con claves: source_id, doi, title, publication_year,
        publication_type, source_journal, authors, citation_count, raw_data.
    """
    identifiers = hit.get("identifiers", {}) or {}
    source_block = hit.get("source", {}) or {}

    return {
        "source_id":        hit.get("uid", ""),
        "doi":              identifiers.get("doi"),
        "title":            hit.get("title"),
        "publication_year": parse_publication_year(source_block),
        "publication_type": parse_publication_type(source_block),
        "source_journal":   source_block.get("sourceTitle"),
        "authors":          parse_authors(hit.get("names", {})),
        "citation_count":   parse_citation_count(hit),
        "raw_data":         hit,
    }
