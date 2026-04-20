"""
Parseo puro de entradas XML de Scopus Search API → campos de StandardRecord.

La Scopus Search API devuelve XML con múltiples namespaces. Este módulo
encapsula toda la lógica de extracción de campos: namespaces, fallbacks
entre campos alternativos, clasificación de Open Access, y parseo de autores.

No hace llamadas HTTP ni I/O de ningún tipo.

Namespaces XML de Scopus:
  atom    → http://www.w3.org/2005/Atom (entradas principales)
  dc      → http://purl.org/dc/elements/1.1/ (Dublin Core: título, ID)
  prism   → http://prismstandard.org/namespaces/basic/2.0/ (metadatos de publicación)
  opensearch → http://a9.com/-/spec/opensearch/1.1/ (paginación)
"""

from typing import List, Dict, Optional
import xml.etree.ElementTree as ET

# Namespaces XML que usa Scopus Search API
NS = {
    "atom":       "http://www.w3.org/2005/Atom",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
    "dc":         "http://purl.org/dc/elements/1.1/",
    "prism":      "http://prismstandard.org/namespaces/basic/2.0/",
}


def classify_oa_status(oa_raw: Optional[str]) -> Optional[bool]:
    """
    Determina si un documento es Open Access a partir del string de estado OA
    que devuelve Scopus.

    Scopus puede devolver el estado OA en varios formatos:
      - Descriptivo: 'All Open Access; Bronze Open Access'
      - Flag: 'true' / 'false'
      - Numérico: '1' / '0'
      - Etiqueta: 'Gold', 'Bronze', 'Green', 'Hybrid'

    Args:
        oa_raw: String crudo del campo de Open Access de Scopus.
                Puede ser None si el campo no está en la respuesta.

    Returns:
        True si es Open Access, False si no lo es, None si no hay información.
    """
    if oa_raw is None:
        return None
    oa_lower = str(oa_raw).lower().strip()
    # Indicadores positivos de OA en cualquiera de sus formas
    oa_indicators = [
        "all open access", "gold", "bronze", "green", "hybrid",
        "open access", "true", "1", "yes",
    ]
    return any(indicator in oa_lower for indicator in oa_indicators)


def extract_oa_fields(entry: ET.Element) -> tuple:
    """
    Extrae el estado Open Access de una entrada XML de Scopus.

    Scopus distribuye la información de OA en múltiples campos opcionales.
    Se intenta cada campo en orden de prioridad hasta encontrar un valor.

    Args:
        entry: Elemento XML <entry> de la respuesta de Scopus.

    Returns:
        Tupla (oa_status_str, is_oa_bool):
          - oa_status_str: String original del estado OA (para almacenar).
          - is_oa_bool: Booleano clasificado, o None si no hay información.
    """
    oa_status = None

    # Intentar campos en orden de prioridad
    oa_status = entry.findtext("prism:openAccessStatus", namespaces=NS)

    if not oa_status:
        oa_status = entry.findtext("atom:openaccessFlag", namespaces=NS)

    if not oa_status:
        oa_text = entry.findtext("atom:openaccess", namespaces=NS)
        if oa_text:
            oa_status = oa_text

    if not oa_status:
        freetoread_label = entry.findtext("atom:freetoreadLabel", namespaces=NS)
        if freetoread_label:
            oa_status = freetoread_label

    is_oa = classify_oa_status(oa_status)
    return oa_status, is_oa


def extract_publication_type(entry: ET.Element) -> Optional[str]:
    """
    Extrae el tipo de publicación de una entrada XML de Scopus.

    Scopus distribuye esta información en varios campos. Se intenta
    cada uno en orden de especificidad descendente.

    Args:
        entry: Elemento XML <entry> de la respuesta de Scopus.

    Returns:
        String del tipo de publicación (ej: 'Article', 'Review'), o None.
    """
    # subtypeDescription es el más descriptivo
    subtype = entry.findtext("atom:subtypeDescription", namespaces=NS)
    if not subtype:
        subtype = entry.findtext("atom:subtype", namespaces=NS)
    if not subtype:
        subtype = entry.findtext("prism:aggregationType", namespaces=NS)
    return subtype


def extract_authors(entry: ET.Element) -> List[Dict]:
    """
    Extrae la lista de autores de una entrada XML de Scopus.

    Busca elementos <author> tanto en el namespace atom como sin namespace,
    para cubrir variantes en la respuesta XML de Scopus.

    Args:
        entry: Elemento XML <entry> de la respuesta de Scopus.

    Returns:
        Lista de dicts con claves: name, orcid, scopus_id, is_institutional.
        is_institutional siempre False: Scopus Search no expone afiliación
        detallada por autor en el endpoint de búsqueda.
    """
    authors = []

    # Intentar con namespace atom primero, luego sin namespace
    author_elements = (
        entry.findall("atom:author", NS)
        or entry.findall("author")
    )

    for author in author_elements:
        # authname puede estar con o sin namespace
        name = (
            author.findtext("atom:authname", namespaces=NS)
            or author.findtext("authname")
        )
        authid = (
            author.findtext("atom:authid", namespaces=NS)
            or author.findtext("authid")
        )
        if name:
            authors.append({
                "name":             name,
                "orcid":            None,    # Scopus Search no incluye ORCID
                "scopus_id":        authid,
                "is_institutional": False,
            })

    return authors


def parse_xml_entry(entry: ET.Element) -> Optional[dict]:
    """
    Convierte una entrada XML <entry> de Scopus Search en un dict de campos
    listos para construir un StandardRecord.

    Maneja:
    - Limpieza del prefijo 'SCOPUS_ID:' del campo identifier.
    - Extracción de año desde coverDate (formato YYYY-MM-DD).
    - Clasificación de Open Access con fallbacks entre múltiples campos.
    - Autores con doble búsqueda de namespace.
    - E-ISSN en raw_data para uso posterior.

    Args:
        entry: Elemento XML <entry> de la respuesta de Scopus Search API.

    Returns:
        Dict con todos los campos para StandardRecord, o None si la entrada
        es un registro de error (contiene elemento <error>).
    """
    # Las entradas de error tienen un elemento <error> directo
    if entry.find("error") is not None or entry.findtext("error") is not None:
        return None

    # ── Identificadores ───────────────────────────────────────────────
    doi = entry.findtext("prism:doi", namespaces=NS)

    scopus_id = entry.findtext("dc:identifier", namespaces=NS)
    if scopus_id and isinstance(scopus_id, str):
        # El campo viene como 'SCOPUS_ID:85207865300' → limpiar el prefijo
        scopus_id = scopus_id.replace("SCOPUS_ID:", "").strip()

    # ── Metadatos básicos ─────────────────────────────────────────────
    title         = entry.findtext("dc:title", namespaces=NS)
    cover_date    = entry.findtext("prism:coverDate", namespaces=NS)
    pub_year      = int(cover_date[:4]) if cover_date and len(cover_date) >= 4 else None
    source_journal = entry.findtext("prism:publicationName", namespaces=NS)

    # ── ISSN e E-ISSN ─────────────────────────────────────────────────
    issn  = entry.findtext("prism:issn", namespaces=NS)
    eissn = entry.findtext("prism:eIssn", namespaces=NS)

    # ── Tipo de publicación ───────────────────────────────────────────
    pub_type = extract_publication_type(entry)

    # ── Citas ─────────────────────────────────────────────────────────
    citedby_count = int(
        entry.findtext("atom:citedby-count", default="0", namespaces=NS) or "0"
    )

    # ── Open Access ───────────────────────────────────────────────────
    oa_status, is_oa = extract_oa_fields(entry)

    # ── Autores ───────────────────────────────────────────────────────
    authors = extract_authors(entry)

    # Abstract (dc:description en namespace dc)
    abstract = entry.findtext("dc:description", namespaces=NS)

    # Rango de páginas
    page_range = entry.findtext("prism:pageRange", namespaces=NS)

    # Editorial
    publisher = entry.findtext("prism:publisher", namespaces=NS)

    return {
        "source_id":        scopus_id,
        "doi":              doi,
        "title":            title,
        "publication_year": pub_year,
        "publication_date": cover_date,
        "publication_type": pub_type,
        "source_journal":   source_journal,
        "issn":             issn,
        "is_open_access":   is_oa,
        "oa_status":        oa_status,
        "authors":          authors,
        "citation_count":   citedby_count,
        "abstract":         abstract,
        "page_range":       page_range,
        "publisher":        publisher,
        # E-ISSN se guarda en raw_data para uso en cobertura de revistas
        "raw_data":         {"eissn": eissn} if eissn else {},
    }


def parse_json_entry(entry: dict) -> dict:
    """
    Convierte una entrada JSON de Scopus (usada en search_by_doi) en un
    dict de campos listos para StandardRecord.

    La búsqueda por DOI devuelve JSON en lugar de XML (Accept: application/json).
    Este método maneja ese formato alternativo.

    Args:
        entry: Dict crudo del resultado JSON de Scopus Search API.

    Returns:
        Dict con los campos para StandardRecord.
    """
    # Limpiar Scopus ID
    scopus_id = entry.get("dc:identifier", "")
    if scopus_id.startswith("SCOPUS_ID:"):
        scopus_id = scopus_id.replace("SCOPUS_ID:", "")

    # Año desde coverDate
    cover_date = entry.get("prism:coverDate", "")
    pub_year = int(cover_date[:4]) if cover_date and len(cover_date) >= 4 else None

    # Tipo de publicación con fallbacks
    pub_type = (
        entry.get("subtypeDescription")
        or entry.get("aggregationType")
        or entry.get("subtype")
    )

    # Open Access
    oa_raw = (
        entry.get("openaccessFlag")
        or entry.get("openaccess")
        or entry.get("openAccessStatus")
    )
    is_oa = classify_oa_status(str(oa_raw) if oa_raw is not None else None)

    # E-ISSN
    eissn = entry.get("prism:eIssn") or entry.get("prism:eissn")

    # Autores (formato JSON de Scopus)
    authors = []
    for auth in entry.get("author", []) or []:
        name = auth.get("authname")
        if name:
            authors.append({
                "name":             name,
                "orcid":            None,
                "scopus_id":        auth.get("authid"),
                "is_institutional": False,
            })

    return {
        "source_id":        scopus_id,
        "doi":              entry.get("prism:doi") or entry.get("doi"),
        "title":            entry.get("dc:title"),
        "publication_year": pub_year,
        "publication_date": cover_date,
        "publication_type": pub_type,
        "source_journal":   entry.get("prism:publicationName"),
        "issn":             entry.get("prism:issn"),
        "is_open_access":   is_oa,
        "oa_status":        str(oa_raw) if oa_raw is not None else None,
        "authors":          authors,
        "citation_count":   int(entry.get("citedby-count", 0) or 0),
        "abstract":         entry.get("dc:description") or entry.get("description"),
        "page_range":       entry.get("prism:pageRange") or entry.get("pageRange"),
        "publisher":        entry.get("dc:publisher") or entry.get("prism:publisher"),
        "raw_data":         {**entry, "eissn": eissn} if eissn else entry,
    }
