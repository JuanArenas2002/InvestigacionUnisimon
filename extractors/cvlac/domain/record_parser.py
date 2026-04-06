"""
Conversión de dicts crudos de CVLAC a campos de StandardRecord.

Este módulo es la única interfaz entre la representación interna
de CVLAC (dict con claves propias) y el contrato estándar del sistema
(StandardRecord). No hace HTTP ni accede a disco.

Particularidad de CVLAC:
  - El perfil muestra la producción de UN investigador.
  - Los autores siempre se marcan como institucionales (is_institutional=True)
    porque CVLAC solo indexa investigadores registrados en Minciencias.
  - No hay forma de distinguir co-autores externos desde el portal público.
"""

from typing import List, Dict


def build_authors(raw_authors: List[str]) -> List[Dict]:
    """
    Construye la lista de autores en el formato estándar del sistema.

    Como CVLAC solo muestra el perfil de un investigador, la lista de autores
    viene vacía del html_parser. En el futuro, si se implementa extracción
    de autores desde el HTML, esta función los formatea correctamente.

    Args:
        raw_authors: Lista de nombres de autores (strings). Usualmente vacía
                     en la implementación actual de CVLAC.

    Returns:
        Lista de dicts con claves: name, orcid, is_institutional.
        is_institutional=True porque todos los autores de CVLAC son
        investigadores registrados en Minciencias.
    """
    return [
        {
            "name":             name,
            "orcid":            None,   # CVLAC no expone ORCIDs en el portal público
            "is_institutional": True,   # Siempre institucional en CVLAC
        }
        for name in raw_authors
        if name
    ]


def parse_raw(raw: dict) -> dict:
    """
    Convierte un dict crudo de CVLAC en los campos necesarios para StandardRecord.

    Actúa como adaptador entre la representación de html_parser y el contrato
    de StandardRecord. No crea el StandardRecord directamente para mantener
    la capa de dominio independiente de la clase base.

    Args:
        raw: Dict producido por html_parser.extract_row_data con claves:
             cvlac_product_id, title, year, type, issn, doi, journal, authors.

    Returns:
        Dict con los campos listos para construir un StandardRecord:
          source_id, doi, title, publication_year, publication_type,
          source_journal, issn, authors, institutional_authors, raw_data.
    """
    authors = build_authors(raw.get("authors", []))

    return {
        "source_id":          raw.get("cvlac_product_id"),
        "doi":                raw.get("doi"),
        "title":              raw.get("title"),
        "publication_year":   raw.get("year"),
        "publication_type":   raw.get("type", "article"),
        "source_journal":     raw.get("journal"),
        "issn":               raw.get("issn"),
        # En CVLAC todos los autores son también autores institucionales
        "authors":            authors,
        "institutional_authors": authors,
        "raw_data":           raw,
    }
