"""
Parseo flexible de registros de datasets de Datos Abiertos Colombia.

Los datasets de datos.gov.co (Socrata/SODA) no tienen un esquema fijo:
cada dataset puede usar nombres de columna distintos para los mismos
campos conceptuales (título, año, DOI, revista, autores, tipo).

Este módulo centraliza los alias de columna conocidos para cada campo
y los usa para extraer los datos de forma robusta, probando variantes
en orden de prioridad.

Si se incorpora un nuevo dataset con nombres no cubiertos, agregar
los alias correspondientes en los diccionarios FIELD_ALIASES.
"""

from typing import List, Dict, Optional


# ---------------------------------------------------------------------------
# Aliases de columnas por campo conceptual
# (en orden de prioridad, de más específico a más genérico)
# ---------------------------------------------------------------------------

# Nombres de columna conocidos para el título del producto
TITLE_ALIASES = [
    "titulo_del_articulo",
    "titulo",
    "nombre_del_producto",
    "title",
    "nombre",
]

# Nombres de columna conocidos para el año de publicación
YEAR_ALIASES = [
    "ano_de_publicacion",
    "ano",
    "anio",
    "year",
    "fecha_publicacion",
]

# Nombres de columna conocidos para el DOI
DOI_ALIASES = [
    "identificador_doi",
    "doi",
]

# Nombres de columna conocidos para la revista / fuente
JOURNAL_ALIASES = [
    "nombre_de_la_revista",
    "revista",
    "fuente",
    "journal",
]

# Nombres de columna conocidos para autores
AUTHOR_ALIASES = [
    "nombres_autores",
    "autores",
    "autor",
    "authors",
]

# Nombres de columna conocidos para el tipo de publicación
TYPE_ALIASES = [
    "tipo_de_producto",
    "tipologia",
    "tipo",
    "type",
]


def _get_field(entry: dict, aliases: List[str]) -> Optional[str]:
    """
    Busca el valor de un campo en un dict probando múltiples nombres de columna.

    Itera los aliases en orden y retorna el primer valor no nulo encontrado.
    Retorna None si ningún alias produce un valor.

    Args:
        entry:   Dict del registro crudo del dataset.
        aliases: Lista de nombres de columna a probar, en orden de prioridad.

    Returns:
        Valor del campo como string, o None si no se encuentra.
    """
    for key in aliases:
        value = entry.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def parse_year(raw_year: Optional[str]) -> Optional[int]:
    """
    Normaliza el año de publicación a entero.

    Acepta strings con año completo o solo los primeros 4 caracteres
    (para casos como '2020-01-01' o '2020.0').

    Args:
        raw_year: Valor crudo del campo año (puede ser str, int, float o None).

    Returns:
        Año como entero de 4 dígitos, o None si no es parseable.
    """
    if raw_year is None:
        return None
    try:
        return int(str(raw_year)[:4])
    except (ValueError, TypeError):
        return None


def parse_authors(raw_authors: Optional[str]) -> List[Dict]:
    """
    Parsea el campo de autores, que en Datos Abiertos viene como un string
    con autores separados por punto y coma (';').

    Args:
        raw_authors: String con autores separados por ';', o None.

    Returns:
        Lista de dicts con claves: name, orcid, is_institutional.
    """
    if not raw_authors:
        return []
    authors = []
    for name in str(raw_authors).split(";"):
        name = name.strip()
        if name:
            authors.append({
                "name":             name,
                "orcid":            None,   # Datos Abiertos no incluye ORCIDs
                "is_institutional": False,
            })
    return authors


def parse_entry(entry: dict) -> dict:
    """
    Convierte un registro crudo de un dataset SODA a los campos de StandardRecord.

    Usa los aliases de columna para extraer cada campo de forma flexible,
    adaptándose a diferentes esquemas de dataset sin necesidad de cambiar
    el código del extractor.

    Args:
        entry: Dict crudo del registro SODA (una fila del dataset).

    Returns:
        Dict con los campos listos para construir un StandardRecord:
          source_id, doi, title, publication_year, publication_type,
          source_journal, issn, authors, raw_data.
    """
    return {
        # ID interno de Socrata (:id) o campo 'id' si existe
        "source_id":        entry.get(":id") or entry.get("id"),
        "doi":              _get_field(entry, DOI_ALIASES),
        "title":            _get_field(entry, TITLE_ALIASES),
        "publication_year": parse_year(_get_field(entry, YEAR_ALIASES)),
        "publication_type": _get_field(entry, TYPE_ALIASES) or "article",
        "source_journal":   _get_field(entry, JOURNAL_ALIASES),
        "issn":             entry.get("issn"),
        "authors":          parse_authors(_get_field(entry, AUTHOR_ALIASES)),
        "raw_data":         entry,
    }
