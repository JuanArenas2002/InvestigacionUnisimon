"""
Funciones de normalización compartidas por toda la aplicación.

Consolida los normalizadores de:
  - extractors/base.py         (normalize_doi, normalize_year, normalize_text,
                                  normalize_author_name)
  - extractors/openalex.py     (_normalize_title → normalize_title_for_search)
  - reconciliation/fuzzy_matcher.py (normalize_for_comparison — alias de
                                      normalize_text)
"""
import re
import unicodedata
from typing import Optional

from unidecode import unidecode


def normalize_doi(doi: str) -> str:
    """
    Normaliza un DOI a formato canónico: 10.xxxx/yyyy (sin URL prefix).

    Ejemplos:
        "https://doi.org/10.1000/xyz" → "10.1000/xyz"
        "DOI: 10.1000/xyz"           → "10.1000/xyz"
    """
    if not doi:
        return ""
    doi = str(doi).strip().lower()
    for prefix in ["https://doi.org/", "http://doi.org/", "doi:", "doi.org/"]:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
    return doi.strip()


def normalize_year(year) -> Optional[int]:
    """Normaliza año a entero. Devuelve None si no es válido."""
    if year is None:
        return None
    year_str = re.sub(r'\D', '', str(year))
    if year_str and len(year_str) == 4:
        return int(year_str)
    return None


def normalize_text(text: str) -> str:
    """
    Normaliza texto para comparación fuzzy:
      - minúsculas
      - sin tildes ni diacríticos (via unidecode)
      - solo alfanuméricos y espacios
      - espacios múltiples colapsados
    """
    if not text:
        return ""
    text = str(text).lower().strip()
    text = unidecode(text)
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_author_name(name: str) -> str:
    """
    Limpia un nombre de autor para almacenamiento legible:
      - Reemplaza guiones Unicode y ASCII por espacios.
      - Colapsa espacios múltiples.
      - Conserva tildes (este normalizador es para display, no comparación).
    """
    if not name:
        return ""
    name = str(name).strip()
    # Reemplazar todos los tipos de guion/hyphen por espacio
    name = re.sub(
        r'[\u2010\u2011\u2012\u2013\u2014\u2015\u00AD\u002D\uFE58\uFE63\uFF0D]+',
        ' ',
        name,
    )
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_title_for_search(title: str) -> str:
    """
    Convierte a lowercase y elimina tildes/diacríticos para búsquedas fuzzy.

    Diferencia con normalize_text: no elimina caracteres especiales ni
    espacios; solo quita las marcas de combinación Unicode. Esto preserva
    mejor la estructura lexicográfica del título.

    Ejemplo:
        "Vitamina D: EFECTOS Y BENEFICIOS" → "vitamina d: efectos y beneficios"
    """
    nfkd = unicodedata.normalize("NFKD", title.lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_publication_type(pub_type: Optional[str]) -> Optional[str]:
    """
    Normaliza el tipo de publicación a mayúsculas.
    
    Esto evita duplicados como: article/ARTICLE, review/REVIEW, etc.
    
    Ejemplo:
        "article" → "ARTICLE"
        "REVIEW" → "REVIEW"
        "book-chapter" → "BOOK-CHAPTER"
        None → None
    """
    if not pub_type:
        return None
    return pub_type.strip().upper()


def normalize_author_name(name: Optional[str]) -> Optional[str]:
    """
    Normaliza el nombre de autor a mayúsculas.
    
    Esto evita duplicados de autores como: "juan Pérez" vs "JUAN PÉREZ".
    
    Ejemplo:
        "juan pérez" → "JUAN PÉREZ"
        "MARIA GARCIA" → "MARIA GARCIA"
        "José Luis" → "JOSÉ LUIS"
        None → None
    """
    if not name:
        return None
    return name.strip().upper()
