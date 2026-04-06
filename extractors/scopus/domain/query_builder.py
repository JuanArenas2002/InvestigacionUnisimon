"""
Constructor de queries avanzadas para la Scopus Search API (Elsevier).

Scopus usa un lenguaje de búsqueda propio con operadores de campo que
equivalen a la pestaña 'Advanced search' de la interfaz web de Scopus.

Operadores principales:
  TITLE(...)         → Buscar en título
  ABS(...)           → Buscar en resumen
  KEY(...)           → Buscar en palabras clave
  TITLE-ABS-KEY(...) → Buscar en título + resumen + palabras clave
  AUTH(...)          → Buscar por apellido de autor
  ORCID(...)         → Buscar por ORCID del autor
  AU-ID(...)         → Buscar por Scopus Author ID
  AF-ID(...)         → Buscar por Scopus Affiliation ID (institucional)
  AFFIL(...)         → Buscar por nombre de afiliación
  SRCTITLE(...)      → Buscar por nombre de revista
  ISSN(...)          → Buscar por ISSN
  DOI(...)           → Buscar por DOI exacto
  PUBYEAR > N        → Año de publicación mayor que N
  DOCTYPE(código)    → Tipo de documento (ar=article, re=review, etc.)
  SUBJAREA(código)   → Área temática (MEDI, COMP, ENGI, etc.)
  OPENACCESS(1)      → Solo documentos Open Access
  FUND-SPONSOR(...)  → Organismo financiador

Referencia oficial:
  https://dev.elsevier.com/sc_search_tips.html
"""

from typing import Optional, List, Dict


# Mapa de tipos de documento: nombre legible → código de Scopus para DOCTYPE(...)
DOCTYPE_CODES: Dict[str, str] = {
    "article":          "ar",
    "review":           "re",
    "conference paper": "cp",
    "book":             "bk",
    "book chapter":     "ch",
    "editorial":        "ed",
    "letter":           "le",
    "note":             "no",
    "short survey":     "sh",
    "erratum":          "er",
    "report":           "rp",
    "abstract report":  "ab",
}


def build_advanced_query(
    *,
    # ── Contenido ──────────────────────────────────────────────────────
    title: Optional[str] = None,
    abstract: Optional[str] = None,
    keywords: Optional[str] = None,
    title_abs_key: Optional[str] = None,
    # ── Autoría ────────────────────────────────────────────────────────
    author: Optional[str] = None,
    first_author: Optional[str] = None,
    author_id: Optional[str] = None,
    orcid: Optional[str] = None,
    # ── Afiliación ─────────────────────────────────────────────────────
    affiliation_id: Optional[str] = None,
    affiliation_name: Optional[str] = None,
    # ── Fuente ─────────────────────────────────────────────────────────
    source_title: Optional[str] = None,
    issn: Optional[str] = None,
    doi: Optional[str] = None,
    publisher: Optional[str] = None,
    # ── Rango de años ──────────────────────────────────────────────────
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    year_exact: Optional[int] = None,
    # ── Clasificación ──────────────────────────────────────────────────
    document_type: Optional[str] = None,
    subject_area: Optional[str] = None,
    language: Optional[str] = None,
    open_access: Optional[bool] = None,
    # ── Financiación ───────────────────────────────────────────────────
    funder: Optional[str] = None,
    grant_number: Optional[str] = None,
    # ── Cláusula libre adicional ───────────────────────────────────────
    extra: Optional[str] = None,
    operator: str = "AND",
) -> str:
    """
    Construye una query para la Scopus Search API combinando operadores de campo.

    Cada argumento corresponde a un operador de campo de Scopus. Solo los
    argumentos no nulos se incluyen en la query. Las cláusulas se unen
    con el operador especificado (AND por defecto).

    Args:
        title:           Buscar en título → TITLE(...)
        abstract:        Buscar en resumen → ABS(...)
        keywords:        Buscar en palabras clave → KEY(...)
        title_abs_key:   Buscar en título+resumen+kw → TITLE-ABS-KEY(...)
        author:          Apellido o 'Apellido, N.' → AUTH(...)
        first_author:    Solo primer autor → AUTHFIRST(...)
        author_id:       Scopus Author ID numérico → AU-ID(...)
        orcid:           ORCID del autor → ORCID(...)
        affiliation_id:  AF-ID de institución → AF-ID(...)
                         Acepta múltiples separados por coma: '60106970,60112687'
        affiliation_name: Nombre de institución → AFFIL(...)
        source_title:    Nombre de revista → SRCTITLE(...)
        issn:            ISSN de la revista → ISSN(...)
        doi:             DOI exacto → DOI(...)
        publisher:       Editorial → PUBLISHER(...)
        year_from:       Año mínimo (inclusive) → PUBYEAR > year-1
        year_to:         Año máximo (inclusive) → PUBYEAR < year+1
        year_exact:      Año exacto → PUBYEAR = year
        document_type:   Tipo de documento (nombre o código) → DOCTYPE(...)
        subject_area:    Código de área temática → SUBJAREA(...)
        language:        Idioma → LANGUAGE(...)
        open_access:     True = solo OA → OPENACCESS(1)
        funder:          Organismo financiador → FUND-SPONSOR(...)
        grant_number:    Número de grant → FUND-NO(...)
        extra:           Cláusula libre adicional (se añade tal cual).
        operator:        Operador entre cláusulas: 'AND' | 'OR' | 'AND NOT'.

    Returns:
        String de query lista para el parámetro ?query=... de la API.

    Raises:
        ValueError: Si no se especifica ningún criterio de búsqueda.

    Ejemplos de salida:
        'AF-ID(60106970) AND PUBYEAR > 2019 AND PUBYEAR < 2026'
        'ORCID(0000-0002-1234-5678) AND TITLE-ABS-KEY("machine learning")'
        'SRCTITLE("Biomédica") AND FUND-SPONSOR("Minciencias")'
    """
    parts: List[str] = []

    # ── Contenido ──────────────────────────────────────────────────────
    if title_abs_key:
        parts.append(f'TITLE-ABS-KEY("{title_abs_key}")')
    if title:
        parts.append(f'TITLE("{title}")')
    if abstract:
        parts.append(f'ABS("{abstract}")')
    if keywords:
        parts.append(f'KEY("{keywords}")')

    # ── Autoría ────────────────────────────────────────────────────────
    if author:
        parts.append(f'AUTH("{author}")')
    if first_author:
        parts.append(f'AUTHFIRST("{first_author}")')
    if author_id:
        parts.append(f"AU-ID({author_id})")
    if orcid:
        # Normalizar ORCID: quitar prefijo URL si viene con él
        cleaned_orcid = orcid.replace("https://orcid.org/", "").strip()
        parts.append(f"ORCID({cleaned_orcid})")

    # ── Afiliación ─────────────────────────────────────────────────────
    if affiliation_id:
        # Soporta múltiples AF-IDs separados por coma: '60106970,60112687'
        ids = [i.strip() for i in str(affiliation_id).split(",") if i.strip()]
        if len(ids) == 1:
            parts.append(f"AF-ID({ids[0]})")
        else:
            # Múltiples IDs → unir con OR dentro de paréntesis
            af_parts = " OR ".join(f"AF-ID({i})" for i in ids)
            parts.append(f"({af_parts})")
    if affiliation_name:
        parts.append(f'AFFIL("{affiliation_name}")')

    # ── Fuente ─────────────────────────────────────────────────────────
    if source_title:
        parts.append(f'SRCTITLE("{source_title}")')
    if issn:
        # Scopus ISSN sin guión en la query
        clean_issn = issn.replace("-", "")
        parts.append(f"ISSN({clean_issn})")
    if doi:
        clean_doi = (
            doi.replace("https://doi.org/", "")
               .replace("http://doi.org/", "")
               .strip()
        )
        parts.append(f"DOI({clean_doi})")
    if publisher:
        parts.append(f'PUBLISHER("{publisher}")')

    # ── Rango de años ──────────────────────────────────────────────────
    if year_exact is not None:
        # Año exacto tiene precedencia sobre rango
        parts.append(f"PUBYEAR = {year_exact}")
    else:
        if year_from is not None:
            # PUBYEAR > year-1 equivale a PUBYEAR >= year (Scopus usa >/<, no >=/<= )
            parts.append(f"PUBYEAR > {year_from - 1}")
        if year_to is not None:
            parts.append(f"PUBYEAR < {year_to + 1}")

    # ── Clasificación ──────────────────────────────────────────────────
    if document_type:
        # Acepta nombre legible ('article') o código directo ('ar')
        dt_lower = document_type.lower().strip()
        code = DOCTYPE_CODES.get(dt_lower, dt_lower)
        parts.append(f"DOCTYPE({code})")
    if subject_area:
        parts.append(f"SUBJAREA({subject_area.upper()})")
    if language:
        parts.append(f"LANGUAGE({language})")
    if open_access is True:
        parts.append("OPENACCESS(1)")

    # ── Financiación ───────────────────────────────────────────────────
    if funder:
        parts.append(f'FUND-SPONSOR("{funder}")')
    if grant_number:
        parts.append(f'FUND-NO("{grant_number}")')

    # ── Cláusula libre ─────────────────────────────────────────────────
    if extra:
        parts.append(extra.strip())

    if not parts:
        raise ValueError(
            "build_advanced_query: debes especificar al menos un criterio de búsqueda."
        )

    sep = f" {operator.upper()} "
    return sep.join(parts)
