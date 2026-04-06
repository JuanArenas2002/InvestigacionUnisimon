"""
Constructor de queries para Web of Science Starter API.

WoS usa su propio lenguaje de búsqueda basado en etiquetas de campo:
  OG=  → Organization-Enhanced (nombre oficial de la institución en WoS)
  PY=  → Publication Year (rango exacto o con operadores >= / <=)

La etiqueta OG= busca por el nombre "mejorado" de la organización,
que WoS normaliza internamente para cubrir variantes de nombre.

Referencia oficial:
  https://developer.clarivate.com/apis/wos-starter#field_tags
"""

from typing import Optional


def build_query(
    year_from: Optional[int],
    year_to: Optional[int],
    org_enhanced: Optional[str],
    institution_name: str,
) -> str:
    """
    Construye la query de búsqueda WoS combinando organización y rango de años.

    Lógica de construcción:
      - Si se provee org_enhanced, lo usa directamente; si no, usa institution_name.
      - El rango de años genera PY=(from-to), PY=(>=from), PY=(<=to) o se omite.
      - Las cláusulas se unen con AND.

    Args:
        year_from: Año inicial (inclusive). None = sin límite inferior.
        year_to:   Año final (inclusive). None = sin límite superior.
        org_enhanced: Nombre Organization-Enhanced de WoS. Si es None,
                      se usa institution_name como fallback.
        institution_name: Nombre de la institución desde la configuración global.

    Returns:
        String de query lista para el parámetro ?q=... de la API.

    Ejemplos de salida:
        'OG=(Universidad de Antioquia) AND PY=(2020-2025)'
        'OG=(Universidad de Antioquia) AND PY=(>=2020)'
        'OG=(Universidad de Antioquia)'
    """
    parts = []

    # Usar el nombre provisto o el nombre institucional por defecto
    org = org_enhanced or institution_name
    parts.append(f"OG=({org})")

    # Construir filtro de año según los parámetros disponibles
    if year_from and year_to:
        parts.append(f"PY=({year_from}-{year_to})")
    elif year_from:
        parts.append(f"PY=(>={year_from})")
    elif year_to:
        parts.append(f"PY=(<={year_to})")

    return " AND ".join(parts)
