"""
Constructor de cláusulas SoQL para la SODA API de datos.gov.co.

SODA (Socrata Open Data API) usa un subconjunto de SQL llamado SoQL
para filtrar datasets. Los parámetros se pasan como query strings:
  $where=<condición SQL>
  $limit=<n>
  $offset=<n>

NOTA: Los nombres de columna varían entre datasets. Esta implementación
usa los nombres de columna más comunes en datasets de Minciencias, pero
puede necesitar ajuste para datasets específicos.

Referencia SoQL:
  https://dev.socrata.com/docs/queries/
"""

from typing import Optional


def build_where(
    year_from: Optional[int],
    year_to: Optional[int],
    institution_filter: Optional[str],
) -> str:
    """
    Construye la cláusula WHERE en SoQL para filtrar publicaciones.

    Combina condiciones de año e institución con AND. Cada condición
    es opcional — si no se provee ninguna, retorna string vacío (sin filtro).

    NOTA: El campo de año en el $where usa 'ano' (nombre más común en
    datasets de Minciencias), pero puede diferir según el dataset.
    Para datasets con nombres de columna distintos, se debe adaptar
    esta función o usar el parámetro extra.

    Args:
        year_from: Año mínimo de publicación (inclusive). None = sin límite.
        year_to:   Año máximo de publicación (inclusive). None = sin límite.
        institution_filter: Nombre parcial de institución para filtrar.
                            Se aplica LIKE case-insensitive. None = sin filtro.

    Returns:
        Cláusula SoQL lista para el parámetro $where, o '' si sin filtros.

    Ejemplos de salida:
        "ano >= '2020' AND ano <= '2025'"
        "upper(institucion) like upper('%Universidad de Antioquia%')"
        "ano >= '2020' AND upper(institucion) like upper('%Antioquia%')"
        ""  (sin filtros)
    """
    conditions = []

    # Filtro de año inferior
    if year_from is not None:
        conditions.append(f"ano >= '{year_from}'")

    # Filtro de año superior
    if year_to is not None:
        conditions.append(f"ano <= '{year_to}'")

    # Filtro de institución: LIKE case-insensitive con UPPER()
    if institution_filter:
        safe_filter = institution_filter.replace("'", "''")  # Escapar comillas simples
        conditions.append(
            f"upper(institucion) like upper('%{safe_filter}%')"
        )

    return " AND ".join(conditions) if conditions else ""
