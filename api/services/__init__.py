"""
Servicios de aplicación — módulo para lógica reutilizable.
"""

from .chart_generator import (
    generate_investigator_chart_file,
    extract_publications_by_year,
    make_investigator_chart,
    configure_matplotlib_styles,
    CHART_COLORS,
)

__all__ = [
    'generate_investigator_chart_file',
    'extract_publications_by_year',
    'make_investigator_chart',
    'configure_matplotlib_styles',
    'CHART_COLORS',
]
