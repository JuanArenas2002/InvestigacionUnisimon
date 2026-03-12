"""
Paquete `api.exporters.excel`.

Re-exporta las funciones públicas de generación y lectura de archivos Excel
para su uso desde los routers de FastAPI.
"""
from .journal_coverage import generate_journal_coverage_excel
from .reader import read_issns_from_excel, read_publications_from_excel
from .publications_coverage import (
    generate_publications_coverage_excel,
    get_column_letter_offset,
)

__all__ = [
    "generate_journal_coverage_excel",
    "read_issns_from_excel",
    "read_publications_from_excel",
    "generate_publications_coverage_excel",
    "get_column_letter_offset",
]
