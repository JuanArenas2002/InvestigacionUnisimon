from project.infrastructure.sources.cvlac_adapter import CvlacAdapter
from project.infrastructure.sources.datos_abiertos_adapter import DatosAbiertosAdapter
from project.infrastructure.sources.openalex_adapter import OpenAlexAdapter
from project.infrastructure.sources.scopus_adapter import ScopusAdapter
from project.infrastructure.sources.wos_adapter import WosAdapter

__all__ = [
    "OpenAlexAdapter",
    "ScopusAdapter",
    "WosAdapter",
    "CvlacAdapter",
    "DatosAbiertosAdapter",
]
