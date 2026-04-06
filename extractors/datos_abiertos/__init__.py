"""
Paquete extractor de Datos Abiertos Colombia (datos.gov.co).

Portal oficial del gobierno colombiano de datos abiertos. Expone datasets
de producción científica de Minciencias a través de la SODA API (Socrata).

Estructura interna (arquitectura DDD):
  domain/         → lógica pura: query_builder (SoQL), record_parser (mapeo flexible)
  application/    → orquestación: dataset_service (paginación offset)
  infrastructure/ → I/O: http_client (sesión SODA con App Token)
  extractor.py    → implementación de BaseExtractor (glue delgado)

Punto de entrada público. Re-exporta los símbolos que el resto del
proyecto importa, manteniendo compatibilidad total con todos los
imports existentes:

    from extractors.datos_abiertos import DatosAbiertosExtractor
    from extractors.datos_abiertos import DatosAbiertosExtractor, DatosAbiertosError
"""

from extractors.datos_abiertos.extractor import DatosAbiertosExtractor
from extractors.datos_abiertos._exceptions import DatosAbiertosError

__all__ = ["DatosAbiertosExtractor", "DatosAbiertosError"]
