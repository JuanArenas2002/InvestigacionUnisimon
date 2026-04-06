"""
Excepciones específicas del módulo Web of Science.

Centralizadas aquí para que cualquier capa (aplicación, infraestructura,
extractor) pueda lanzarlas y el código externo pueda capturarlas sin
necesidad de importar detalles internos de la implementación.
"""


class WosAPIError(Exception):
    """
    Se lanza cuando la API de Web of Science devuelve un error HTTP
    no recuperable o cuando la API key no está configurada.
    """
    pass
