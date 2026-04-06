"""
Excepciones específicas del módulo Scopus.

Centralizadas aquí para que las capas de aplicación e infraestructura
puedan lanzarlas sin acoplarse entre sí, y el código externo pueda
capturarlas con un import único.
"""


class ScopusAPIError(Exception):
    """
    Se lanza cuando la API de Scopus (Elsevier) devuelve un error HTTP
    no recuperable, cuando la API key no está configurada, o cuando
    el XML de respuesta no puede parsearse.
    """
    pass
