"""
Excepciones específicas del módulo Serial Title (Scopus).

Centralizadas aquí para que las capas de aplicación e infraestructura
puedan lanzarlas sin acoplarse entre sí, y el código externo pueda
capturarlas con un import único.
"""


class SerialTitleAPIError(Exception):
    """
    Se lanza cuando el Serial Title API de Scopus devuelve un error HTTP
    no recuperable, o cuando la SCOPUS_API_KEY no está configurada.
    """
    pass
