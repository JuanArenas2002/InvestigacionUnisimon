"""
Excepciones específicas del módulo CVLAC.

Centralizadas aquí para que las capas de aplicación e infraestructura
puedan lanzarlas sin acoplarse entre sí, y el código externo pueda
capturarlas con un import único.
"""


class CvlacScrapingError(Exception):
    """
    Se lanza cuando el scraping de CVLAC falla:
    - Error HTTP al acceder al perfil.
    - BeautifulSoup no está instalado.
    - El HTML del portal cambió y no se puede parsear.
    """
    pass
