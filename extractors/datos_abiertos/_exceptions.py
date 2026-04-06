"""
Excepciones específicas del módulo Datos Abiertos Colombia.

Centralizadas aquí para que las capas de aplicación e infraestructura
puedan lanzarlas sin acoplarse entre sí, y el código externo pueda
capturarlas con un import único.
"""


class DatosAbiertosError(Exception):
    """
    Se lanza cuando la SODA API de datos.gov.co devuelve un error HTTP
    no recuperable, o cuando el dataset_id no está configurado.
    """
    pass
