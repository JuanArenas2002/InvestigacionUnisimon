"""
Cliente HTTP para la SODA API de datos.gov.co (Socrata).

Responsabilidad única: crear la sesión requests con:
  - App Token de Socrata (opcional pero reduce throttling).
  - Política de reintentos automáticos.
  - Header Accept: application/json.

Sobre el App Token:
  Sin token, Socrata limita las peticiones a ~1000 por hora por IP.
  Con token (gratuito en datos.gov.co), el límite es significativamente mayor.
  Configurar DATOS_ABIERTOS_TOKEN en variables de entorno.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_session(config, app_token: str = None) -> requests.Session:
    """
    Crea una sesión HTTP configurada para la API SODA de datos.gov.co.

    Incluye:
    - Header X-App-Token si se provee app_token (aumenta cuota de Socrata).
    - Reintentos automáticos con backoff en errores de servidor (5xx) y 429.
    - Header Accept: application/json (SODA devuelve JSON por defecto).

    Args:
        config:    Configuración de Datos Abiertos. Se usa config.max_retries
                   si está disponible, o 3 por defecto.
        app_token: App Token de Socrata. None = sin autenticación (cuota reducida).

    Returns:
        Sesión requests lista para consultar datasets SODA.
    """
    session = requests.Session()

    max_retries = getattr(config, "max_retries", 3)

    retry = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    headers = {"Accept": "application/json"}
    if app_token:
        # El App Token reduce el throttling de Socrata significativamente
        headers["X-App-Token"] = app_token

    session.headers.update(headers)
    return session
