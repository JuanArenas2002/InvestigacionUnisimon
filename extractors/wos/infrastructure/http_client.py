"""
Cliente HTTP para la API de Web of Science (Clarivate).

Responsabilidad única: crear y configurar la sesión requests con:
  - Header de autenticación X-ApiKey.
  - Política de reintentos automáticos con backoff exponencial.
  - Montaje del adaptador en https:// y http://.

No contiene lógica de negocio ni parseo de datos.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_session(api_key: str, config) -> requests.Session:
    """
    Crea y devuelve una sesión HTTP configurada para la API de WoS.

    La sesión incluye:
    - Header X-ApiKey para autenticación.
    - Header Accept: application/json (WoS Starter usa JSON).
    - Reintentos automáticos en errores de servidor (5xx) y rate limit (429),
      con backoff exponencial para reducir presión ante fallos consecutivos.

    Args:
        api_key: API key de Clarivate Developer Portal.
        config: Objeto de configuración de WoS. Se usa config.max_retries.

    Returns:
        Sesión requests lista para realizar llamadas a la API.
    """
    session = requests.Session()

    # Política de reintentos: backoff_factor=1 → esperas de 1s, 2s, 4s...
    retry = Retry(
        total=config.max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "X-ApiKey": api_key,
        "Accept":   "application/json",
    })

    return session
