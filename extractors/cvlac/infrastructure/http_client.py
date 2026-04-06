"""
Cliente HTTP para el portal web de CVLAC (Minciencias Colombia).

Responsabilidad única: crear la sesión requests con:
  - User-Agent descriptivo (buena práctica para scraping respetuoso).
  - Política de reintentos con backoff exponencial.
  - Headers Accept para HTML.

CVLAC no tiene API REST pública. Las páginas se acceden como HTML
normal con requests + BeautifulSoup.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_session(config, institution_email: str) -> requests.Session:
    """
    Crea una sesión HTTP configurada para el scraping del portal CVLAC.

    Incluye:
    - User-Agent identificatorio con el email de contacto institucional.
      Esto es buena práctica para que Minciencias pueda contactar al
      operador si hay problemas con el scraper.
    - Reintentos automáticos con backoff exponencial (2s, 4s, 8s).
    - Montaje del adaptador tanto en https:// como en http://.

    Args:
        config: Configuración de CVLAC con atributos: max_retries.
        institution_email: Email de contacto de la institución para el User-Agent.

    Returns:
        Sesión requests lista para hacer scraping del portal CVLAC.
    """
    session = requests.Session()

    # backoff_factor=2 → esperas progresivas de 2s, 4s, 8s entre reintentos
    retry = Retry(
        total=config.max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        # User-Agent identificatorio: buena práctica para scraping responsable
        "User-Agent": (
            f"Mozilla/5.0 (compatible; BiblioReconciler/1.0; "
            f"+mailto:{institution_email})"
        ),
        "Accept": "text/html,application/xhtml+xml",
    })

    return session
