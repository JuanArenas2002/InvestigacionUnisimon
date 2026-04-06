"""
Cliente HTTP para la API de Scopus (Elsevier Developer Portal).

Responsabilidad única: crear y configurar la sesión requests con:
  - Header X-ELS-APIKey: autenticación principal.
  - Header X-ELS-Insttoken: token institucional (opcional, aumenta cuota).
  - Header Accept: application/xml (Scopus Search devuelve XML por defecto).
  - Política de reintentos con backoff exponencial.

Sobre el Inst Token:
  Sin token institucional, la cuota es de ~20.000 peticiones/semana.
  Con token institucional (obtenido a través de la biblioteca universitaria),
  la cuota es significativamente mayor y se permite acceso a más campos.

Documentación:
  https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl
"""

import logging
import xml.etree.ElementTree as ET
from typing import Optional, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def create_session(config, api_key: str, inst_token: Optional[str] = None) -> requests.Session:
    """
    Crea una sesión HTTP configurada para la API de Scopus (Elsevier).

    Incluye:
    - Header X-ELS-APIKey con la API key de Elsevier.
    - Header X-ELS-Insttoken si se provee (mayor cuota institucional).
    - Reintentos automáticos con backoff en 429, 500-504.
    - Accept: application/xml (formato por defecto de Scopus Search).

    Args:
        config:     Configuración de Scopus con atributos: max_retries.
        api_key:    API key de Elsevier Developer Portal.
        inst_token: Token institucional de Elsevier (opcional).

    Returns:
        Sesión requests lista para llamadas a la API de Scopus.
    """
    session = requests.Session()

    # backoff_factor=1 → esperas de 1s, 2s, 4s entre reintentos
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
        "X-ELS-APIKey": api_key,
        "Accept":       "application/xml",
    })

    # El Inst Token aumenta la cuota de uso de la API
    if inst_token:
        session.headers["X-ELS-Insttoken"] = inst_token

    return session


def get_author_by_orcid(
    session: requests.Session,
    orcid: str,
) -> Optional[Dict[str, str]]:
    """
    Busca un autor en Scopus Author Search API por su ORCID y retorna su AU-ID.

    NOTA: El Author Search API tiene restricciones de acceso. No todas las
    API keys tienen acceso. Si retorna 400, el método devuelve None
    sin lanzar excepción (fallo silencioso para continuar el flujo).

    Args:
        session: Sesión HTTP ya configurada con headers de Scopus.
        orcid:   ORCID del autor (ej: '0000-0002-2096-7900').

    Returns:
        Dict con claves {'scopus_id': '...', 'name': '...'} si se encuentra,
        None si no se encuentra o el API key no tiene acceso.
    """
    url = "https://api.elsevier.com/content/search/author"
    params = {"query": f"ORCID({orcid})"}

    logger.info(f"[Scopus] Buscando autor por ORCID {orcid}")

    try:
        resp = session.get(url, params=params, timeout=10)

        if resp.status_code == 400:
            # 400 típicamente indica que la API key no tiene acceso al Author Search
            logger.warning(
                "[Scopus] Author Search API retornó 400. "
                "La API key probablemente no tiene acceso al Author Search."
            )
            return None

        resp.raise_for_status()

        # Parsear XML de la respuesta del Author Search
        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "dc":   "http://purl.org/dc/elements/1.1/",
        }
        root = ET.fromstring(resp.content)
        entries = root.findall("atom:entry", ns)

        if not entries:
            logger.warning(f"[Scopus] No se encontró autor con ORCID {orcid}.")
            return None

        entry = entries[0]
        au_id = entry.findtext("dc:identifier", namespaces=ns)
        if au_id and au_id.startswith("AUTHOR_ID:"):
            au_id = au_id.replace("AUTHOR_ID:", "").strip()

        name = entry.findtext("dc:title", namespaces=ns)

        if au_id:
            logger.info(f"[Scopus] Autor encontrado: {name} (AU-ID: {au_id})")
            return {"scopus_id": au_id, "name": name}

        logger.warning(f"[Scopus] No se extrajo AU-ID para ORCID {orcid}.")
        return None

    except requests.exceptions.HTTPError as e:
        logger.warning(f"[Scopus] Error HTTP en Author Search: {e}")
        return None
    except Exception as e:
        logger.warning(f"[Scopus] Error inesperado en Author Search: {e}")
        return None
