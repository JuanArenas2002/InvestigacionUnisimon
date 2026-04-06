"""
Servicio de búsqueda de Scopus: paginación, DOI lookup y búsqueda por autor ORCID.

Orquesta las llamadas HTTP a la Scopus Search API separando la lógica de
paginación y gestión de resultados del parseo XML (dominio) y de la
configuración de sesión (infraestructura).

La Scopus Search API usa paginación por offset:
  ?start=0&count=25   → primera página
  ?start=25&count=25  → segunda página
  etc.

El campo opensearch:totalResults indica el total disponible.
"""

import logging
import time
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple

import requests

from extractors.scopus._exceptions import ScopusAPIError
from extractors.scopus.domain.record_parser import NS, parse_xml_entry

logger = logging.getLogger(__name__)

# Campos a solicitar en la query de Scopus Search (reduce tamaño de respuesta)
SEARCH_FIELDS = (
    "dc:identifier,doi,dc:title,prism:publicationName,"
    "prism:coverDate,subtypeDescription,citedby-count,"
    "author,prism:issn,openaccess,openaccessFlag,"
    "dc:description,authkeywords,prism:volume,prism:issueIdentifier,"
    "prism:pageRange,afid,affiliation"
)


def _parse_xml_response(xml_content: str) -> Tuple[Optional[ET.Element], Optional[str]]:
    """
    Parsea el XML de respuesta de Scopus y devuelve la raíz o un error.

    Args:
        xml_content: Contenido XML de la respuesta HTTP.

    Returns:
        Tupla (root_element, error_msg). Si el parseo falla, root=None y
        error_msg contiene la descripción del error.
    """
    try:
        root = ET.fromstring(xml_content)
        return root, None
    except ET.ParseError as e:
        return None, f"XML inválido: {e}. Respuesta: {xml_content[:200]}"


def paginated_search(
    session: requests.Session,
    config,
    search_url: str,
    query: str,
    max_results: Optional[int] = None,
    start: int = 0,
) -> List[dict]:
    """
    Ejecuta una búsqueda paginada en Scopus Search API y devuelve
    los campos parseados de todos los registros encontrados.

    Itera páginas usando paginación por offset hasta agotar resultados
    o alcanzar max_results. Parsea el XML de cada página en el dominio.

    Args:
        session:     Sesión HTTP ya configurada con headers de Scopus.
        config:      Configuración de Scopus (max_per_page, timeout).
        search_url:  URL del endpoint de búsqueda de Scopus.
        query:       Query Scopus ya construida.
        max_results: Límite total de resultados. None = todos.
        start:       Offset inicial (para reanudar búsquedas interrumpidas).

    Returns:
        Lista de dicts de campos, uno por cada registro encontrado.
        Los dicts están listos para construir StandardRecord.

    Raises:
        ScopusAPIError: Si la API devuelve un error HTTP no recuperable.
    """
    all_fields: List[dict] = []
    total_fetched = 0
    current_start = start

    logger.info(f"[Scopus] Iniciando búsqueda: {query}")

    while True:
        params = {
            "query": query,
            "start": current_start,
            "count": config.max_per_page,
            "sort":  "pubyear",
            "field": SEARCH_FIELDS,
        }

        try:
            resp = session.get(search_url, params=params, timeout=config.timeout)
            resp.raise_for_status()
            xml_content = resp.text
        except requests.exceptions.RequestException as e:
            raise ScopusAPIError(f"Error en Scopus API (start={current_start}): {e}")

        # Parsear el XML de la respuesta
        root, parse_error = _parse_xml_response(xml_content)
        if parse_error:
            logger.warning(f"[Scopus] {parse_error}")
            break

        entries = root.findall("atom:entry", NS)
        if not entries:
            logger.info(f"[Scopus] Sin resultados en start={current_start}.")
            break

        # Parsear cada entrada usando el dominio
        for entry in entries:
            fields = parse_xml_entry(entry)
            if fields is None:
                # Entrada de error (ej: 'Result set was empty')
                break
            all_fields.append(fields)
            total_fetched += 1
            if max_results and total_fetched >= max_results:
                break

        logger.info(f"[Scopus] Start {current_start}: {len(entries)} entradas. Total: {total_fetched}")

        if max_results and total_fetched >= max_results:
            logger.info(f"[Scopus] Límite de {max_results} resultados alcanzado.")
            break

        # Verificar si hay más páginas
        total_el = root.find("opensearch:totalResults", NS)
        total_results = int(total_el.text) if total_el is not None and total_el.text else 0
        current_start += config.max_per_page
        if current_start >= total_results:
            logger.info(f"[Scopus] Todos los registros extraídos ({total_results} total).")
            break

        time.sleep(0.2)  # Respetar rate limit de Scopus

    return all_fields


def search_by_doi_json(
    session: requests.Session,
    config,
    search_url: str,
    doi: str,
) -> Optional[dict]:
    """
    Busca un documento individual en Scopus por su DOI.

    Usa Accept: application/json para simplificar el parseo en búsquedas
    individuales. Retorna None si el DOI no se encuentra.

    Args:
        session:    Sesión HTTP configurada con headers de Scopus.
        config:     Configuración de Scopus (timeout).
        search_url: URL del endpoint de búsqueda de Scopus.
        doi:        DOI a buscar (con o sin prefijo https://doi.org/).

    Returns:
        Dict de campos para StandardRecord si se encuentra, None si no.
    """
    # Normalizar el DOI: quitar prefijo URL
    clean_doi = doi.strip()
    for prefix in ("https://doi.org/", "http://doi.org/"):
        if clean_doi.startswith(prefix):
            clean_doi = clean_doi[len(prefix):]
            break

    query = f"DOI({clean_doi})"
    params = {
        "query": query,
        "count": 1,
        "field": SEARCH_FIELDS,
    }

    try:
        # Para búsqueda por DOI se usa JSON para simplificar el parseo
        resp = session.get(
            search_url,
            params=params,
            timeout=config.timeout,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        logger.warning(f"[Scopus] Error buscando DOI {clean_doi}: {e}")
        return None

    entries = data.get("search-results", {}).get("entry", [])
    if not entries or (len(entries) == 1 and "error" in entries[0]):
        return None

    from extractors.scopus.domain.record_parser import parse_json_entry
    try:
        return parse_json_entry(entries[0])
    except Exception as e:
        logger.warning(f"[Scopus] Error parseando resultado DOI {clean_doi}: {e}")
        return None


def search_dois_batch(
    session: requests.Session,
    config,
    search_url: str,
    dois: List[str],
    delay: float = 0.25,
) -> List[dict]:
    """
    Busca múltiples documentos en Scopus por DOI, uno a uno con delay.

    Args:
        session:    Sesión HTTP configurada con headers de Scopus.
        config:     Configuración de Scopus.
        search_url: URL del endpoint de búsqueda.
        dois:       Lista de DOIs a buscar.
        delay:      Pausa en segundos entre peticiones (respeta rate-limit).

    Returns:
        Lista de dicts de campos para los DOIs encontrados en Scopus.
    """
    results = []
    total = len(dois)

    for i, doi in enumerate(dois, 1):
        fields = search_by_doi_json(session, config, search_url, doi)
        if fields:
            results.append(fields)

        if i % 50 == 0:
            logger.info(
                f"[Scopus] Progreso DOI: {i}/{total} — encontrados: {len(results)}"
            )
        if delay and i < total:
            time.sleep(delay)

    logger.info(
        f"[Scopus] Búsqueda por DOI completada: "
        f"{len(results)} encontrados de {total} consultados."
    )
    return results
