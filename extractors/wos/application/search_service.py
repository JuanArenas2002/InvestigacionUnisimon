"""
Servicio de búsqueda de Web of Science: paginación y orquestación HTTP.

Separa la lógica de paginación (cuántas páginas pedir, cuándo parar)
del parseo puro de registros (dominio) y de la configuración de sesión
(infraestructura).

La WoS Starter API usa paginación por página (page=1, 2, 3...) con
un límite configurable por página. El campo metadata.total indica
el número total de resultados disponibles.
"""

import logging
import time
from typing import List, Optional

import requests

from extractors.wos._exceptions import WosAPIError

logger = logging.getLogger(__name__)


def paginated_search(
    session: requests.Session,
    config,
    query: str,
    max_results: Optional[int] = None,
) -> List[dict]:
    """
    Ejecuta la búsqueda paginada en la WoS Starter API y devuelve todos los
    hits crudos. Itera páginas hasta agotar resultados o alcanzar max_results.

    Flujo:
      1. Hace GET /documents con la query y el número de página.
      2. Extrae los hits de la respuesta.
      3. Verifica metadata.total para saber si hay más páginas.
      4. Repite hasta no haber más resultados o alcanzar el límite.

    Args:
        session: Sesión HTTP ya configurada con headers y reintentos.
        config: Configuración de WoS (base_url, max_per_page, timeout).
        query: Query WoS ya construida (ej: 'OG=(...) AND PY=(...)').
        max_results: Límite total de resultados a devolver. None = todos.

    Returns:
        Lista de dicts crudos tal como los devuelve la API (campo 'hits').

    Raises:
        WosAPIError: Si la API devuelve un error HTTP no recuperable.
    """
    all_hits: List[dict] = []
    page = 1
    total_fetched = 0

    logger.info(f"[WoS] Iniciando búsqueda paginada. Query: {query}")

    while True:
        params = {
            "q":         query,
            "limit":     config.max_per_page,
            "page":      page,
            "sortField": "PY",    # Ordenar por año de publicación
        }

        try:
            resp = session.get(
                f"{config.base_url}/documents",
                params=params,
                timeout=config.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            raise WosAPIError(f"Error en WoS API (página {page}): {e}")

        hits = data.get("hits", [])
        if not hits:
            # Sin resultados en esta página → fin de la paginación
            logger.info(f"[WoS] Sin resultados en página {page}. Terminando.")
            break

        all_hits.extend(hits)
        total_fetched += len(hits)
        logger.info(
            f"[WoS] Página {page}: {len(hits)} hits. "
            f"Acumulado: {total_fetched}"
        )

        # Verificar si se alcanzó el límite pedido por el caller
        if max_results and total_fetched >= max_results:
            all_hits = all_hits[:max_results]
            logger.info(f"[WoS] Límite de {max_results} resultados alcanzado.")
            break

        # Verificar si hay más páginas según el total declarado por la API
        metadata = data.get("metadata", {}) or {}
        total_records = metadata.get("total", 0)
        if page * config.max_per_page >= total_records:
            logger.info(
                f"[WoS] Todos los registros extraídos "
                f"({total_fetched}/{total_records})."
            )
            break

        page += 1
        time.sleep(0.5)  # WoS es más estricto con rate limit que otras APIs

    return all_hits
