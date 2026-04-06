"""
Servicio de extracción paginada de datasets SODA (Socrata Open Data API).

La SODA API usa paginación por offset ($offset) y límite ($limit).
Se itera hasta que la respuesta devuelva menos registros que el límite
(señal de que se llegó al final del dataset) o se alcance max_results.

Referencia de paginación SODA:
  https://dev.socrata.com/docs/paging.html
"""

import logging
import time
from typing import List, Optional

import requests

from extractors.datos_abiertos._exceptions import DatosAbiertosError

logger = logging.getLogger(__name__)


def paginated_fetch(
    session: requests.Session,
    config,
    base_url: str,
    where_clause: str,
    max_results: Optional[int] = None,
) -> List[dict]:
    """
    Extrae todos los registros de un dataset SODA con paginación por offset.

    Flujo:
      1. Construye los params con $limit, $offset y $where (si hay filtro).
      2. Hace GET al endpoint del dataset.
      3. Acumula los resultados.
      4. Para cuando la respuesta trae menos de $limit registros o
         se alcanza max_results.

    Args:
        session:      Sesión HTTP ya configurada con headers y reintentos.
        config:       Configuración de Datos Abiertos (max_per_page, timeout).
        base_url:     URL completa del dataset (ej: 'https://datos.gov.co/resource/abc1.json').
        where_clause: Cláusula SoQL $where ya construida, o '' si sin filtros.
        max_results:  Límite total de registros a devolver. None = todos.

    Returns:
        Lista de dicts crudos tal como los devuelve la API SODA.

    Raises:
        DatosAbiertosError: Si la API devuelve un error HTTP no recuperable.
    """
    all_records: List[dict] = []
    offset = 0
    total_fetched = 0

    # El límite por página no puede exceder max_results si se especificó
    page_size = config.max_per_page
    if max_results:
        page_size = min(page_size, max_results)

    logger.info(f"[DatosAbiertos] Iniciando extracción paginada desde: {base_url}")

    while True:
        params = {
            "$limit":  page_size,
            "$offset": offset,
            "$order":  ":id",    # Orden estable para paginación reproducible
        }
        # Solo agregar $where si hay filtro (evita error en datasets sin esa columna)
        if where_clause:
            params["$where"] = where_clause

        try:
            resp = session.get(base_url, params=params, timeout=config.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            raise DatosAbiertosError(f"Error en Datos Abiertos API (offset {offset}): {e}")

        if not data:
            # Dataset agotado
            logger.info(f"[DatosAbiertos] Sin más datos en offset {offset}.")
            break

        all_records.extend(data)
        total_fetched += len(data)
        logger.info(
            f"[DatosAbiertos] Offset {offset}: {len(data)} registros. "
            f"Total acumulado: {total_fetched}"
        )

        # Verificar si se alcanzó el límite pedido
        if max_results and total_fetched >= max_results:
            all_records = all_records[:max_results]
            logger.info(f"[DatosAbiertos] Límite de {max_results} registros alcanzado.")
            break

        # Si la página vino incompleta → se llegó al final del dataset
        if len(data) < page_size:
            logger.info(f"[DatosAbiertos] Última página ({len(data)} < {page_size}). Fin.")
            break

        offset += page_size
        time.sleep(0.1)  # Pausa cortés entre páginas

    return all_records
