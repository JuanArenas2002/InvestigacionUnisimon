"""
Servicio de extracción de perfiles Google Scholar.

Usa la librería `scholarly` para descargar publicaciones de un perfil dado.
`scholarly` hace scraping del portal público de Google Scholar.

Nota: Google Scholar aplica rate-limiting y bloqueos agresivos. Se recomienda
usar un proxy o `scholarly.use_proxy()` en entornos de producción.
"""

import logging
import time
from typing import List, Optional

from extractors.google_scholar._exceptions import GoogleScholarError
from extractors.google_scholar.domain import record_parser

logger = logging.getLogger(__name__)

_DELAY_BETWEEN_PROFILES = 3.0


def fetch_profile_publications(
    scholar_id: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    max_results: Optional[int] = None,
) -> List[dict]:
    """
    Descarga y parsea las publicaciones de un perfil Google Scholar.

    Args:
        scholar_id:  ID del perfil (parámetro `user` de la URL).
                     Ej: "Ozm565YAAAAJ"
        year_from:   Año mínimo de publicación (inclusive). None = sin límite.
        year_to:     Año máximo de publicación (inclusive). None = sin límite.
        max_results: Límite de publicaciones. None = todas.

    Returns:
        Lista de dicts con campos para StandardRecord.

    Raises:
        GoogleScholarError: Si `scholarly` no está instalado o falla la consulta.
    """
    try:
        from scholarly import scholarly as scholar_api
    except ImportError:
        raise GoogleScholarError(
            "La librería `scholarly` es requerida. "
            "Instálala con: pip install scholarly"
        )

    try:
        logger.info("Iniciando búsqueda de perfil: %s", scholar_id)
        author = scholar_api.search_author_id(scholar_id)
        author = scholar_api.fill(author, sections=["publications"])
    except Exception as e:
        logger.error("Error en extracción del perfil %s: %s", scholar_id, e)
        raise GoogleScholarError(
            f"Error al consultar perfil Google Scholar '{scholar_id}': {e}"
        )

    profile_name = author.get("name", "")
    raw_publications = author.get("publications", [])
    logger.debug("Perfil %s: %d publicaciones encontradas", scholar_id, len(raw_publications))

    fields_list = []
    for pub in raw_publications:
        year = pub.get("bib", {}).get("pub_year") or pub.get("bib", {}).get("year")
        if year:
            try:
                year_int = int(year)
                if year_from and year_int < year_from:
                    continue
                if year_to and year_int > year_to:
                    continue
            except (ValueError, TypeError):
                pass

        fields = record_parser.parse_publication(pub, scholar_id, profile_name)
        fields_list.append(fields)

        if max_results and len(fields_list) >= max_results:
            break

    logger.info(
        "[GoogleScholar] Perfil %s (%s): %d publicaciones extraídas.",
        scholar_id, profile_name, len(fields_list),
    )
    time.sleep(_DELAY_BETWEEN_PROFILES)
    return fields_list
