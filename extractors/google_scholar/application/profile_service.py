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

# Delay entre perfiles para evitar bloqueos de Google
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
        print(f"[DEBUG] Iniciando búsqueda de perfil: {scholar_id}")
        logger.info(f"[DEBUG] Iniciando búsqueda de perfil: {scholar_id}")
        
        print(f"[DEBUG] Llamando a search_author_id({scholar_id})...")
        author = scholar_api.search_author_id(scholar_id)
        print(f"[DEBUG] search_author_id completado. Author: {author.get('name', 'SIN NOMBRE')}")
        
        print(f"[DEBUG] Llamando a fill(author, sections=['publications'])...")
        author = scholar_api.fill(author, sections=["publications"])
        print(f"[DEBUG] fill completado. Publicaciones encontradas: {len(author.get('publications', []))}")
    except Exception as e:
        logger.error(f"[DEBUG] Error en extracción: {str(e)}")
        print(f"[DEBUG] ERROR: {str(e)}")
        raise GoogleScholarError(
            f"Error al consultar perfil Google Scholar '{scholar_id}': {e}"
        )

    profile_name = author.get("name", "")
    raw_publications = author.get("publications", [])

    print(f"[DEBUG] Nombre del perfil: {profile_name}")
    print(f"[DEBUG] Total de publicaciones a procesar: {len(raw_publications)}")
    
    fields_list = []
    for idx, pub in enumerate(raw_publications):
        print(f"[DEBUG] Procesando publicación {idx + 1}/{len(raw_publications)}...")
        
        # Aplicar filtro de año si está disponible en el bib
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

        print(f"[DEBUG] Parseando publicación {idx + 1}...")
        fields = record_parser.parse_publication(pub, scholar_id, profile_name)
        fields_list.append(fields)
        print(f"[DEBUG] Publicación {idx + 1} parseada exitosamente. Total acumulado: {len(fields_list)}")

        if max_results and len(fields_list) >= max_results:
            break

    print(f"[DEBUG] Procesamiento completado. Total extraídas: {len(fields_list)}")
    logger.info(
        f"[GoogleScholar] Perfil {scholar_id} ({profile_name}): "
        f"{len(fields_list)} publicaciones extraídas."
    )
    time.sleep(_DELAY_BETWEEN_PROFILES)
    return fields_list
