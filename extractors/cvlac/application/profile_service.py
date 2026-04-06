"""
Servicio de extracción de perfiles CVLAC.

Orquesta el flujo completo de extracción de un perfil individual:
  1. Descarga el HTML del perfil (infraestructura).
  2. Parsea cada sección de producción bibliográfica (dominio).
  3. Convierte los registros crudos a campos de StandardRecord (dominio).

Separa la responsabilidad de "qué secciones parsear y cómo integrarlas"
del "cómo hacer la petición HTTP" y del "cómo parsear el HTML de cada fila".
"""

import logging
from typing import List, Optional

from extractors.cvlac._exceptions import CvlacScrapingError
from extractors.cvlac.domain.html_parser import SECTIONS_TO_PARSE, parse_section
from extractors.cvlac.domain import record_parser

logger = logging.getLogger(__name__)

# URL base del visualizador de perfiles CVLAC
PROFILE_URL = "/visualizador/generarCurriculoCv.do"


def scrape_profile(
    session,
    config,
    base_url: str,
    cvlac_code: str,
    source_name: str,
    year_from: Optional[int],
    year_to: Optional[int],
) -> List[dict]:
    """
    Descarga y parsea el perfil completo de un investigador en CVLAC.

    Itera todas las secciones de producción bibliográfica definidas en
    SECTIONS_TO_PARSE (artículos, libros, capítulos, etc.) y acumula
    los registros que pasen el filtro de años.

    Args:
        session: Sesión HTTP ya configurada (de infraestructura.http_client).
        config: Configuración de CVLAC con atributos: timeout.
        base_url: URL base del portal CVLAC (ej: 'https://scienti.minciencias.gov.co').
        cvlac_code: Código numérico del investigador en CVLAC.
        source_name: Nombre canónico de la fuente para el StandardRecord.
        year_from: Año inicial del filtro (None = sin límite).
        year_to:   Año final del filtro (None = sin límite).

    Returns:
        Lista de dicts de campos para StandardRecord, uno por producto encontrado.

    Raises:
        CvlacScrapingError: Si el HTTP falla o la página no es accesible.
    """
    # Importar BeautifulSoup aquí para que el error de importación se
    # propague correctamente y sea manejado por el extractor
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        raise CvlacScrapingError(
            "beautifulsoup4 es requerido. Instálalo con: pip install beautifulsoup4 lxml"
        )

    url = f"{base_url}{PROFILE_URL}?cod_rh={cvlac_code}"

    try:
        resp = session.get(url, timeout=config.timeout)
        resp.raise_for_status()
    except Exception as e:
        raise CvlacScrapingError(f"Error al acceder a CVLAC {cvlac_code}: {e}")

    soup = BeautifulSoup(resp.text, "lxml")
    all_fields = []

    # Parsear cada sección de producción bibliográfica
    for section_title, pub_type in SECTIONS_TO_PARSE:
        raw_records = parse_section(
            soup=soup,
            section_title=section_title,
            pub_type=pub_type,
            cvlac_code=cvlac_code,
            year_from=year_from,
            year_to=year_to,
        )
        for raw in raw_records:
            # Convertir dict crudo a campos de StandardRecord
            fields = record_parser.parse_raw(raw)
            all_fields.append(fields)

    logger.info(
        f"[CVLAC] Perfil {cvlac_code}: "
        f"{len(all_fields)} productos extraídos."
    )
    return all_fields
