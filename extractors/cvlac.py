"""
Extractor de CVLAC / GrupLAC (Minciencias Colombia).

CVLAC (Currículum Vitae de Latinoamérica y el Caribe) es el sistema
de Minciencias para registrar la producción de investigadores colombianos.

NOTA: CVLAC no tiene una API REST pública oficial. Este extractor
usa web scraping de las páginas públicas de los perfiles.
Se recomienda usar delays entre requests para no sobrecargar el servidor.
"""

import logging
import re
import time
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

from config import cvlac_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord

logger = logging.getLogger(__name__)


class CvlacScrapingError(Exception):
    """Excepción para errores de scraping de CVLAC"""
    pass


class CvlacExtractor(BaseExtractor):
    """
    Extractor de producción científica desde perfiles CVLAC.

    Requiere:
      - beautifulsoup4 y lxml instalados
      - Lista de códigos CVLAC de los investigadores a consultar

    Uso:
        extractor = CvlacExtractor()
        records = extractor.extract(cvlac_codes=["0000123456", "0000789012"])
    """

    source_name = SourceName.CVLAC

    PROFILE_URL = f"{cvlac_config.base_url}/visualizador/generarCurriculoCv.do"

    def __init__(self):
        self.config = cvlac_config
        self.session = self._create_session()

        if not BS4_AVAILABLE:
            logger.warning(
                "beautifulsoup4 no instalado. "
                "Ejecuta: pip install beautifulsoup4 lxml"
            )

        logger.info("CvlacExtractor inicializado.")

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (compatible; BiblioReconciler/1.0; "
                f"+mailto:{institution.contact_email})"
            ),
            "Accept": "text/html,application/xhtml+xml",
        })
        return session

    # ---------------------------------------------------------
    # INTERFAZ BaseExtractor
    # ---------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        cvlac_codes: Optional[List[str]] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae producción de perfiles CVLAC.

        Args:
            year_from: Año inicial para filtrar
            year_to: Año final para filtrar
            max_results: Límite de resultados
            cvlac_codes: Lista de códigos CVLAC a consultar (REQUERIDO)
        """
        if not BS4_AVAILABLE:
            raise CvlacScrapingError(
                "beautifulsoup4 es requerido. Instálalo con: pip install beautifulsoup4 lxml"
            )

        if not cvlac_codes:
            raise ValueError(
                "Debes proporcionar una lista de códigos CVLAC. "
                "Ejemplo: cvlac_codes=['0000123456']"
            )

        records: List[StandardRecord] = []
        total_fetched = 0

        for idx, code in enumerate(cvlac_codes):
            logger.info(f"  Consultando CVLAC {code} ({idx+1}/{len(cvlac_codes)})")

            try:
                profile_records = self._scrape_profile(code, year_from, year_to)
                records.extend(profile_records)
                total_fetched += len(profile_records)

                if max_results and total_fetched >= max_results:
                    records = records[:max_results]
                    break

            except Exception as e:
                logger.warning(f"Error con CVLAC {code}: {e}")
                continue

            # Delay entre requests
            time.sleep(self.config.delay_between_requests)

        return self._post_process(records)

    def _parse_record(self, raw: dict) -> StandardRecord:
        """Convierte un producto CVLAC parseado a StandardRecord"""
        return StandardRecord(
            source_name=self.source_name,
            source_id=raw.get("cvlac_product_id"),
            doi=raw.get("doi"),
            title=raw.get("title"),
            publication_year=raw.get("year"),
            publication_type=raw.get("type", "article"),
            source_journal=raw.get("journal"),
            issn=raw.get("issn"),
            authors=[
                {"name": name, "orcid": None, "is_institutional": True}
                for name in raw.get("authors", [])
            ],
            institutional_authors=[
                {"name": name, "orcid": None, "is_institutional": True}
                for name in raw.get("authors", [])
            ],
            raw_data=raw,
        )

    # ---------------------------------------------------------
    # SCRAPING
    # ---------------------------------------------------------

    def _scrape_profile(
        self,
        cvlac_code: str,
        year_from: Optional[int],
        year_to: Optional[int],
    ) -> List[StandardRecord]:
        """
        Scrapea un perfil CVLAC individual.

        La página CVLAC tiene secciones:
          - Artículos publicados
          - Libros publicados
          - Capítulos de libro
          etc.

        Cada sección tiene una tabla con los productos.
        """
        url = f"{self.PROFILE_URL}?cod_rh={cvlac_code}"

        try:
            resp = self.session.get(url, timeout=self.config.timeout)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise CvlacScrapingError(f"Error al acceder a CVLAC {cvlac_code}: {e}")

        soup = BeautifulSoup(resp.text, "lxml")
        records = []

        # Buscar secciones de producción bibliográfica
        # Las secciones están identificadas por headers específicos
        sections_to_parse = [
            ("Artículos publicados", "article"),
            ("Libros publicados", "book"),
            ("Capítulos de libro publicados", "book-chapter"),
            ("Documentos de trabajo", "working-paper"),
            ("Otra producción bibliográfica", "other"),
        ]

        for section_title, pub_type in sections_to_parse:
            section_records = self._parse_section(
                soup, section_title, pub_type, cvlac_code, year_from, year_to
            )
            records.extend(section_records)

        return records

    def _parse_section(
        self,
        soup: BeautifulSoup,
        section_title: str,
        pub_type: str,
        cvlac_code: str,
        year_from: Optional[int],
        year_to: Optional[int],
    ) -> List[StandardRecord]:
        """
        Parsea una sección específica de la página CVLAC.

        NOTA: La estructura HTML de CVLAC puede cambiar.
        Esta implementación se basa en la estructura conocida a 2025.
        Puede necesitar actualizaciones si Minciencias modifica el portal.
        """
        records = []

        # Buscar el encabezado de la sección
        header = soup.find(string=re.compile(section_title, re.IGNORECASE))
        if not header:
            return records

        # La tabla de productos suele estar después del header
        # La estructura exacta depende de la versión del portal
        table = header.find_next("table")
        if not table:
            return records

        rows = table.find_all("tr")
        for row in rows:
            try:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                # Extraer datos básicos (estructura típica de CVLAC)
                raw = self._extract_row_data(cells, pub_type, cvlac_code)
                if not raw or not raw.get("title"):
                    continue

                # Filtrar por año
                year = raw.get("year")
                if year_from and year and year < year_from:
                    continue
                if year_to and year and year > year_to:
                    continue

                record = self._parse_record(raw)
                records.append(record)

            except Exception as e:
                logger.debug(f"Error parseando fila CVLAC: {e}")
                continue

        return records

    def _extract_row_data(
        self, cells, pub_type: str, cvlac_code: str
    ) -> Optional[dict]:
        """
        Extrae datos de una fila de la tabla de productos.

        La estructura varía pero típicamente contiene:
          - Título del producto
          - Revista/Editorial
          - Año
          - ISSN (para artículos)
          - Autores
        """
        text_content = [cell.get_text(strip=True) for cell in cells]

        # Heurísticas para extraer datos
        # (la estructura exacta depende de la sección)
        title = text_content[0] if text_content else None

        # Buscar año (4 dígitos entre 1900 y 2099)
        year = None
        for text in text_content:
            year_match = re.search(r'(19|20)\d{2}', text)
            if year_match:
                year = int(year_match.group())
                break

        # Buscar ISSN
        issn = None
        for text in text_content:
            issn_match = re.search(r'\d{4}-\d{3}[\dxX]', text)
            if issn_match:
                issn = issn_match.group()
                break

        # Buscar DOI
        doi = None
        for cell in cells:
            link = cell.find("a", href=re.compile(r"doi\.org"))
            if link:
                doi = link.get("href", "")
                break

        return {
            "cvlac_product_id": f"cvlac_{cvlac_code}_{hash(title) if title else 'unknown'}",
            "title": title,
            "year": year,
            "type": pub_type,
            "issn": issn,
            "doi": doi,
            "journal": text_content[1] if len(text_content) > 1 else None,
            "authors": [],  # CVLAC muestra el perfil de un autor
        }
