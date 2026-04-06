"""
Orquestador principal del extractor CVLAC (Minciencias Colombia).

CVLAC (Currículum Vitae de Latinoamérica y el Caribe) es el sistema de
Minciencias para registrar la producción científica de investigadores colombianos.

Este módulo implementa la interfaz BaseExtractor usando arquitectura DDD:
  infrastructure.http_client  → sesión HTTP con User-Agent apropiado
  application.profile_service → descarga y parsea cada perfil individual
  domain.html_parser          → extrae datos del HTML por sección
  domain.record_parser        → transforma dicts crudos a campos de StandardRecord

NOTA: CVLAC no tiene API REST pública. Este extractor usa web scraping
del portal público. La estructura HTML puede cambiar si Minciencias
actualiza el portal — en ese caso, actualizar domain/html_parser.py.

Portal: https://scienti.minciencias.gov.co/cvlac/visualizador/
"""

import logging
import time
from typing import List, Optional

from config import cvlac_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord
from extractors.cvlac._exceptions import CvlacScrapingError
from extractors.cvlac.application import profile_service
from extractors.cvlac.infrastructure import http_client

logger = logging.getLogger(__name__)


class CvlacExtractor(BaseExtractor):
    """
    Extractor de producción científica desde perfiles CVLAC.

    Itera una lista de códigos CVLAC, descarga cada perfil y extrae
    los productos bibliográficos (artículos, libros, capítulos, etc.).

    Requiere:
      - beautifulsoup4 y lxml instalados:
        pip install beautifulsoup4 lxml

    Uso típico:
        extractor = CvlacExtractor()
        records = extractor.extract(
            cvlac_codes=["0000123456", "0000789012"],
            year_from=2020,
            year_to=2025,
        )
    """

    source_name = SourceName.CVLAC

    def __init__(self):
        """
        Inicializa el extractor y crea la sesión HTTP con User-Agent institucional.
        """
        self.config = cvlac_config
        # La sesión se crea en infraestructura con el email de contacto institucional
        self.session = http_client.create_session(
            config=self.config,
            institution_email=institution.contact_email,
        )
        logger.info("CvlacExtractor inicializado.")

    # ------------------------------------------------------------------
    # Interfaz BaseExtractor
    # ------------------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        cvlac_codes: Optional[List[str]] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae producción bibliográfica de una lista de perfiles CVLAC.

        Flujo por cada código:
          1. Descarga la página HTML del perfil (aplicación + infraestructura).
          2. Parsea cada sección bibliográfica (dominio).
          3. Convierte a StandardRecord (dominio + base).
          4. Aplica delay entre requests para no sobrecargar el servidor.

        Args:
            year_from:    Año inicial del filtro (inclusive). None = sin límite.
            year_to:      Año final del filtro (inclusive). None = sin límite.
            max_results:  Límite total de registros. None = todos.
            cvlac_codes:  Lista de códigos CVLAC a consultar (REQUERIDO).
                          Ejemplo: ['0000123456', '0000789012']

        Returns:
            Lista de StandardRecord normalizados y post-procesados.

        Raises:
            CvlacScrapingError: Si beautifulsoup4 no está instalado.
            ValueError: Si no se proveen cvlac_codes.
        """
        if not cvlac_codes:
            raise ValueError(
                "Debes proporcionar una lista de códigos CVLAC. "
                "Ejemplo: cvlac_codes=['0000123456']"
            )

        records: List[StandardRecord] = []
        total_fetched = 0

        for idx, code in enumerate(cvlac_codes):
            logger.info(
                f"[CVLAC] Consultando perfil {code} "
                f"({idx + 1}/{len(cvlac_codes)})"
            )
            try:
                # La aplicación orquesta el scraping completo del perfil
                profile_fields = profile_service.scrape_profile(
                    session=self.session,
                    config=self.config,
                    base_url=self.config.base_url,
                    cvlac_code=code,
                    source_name=self.source_name,
                    year_from=year_from,
                    year_to=year_to,
                )

                # Construir StandardRecords a partir de los campos parseados
                for fields in profile_fields:
                    record = self._parse_record(fields)
                    records.append(record)
                    total_fetched += 1
                    if max_results and total_fetched >= max_results:
                        break

            except CvlacScrapingError as e:
                logger.warning(f"[CVLAC] Error con perfil {code}: {e}")
                continue
            except Exception as e:
                logger.warning(f"[CVLAC] Error inesperado con perfil {code}: {e}")
                continue

            if max_results and total_fetched >= max_results:
                break

            # Delay entre requests para respetar el servidor de Minciencias
            time.sleep(self.config.delay_between_requests)

        return self._post_process(records)

    def _parse_record(self, fields: dict) -> StandardRecord:
        """
        Construye un StandardRecord a partir del dict de campos parseados.

        Los campos ya vienen procesados por domain.record_parser;
        este método solo construye el objeto StandardRecord.

        Args:
            fields: Dict con claves listas para StandardRecord, producido
                    por domain.record_parser.parse_raw().

        Returns:
            StandardRecord completo con source_name incluido.
        """
        return StandardRecord(
            source_name=self.source_name,
            source_id=fields["source_id"],
            doi=fields["doi"],
            title=fields["title"],
            publication_year=fields["publication_year"],
            publication_type=fields["publication_type"],
            source_journal=fields["source_journal"],
            issn=fields["issn"],
            authors=fields["authors"],
            institutional_authors=fields["institutional_authors"],
            raw_data=fields["raw_data"],
        )
