"""
Orquestador principal del extractor CVLAC.

Fuente primaria: API JSON de Metrik Unisimon
    GET https://metrik.unisimon.edu.co/scienti/cvlac/{cc_investigador}

Entrada:
    cc_investigadores: lista de cédulas de ciudadanía.

Flujo por cédula:
  1. metrik_service.fetch_profile(cc)  →  {investigador, produccion[]}
  2. Filtro por año (year_from / year_to).
  3. record_parser.parse_raw(item)     →  fields dict
  4. _parse_record(fields)             →  StandardRecord

El scraper HTML de Minciencias (profile_service / html_parser) queda como
módulo legacy para referencia pero ya no es el camino principal.
"""

import logging
import time
from typing import List, Optional

import requests as _requests

from config import cvlac_config, SourceName
from extractors.base import BaseExtractor, StandardRecord
from extractors.cvlac.application import metrik_service
from extractors.cvlac.domain import record_parser

from unidecode import unidecode

logger = logging.getLogger(__name__)


class CvlacExtractor(BaseExtractor):
    """
    Extractor de producción científica desde CvLAC vía API Metrik Unisimon.

    Uso:
        extractor = CvlacExtractor()
        records = extractor.extract(
            cc_investigadores=["7977197", "12345678"],
            year_from=2020,
            year_to=2025,
        )
    """

    source_name = SourceName.CVLAC

    def __init__(self):
        self.config = cvlac_config
        logger.info("CvlacExtractor inicializado (fuente: Metrik Unisimon JSON API).")

    # ------------------------------------------------------------------
    # Interfaz BaseExtractor
    # ------------------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        cc_investigadores: Optional[List[str]] = None,
        # alias legacy por si algún caller pasa cvlac_codes
        cvlac_codes: Optional[List[str]] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae producción bibliográfica de una lista de investigadores por cédula.

        Args:
            year_from:           Año inicial del filtro (inclusive). None = sin límite.
            year_to:             Año final del filtro (inclusive). None = sin límite.
            max_results:         Límite total de registros. None = todos.
            cc_investigadores:   Lista de cédulas de ciudadanía (REQUERIDO).

        Returns:
            Lista de StandardRecord normalizados y post-procesados.
        """
        cedulas = cc_investigadores or cvlac_codes or []
        if not cedulas:
            raise ValueError(
                "Debes proporcionar cc_investigadores. "
                "Ejemplo: cc_investigadores=['7977197']"
            )

        records: List[StandardRecord] = []
        total_fetched = 0

        for idx, cc in enumerate(cedulas):
            logger.info(
                f"[CVLAC] Consultando cc={cc} "
                f"({idx + 1}/{len(cedulas)})"
            )
            try:
                profile = metrik_service.fetch_profile(
                    cc_investigador=cc,
                    timeout=self.config.timeout,
                )
            except _requests.HTTPError as e:
                logger.warning(f"[CVLAC] HTTP error para cc={cc}: {e}")
                continue
            except (_requests.Timeout, _requests.ConnectionError) as e:
                logger.warning(f"[CVLAC] Conexión fallida para cc={cc}: {e}")
                continue
            except ValueError as e:
                logger.warning(f"[CVLAC] Respuesta inválida para cc={cc}: {e}")
                continue
            except Exception as e:
                logger.warning(f"[CVLAC] Error inesperado para cc={cc}: {e}")
                continue

            investigador = profile.get("investigador", {})

            for item in profile.get("produccion", []):
                # Filtro por año
                anio = item.get("anio")
                if year_from and anio and anio < year_from:
                    continue
                if year_to and anio and anio > year_to:
                    continue

                fields = record_parser.parse_raw(item, investigador)
                records.append(self._parse_record(fields))
                total_fetched += 1

                if max_results and total_fetched >= max_results:
                    break

            if max_results and total_fetched >= max_results:
                break

            if idx < len(cedulas) - 1:
                time.sleep(self.config.delay_between_requests)

        return self._post_process(records)

    def _parse_record(self, fields: dict) -> StandardRecord:
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
