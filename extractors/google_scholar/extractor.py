"""
Extractor de producción científica desde perfiles de Google Scholar.

Google Scholar no tiene API pública oficial. Este extractor usa la
librería `scholarly` (web scraping) para obtener publicaciones desde
los perfiles de autores.

Uso típico:
    extractor = GoogleScholarExtractor()
    records = extractor.extract(
        scholar_ids=["Ozm565YAAAAJ", "_xxTOIEAAAAJ"],
        year_from=2020,
        year_to=2025,
    )

Prerequisito:
    pip install scholarly

Advertencia:
    Google Scholar aplica rate-limiting. Para uso intensivo considera
    configurar un proxy:
        from scholarly import scholarly, ProxyGenerator
        pg = ProxyGenerator()
        pg.FreeProxies()
        scholarly.use_proxy(pg)
"""

import logging
from typing import List, Optional

from extractors.base import BaseExtractor, StandardRecord
from extractors.google_scholar._exceptions import GoogleScholarError
from extractors.google_scholar.application import profile_service

logger = logging.getLogger(__name__)


class GoogleScholarExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde perfiles de Google Scholar.

    Itera una lista de Scholar IDs, descarga el perfil de cada autor
    y extrae sus publicaciones.

    Args:
        (sin parámetros — scholarly gestiona la sesión HTTP internamente)
    """

    source_name = "google_scholar"

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        scholar_ids: Optional[List[str]] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae publicaciones de una lista de perfiles Google Scholar.

        Args:
            year_from:    Año inicial del filtro (inclusive). None = sin límite.
            year_to:      Año final del filtro (inclusive). None = sin límite.
            max_results:  Límite total de registros. None = todos.
            scholar_ids:  Lista de IDs de perfil Google Scholar (REQUERIDO).
                          Ej: ['Ozm565YAAAAJ', '_xxTOIEAAAAJ']

        Returns:
            Lista de StandardRecord normalizados.

        Raises:
            GoogleScholarError: Si `scholarly` no está instalado.
            ValueError: Si no se proveen scholar_ids.
        """
        if not scholar_ids:
            raise ValueError(
                "Debes proporcionar una lista de Scholar IDs. "
                "Ejemplo: scholar_ids=['Ozm565YAAAAJ']"
            )

        print(f"\n[DEBUG EXTRACTOR] Iniciando extracción de {len(scholar_ids)} perfiles")
        
        records: List[StandardRecord] = []
        total_fetched = 0

        for idx, scholar_id in enumerate(scholar_ids):
            logger.info(
                f"[GoogleScholar] Consultando perfil {scholar_id} "
                f"({idx + 1}/{len(scholar_ids)})"
            )
            print(f"\n[DEBUG EXTRACTOR] ========== Perfil {idx + 1}/{len(scholar_ids)}: {scholar_id} ==========")
            
            remaining = (max_results - total_fetched) if max_results else None
            try:
                print(f"[DEBUG EXTRACTOR] Llamando fetch_profile_publications() para {scholar_id}")
                fields_list = profile_service.fetch_profile_publications(
                    scholar_id=scholar_id,
                    year_from=year_from,
                    year_to=year_to,
                    max_results=remaining,
                )
                print(f"[DEBUG EXTRACTOR] fetch_profile_publications retornó {len(fields_list)} registros")
                
                for idx_pub, fields in enumerate(fields_list):
                    print(f"[DEBUG EXTRACTOR]   Parseando publicación {idx_pub + 1}/{len(fields_list)}")
                    records.append(self._parse_record(fields))
                    total_fetched += 1
                    if max_results and total_fetched >= max_results:
                        print(f"[DEBUG EXTRACTOR] Límite de {max_results} registros alcanzado")
                        break
                
                print(f"[DEBUG EXTRACTOR] Perfil {scholar_id} completado. Total acumulado: {total_fetched}")

            except GoogleScholarError as e:
                logger.warning(f"[GoogleScholar] Error con perfil {scholar_id}: {e}")
                print(f"[DEBUG EXTRACTOR] ERROR GoogleScholarError: {e}")
                continue
            except Exception as e:
                logger.warning(
                    f"[GoogleScholar] Error inesperado con perfil {scholar_id}: {e}"
                )
                print(f"[DEBUG EXTRACTOR] ERROR Inesperado: {e}")
                import traceback
                print(traceback.format_exc())
                continue

            if max_results and total_fetched >= max_results:
                print(f"[DEBUG EXTRACTOR] Límite global alcanzado, saliendo del loop")
                break

        print(f"\n[DEBUG EXTRACTOR] Extracción completada. Total de registros: {len(records)}")
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
            citation_count=fields["citation_count"],
            url=fields["url"],
            raw_data=fields["raw_data"],
        )
