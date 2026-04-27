"""
Orquestador principal del extractor de Web of Science.

Este módulo implementa la interfaz BaseExtractor actuando como 'glue'
delgado entre las capas DDD del paquete:

  infrastructure.http_client  → crea la sesión autenticada
  domain.query_builder        → construye la query WoS
  application.search_service  → orquesta la paginación HTTP
  domain.record_parser        → transforma hits crudos en campos del StandardRecord

Documentación de la API:
  https://developer.clarivate.com/apis/wos-starter

Autenticación:
  Header X-ApiKey — configurar WOS_API_KEY en variables de entorno.
"""

import logging
from typing import List, Optional

from config import wos_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord
from extractors.wos._exceptions import WosAPIError
from extractors.wos.domain import query_builder, record_parser
from extractors.wos.application import search_service
from extractors.wos.infrastructure import http_client

logger = logging.getLogger(__name__)


class WosExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde Web of Science Starter API.

    Busca publicaciones institucionales usando el operador OG= (Organization-Enhanced)
    que WoS normaliza internamente para cubrir variantes del nombre institucional.

    Requiere:
      WOS_API_KEY en variables de entorno (Clarivate Developer Portal).

    Uso típico:
        extractor = WosExtractor()
        records = extractor.extract(year_from=2020, year_to=2025)
    """

    source_name = SourceName.WOS

    def __init__(self, api_key: str = None):
        """
        Inicializa el extractor y crea la sesión HTTP autenticada.

        Args:
            api_key: API key de Clarivate. Si es None, usa WOS_API_KEY del entorno.
        """
        self.api_key = api_key or wos_config.api_key
        self.config  = wos_config

        if not self.api_key:
            logger.warning(
                "WOS_API_KEY no configurada. "
                "Obtén una en https://developer.clarivate.com/"
            )

        # La sesión se crea en infraestructura; el extractor no conoce los detalles HTTP
        self.session = http_client.create_session(self.api_key, self.config)
        logger.info("WosExtractor inicializado.")

    # ------------------------------------------------------------------
    # Interfaz BaseExtractor
    # ------------------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        org_enhanced: Optional[str] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae publicaciones de WoS para la institución configurada.

        Flujo:
          1. Construye la query WoS (dominio).
          2. Ejecuta la búsqueda paginada (aplicación).
          3. Parsea cada hit a StandardRecord (dominio).
          4. Normaliza campos calculados (base).

        Args:
            year_from:    Año inicial de publicación (inclusive).
            year_to:      Año final de publicación (inclusive).
            max_results:  Límite de registros. None = todos.
            org_enhanced: Nombre Organization-Enhanced de WoS. Si es None,
                          usa el nombre institucional de la configuración.

        Returns:
            Lista de StandardRecord normalizados y post-procesados.

        Raises:
            WosAPIError: Si la API key no está configurada o la API falla.
        """
        if not self.api_key:
            raise WosAPIError(
                "WOS_API_KEY no configurada. "
                "Configúrala en las variables de entorno."
            )

        # 1. Construir query en la capa de dominio
        query = query_builder.build_query(
            year_from=year_from,
            year_to=year_to,
            org_enhanced=org_enhanced,
            institution_name=institution.name,
        )

        # 2. Búsqueda paginada en la capa de aplicación
        hits = search_service.paginated_search(
            session=self.session,
            config=self.config,
            query=query,
            max_results=max_results,
        )

        # 3. Parsear cada hit a StandardRecord
        records = []
        for hit in hits:
            try:
                record = self._parse_record(hit)
                records.append(record)
            except Exception as e:
                logger.warning(f"[WoS] Error parseando hit '{hit.get('uid', '?')}': {e}")

        # 4. Post-procesar: calcular campos normalizados (normalized_title, etc.)
        return self._post_process(records)

    def search_by_doi(self, doi: str) -> Optional[StandardRecord]:
        """Busca un documento en WoS por DOI. Retorna StandardRecord o None."""
        if not self.api_key:
            return None
        clean_doi = doi.strip().lstrip("https://doi.org/").lstrip("http://doi.org/")
        try:
            hits = search_service.paginated_search(
                session=self.session,
                config=self.config,
                query=f'DO="{clean_doi}"',
                max_results=1,
            )
            if hits:
                record = self._parse_record(hits[0])
                record.compute_normalized_fields()
                return record
        except Exception as e:
            logger.debug(f"WoS search_by_doi {clean_doi!r}: {e}")
        return None

    def _parse_record(self, hit: dict) -> StandardRecord:
        """
        Convierte un hit crudo de la WoS Starter API a un StandardRecord.

        Delega la extracción de campos al módulo de dominio record_parser,
        manteniendo este método como glue puro sin lógica de negocio.

        Args:
            hit: Dict crudo de un resultado de WoS Starter API.

        Returns:
            StandardRecord con los campos mapeados desde el hit.
        """
        fields = record_parser.parse_hit(hit)

        return StandardRecord(
            source_name=self.source_name,
            source_id=fields["source_id"],
            doi=fields["doi"],
            title=fields["title"],
            publication_year=fields["publication_year"],
            publication_type=fields["publication_type"],
            source_journal=fields["source_journal"],
            authors=fields["authors"],
            citation_count=fields["citation_count"],
            raw_data=fields["raw_data"],
        )
