"""
Orquestador principal del extractor de Datos Abiertos Colombia.

Datos Abiertos Colombia (datos.gov.co) es el portal oficial del gobierno
colombiano de datos abiertos. Usa la SODA API (Socrata Open Data API)
para exponer datasets públicos, incluidos datasets de producción científica
de Minciencias (grupos de investigación, investigadores, bibliografía).

Este módulo implementa la interfaz BaseExtractor usando arquitectura DDD:
  infrastructure.http_client  → sesión HTTP con App Token opcional
  domain.query_builder        → construye cláusulas SoQL ($where)
  application.dataset_service → paginación offset-based del dataset
  domain.record_parser        → mapeo flexible de columnas a StandardRecord

Documentación SODA API:
  https://dev.socrata.com/docs/

Configuración:
  DATOS_ABIERTOS_TOKEN: App Token de Socrata (opcional, aumenta cuota).
"""

import logging
from typing import List, Optional

from config import datos_abiertos_config, SourceName
from extractors.base import BaseExtractor, StandardRecord
from extractors.datos_abiertos._exceptions import DatosAbiertosError
from extractors.datos_abiertos.domain import query_builder, record_parser
from extractors.datos_abiertos.application import dataset_service
from extractors.datos_abiertos.infrastructure import http_client

logger = logging.getLogger(__name__)


class DatosAbiertosExtractor(BaseExtractor):
    """
    Extractor de producción científica desde datasets de datos.gov.co.

    Usa la SODA API para consultar datasets en formato JSON con paginación
    por offset. El mapeo de columnas es flexible: busca los datos del registro
    probando múltiples nombres de columna conocidos (ver domain/record_parser.py).

    Uso típico:
        extractor = DatosAbiertosExtractor(dataset_id="abc1-def2")
        records = extractor.extract(year_from=2020, institution_filter="Antioquia")

    Para encontrar datasets:
        https://www.datos.gov.co/browse?category=Ciencia+Tecnolog%C3%ADa+e+Innovaci%C3%B3n
    """

    source_name = SourceName.DATOS_ABIERTOS

    def __init__(
        self,
        dataset_id: str = None,
        app_token: str = None,
    ):
        """
        Inicializa el extractor con el ID del dataset y App Token opcionales.

        Args:
            dataset_id: ID del dataset en datos.gov.co (ej: 'abc1-def2').
                        Se puede omitir aquí y pasar en extract().
            app_token:  App Token de Socrata. Si es None, usa
                        DATOS_ABIERTOS_TOKEN del entorno (o sin token).
        """
        self.dataset_id = dataset_id
        self.app_token  = app_token or datos_abiertos_config.app_token
        self.config     = datos_abiertos_config

        # La sesión se crea en infraestructura
        self.session = http_client.create_session(
            config=self.config,
            app_token=self.app_token,
        )
        logger.info("DatosAbiertosExtractor inicializado.")

    # ------------------------------------------------------------------
    # Interfaz BaseExtractor
    # ------------------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        dataset_id: Optional[str] = None,
        institution_filter: Optional[str] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Consulta y extrae registros de un dataset de datos.gov.co.

        Flujo:
          1. Construye la cláusula $where en SoQL (dominio).
          2. Extrae todos los registros con paginación offset (aplicación).
          3. Parsea cada registro al formato estándar (dominio).
          4. Normaliza campos calculados (base).

        Args:
            year_from:          Año inicial del filtro. None = sin límite.
            year_to:            Año final del filtro. None = sin límite.
            max_results:        Límite total de registros. None = todos.
            dataset_id:         ID del dataset. Sobrescribe el configurado en __init__.
                                Al menos uno de los dos debe estar presente.
            institution_filter: Nombre parcial de institución para filtrar
                                (aplica LIKE case-insensitive en la columna 'institucion').

        Returns:
            Lista de StandardRecord normalizados y post-procesados.

        Raises:
            DatosAbiertosError: Si ocurre un error HTTP o falta el dataset_id.
            ValueError: Si no se puede determinar el dataset_id.
        """
        ds_id = dataset_id or self.dataset_id
        if not ds_id:
            raise ValueError(
                "Debes especificar un dataset_id. "
                "Encuéntralo en https://www.datos.gov.co/"
            )

        # 1. Construir filtro SoQL en el dominio
        where_clause = query_builder.build_where(
            year_from=year_from,
            year_to=year_to,
            institution_filter=institution_filter,
        )

        # 2. URL del endpoint del dataset en formato JSON (SODA)
        base_url = f"{self.config.base_url}/{ds_id}.json"

        # 3. Extracción paginada en la capa de aplicación
        raw_records = dataset_service.paginated_fetch(
            session=self.session,
            config=self.config,
            base_url=base_url,
            where_clause=where_clause,
            max_results=max_results,
        )

        # 4. Parsear cada registro crudo a StandardRecord
        records = []
        for entry in raw_records:
            try:
                record = self._parse_record(entry)
                records.append(record)
            except Exception as e:
                logger.warning(f"[DatosAbiertos] Error parseando registro: {e}")

        return self._post_process(records)

    def _parse_record(self, entry: dict) -> StandardRecord:
        """
        Convierte un registro crudo de la SODA API a StandardRecord.

        Delega el mapeo flexible de columnas al módulo record_parser del dominio.

        Args:
            entry: Dict crudo de un registro del dataset SODA.

        Returns:
            StandardRecord con los campos mapeados.
        """
        fields = record_parser.parse_entry(entry)

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
            raw_data=fields["raw_data"],
        )
