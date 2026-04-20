"""
Orquestador principal del extractor de Scopus (Elsevier).

Scopus es la base de datos bibliográfica de Elsevier. Este módulo
implementa la interfaz BaseExtractor usando arquitectura DDD:

  infrastructure.http_client  → sesión autenticada con API Key / Inst Token
  domain.query_builder        → construye queries con operadores de campo Scopus
  application.search_service  → paginación, búsqueda por DOI, búsqueda avanzada
  domain.record_parser        → parsea XML/JSON → campos de StandardRecord

Además expone métodos de utilidad para búsquedas específicas:
  - search_by_doi / search_by_dois: búsqueda puntual por DOI
  - extract_advanced: búsqueda con todos los operadores de campo
  - get_author_by_orcid: resolución de ORCID → Scopus AU-ID
  - build_advanced_query: constructor público de queries

API usada:
  Scopus Search API — https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl

Autenticación:
  SCOPUS_API_KEY: API key de Elsevier Developer Portal (requerido)
  SCOPUS_INST_TOKEN: Token institucional (opcional, aumenta cuota)
"""

import logging
import time
from typing import List, Dict, Optional

from config import scopus_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord
from extractors.scopus._exceptions import ScopusAPIError
from extractors.scopus.domain import query_builder, record_parser
from extractors.scopus.application import search_service
from extractors.scopus.infrastructure import http_client

logger = logging.getLogger(__name__)


class ScopusExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde la Scopus Search API (Elsevier).

    Soporta múltiples modos de búsqueda:
      - Institucional (AF-ID / AFFIL) para extraer producción de una institución.
      - Por ORCID para extraer publicaciones de un autor específico.
      - Avanzada con cualquier combinación de operadores de campo Scopus.
      - Por DOI individual o en lote.

    Requiere:
      SCOPUS_API_KEY en variables de entorno (Elsevier Developer Portal).
      SCOPUS_INST_TOKEN (opcional, para mayor cuota institucional).

    Uso típico:
        extractor = ScopusExtractor()
        # Buscar por afiliación institucional
        records = extractor.extract(affiliation_id="60106970")
        # Buscar con query avanzada
        records = extractor.extract_advanced(year_from=2020, open_access=True)
        # Buscar por DOI
        record = extractor.search_by_doi("10.1016/j.jhydrol.2020.125741")
    """

    source_name = SourceName.SCOPUS

    SEARCH_URL = f"{scopus_config.base_url}/search/scopus"

    def __init__(self, api_key: str = None, inst_token: str = None):
        """
        Inicializa el extractor y crea la sesión HTTP autenticada.

        Args:
            api_key:    API key de Elsevier. Si es None, usa SCOPUS_API_KEY del entorno.
            inst_token: Token institucional. Si es None, usa SCOPUS_INST_TOKEN del entorno.
        """
        self.api_key    = api_key    or scopus_config.api_key
        self.inst_token = inst_token or scopus_config.inst_token
        self.config     = scopus_config

        if not self.api_key:
            logger.warning(
                "SCOPUS_API_KEY no configurada. "
                "Obtén una en https://dev.elsevier.com/"
            )

        self.session = http_client.create_session(
            config=self.config,
            api_key=self.api_key,
            inst_token=self.inst_token,
        )
        logger.info("ScopusExtractor inicializado.")

    # ------------------------------------------------------------------
    # Interfaz BaseExtractor
    # ------------------------------------------------------------------

    def extract(
        self,
        query: Optional[str] = None,
        start: int = 0,
        max_results: Optional[int] = None,
        affiliation_id: Optional[str] = None,
        orcid: Optional[str] = None,
    ) -> List[StandardRecord]:
        """
        Extrae registros de Scopus usando query, affiliation_id u ORCID.

        Si no se provee query, la construye automáticamente desde
        affiliation_id u orcid. Al menos uno de los tres debe estar presente.

        Args:
            query:          Query Scopus completa (ej: 'AF-ID(60106970) AND PUBYEAR > 2019').
            start:          Offset inicial para paginación.
            max_results:    Límite total de registros. None = todos.
            affiliation_id: AF-ID de Scopus para búsqueda institucional.
            orcid:          ORCID del autor para búsqueda individual.

        Returns:
            Lista de StandardRecord normalizados y post-procesados.

        Raises:
            ScopusAPIError: Si la API falla o falta la API key.
            ValueError: Si no se provee ningún criterio de búsqueda.
        """
        # Construir query si no se pasa explícitamente
        if query is None:
            if affiliation_id:
                query = f"AF-ID({affiliation_id})"
            elif orcid:
                query = f"ORCID({orcid})"
            else:
                raise ValueError(
                    "Debes proporcionar 'query', 'affiliation_id' o 'orcid'."
                )

        # Delegar la búsqueda paginada a la capa de aplicación
        all_fields = search_service.paginated_search(
            session=self.session,
            config=self.config,
            search_url=self.SEARCH_URL,
            query=query,
            max_results=max_results,
            start=start,
        )

        # Construir StandardRecords a partir de los campos parseados
        records = []
        for fields in all_fields:
            try:
                record = self._parse_record(fields)
                records.append(record)
            except Exception as e:
                logger.warning(f"[Scopus] Error construyendo StandardRecord: {e}")

        return self._post_process(records)

    def _parse_record(self, fields: dict) -> StandardRecord:
        """
        Construye un StandardRecord a partir del dict de campos ya parseados.

        Los campos provienen de domain.record_parser (parse_xml_entry o
        parse_json_entry). Este método solo construye el objeto final.

        Args:
            fields: Dict con todos los campos necesarios para StandardRecord.

        Returns:
            StandardRecord completo con source_name incluido.
        """
        return StandardRecord(
            source_name=self.source_name,
            source_id=fields["source_id"],
            doi=fields["doi"],
            title=fields["title"],
            publication_year=fields["publication_year"],
            publication_date=fields.get("publication_date"),
            publication_type=fields["publication_type"],
            source_journal=fields["source_journal"],
            issn=fields["issn"],
            is_open_access=fields["is_open_access"],
            oa_status=fields["oa_status"],
            authors=fields["authors"],
            citation_count=fields["citation_count"],
            abstract=fields.get("abstract"),
            page_range=fields.get("page_range"),
            publisher=fields.get("publisher"),
            raw_data=fields.get("raw_data") or {},
        )

    # ------------------------------------------------------------------
    # Búsqueda por DOI
    # ------------------------------------------------------------------

    def search_by_doi(self, doi: str) -> Optional[StandardRecord]:
        """
        Busca un documento individual en Scopus por su DOI.

        Args:
            doi: DOI del documento (con o sin prefijo https://doi.org/).

        Returns:
            StandardRecord si se encuentra en Scopus, None si no.
        """
        if not self.api_key:
            raise ScopusAPIError("SCOPUS_API_KEY no configurada.")

        fields = search_service.search_by_doi_json(
            session=self.session,
            config=self.config,
            search_url=self.SEARCH_URL,
            doi=doi,
        )
        if fields is None:
            return None

        try:
            record = self._parse_record(fields)
            record.compute_normalized_fields()
            return record
        except Exception as e:
            logger.warning(f"[Scopus] Error construyendo registro para DOI {doi}: {e}")
            return None

    def extract_by_author(
        self,
        scopus_author_id: str,
        max_results: int = 50,
    ) -> List[StandardRecord]:
        """
        Busca publicaciones de un autor en Scopus por su Scopus Author ID.

        Args:
            scopus_author_id: AU-ID de Scopus (solo dígitos)
            max_results: Límite de resultados

        Returns:
            Lista de StandardRecords del autor.
        """
        clean_id = str(scopus_author_id).strip().lstrip("AU-ID(").rstrip(")")
        query = f"AU-ID({clean_id})"
        try:
            return self.extract(query=query, max_results=max_results)
        except Exception as e:
            import logging
            logging.getLogger(__name__).debug(f"extract_by_author scopus_id={clean_id!r}: {e}")
            return []

    def search_by_dois(
        self,
        dois: List[str],
        delay: float = 0.25,
    ) -> List[StandardRecord]:
        """
        Busca múltiples documentos en Scopus por DOI, uno a uno.

        Args:
            dois:  Lista de DOIs a buscar.
            delay: Pausa en segundos entre peticiones (respeta rate-limit).

        Returns:
            Lista de StandardRecords encontrados en Scopus.
        """
        if not self.api_key:
            raise ScopusAPIError("SCOPUS_API_KEY no configurada.")

        all_fields = search_service.search_dois_batch(
            session=self.session,
            config=self.config,
            search_url=self.SEARCH_URL,
            dois=dois,
            delay=delay,
        )
        records = []
        for fields in all_fields:
            try:
                record = self._parse_record(fields)
                record.compute_normalized_fields()
                records.append(record)
            except Exception as e:
                logger.warning(f"[Scopus] Error en batch DOI: {e}")
        return records

    # ------------------------------------------------------------------
    # Búsqueda avanzada
    # ------------------------------------------------------------------

    @staticmethod
    def build_advanced_query(**kwargs) -> str:
        """
        Constructor público de queries avanzadas de Scopus.

        Delega a domain.query_builder.build_advanced_query().
        Ver esa función para documentación completa de todos los parámetros.

        Returns:
            String de query lista para la Scopus Search API.
        """
        return query_builder.build_advanced_query(**kwargs)

    def extract_advanced(
        self,
        *,
        title: Optional[str] = None,
        abstract: Optional[str] = None,
        keywords: Optional[str] = None,
        title_abs_key: Optional[str] = None,
        author: Optional[str] = None,
        first_author: Optional[str] = None,
        author_id: Optional[str] = None,
        orcid: Optional[str] = None,
        affiliation_id: Optional[str] = None,
        affiliation_name: Optional[str] = None,
        source_title: Optional[str] = None,
        issn: Optional[str] = None,
        doi_filter: Optional[str] = None,
        publisher: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        year_exact: Optional[int] = None,
        document_type: Optional[str] = None,
        subject_area: Optional[str] = None,
        language: Optional[str] = None,
        open_access: Optional[bool] = None,
        funder: Optional[str] = None,
        grant_number: Optional[str] = None,
        extra: Optional[str] = None,
        operator: str = "AND",
        max_results: Optional[int] = None,
    ) -> List[StandardRecord]:
        """
        Extrae registros usando la búsqueda avanzada de Scopus.

        Equivale a la pestaña 'Advanced search' de la web de Scopus,
        permitiendo combinar cualquier operador de campo. Ver
        domain.query_builder.build_advanced_query() para documentación
        completa de cada parámetro.

        Ejemplos de uso:
            # Artículos de institución entre 2020-2024
            extractor.extract_advanced(
                affiliation_id="60106970",
                year_from=2020, year_to=2024,
                document_type="article",
            )
            # Publicaciones OA de un autor con machine learning
            extractor.extract_advanced(
                orcid="0000-0002-2096-7900",
                title_abs_key="machine learning",
                open_access=True,
            )

        Returns:
            Lista de StandardRecord normalizados.
        """
        built_query = query_builder.build_advanced_query(
            title=title, abstract=abstract, keywords=keywords,
            title_abs_key=title_abs_key, author=author,
            first_author=first_author, author_id=author_id,
            orcid=orcid, affiliation_id=affiliation_id,
            affiliation_name=affiliation_name, source_title=source_title,
            issn=issn, doi=doi_filter, publisher=publisher,
            year_from=year_from, year_to=year_to, year_exact=year_exact,
            document_type=document_type, subject_area=subject_area,
            language=language, open_access=open_access,
            funder=funder, grant_number=grant_number,
            extra=extra, operator=operator,
        )
        logger.info(f"[Scopus] Búsqueda avanzada: {built_query}")
        return self.extract(query=built_query, max_results=max_results)

    # ------------------------------------------------------------------
    # Utilidades de autor
    # ------------------------------------------------------------------

    def get_author_by_orcid(self, orcid: str) -> Optional[Dict[str, str]]:
        """
        Busca un autor en Scopus Author Search API por su ORCID.

        Args:
            orcid: ORCID del autor (ej: '0000-0002-2096-7900').

        Returns:
            Dict con {'scopus_id': '...', 'name': '...'} o None.
        """
        return http_client.get_author_by_orcid(
            session=self.session,
            orcid=orcid,
        )
