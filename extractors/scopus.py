"""
Extractor de Scopus API (Elsevier).

Requiere API Key de Elsevier Developer Portal:
  https://dev.elsevier.com/

Endpoints principales:
  - Scopus Search API: búsqueda de documentos
  - Scopus Abstract Retrieval: detalle de un documento
"""

import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import scopus_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord

logger = logging.getLogger(__name__)


class ScopusAPIError(Exception):
    """Excepción para errores de la API de Scopus"""
    pass


class ScopusExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde Scopus Search API.

    Documentación: https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl

    Requiere configurar en variables de entorno:
      - SCOPUS_API_KEY: API key de Elsevier
      - SCOPUS_INST_TOKEN: Token institucional (opcional, para mayor cuota)
    """

    source_name = SourceName.SCOPUS

    SEARCH_URL = f"{scopus_config.base_url}/search/scopus"
    ABSTRACT_URL = f"{scopus_config.base_url}/abstract/scopus_id"

    def __init__(self, api_key: str = None, inst_token: str = None):
        self.api_key = api_key or scopus_config.api_key
        self.inst_token = inst_token or scopus_config.inst_token
        self.config = scopus_config

        if not self.api_key:
            logger.warning(
                "SCOPUS_API_KEY no configurada. "
                "Obten una en https://dev.elsevier.com/"
            )

        self.session = self._create_session()
        logger.info("ScopusExtractor inicializado.")

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Headers comunes para Scopus
        session.headers.update({
            "X-ELS-APIKey": self.api_key,
            "Accept": "application/xml",
        })
        if self.inst_token:
            session.headers["X-ELS-Insttoken"] = self.inst_token

        return session



    # ---------------------------------------------------------
    # INTERFAZ BaseExtractor
    # ---------------------------------------------------------

    def extract(
        self,
        query: Optional[str] = None,
        start: int = 0,
        max_results: Optional[int] = None,
        affiliation_id: Optional[str] = None,
        orcid: Optional[str] = None,
    ) -> List[StandardRecord]:
        """
        Extrae registros de Scopus Search API y normaliza campos.
        Puede buscar por query, affiliation_id o orcid.
        """
        # Construir query si no se pasa explícitamente
        if query is None:
            if affiliation_id:
                query = f"AF-ID({affiliation_id})"
            elif orcid:
                query = f"ORCID({orcid})"
            else:
                raise ValueError("Debes proporcionar 'query', 'affiliation_id' o 'orcid' para la extracción de Scopus.")

        records = []
        total_fetched = 0
        import xml.etree.ElementTree as ET
        while True:
            params = {
                "query": query,
                "start": start,
                "count": self.config.max_per_page,
                "sort": "pubyear",
                "field": (
                    "dc:identifier,doi,dc:title,prism:publicationName,"\
                    "prism:coverDate,subtypeDescription,citedby-count,"\
                    "author,prism:issn,openaccess,openaccessFlag,"\
                    "dc:description,authkeywords,prism:volume,prism:issueIdentifier,"\
                    "prism:pageRange,afid,affiliation"
                ),
            }

            try:
                resp = self.session.get(
                    self.SEARCH_URL,
                    params=params,
                    timeout=self.config.timeout,
                )
                print(f"ScopusExtractor: URL={resp.url}")
                resp.raise_for_status()
                xml_content = resp.text
                print("Respuesta cruda Scopus (XML):", xml_content[:2000])
            except requests.exceptions.RequestException as e:
                print(f"Error en Scopus API: {e}")
                raise ScopusAPIError(f"Error en Scopus API: {e}")

            # Parsear XML
            root = ET.fromstring(xml_content)
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'opensearch': 'http://a9.com/-/spec/opensearch/1.1/',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'prism': 'http://prismstandard.org/namespaces/basic/2.0/',
                'scopus': 'http://www.elsevier.com/xml/svapi/abstract/dtd',
            }
            entries = root.findall('atom:entry', ns)

            if not entries:
                print("ScopusExtractor: Sin resultados o error en entries.")
                break

            for entry in entries:
                try:
                    # Extraer campos principales del XML
                    doi = entry.findtext('prism:doi', default=None, namespaces=ns)
                    title = entry.findtext('dc:title', default=None, namespaces=ns)
                    scopus_id = entry.findtext('dc:identifier', default=None, namespaces=ns)
                    cover_date = entry.findtext('prism:coverDate', default=None, namespaces=ns)
                    pub_year = int(cover_date[:4]) if cover_date and len(cover_date) >= 4 else None
                    source_journal = entry.findtext('prism:publicationName', default=None, namespaces=ns)
                    issn = entry.findtext('prism:issn', default=None, namespaces=ns)
                    subtype = entry.findtext('subtypeDescription', default=None, namespaces=ns)
                    # citedby-count está en el namespace atom
                    citedby_count = int(entry.findtext('atom:citedby-count', default='0', namespaces=ns))
                    oa_flag = entry.findtext('openaccessFlag', default=None, namespaces=ns)
                    is_oa = oa_flag == "true" if oa_flag else None
                    
                    # Obtener total de citaciones desde Scopus
                    # (No disponible años precisos de citación desde Scopus API)

                    # Autores
                    authors = []
                    # Buscar con ambos formatos de namespace por si acaso
                    author_elements = entry.findall('atom:author', ns) or entry.findall('author', ns)
                    for author in author_elements:
                        name = author.findtext('atom:authname', default=None, namespaces=ns) or author.findtext('authname', default=None)
                        authid = author.findtext('atom:authid', default=None, namespaces=ns) or author.findtext('authid', default=None)
                        if name:  # Solo agregar si tiene nombre
                            authors.append({
                                "name": name,
                                "orcid": None,
                                "scopus_id": authid,
                                "is_institutional": False,
                            })

                    record = StandardRecord(
                        source_name=self.source_name,
                        source_id=scopus_id,
                        doi=doi,
                        title=title,
                        publication_year=pub_year,
                        publication_date=cover_date,
                        publication_type=subtype,
                        source_journal=source_journal,
                        issn=issn,
                        is_open_access=is_oa,
                        authors=authors,
                        citation_count=citedby_count,
                        citations_by_year={},  # No disponible desde Scopus API
                        url=None,
                        raw_data=None,
                    )
                    record.compute_normalized_fields()
                    records.append(record)
                    total_fetched += 1
                    if max_results and total_fetched >= max_results:
                        break
                except Exception as e:
                    print(f"Error parseando entrada Scopus XML: {e}")
                    continue

            print(f"  Extraídos de Scopus: {total_fetched}")

            # Paginación
            total_results_el = root.find('opensearch:totalResults', ns)
            total_results = int(total_results_el.text) if total_results_el is not None else 0
            start += self.config.max_per_page
            if start >= total_results:
                break

            time.sleep(0.2)  # Rate limit

        return self._post_process(records)

    def _parse_record(self, entry: dict) -> StandardRecord:
        """Convierte una entrada de Scopus Search a StandardRecord"""

        # Autores
        authors = []
        for auth in entry.get("author", []) or []:
            authors.append({
                "name": auth.get("authname"),
                "orcid": None,  # Scopus Search no incluye ORCID
                "scopus_id": auth.get("authid"),
                "is_institutional": False,  # Se determina después
            })

        # DOI
        doi = entry.get("prism:doi") or entry.get("doi")

        # Scopus ID
        scopus_id = entry.get("dc:identifier", "")  # Formato: SCOPUS_ID:xxxxx
        if scopus_id.startswith("SCOPUS_ID:"):
            scopus_id = scopus_id.replace("SCOPUS_ID:", "")

        # Año de publicación
        cover_date = entry.get("prism:coverDate", "")
        pub_year = int(cover_date[:4]) if cover_date and len(cover_date) >= 4 else None

        # Open Access
        oa_flag = entry.get("openaccessFlag")
        is_oa = oa_flag == "true" if oa_flag else None

        return StandardRecord(
            source_name=self.source_name,
            source_id=scopus_id,
            doi=doi,
            title=entry.get("dc:title"),
            publication_year=pub_year,
            publication_date=cover_date,
            publication_type=entry.get("subtypeDescription"),
            source_journal=entry.get("prism:publicationName"),
            issn=entry.get("prism:issn"),
            is_open_access=is_oa,
            authors=authors,
            citation_count=int(entry.get("citedby-count", 0)),
            url=None,  # Se puede obtener del link
            raw_data=entry,
        )

    # ---------------------------------------------------------
    # BÚSQUEDA POR DOI (para cruce con inventario)
    # ---------------------------------------------------------

    def search_by_doi(self, doi: str) -> Optional[StandardRecord]:
        """
        Busca un solo documento en Scopus por su DOI.

        Args:
            doi: DOI del documento (ej: 10.1016/j.jhydrol.2020.125741)

        Returns:
            StandardRecord si se encuentra, None si no.
        """
        if not self.api_key:
            raise ScopusAPIError("API key de Scopus no configurada.")

        # Limpiar DOI
        clean_doi = doi.strip()
        if clean_doi.startswith("https://doi.org/"):
            clean_doi = clean_doi.replace("https://doi.org/", "")
        elif clean_doi.startswith("http://doi.org/"):
            clean_doi = clean_doi.replace("http://doi.org/", "")

        query = f"DOI({clean_doi})"
        params = {
            "query": query,
            "count": 1,
            "field": (
                "dc:identifier,doi,dc:title,prism:publicationName,"
                "prism:coverDate,subtypeDescription,citedby-count,"
                "author,prism:issn,openaccess,openaccessFlag,"
                "dc:description,authkeywords,prism:volume,prism:issueIdentifier,"
                "prism:pageRange,afid,affiliation"
            ),
        }

        try:
            resp = self.session.get(
                self.SEARCH_URL,
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error buscando DOI {clean_doi} en Scopus: {e}")
            return None

        entries = data.get("search-results", {}).get("entry", [])
        if not entries or (len(entries) == 1 and "error" in entries[0]):
            return None

        try:
            record = self._parse_record(entries[0])
            record.compute_normalized_fields()
            return record
        except Exception as e:
            logger.warning(f"Error parseando resultado Scopus para DOI {clean_doi}: {e}")
            return None

    def search_by_dois(
        self, dois: List[str], delay: float = 0.25
    ) -> List[StandardRecord]:
        """
        Busca múltiples documentos en Scopus por DOI.

        Args:
            dois: Lista de DOIs a buscar.
            delay: Pausa entre peticiones (seg) para respetar rate-limit.

        Returns:
            Lista de StandardRecords encontrados.
        """
        records: List[StandardRecord] = []
        total = len(dois)

        for i, doi in enumerate(dois, 1):
            record = self.search_by_doi(doi)
            if record:
                records.append(record)
            if i % 50 == 0:
                logger.info(f"  Progreso Scopus DOI: {i}/{total} — encontrados: {len(records)}")
            if delay and i < total:
                time.sleep(delay)

        logger.info(
            f"Búsqueda Scopus por DOI completada: {len(records)} encontrados de {total} consultados."
        )
        return records

    # ---------------------------------------------------------
    # LÓGICA INTERNA
    # ---------------------------------------------------------

    def _build_query(
        self,
        year_from: Optional[int],
        year_to: Optional[int],
        affiliation_id: Optional[str],
    ) -> str:
        """
        Construye query Scopus.
        Ejemplo: AF-ID(60000000) AND PUBYEAR > 2019 AND PUBYEAR < 2026
        """
        parts = []

        if affiliation_id:
            parts.append(f"AF-ID({affiliation_id})")
        else:
            # Buscar por nombre de institución como fallback
            parts.append(f'AFFIL("{institution.name}")')

        if year_from:
            parts.append(f"PUBYEAR > {year_from - 1}")
        if year_to:
            parts.append(f"PUBYEAR < {year_to + 1}")

        return " AND ".join(parts)
