"""
Extractor de Web of Science API (Clarivate).

Requiere API Key de Clarivate Developer Portal:
  https://developer.clarivate.com/

Usa el endpoint WoS Starter API (más accesible que Expanded API).
"""

import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import wos_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord

logger = logging.getLogger(__name__)


class WosAPIError(Exception):
    """Excepción para errores de la API de Web of Science"""
    pass


class WosExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde Web of Science Starter API.

    Documentación: https://developer.clarivate.com/apis/wos-starter

    Requiere configurar en variables de entorno:
      - WOS_API_KEY: API key de Clarivate
    """

    source_name = SourceName.WOS

    def __init__(self, api_key: str = None):
        self.api_key = api_key or wos_config.api_key
        self.config = wos_config

        if not self.api_key:
            logger.warning(
                "WOS_API_KEY no configurada. "
                "Obten una en https://developer.clarivate.com/"
            )

        self.session = self._create_session()
        logger.info("WosExtractor inicializado.")

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

        session.headers.update({
            "X-ApiKey": self.api_key,
            "Accept": "application/json",
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
        org_enhanced: Optional[str] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Busca publicaciones en Web of Science.

        Args:
            year_from: Año inicial
            year_to: Año final
            max_results: Límite de resultados
            org_enhanced: Nombre de organización en WoS (Organization-Enhanced)
        """
        if not self.api_key:
            raise WosAPIError(
                "API key de WoS no configurada. "
                "Configura WOS_API_KEY en las variables de entorno."
            )

        query = self._build_query(year_from, year_to, org_enhanced)
        records: List[StandardRecord] = []
        page = 1
        total_fetched = 0

        logger.info(f"Buscando en WoS: {query}")

        while True:
            params = {
                "q": query,
                "limit": self.config.max_per_page,
                "page": page,
                "sortField": "PY",  # Publication Year
            }

            try:
                resp = self.session.get(
                    f"{self.config.base_url}/documents",
                    params=params,
                    timeout=self.config.timeout,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e:
                raise WosAPIError(f"Error en WoS API: {e}")

            hits = data.get("hits", [])
            if not hits:
                break

            for hit in hits:
                try:
                    record = self._parse_record(hit)
                    records.append(record)
                    total_fetched += 1
                    if max_results and total_fetched >= max_results:
                        break
                except Exception as e:
                    logger.warning(f"Error parseando hit WoS: {e}")
                    continue

            logger.info(f"  Extraídos de WoS: {total_fetched}")

            if max_results and total_fetched >= max_results:
                break

            # Paginación
            metadata = data.get("metadata", {})
            total_records = metadata.get("total", 0)
            if page * self.config.max_per_page >= total_records:
                break

            page += 1
            time.sleep(0.5)  # WoS es más estricto con rate limit

        return self._post_process(records)

    def _parse_record(self, hit: dict) -> StandardRecord:
        """Convierte un hit de WoS Starter a StandardRecord"""

        # Identificadores
        identifiers = hit.get("identifiers", {})
        doi = identifiers.get("doi")
        wos_id = hit.get("uid", "")

        # Autores
        authors = []
        for name_entry in hit.get("names", {}).get("authors", []) or []:
            authors.append({
                "name": name_entry.get("displayName") or name_entry.get("wosStandard"),
                "orcid": None,
                "wos_id": None,
                "is_institutional": False,
            })

        # Año
        pub_year = hit.get("source", {}).get("publishYear")
        if pub_year:
            pub_year = int(pub_year)

        # Tipo
        doc_types = hit.get("source", {}).get("sourceType", [])
        pub_type = doc_types if isinstance(doc_types, str) else (doc_types[0] if doc_types else None)

        # Fuente
        source_title = hit.get("source", {}).get("sourceTitle")

        source = hit.get("source") or {}
        pages = source.get("pages") or {}

        # Abstract
        abstract = None
        abstracts_items = (hit.get("abstracts") or {}).get("items") or []
        if abstracts_items:
            abstract = abstracts_items[0].get("value")

        record = StandardRecord(
            source_name=self.source_name,
            source_id=wos_id,
            doi=doi,
            title=hit.get("title"),
            publication_year=pub_year,
            publication_type=pub_type,
            source_journal=source_title,
            authors=authors,
            citation_count=hit.get("citations", [{}])[0].get("count", 0) if hit.get("citations") else 0,
            abstract=abstract,
            page_range=pages.get("range") or pages.get("compact"),
            publisher=source.get("publisherName"),
            raw_data=hit,
        )
        record.compute_normalized_fields()
        return record

    def extract_by_author(
        self,
        wos_author_id: str,
        max_results: int = 50,
    ) -> List[StandardRecord]:
        """
        Busca publicaciones de un autor en WoS por su Researcher ID (RI field).

        Args:
            wos_author_id: Researcher ID de WoS (ej. A-1234-2010)
            max_results: Límite de resultados

        Returns:
            Lista de StandardRecords del autor.
        """
        if not self.api_key:
            return []
        clean_id = str(wos_author_id).strip()
        params = {
            "q": f'AI=("{clean_id}")',
            "limit": min(max_results, self.config.max_per_page),
            "page": 1,
        }
        records: List[StandardRecord] = []
        try:
            resp = self.session.get(
                f"{self.config.base_url}/documents",
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])
            for hit in hits[:max_results]:
                try:
                    rec = self._parse_record(hit)
                    rec.compute_normalized_fields()
                    records.append(rec)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"extract_by_author wos_id={clean_id!r}: {e}")
        return records

    def search_by_doi(self, doi: str) -> Optional[StandardRecord]:
        """
        Busca un documento en WoS Starter API por DOI.
        Retorna StandardRecord o None si no encontrado / sin API key.
        """
        if not self.api_key:
            return None
        clean_doi = doi.strip().lstrip("https://doi.org/").lstrip("http://doi.org/")
        params = {"q": f'DO="{clean_doi}"', "limit": 1, "page": 1}
        try:
            resp = self.session.get(
                f"{self.config.base_url}/documents",
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])
            if hits:
                record = self._parse_record(hits[0])
                record.compute_normalized_fields()
                return record
        except Exception as e:
            logger.debug(f"WoS search_by_doi {clean_doi!r}: {e}")
        return None

    # ---------------------------------------------------------
    # LÓGICA INTERNA
    # ---------------------------------------------------------

    def _build_query(
        self,
        year_from: Optional[int],
        year_to: Optional[int],
        org_enhanced: Optional[str],
    ) -> str:
        """
        Construye query WoS.
        Ejemplo: OG=(Universidad de Antioquia) AND PY=(2020-2025)
        """
        parts = []

        org = org_enhanced or institution.name
        parts.append(f"OG=({org})")

        if year_from and year_to:
            parts.append(f"PY=({year_from}-{year_to})")
        elif year_from:
            parts.append(f"PY=(>={year_from})")
        elif year_to:
            parts.append(f"PY=(<={year_to})")

        return " AND ".join(parts)
