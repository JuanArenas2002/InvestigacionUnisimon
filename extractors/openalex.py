"""
Extractor de OpenAlex API.

Refactorizado para usar la interfaz BaseExtractor y producir StandardRecord.
Conserva toda la lógica robusta del extractor original (retry, paginación, rate limit).
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import openalex_config, institution, SourceName
from extractors.base import (
    BaseExtractor,
    StandardRecord,
    normalize_doi,
    normalize_year,
    normalize_author_name,
)

logger = logging.getLogger(__name__)


class OpenAlexAPIError(Exception):
    """Excepción para errores de la API de OpenAlex"""
    pass


class OpenAlexExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde OpenAlex API.

    Usa cursor-based pagination, retry con backoff exponencial,
    y rate limiting para respetar los límites de la API.
    """

    source_name = SourceName.OPENALEX

    def __init__(
        self,
        ror_id: str = None,
        email: str = None,
        max_retries: int = None,
    ):
        self.ror_id = self._validate_ror_id(ror_id or institution.ror_id)
        self.email = email or institution.contact_email
        self.config = openalex_config
        self.session = self._create_session(max_retries or self.config.max_retries)

        logger.info(f"OpenAlexExtractor inicializado para ROR: {self.ror_id}")

    # ---------------------------------------------------------
    # INTERFAZ BaseExtractor
    # ---------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        publication_types: Optional[List[str]] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae publicaciones de OpenAlex y las devuelve como StandardRecord.
        """
        self._validate_year_range(year_from, year_to)

        filters = self._build_filters(year_from, year_to, publication_types)
        params = self._build_query_params(filters, self.config.max_per_page)

        records: List[StandardRecord] = []
        total_fetched = 0

        logger.info(f"Extrayendo de OpenAlex: {year_from or 'inicio'} – {year_to or 'presente'}")

        try:
            while True:
                data = self._fetch_page(params)
                results = data.get("results", [])

                if not results:
                    break

                for work in results:
                    try:
                        record = self._parse_record(work)
                        records.append(record)
                        total_fetched += 1

                        if max_results and total_fetched >= max_results:
                            break
                    except Exception as e:
                        logger.warning(f"Error parseando work: {e}")
                        continue

                logger.info(f"  Extraídos: {total_fetched}")

                if max_results and total_fetched >= max_results:
                    break

                next_cursor = data.get("meta", {}).get("next_cursor")
                if not next_cursor:
                    break

                params["cursor"] = next_cursor
                time.sleep(self.config.rate_limit_delay)

        except requests.exceptions.RequestException as e:
            raise OpenAlexAPIError(f"Error comunicándose con OpenAlex: {e}")

        return self._post_process(records)

    def _parse_record(self, work: dict) -> StandardRecord:
        """Convierte un work de OpenAlex a StandardRecord"""
        ids_data = work.get("ids") or {}
        primary_location = work.get("primary_location") or {}
        source_data = primary_location.get("source") or {}
        open_access = work.get("open_access") or {}

        # Autores
        all_authors, institutional_authors = self._extract_authors(work)

        # URL
        url = (
            primary_location.get("landing_page_url")
            or work.get("doi")
            or work.get("id")
        )

        return StandardRecord(
            source_name=self.source_name,
            source_id=work.get("id"),
            doi=work.get("doi"),
            pmid=ids_data.get("pmid"),
            pmcid=ids_data.get("pmcid"),
            title=work.get("title"),
            publication_year=work.get("publication_year"),
            publication_date=work.get("publication_date"),
            publication_type=work.get("type"),
            language=work.get("language"),
            source_journal=source_data.get("display_name"),
            issn=source_data.get("issn_l"),
            is_open_access=open_access.get("is_oa", False),
            oa_status=open_access.get("oa_status"),
            authors=all_authors,
            institutional_authors=institutional_authors,
            citation_count=work.get("cited_by_count", 0),
            url=url,
            raw_data=work,
        )

    # ---------------------------------------------------------
    # LÓGICA INTERNA
    # ---------------------------------------------------------

    @staticmethod
    def _validate_ror_id(ror_id: str) -> str:
        if not ror_id:
            raise ValueError("ROR ID no puede estar vacío")
        if not ror_id.startswith("https://ror.org/"):
            if ror_id.startswith("ror.org/"):
                ror_id = f"https://{ror_id}"
            elif "/" not in ror_id:
                ror_id = f"https://ror.org/{ror_id}"
            else:
                raise ValueError(f"Formato de ROR ID inválido: {ror_id}")
        return ror_id

    def _create_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @staticmethod
    def _validate_year_range(year_from, year_to):
        current_year = datetime.now().year
        if year_from and (year_from < 1900 or year_from > current_year + 1):
            raise ValueError(f"Año inicial inválido: {year_from}")
        if year_to and (year_to < 1900 or year_to > current_year + 1):
            raise ValueError(f"Año final inválido: {year_to}")
        if year_from and year_to and year_from > year_to:
            raise ValueError(f"year_from ({year_from}) > year_to ({year_to})")

    def _build_filters(self, year_from, year_to, pub_types):
        filters = [f"authorships.institutions.ror:{self.ror_id}"]
        if year_from and year_to:
            filters.append(f"publication_year:{year_from}-{year_to}")
        elif year_from:
            filters.append(f"from_publication_date:{year_from}-01-01")
        elif year_to:
            filters.append(f"to_publication_date:{year_to}-12-31")
        if pub_types:
            filters.append(f"type:{'|'.join(pub_types)}")
        return ",".join(filters)

    def _build_query_params(self, filters, per_page):
        return {
            "filter": filters,
            "per_page": min(per_page, self.config.max_per_page),
            "mailto": self.email,
            "cursor": "*",
        }

    def _fetch_page(self, params):
        try:
            resp = self.session.get(
                self.config.base_url,
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            raise OpenAlexAPIError("Timeout en OpenAlex API")
        except requests.exceptions.HTTPError as e:
            raise OpenAlexAPIError(f"HTTP {e.response.status_code}: {e}")
        except json.JSONDecodeError:
            raise OpenAlexAPIError("Respuesta inválida (no es JSON)")

    def _extract_authors(self, work: dict):
        """Retorna (all_authors, institutional_authors) como listas de dicts"""
        all_authors = []
        institutional = []

        for authorship in work.get("authorships", []):
            author_data = authorship.get("author") or {}
            raw_name = author_data.get("display_name") or ""
            clean_name = normalize_author_name(raw_name)

            info = {
                "name": clean_name,
                "orcid": author_data.get("orcid"),
                "openalex_id": author_data.get("id"),
                "is_institutional": False,
            }
            all_authors.append(info)

            for inst in authorship.get("institutions", []):
                if inst.get("ror") == self.ror_id:
                    info["is_institutional"] = True
                    institutional.append(info)
                    break

        return all_authors, institutional

    # ---------------------------------------------------------
    # UTILIDADES
    # ---------------------------------------------------------

    def save_to_json(
        self,
        records: List[StandardRecord],
        filename: str,
        output_dir: str = "OpenAlexJson",
    ) -> Path:
        """Guarda registros en JSON"""
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        filepath = path / filename

        data = [r.to_dict() for r in records]
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Guardado: {filepath}")
        return filepath
