"""
Extractor de publicaciones desde OpenAlex API (vía PyAlex).

Responsabilidades:
  - Extracción masiva de publicaciones por ROR institucional (cursor-based pagination).
  - Búsqueda puntual de un work por DOI.
  - Serialización a JSON.

PyAlex se encarga de:
  - cursor-based pagination (paginate())
  - retry con backoff exponencial
  - polite pool (mailto)
  - rate limiting automático
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pyalex
from pyalex import Works

from config import openalex_config, institution, SourceName
from extractors.base import (
    BaseExtractor,
    StandardRecord,
    normalize_doi,
    normalize_author_name,
)
from ._rate_limit import OpenAlexAPIError

logger = logging.getLogger(__name__)


class OpenAlexExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde OpenAlex API (vía PyAlex).

    PyAlex se encarga de:
      - cursor-based pagination (paginate())
      - retry con backoff exponencial
      - polite pool (mailto)
      - rate limiting automático
    """

    source_name = SourceName.OPENALEX

    def __init__(
        self,
        ror_id: str = None,
        email: str = None,
        max_retries: int = None,
    ):
        self.ror_id = self._validate_ror_id(ror_id or institution.ror_id)
        self.email  = email or institution.contact_email
        self.config = openalex_config

        # Configurar PyAlex globalmente (polite pool + retry)
        pyalex.config.email                = self.email
        pyalex.config.max_retries          = max_retries or self.config.max_retries
        pyalex.config.retry_backoff_factor = 0.5
        pyalex.config.retry_http_codes     = [429, 500, 502, 503, 504]

        logger.info(f"OpenAlexExtractor inicializado para ROR: {self.ror_id}")

    # ---------------------------------------------------------
    # INTERFAZ BaseExtractor
    # ---------------------------------------------------------

    def extract(
        self,
        year_from: Optional[int] = None,
        year_to:   Optional[int] = None,
        max_results: Optional[int] = None,
        publication_types: Optional[List[str]] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae publicaciones de OpenAlex filtrando por ROR de la institución.
        PyAlex maneja automáticamente la paginación con cursor.
        """
        self._validate_year_range(year_from, year_to)

        # ── Construir query ──────────────────────────────────────────────────
        query = Works().filter(
            authorships={"institutions": {"ror": self.ror_id}}
        )

        if year_from and year_to:
            query = query.filter(publication_year=f"{year_from}-{year_to}")
        elif year_from:
            query = query.filter(from_publication_date=f"{year_from}-01-01")
        elif year_to:
            query = query.filter(to_publication_date=f"{year_to}-12-31")

        if publication_types:
            query = query.filter(type="|".join(publication_types))

        logger.info(
            f"Extrayendo de OpenAlex: "
            f"{year_from or 'inicio'} – {year_to or 'presente'}"
        )

        records: List[StandardRecord] = []
        total_fetched = 0

        try:
            for work in query.paginate(
                per_page=self.config.max_per_page,
                n_max=max_results,
            ):
                try:
                    record = self._parse_record(work)
                    records.append(record)
                    total_fetched += 1
                except Exception as e:
                    logger.warning(f"Error parseando work: {e}")
                    continue

                if total_fetched % 200 == 0:
                    logger.info(f"  Extraídos: {total_fetched}")

        except Exception as e:
            raise OpenAlexAPIError(f"Error comunicándose con OpenAlex: {e}")

        logger.info(f"Extracción completa: {total_fetched} registros")
        return self._post_process(records)

    def search_by_doi(self, doi: str) -> Optional[StandardRecord]:
        """
        Busca un work de OpenAlex por DOI y lo devuelve como StandardRecord.
        Retorna None si no existe (404) o si hay error.
        """
        doi_clean = normalize_doi(str(doi or "").strip())
        if not doi_clean:
            return None
        doi_url = (
            doi_clean
            if doi_clean.startswith("https://")
            else f"https://doi.org/{doi_clean}"
        )
        try:
            work = Works()[doi_url]
            return self._parse_record(work)
        except Exception as e:
            logger.debug(f"search_by_doi: DOI {doi_clean!r} → {e}")
            return None

    def _parse_record(self, work: dict) -> StandardRecord:
        """Convierte un work de OpenAlex a StandardRecord."""
        ids_data         = work.get("ids") or {}
        primary_location = work.get("primary_location") or {}
        source_data      = primary_location.get("source") or {}
        open_access      = work.get("open_access") or {}

        all_authors, institutional_authors = self._extract_authors(work)

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

    @staticmethod
    def _validate_year_range(year_from, year_to):
        current_year = datetime.now().year
        if year_from and (year_from < 1900 or year_from > current_year + 1):
            raise ValueError(f"Año inicial inválido: {year_from}")
        if year_to and (year_to < 1900 or year_to > current_year + 1):
            raise ValueError(f"Año final inválido: {year_to}")
        if year_from and year_to and year_from > year_to:
            raise ValueError(f"year_from ({year_from}) > year_to ({year_to})")

    def _extract_authors(self, work: dict):
        """Retorna (all_authors, institutional_authors) como listas de dicts."""
        all_authors  = []
        institutional = []

        for authorship in work.get("authorships", []):
            author_data = authorship.get("author") or {}
            raw_name    = author_data.get("display_name") or ""
            clean_name  = normalize_author_name(raw_name)

            info = {
                "name":             clean_name,
                "orcid":            author_data.get("orcid"),
                "openalex_id":      author_data.get("id"),
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
        filename:   str,
        output_dir: str = "OpenAlexJson",
    ) -> Path:
        """Guarda registros en JSON."""
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        filepath = path / filename

        data = [r.to_dict() for r in records]
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Guardado: {filepath}")
        return filepath
