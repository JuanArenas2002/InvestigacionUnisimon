"""
Extractor de Datos Abiertos Colombia (datos.gov.co).

Usa SODA (Socrata Open Data API) para consultar datasets públicos
de producción científica colombiana.

Datasets relevantes:
  - Producción bibliográfica de grupos de investigación
  - Investigadores reconocidos por Minciencias
  - Grupos de investigación
"""

import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import datos_abiertos_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord

logger = logging.getLogger(__name__)


class DatosAbiertosError(Exception):
    """Excepción para errores de la API de Datos Abiertos"""
    pass


class DatosAbiertosExtractor(BaseExtractor):
    """
    Extractor de producción científica desde Datos Abiertos Colombia.

    Usa SODA API (Socrata) para consultar datasets.

    Datasets de ejemplo:
      - Producción bibliográfica: https://www.datos.gov.co/resource/XXXX.json
      - Grupos: https://www.datos.gov.co/resource/YYYY.json

    Configurar:
      - DATOS_ABIERTOS_TOKEN: App token de Socrata (opcional pero recomendado)
    """

    source_name = SourceName.DATOS_ABIERTOS

    def __init__(
        self,
        dataset_id: str = None,
        app_token: str = None,
    ):
        """
        Args:
            dataset_id: ID del dataset en datos.gov.co (e.g., "abc1-def2")
            app_token: Token de aplicación Socrata
        """
        self.dataset_id = dataset_id
        self.app_token = app_token or datos_abiertos_config.app_token
        self.config = datos_abiertos_config
        self.session = self._create_session()

        logger.info("DatosAbiertosExtractor inicializado.")

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        headers = {"Accept": "application/json"}
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        session.headers.update(headers)

        return session

    # ---------------------------------------------------------
    # INTERFAZ BaseExtractor
    # ---------------------------------------------------------

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
        Consulta un dataset de Datos Abiertos Colombia.

        Args:
            year_from: Año inicial
            year_to: Año final
            max_results: Límite de resultados
            dataset_id: ID del dataset (sobrescribe el configurado)
            institution_filter: Nombre de institución para filtrar
        """
        ds_id = dataset_id or self.dataset_id
        if not ds_id:
            raise ValueError(
                "Debes especificar un dataset_id. "
                "Encuentra datasets en https://www.datos.gov.co/"
            )

        base_url = f"{self.config.base_url}/{ds_id}.json"
        records: List[StandardRecord] = []
        offset = 0
        limit = min(self.config.max_per_page, max_results or self.config.max_per_page)
        total_fetched = 0

        # Construir filtro SoQL
        where_clause = self._build_where(year_from, year_to, institution_filter)

        logger.info(f"Consultando dataset: {ds_id}")
        if where_clause:
            logger.info(f"  Filtro: {where_clause}")

        while True:
            params = {
                "$limit": limit,
                "$offset": offset,
                "$order": ":id",
            }
            if where_clause:
                params["$where"] = where_clause

            try:
                resp = self.session.get(
                    base_url,
                    params=params,
                    timeout=self.config.timeout,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.exceptions.RequestException as e:
                raise DatosAbiertosError(f"Error en Datos Abiertos API: {e}")

            if not data:
                break

            for entry in data:
                try:
                    record = self._parse_record(entry)
                    records.append(record)
                    total_fetched += 1
                    if max_results and total_fetched >= max_results:
                        break
                except Exception as e:
                    logger.warning(f"Error parseando registro: {e}")
                    continue

            logger.info(f"  Extraídos: {total_fetched}")

            if max_results and total_fetched >= max_results:
                break

            if len(data) < limit:
                break  # No hay más datos

            offset += limit
            time.sleep(0.1)

        return self._post_process(records)

    def _parse_record(self, entry: dict) -> StandardRecord:
        """
        Convierte un registro de Datos Abiertos a StandardRecord.

        NOTA: Los campos dependen del dataset específico.
        Esta implementación cubre campos comunes en datasets de
        producción bibliográfica de Minciencias.
        Se debe adaptar al dataset real que se use.
        """
        # Intentar obtener campos con nombres comunes
        # (los nombres varían entre datasets)
        title = (
            entry.get("titulo_del_articulo")
            or entry.get("titulo")
            or entry.get("nombre_del_producto")
            or entry.get("title")
        )

        year = (
            entry.get("ano")
            or entry.get("anio")
            or entry.get("ano_de_publicacion")
            or entry.get("year")
        )
        if year:
            try:
                year = int(str(year)[:4])
            except (ValueError, TypeError):
                year = None

        doi = entry.get("doi") or entry.get("identificador_doi")

        journal = (
            entry.get("revista")
            or entry.get("nombre_de_la_revista")
            or entry.get("fuente")
        )

        issn = entry.get("issn")

        # Autores (pueden venir como string separado por ;)
        authors_raw = (
            entry.get("autores")
            or entry.get("autor")
            or entry.get("nombres_autores")
            or ""
        )
        authors = []
        if authors_raw:
            for name in str(authors_raw).split(";"):
                name = name.strip()
                if name:
                    authors.append({
                        "name": name,
                        "orcid": None,
                        "is_institutional": False,
                    })

        # Tipo
        pub_type = (
            entry.get("tipo_de_producto")
            or entry.get("tipo")
            or "article"
        )

        return StandardRecord(
            source_name=self.source_name,
            source_id=entry.get(":id") or entry.get("id"),
            doi=doi,
            title=title,
            publication_year=year,
            publication_type=pub_type,
            source_journal=journal,
            issn=issn,
            authors=authors,
            raw_data=entry,
        )

    # ---------------------------------------------------------
    # LÓGICA INTERNA
    # ---------------------------------------------------------

    def _build_where(
        self,
        year_from: Optional[int],
        year_to: Optional[int],
        institution_filter: Optional[str],
    ) -> str:
        """
        Construye cláusula WHERE en SoQL (SQL de Socrata).
        """
        conditions = []

        # Filtro de año (campo genérico, adaptar al dataset)
        if year_from:
            conditions.append(f"ano >= '{year_from}'")
        if year_to:
            conditions.append(f"ano <= '{year_to}'")

        if institution_filter:
            conditions.append(f"upper(institucion) like upper('%{institution_filter}%')")

        return " AND ".join(conditions) if conditions else ""
