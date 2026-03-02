"""
Extractor dedicado al Serial Title API de Scopus (Elsevier).

Permite consultar metadatos de revistas (títulos de serie) por ISSN:
  - Años de cobertura en Scopus (coverageStartYear / coverageEndYear)
  - Estado: Active / Discontinued
  - Editorial, áreas temáticas

Documentación oficial:
  https://dev.elsevier.com/documentation/SerialTitleAPI.wadl

Requiere:
  - SCOPUS_API_KEY en variables de entorno
  - (Opcional) SCOPUS_INST_TOKEN para mayor cuota
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import scopus_config

logger = logging.getLogger(__name__)


def _clean_issn(value: str) -> str:
    """Normaliza un ISSN: quita espacios y guiones. Retorna '' si no es válido."""
    if not value:
        return ""
    cleaned = str(value).strip().replace("-", "").replace(" ", "")
    # ISSN válido: 7 dígitos + dígito/X
    import re
    if re.match(r"^[\dXx]{7,8}$", cleaned, re.IGNORECASE):
        return cleaned
    return ""


class SerialTitleAPIError(Exception):
    """Excepción para errores del Serial Title API de Scopus."""
    pass


class SerialTitleExtractor:
    """
    Extractor de metadatos de revistas desde el Serial Title API de Scopus.

    Uso:
        extractor = SerialTitleExtractor()
        result = extractor.get_journal_coverage("2595-3982")
        bulk   = extractor.get_bulk_coverage(["2595-3982", "0028-0836"])
        pubs   = extractor.check_publications_coverage(publications_list)
    """

    BASE_URL   = f"{scopus_config.base_url}/serial/title/issn"
    SEARCH_URL  = f"{scopus_config.base_url}/serial/title"      # búsqueda por título
    ABSTRACT_URL = f"{scopus_config.base_url}/article"           # para DOI → ISSN

    def __init__(self, api_key: str = None, inst_token: str = None):
        self.api_key = api_key or scopus_config.api_key
        self.inst_token = inst_token or scopus_config.inst_token

        if not self.api_key:
            logger.warning(
                "SCOPUS_API_KEY no configurada. "
                "Consíguela en https://dev.elsevier.com/"
            )

        self.session = self._create_session()
        logger.info("SerialTitleExtractor inicializado.")

    # ------------------------------------------------------------------
    # Sesión HTTP con reintentos
    # ------------------------------------------------------------------

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=scopus_config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "X-ELS-APIKey": self.api_key,
            "Accept": "application/json",
        })
        if self.inst_token:
            session.headers["X-ELS-Insttoken"] = self.inst_token
        return session

    # ------------------------------------------------------------------
    # Consulta individual
    # ------------------------------------------------------------------

    def get_journal_coverage(self, issn: str) -> dict:
        """
        Consulta el Serial Title API de Scopus para un ISSN.

        Args:
            issn: ISSN con o sin guion (ej: '2595-3982' o '25953982').

        Returns:
            dict con:
              issn, title, source_id, publisher,
              status, is_discontinued,
              coverage_from, coverage_to,
              subject_areas, error (si no se encontró)
        """
        if not self.api_key:
            raise SerialTitleAPIError("SCOPUS_API_KEY no configurada.")

        clean_issn = issn.strip().replace("-", "")
        url = f"{self.BASE_URL}/{clean_issn}"

        try:
            resp = self.session.get(
                url,
                params={"view": "ENHANCED"},
                timeout=scopus_config.timeout,
            )
            if resp.status_code == 404:
                return {"issn": issn, "error": "Revista no encontrada en Scopus."}
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error Serial Title API para ISSN {issn}: {e}")
            raise SerialTitleAPIError(f"Error en Serial Title API: {e}")

        return self._parse_entry(issn, data)

    # ------------------------------------------------------------------
    # Consulta masiva (paralela)
    # ------------------------------------------------------------------

    def get_bulk_coverage(
        self,
        issns: list[str],
        max_workers: int = 5,
        delay: float = 0.2,
    ) -> list[dict]:
        """
        Consulta múltiples ISSNs de forma concurrente.

        Args:
            issns:       Lista de ISSNs a consultar.
            max_workers: Máximo de hilos paralelos (default 5, respeta rate-limit).
            delay:       Pausa entre batches (segundos).

        Returns:
            Lista de dicts, uno por ISSN, en el mismo orden de entrada.
        """
        results: dict[str, dict] = {}

        def _fetch(issn: str) -> tuple[str, dict]:
            try:
                time.sleep(delay)
                return issn, self.get_journal_coverage(issn)
            except SerialTitleAPIError as e:
                return issn, {"issn": issn, "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch, issn): issn for issn in issns}
            for future in as_completed(futures):
                original_issn, result = future.result()
                results[original_issn] = result
                logger.info(f"  SerialTitle: procesado ISSN {original_issn}")

        # Devolver en el mismo orden que la entrada
        return [results[issn] for issn in issns]

    # ------------------------------------------------------------------
    # Búsqueda por nombre de revista (cuando no hay ISSN)
    # ------------------------------------------------------------------

    def get_issn_from_doi(self, doi: str) -> tuple[str | None, str | None]:
        """
        Consulta el Abstract Retrieval API de Scopus por DOI y extrae
        el ISSN (o ISBN) y el nombre de la fuente.

        Se usa como paso previo cuando una publicación no tiene ISSN
        pero sí tiene DOI, para luego poder consultar el Serial Title API.

        Args:
            doi: DOI del artículo (con o sin prefijo 'https://doi.org/').

        Returns:
            Tuple (issn, source_title) — cualquiera puede ser None.
        """
        doi_clean = doi.strip().lower()
        # Quitar prefijo de URL si viene con él
        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            if doi_clean.startswith(prefix):
                doi_clean = doi_clean[len(prefix):]
                break

        try:
            resp = self.session.get(
                f"{self.ABSTRACT_URL}/doi/{doi_clean}",
                params={"field": "prism:issn,prism:isbn,prism:publicationName", "view": "META"},
                timeout=scopus_config.timeout,
            )
            if resp.status_code in (404, 400):
                logger.debug(f"DOI no encontrado en Scopus: {doi_clean}")
                return None, None
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error consultando DOI '{doi_clean}': {e}")
            return None, None

        # La respuesta puede venir en abstracts-retrieval-response o full-text-retrieval-response
        coredata = (
            data.get("abstracts-retrieval-response", {})
            .get("coredata", {})
        ) or (
            data.get("full-text-retrieval-response", {})
            .get("coredata", {})
        )

        issn_raw = (
            coredata.get("prism:issn")
            or coredata.get("prism:eIssn")
            or coredata.get("prism:isbn")
        )
        # ISSN puede venir como lista o string
        if isinstance(issn_raw, list):
            issn_raw = issn_raw[0] if issn_raw else None
        if isinstance(issn_raw, dict):
            issn_raw = issn_raw.get("$") or issn_raw.get("#text")

        issn = _clean_issn(str(issn_raw)) if issn_raw else None

        source_title = coredata.get("prism:publicationName")
        if isinstance(source_title, dict):
            source_title = source_title.get("$")

        logger.debug(f"DOI {doi_clean} → ISSN={issn}, fuente={source_title}")
        return issn, source_title or None

    def search_journal_by_title(self, title: str) -> dict:
        """
        Busca una revista en Scopus por su nombre (título parcial o completo).

        Endpoint: GET https://api.elsevier.com/content/serial/title?title=...
        Vista: ENHANCED

        Args:
            title: Nombre de la revista (ej: 'BMC Complementary Medicine and Therapies').

        Returns:
            dict normalizado igual que get_journal_coverage(), usando el título
            como clave 'issn' para uniformidad. Retorna {"error": ...} si no encuentra.
        """
        if not self.api_key:
            raise SerialTitleAPIError("SCOPUS_API_KEY no configurada.")

        try:
            resp = self.session.get(
                self.SEARCH_URL,
                params={"title": title.strip(), "view": "ENHANCED", "count": 1},
                timeout=scopus_config.timeout,
            )
            if resp.status_code == 404:
                return {"issn": title, "error": f"Revista '{title}' no encontrada en Scopus."}
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error buscando revista por título '{title}': {e}")
            return {"issn": title, "error": str(e)}

        result = self._parse_entry(title, data)
        return result

    # ------------------------------------------------------------------
    # Verificación de cobertura para lista de publicaciones
    # ------------------------------------------------------------------

    def check_publications_coverage(
        self,
        publications: list[dict],
        max_workers: int = 5,
        delay: float = 0.2,
    ) -> list[dict]:
        """
        Enriquece una lista de publicaciones con datos de cobertura de sus
        revistas en Scopus.

        Lógica:
        1. Extrae journals únicos (por ISSN primero, luego por Source title).
        2. Consulta el Serial Title API deduplicando por journal.
        3. Determina si el año de publicación cae dentro de la cobertura.

        Args:
            publications: Lista de dicts. Cada dict debe contener al menos:
                - 'issn'         (str, puede ser None/vacío)
                - 'doi'          (str, DOI del artículo — fallback cuando no hay ISSN)
                - 'isbn'         (str, ISBN — usado como ISSN alternativo para libros/series)
                - 'source_title' (str, nombre de la revista — último fallback)
                - 'year'         (int o str, año de publicación)
                - 'title'        (str, título del artículo — para referencia)
            max_workers: Hilos paralelos para consultas a Scopus.
            delay: Pausa entre peticiones.

        Returns:
            La misma lista de dicts, cada uno enriquecido con:
                - 'scopus_journal_title'   nombre oficial en Scopus
                - 'scopus_publisher'       editorial
                - 'journal_status'         Active / Discontinued / Unknown
                - 'coverage_from'          int o None
                - 'coverage_to'            int o None
                - 'in_coverage'            'Sí' | 'No (antes de cobertura)' |
                                           'No (después de cobertura)' | 'Sin datos'
                - 'journal_found'          True/False
                - 'journal_subject_areas'  str separado por ' | '
                - 'coverage_error'         str o None
        """
        # ── 1. Identificar journals únicos ────────────────────────────
        # Prioridad: ISSN > ISBN (como ISSN) > DOI (para resolver ISSN) > nombre revista
        journal_keys: dict[str, dict] = {}   # key → {type, value}
        for pub in publications:
            issn = _clean_issn(pub.get("issn", ""))
            isbn = _clean_issn(pub.get("isbn", ""))   # ISBN puede funcionar como ISSN de serie
            doi  = (pub.get("doi") or "").strip()
            src  = (pub.get("source_title") or "").strip()

            if issn:
                key = f"issn:{issn}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "issn", "value": issn}
            elif isbn:
                # Algunos libros/series tienen ISSN válido donde va el ISBN
                key = f"issn:{isbn}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "issn", "value": isbn}
            elif doi:
                key = f"doi:{doi.lower()}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "doi", "value": doi}
            elif src:
                key = f"title:{src.lower()}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "title", "value": src}

        logger.info(
            f"check_publications_coverage: {len(publications)} publicaciones, "
            f"{len(journal_keys)} journals únicos a consultar."
        )

        # ── 2. Consultar Scopus por cada journal único ────────────────
        journal_cache: dict[str, dict] = {}

        def _fetch_journal(key: str, info: dict) -> tuple[str, dict]:
            time.sleep(delay)
            try:
                if info["type"] == "issn":
                    result = self.get_journal_coverage(info["value"])
                elif info["type"] == "doi":
                    # Paso 1: resolver DOI → ISSN via Abstract Retrieval API
                    resolved_issn, resolved_title = self.get_issn_from_doi(info["value"])
                    if resolved_issn:
                        result = self.get_journal_coverage(resolved_issn)
                    elif resolved_title:
                        result = self.search_journal_by_title(resolved_title)
                    else:
                        result = {"issn": info["value"], "error": "DOI no encontrado en Scopus o sin ISSN asociado."}
                else:
                    result = self.search_journal_by_title(info["value"])
                return key, result
            except SerialTitleAPIError as e:
                return key, {"issn": info["value"], "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_journal, k, v): k
                for k, v in journal_keys.items()
            }
            for future in as_completed(futures):
                key, result = future.result()
                journal_cache[key] = result
                logger.info(f"  Journal cacheado: {key}")

        # ── 3. Enriquecer cada publicación ────────────────────────────
        enriched = []
        for pub in publications:
            row = dict(pub)
            issn = _clean_issn(pub.get("issn", ""))
            isbn = _clean_issn(pub.get("isbn", ""))
            doi  = (pub.get("doi") or "").strip()
            src  = (pub.get("source_title") or "").strip()

            if issn:
                cache_key = f"issn:{issn}"
            elif isbn:
                cache_key = f"issn:{isbn}"
            elif doi:
                cache_key = f"doi:{doi.lower()}"
            elif src:
                cache_key = f"title:{src.lower()}"
            else:
                cache_key = None

            journal_info = journal_cache.get(cache_key) if cache_key else None

            if journal_info and not journal_info.get("error"):
                row["scopus_journal_title"]   = journal_info.get("title")
                row["scopus_publisher"]       = journal_info.get("publisher")
                row["journal_status"]         = journal_info.get("status", "Unknown")
                row["coverage_from"]          = journal_info.get("coverage_from")
                row["coverage_to"]            = journal_info.get("coverage_to")
                row["coverage_periods"]       = journal_info.get("coverage_periods", [])
                row["journal_found"]          = True
                row["coverage_error"]         = None
                areas = journal_info.get("subject_areas") or []
                row["journal_subject_areas"]  = " | ".join(areas) if areas else None

                # —— Verificación de cobertura contra TODOS los periodos ——
                try:
                    pub_year = int(pub.get("year") or 0)
                except (ValueError, TypeError):
                    pub_year = 0

                periods: list[tuple[int, int]] = journal_info.get("coverage_periods") or []
                cf = journal_info.get("coverage_from")
                ct = journal_info.get("coverage_to")

                if pub_year and periods:
                    if any(s <= pub_year <= e for s, e in periods):
                        row["in_coverage"] = "Sí"
                    elif pub_year < periods[0][0]:
                        row["in_coverage"] = "No (antes de cobertura)"
                    elif pub_year > periods[-1][1]:
                        row["in_coverage"] = "No (después de cobertura)"
                    else:
                        # Está entre periodos válidos pero en una laguna
                        row["in_coverage"] = "No (laguna de cobertura)"
                elif pub_year and cf and ct:
                    if cf <= pub_year <= ct:
                        row["in_coverage"] = "Sí"
                    elif pub_year < cf:
                        row["in_coverage"] = "No (antes de cobertura)"
                    else:
                        row["in_coverage"] = "No (después de cobertura)"
                elif pub_year and cf and not ct:
                    row["in_coverage"] = "Sí" if pub_year >= cf else "No (antes de cobertura)"
                else:
                    row["in_coverage"] = "Sin datos"
            else:
                row["scopus_journal_title"]  = None
                row["scopus_publisher"]      = None
                row["journal_status"]        = "No encontrada"
                row["coverage_from"]         = None
                row["coverage_to"]           = None
                row["coverage_periods"]      = []
                row["journal_found"]         = False
                row["journal_subject_areas"] = None
                row["in_coverage"]           = "Sin datos"
                row["coverage_error"]        = (
                    journal_info.get("error") if journal_info else "ISSN/título no disponible"
                )

            enriched.append(row)

        return enriched

    # ------------------------------------------------------------------
    # Parser de respuesta JSON
    # ------------------------------------------------------------------

    def _parse_entry(self, issn: str, data: dict) -> dict:
        """
        Parsea el JSON del Serial Title API y retorna un dict normalizado.
        Basado en la estructura real confirmada de la respuesta ENHANCED.
        """
        entry_list = (
            data.get("serial-metadata-response", {})
            .get("entry", [])
        )
        if not entry_list:
            return {"issn": issn, "error": "Sin datos en la respuesta de Scopus."}

        entry = entry_list[0]

        # ── Periodos de cobertura ──────────────────────────────────────
        # Scopus NO devuelve los periodos en un campo explícito.
        # coverageStartYear/EndYear solo reflejan el PRIMER año indexado.
        # La fuente real son los años con publicationCount > 0 en yearly-data.info,
        # que es exactamente lo que Scopus usa en su UI ("Years currently covered").
        periods_set: set[tuple[int, int]] = set()

        # Fuente principal: yearly-data.info → construir rangos consecutivos
        yearly_info = (entry.get("yearly-data") or {}).get("info") or []
        if isinstance(yearly_info, dict):
            yearly_info = [yearly_info]

        active_years: list[int] = sorted(
            int(y["@year"])
            for y in yearly_info
            if isinstance(y, dict)
            and y.get("@year")
            and int(y.get("publicationCount") or 0) > 0
        )

        if active_years:
            # Agrupar años consecutivos en rangos
            start = active_years[0]
            prev  = active_years[0]
            for yr in active_years[1:]:
                if yr == prev + 1:
                    prev = yr
                else:
                    periods_set.add((start, prev))
                    start = yr
                    prev  = yr
            periods_set.add((start, prev))

        # Fallback: si yearly-data no tiene datos, usar coverageStartYear/EndYear
        if not periods_set:
            try:
                sy = int(entry["coverageStartYear"]) if entry.get("coverageStartYear") else None
                ey = int(entry["coverageEndYear"])   if entry.get("coverageEndYear")   else None
            except (ValueError, TypeError):
                sy, ey = None, None
            if sy and ey:
                periods_set.add((sy, ey))
            elif sy:
                periods_set.add((sy, sy))

        coverage_periods: list[tuple[int, int]] = sorted(periods_set, key=lambda t: t[0])

        # Valores de resumen (primer inicio → último fin)
        coverage_from: Optional[int] = coverage_periods[0][0]  if coverage_periods else None
        coverage_to:   Optional[int] = coverage_periods[-1][1] if coverage_periods else None

        # ── Estado ────────────────────────────────────────────────────
        # Scopus no siempre retorna el estado explícitamente.
        # Se deriva del año final de cobertura.
        current_year = datetime.now().year
        explicit_status = (
            entry.get("sourceRecordStatus")
            or entry.get("@status")
            or entry.get("status")
        )
        if isinstance(explicit_status, dict):
            explicit_status = explicit_status.get("$")

        if explicit_status:
            status = explicit_status
        elif coverage_to and coverage_to >= current_year - 1:
            status = "Active"
        elif coverage_to:
            status = "Discontinued"
        else:
            status = "Unknown"

        logger.info(
            f"  _parse_entry ISSN={issn}: explicit_status={explicit_status!r} "
            f"coverage_to={coverage_to} → status={status!r}"
        )

        is_discontinued = status.lower() in ("inactive", "discontinued")

        # ── Editorial ─────────────────────────────────────────────────
        publisher = (
            entry.get("dc:publisher")
            or entry.get("publisher")
            or entry.get("prism:publisher")
        )
        if isinstance(publisher, dict):
            publisher = publisher.get("$")

        # ── Áreas temáticas ───────────────────────────────────────────
        subject_areas = []
        for area in entry.get("subject-area", []) or []:
            if isinstance(area, dict):
                abbr = area.get("@abbrev", "")
                name = area.get("$", "")
                subject_areas.append(f"{abbr}: {name}" if abbr else name)

        return {
            "issn": issn,
            "title": entry.get("dc:title"),
            "source_id": entry.get("source-id"),
            "publisher": publisher,
            "status": status,
            "is_discontinued": is_discontinued,
            "coverage_from": coverage_from,
            "coverage_to": coverage_to,
            "coverage_periods": coverage_periods,   # lista de (start, end)
            "subject_areas": subject_areas,
            "error": None,
        }
