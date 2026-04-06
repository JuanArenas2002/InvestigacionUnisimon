"""
Extractor de metadatos de revistas desde el Serial Title API de Scopus.

El Serial Title API de Scopus (Elsevier) permite consultar metadatos de
revistas científicas por ISSN, E-ISSN, Scopus Source ID o nombre:
  - Años de cobertura en Scopus (periodos con publicaciones indexadas).
  - Estado: Active / Inactiva / Discontinued.
  - Editorial, áreas temáticas, ISSNs normalizados.

Este módulo usa arquitectura DDD:
  infrastructure.http_client    → ScopusSerialClient (rate limiting, sesión)
  infrastructure.disk_cache     → caché persistente en disco (7 días TTL)
  domain.journal_coverage       → parseo JSON, periodos de cobertura, estado
  application.coverage_service  → orquestación de lookup masivo y enriquecimiento

Documentación del API:
  https://dev.elsevier.com/documentation/SerialTitleAPI.wadl

Autenticación:
  SCOPUS_API_KEY: API key de Elsevier (requerido).
  SCOPUS_INST_TOKEN: Token institucional (opcional, mayor cuota).
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict

from config import scopus_config
from extractors.serial_title._exceptions import SerialTitleAPIError
from extractors.serial_title.domain.journal_coverage import (
    parse_entry_json, title_similarity, clean_issn, is_issn_format,
    check_year_in_coverage,
)
from extractors.serial_title.application.coverage_service import (
    build_journal_keys, enrich_publication,
)
from extractors.serial_title.infrastructure import http_client, disk_cache

logger = logging.getLogger(__name__)


class SerialTitleExtractor:
    """
    Extractor de metadatos de revistas científicas desde el Serial Title API de Scopus.

    Provee tres modos de uso:
      1. Consulta individual: get_journal_coverage(issn)
      2. Consulta masiva paralela: get_bulk_coverage(issns, max_workers=5)
      3. Enriquecimiento de publicaciones: check_publications_coverage(publications)

    Características:
      - Caché en disco (7 días): evita re-consultar revistas ya conocidas.
      - Rate limiting coordinado entre threads (evita 429 en bulk).
      - Fallback chain: EID → ISSN → DOI → título de revista.
      - Búsqueda dual: verifica ISSN y E-ISSN para mayor confiabilidad.
      - Detección de cero inicial perdido por Excel (ISSNs de 7 dígitos).

    Uso típico:
        extractor = SerialTitleExtractor()
        result = extractor.get_journal_coverage("2595-3982")
        bulk   = extractor.get_bulk_coverage(["2595-3982", "0028-0836"])
        pubs   = extractor.check_publications_coverage(publications_list)
    """

    BASE_URL     = f"{scopus_config.base_url}/serial/title/issn"
    SEARCH_URL   = f"{scopus_config.base_url}/serial/title"
    ABSTRACT_URL = f"{scopus_config.base_url}/article"

    def __init__(self, api_key: str = None, inst_token: str = None):
        """
        Inicializa el extractor y crea el cliente HTTP autenticado.

        Args:
            api_key:    API key de Elsevier. Si es None, usa SCOPUS_API_KEY del entorno.
            inst_token: Token institucional (opcional, mayor cuota).
        """
        self.api_key    = api_key    or scopus_config.api_key
        self.inst_token = inst_token or scopus_config.inst_token

        if not self.api_key:
            logger.warning(
                "SCOPUS_API_KEY no configurada. "
                "Consíguela en https://dev.elsevier.com/"
            )

        session = http_client.create_session(
            config=scopus_config,
            api_key=self.api_key,
            inst_token=self.inst_token,
        )
        # El cliente encapsula rate limiting y los dos tipos de GET
        self._client = http_client.ScopusSerialClient(
            session=session,
            config=scopus_config,
        )
        logger.info("SerialTitleExtractor inicializado.")

    # ------------------------------------------------------------------
    # Consulta individual por ISSN
    # ------------------------------------------------------------------

    def get_journal_coverage(self, issn: str) -> dict:
        """
        Consulta el Serial Title API de Scopus para un ISSN o E-ISSN.

        Flujo:
          1. Consulta el ISSN dado.
          2. Si el resultado es 'Active', busca también el identificador
             complementario (E-ISSN si se buscó ISSN, viceversa) para
             reforzar la confiabilidad del resultado.

        Args:
            issn: ISSN o E-ISSN con o sin guión (ej: '2595-3982' o '25953982').

        Returns:
            Dict normalizado con claves: issn, identifier_type, title,
            source_id, publisher, status, is_discontinued, coverage_from,
            coverage_to, coverage_periods, subject_areas, error.
            Si no se encuentra: {'issn': issn, 'error': '...'}.

        Raises:
            SerialTitleAPIError: Si la API falla o la API key no está configurada.
        """
        if not self.api_key:
            raise SerialTitleAPIError("SCOPUS_API_KEY no configurada.")

        clean = issn.strip().replace("-", "")
        url = f"{self.BASE_URL}/{clean}"

        try:
            resp = self._client.get(url, {"view": "ENHANCED"})
            if resp.status_code == 404:
                return {"issn": issn, "error": "Revista no encontrada en Scopus."}
            if resp.status_code == 429:
                return {"issn": issn, "error": "Rate limit Scopus (429) — reintenta más tarde."}
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[SerialTitle] Error para ISSN {issn}: {e}")
            raise SerialTitleAPIError(f"Error en Serial Title API: {e}")

        result = parse_entry_json(issn, data)

        # Verificación dual: buscar el identificador complementario si el resultado
        # es 'Active' para reforzar la confiabilidad
        if not result.get("error") and result.get("status") == "Active":
            resolved_issn  = result.get("resolved_issn")  or ""
            resolved_eissn = result.get("resolved_eissn") or ""
            id_type = result.get("identifier_type", "issn")

            complementary = None
            if id_type == "issn" and resolved_eissn and resolved_eissn != clean:
                complementary = resolved_eissn
            elif id_type == "eissn" and resolved_issn and resolved_issn != clean:
                complementary = resolved_issn

            if complementary:
                try:
                    resp_c = self._client.get(f"{self.BASE_URL}/{complementary}", {"view": "ENHANCED"})
                    if resp_c.status_code == 200:
                        result_c = parse_entry_json(complementary, resp_c.json())
                        if not result_c.get("error"):
                            result["_complementary_data"] = result_c
                except Exception as e:
                    logger.debug(f"[SerialTitle] Error en dual-verify {complementary}: {e}")

        return result

    # ------------------------------------------------------------------
    # Consulta por Scopus Source ID
    # ------------------------------------------------------------------

    def get_journal_coverage_by_source_id(self, source_id: str) -> dict:
        """
        Consulta el Serial Title API por Scopus Source ID (srcid).

        Más confiable que ISSN cuando se tiene el source_id porque
        el source_id es el identificador interno de Scopus y no tiene
        problemas de variantes de formato.

        Args:
            source_id: Scopus Source ID (ej: '21100830991').

        Returns:
            Dict normalizado igual que get_journal_coverage().
        """
        if not self.api_key:
            raise SerialTitleAPIError("SCOPUS_API_KEY no configurada.")

        try:
            resp = self._client.get(
                self.SEARCH_URL,
                {"srcid": source_id.strip(), "view": "ENHANCED"},
            )
            if resp.status_code == 404:
                return {"issn": source_id, "error": "Revista no encontrada por source_id."}
            if resp.status_code == 429:
                return {"issn": source_id, "error": "Rate limit Scopus (429)."}
            resp.raise_for_status()
            return parse_entry_json(source_id, resp.json())
        except SerialTitleAPIError:
            raise
        except Exception as e:
            raise SerialTitleAPIError(f"Error en Serial Title API: {e}")

    # ------------------------------------------------------------------
    # Búsqueda por nombre de revista
    # ------------------------------------------------------------------

    def search_journal_by_title(self, title: str) -> dict:
        """
        Busca una revista en Scopus por nombre (título parcial o completo).

        Aplica validación de similitud (Jaccard ≥ 0.30) para evitar
        falsos positivos cuando Scopus devuelve una revista diferente.

        Args:
            title: Nombre de la revista a buscar.

        Returns:
            Dict normalizado si se encuentra con similitud ≥ 0.30,
            o {'issn': title, 'error': '...'} si no se encuentra o
            la similitud es insuficiente.
        """
        if not self.api_key:
            raise SerialTitleAPIError("SCOPUS_API_KEY no configurada.")

        try:
            resp = self._client.get(
                self.SEARCH_URL,
                {"title": title.strip(), "view": "ENHANCED", "count": 1},
            )
            if resp.status_code == 404:
                return {"issn": title, "error": f"Revista '{title}' no encontrada."}
            if resp.status_code == 429:
                return {"issn": title, "error": "Rate limit Scopus (429)."}
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"[SerialTitle] Error buscando revista '{title}': {e}")
            return {"issn": title, "error": str(e)}

        result = parse_entry_json(title, data)
        if result.get("error"):
            return result

        # Validar similitud del título devuelto vs el buscado
        result_title = str(result.get("title") or "")
        if result_title and title_similarity(title, result_title) < 0.30:
            logger.warning(
                f"[SerialTitle] Similitud baja: '{title}' → '{result_title}'. Se descarta."
            )
            return {
                "issn": title,
                "error": f"Coincidencia débil: Scopus devolvió '{result_title}'",
            }

        return result

    # ------------------------------------------------------------------
    # Resolución de ISSN desde Abstract Retrieval API (EID / DOI)
    # ------------------------------------------------------------------

    def get_issn_from_eid(self, eid: str):
        """
        Consulta el Abstract Retrieval API por EID y extrae ISSN, título y source_id.

        El EID es el identificador nativo de Scopus (ej: '2-s2.0-85207865300').
        Esta vía es la más confiable para llegar al Serial Title API porque
        el source_id que devuelve desambigua revistas con ISSNs duplicados.

        Args:
            eid: EID de Scopus.

        Returns:
            Tupla (issn, source_title, source_id) — cualquiera puede ser None.
        """
        if self._client._abstract_api_forbidden:
            return None, None, None

        resp = self._client.get_abstract(
            f"{self.ABSTRACT_URL}/eid/{eid.strip()}",
            {"view": "META"},
        )
        return self._extract_abstract_meta(resp, eid)

    def get_issn_from_doi(self, doi: str):
        """
        Consulta el Abstract Retrieval API por DOI y extrae ISSN, título y source_id.

        Args:
            doi: DOI del artículo (con o sin prefijo https://doi.org/).

        Returns:
            Tupla (issn, source_title, source_id) — cualquiera puede ser None.
        """
        if self._client._abstract_api_forbidden:
            return None, None, None

        doi_clean = doi.strip().lower()
        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            if doi_clean.startswith(prefix):
                doi_clean = doi_clean[len(prefix):]
                break

        resp = self._client.get_abstract(
            f"{self.ABSTRACT_URL}/doi/{doi_clean}",
            {"view": "META"},
        )
        return self._extract_abstract_meta(resp, doi_clean)

    def _extract_abstract_meta(self, resp, identifier: str):
        """
        Extrae ISSN, título de fuente y source_id del JSON del Abstract Retrieval API.

        Maneja los distintos códigos de error del Abstract Retrieval API:
          - 403: API key sin acceso → activa flag para evitar spam de warnings.
          - 404/400: No encontrado → devuelve None, None, None.
          - 429: Rate limit → devuelve None, None, None.
          - 0: Error de red → devuelve None, None, None.

        Args:
            resp:       Respuesta HTTP del Abstract Retrieval API.
            identifier: EID o DOI buscado (solo para logging).

        Returns:
            Tupla (issn, source_title, source_id) — cualquiera puede ser None.
        """
        if resp.status_code in (404, 400):
            return None, None, None

        if resp.status_code == 403:
            if not self._client._abstract_api_forbidden:
                self._client._abstract_api_forbidden = True
                logger.warning(
                    "[SerialTitle] Abstract Retrieval API devuelve 403. "
                    "La API key no tiene acceso a este endpoint. "
                    "Activa 'Abstract Retrieval' en dev.elsevier.com para mayor precisión."
                )
            return None, None, None

        if resp.status_code in (429, 0):
            return None, None, None

        try:
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return None, None, None

        # La respuesta puede venir en abstracts-retrieval-response o full-text-retrieval-response
        coredata = (
            data.get("abstracts-retrieval-response", {}).get("coredata", {})
            or data.get("full-text-retrieval-response", {}).get("coredata", {})
        )

        # Extraer ISSN con soporte a múltiples variantes del campo
        issn_raw = (
            coredata.get("prism:issn")
            or coredata.get("prism:eIssn")
            or coredata.get("prism:eissn")
            or coredata.get("prism:isbn")
        )
        if isinstance(issn_raw, list):
            issn_raw = issn_raw[0] if issn_raw else None
        if isinstance(issn_raw, dict):
            issn_raw = issn_raw.get("$") or issn_raw.get("#text")
        issn = clean_issn(str(issn_raw)) if issn_raw else None

        source_title = coredata.get("prism:publicationName")
        if isinstance(source_title, dict):
            source_title = source_title.get("$")

        source_id_raw = (
            coredata.get("source-id")
            or coredata.get("sourceid")
            or coredata.get("dc:source")
        )
        if isinstance(source_id_raw, dict):
            source_id_raw = source_id_raw.get("$") or source_id_raw.get("#text")
        source_id = str(source_id_raw).strip() if source_id_raw else None

        logger.debug(
            f"[SerialTitle] {identifier} → ISSN={issn}, source_id={source_id}, "
            f"fuente={source_title}"
        )
        return issn, source_title or None, source_id or None

    # ------------------------------------------------------------------
    # Consulta masiva paralela
    # ------------------------------------------------------------------

    def get_bulk_coverage(
        self,
        issns: List[str],
        max_workers: int = 5,
        delay: float = 0.2,
    ) -> List[dict]:
        """
        Consulta múltiples ISSNs o nombres de revistas de forma concurrente.

        Detecta automáticamente si cada entrada es un ISSN o un nombre de revista
        (usando is_issn_format del dominio) y llama al método apropiado.

        También reintenta ISSNs de 7 dígitos con '0' prepended o appended,
        para compensar la pérdida del cero inicial que Excel aplica a veces.

        Args:
            issns:       Lista de ISSNs o nombres de revistas.
            max_workers: Hilos paralelos (default 5, coordinados por rate limiter).
            delay:       Pausa entre batches (segundos).

        Returns:
            Lista de dicts de cobertura en el mismo orden de entrada.
        """
        results: Dict[str, dict] = {}

        def _fetch(identifier: str):
            """Worker de ThreadPoolExecutor: resuelve un identificador a dict de cobertura."""
            try:
                if is_issn_format(identifier):
                    result = self.get_journal_coverage(identifier)
                    # Reintentar con '0' prepended si falla y tiene < 8 dígitos
                    clean = identifier.strip().replace("-", "")
                    if result.get("error") and len(clean) < 8 and not clean.startswith("0"):
                        alt = self.get_journal_coverage(f"0{identifier}")
                        if not alt.get("error"):
                            result = {**alt, "_prepended_zero": True}
                    # Reintentar con '0' appended si tiene 7 dígitos exactos
                    if result.get("error") and len(clean) == 7:
                        alt = self.get_journal_coverage(f"{identifier}0")
                        if not alt.get("error"):
                            result = {**alt, "_appended_zero": True}
                    return identifier, result
                else:
                    return identifier, self.search_journal_by_title(identifier)
            except SerialTitleAPIError as e:
                return identifier, {"issn": identifier, "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch, i): i for i in issns}
            for future in as_completed(futures):
                original, result = future.result()
                results[original] = result
                status = "✓" if not result.get("error") else "✗"
                logger.info(
                    f"  [SerialTitle] {status} '{original}' → "
                    f"{result.get('title', 'sin título')}"
                )

        # Devolver en el mismo orden de entrada
        return [results[i] for i in issns]

    # ------------------------------------------------------------------
    # Enriquecimiento de lista de publicaciones
    # ------------------------------------------------------------------

    def check_publications_coverage(
        self,
        publications: List[dict],
        max_workers: int = 1,
        delay: float = 1.0,
    ) -> List[dict]:
        """
        Enriquece una lista de publicaciones con datos de cobertura en Scopus.

        Flujo completo:
          1. Identifica journals únicos a consultar (deduplicación por identificador).
          2. Consulta el Serial Title API en paralelo, con caché en disco.
          3. Enriquece cada publicación con los datos de su revista.

        Args:
            publications: Lista de dicts con campos: issn, doi, isbn, eid,
                          source_title, year, title. Pueden incluir columnas
                          _prev_* para reutilizar datos de ejecuciones anteriores.
            max_workers:  Hilos paralelos para las consultas HTTP.
            delay:        Pausa entre peticiones (segundos).

        Returns:
            La misma lista de publicaciones enriquecida con campos de cobertura:
              scopus_journal_title, scopus_publisher, journal_status,
              coverage_from, coverage_to, in_coverage, journal_found,
              journal_subject_areas, coverage_error.
        """
        # ── Paso 1: Identificar journals únicos ───────────────────────
        journal_keys, skipped_prev = build_journal_keys(publications)
        logger.info(
            f"[SerialTitle] {len(publications)} publicaciones, "
            f"{skipped_prev} ya con datos previos, "
            f"{len(journal_keys)} journals únicos a consultar."
        )

        # ── Paso 2: Consultar Serial Title API ────────────────────────
        journal_cache: Dict[str, dict] = {}
        source_id_cache: Dict[str, dict] = {}  # Deduplicación adicional por source_id

        def _resolve_by_meta(r_issn, r_title, r_srcid, found_via: str):
            """Intenta resolver cobertura desde metadatos del Abstract Retrieval API."""
            if r_srcid:
                if r_srcid in source_id_cache:
                    return {**source_id_cache[r_srcid], "_found_via": found_via}
                try:
                    res = self.get_journal_coverage_by_source_id(r_srcid)
                    if not res.get("error"):
                        res["_found_via"] = found_via
                        source_id_cache[r_srcid] = res
                        return res
                except SerialTitleAPIError:
                    pass
            if r_issn:
                res = self.get_journal_coverage(r_issn)
                if not res.get("error"):
                    res["_found_via"] = found_via
                    src_id = res.get("source_id")
                    if src_id and str(src_id) not in source_id_cache:
                        source_id_cache[str(src_id)] = res
                    return res
            if r_title:
                res = self.search_journal_by_title(r_title.strip())
                if not res.get("error"):
                    res["_found_via"] = found_via
                    return res
            return None

        def _try_fallbacks(info: dict, primary_error: str) -> dict:
            """Cadena de fallback cuando el identificador primario falla."""
            issn_fb  = (info.get("issn_fallback")  or "").strip()
            doi_fb   = (info.get("doi_fallback")   or "").strip()
            title_fb = (info.get("title_fallback") or "").strip()

            if issn_fb:
                res = self.get_journal_coverage(issn_fb)
                if not res.get("error"):
                    res["_found_via"] = "issn_fallback"
                    return res
            if doi_fb:
                r_issn, r_title, r_srcid = self.get_issn_from_doi(doi_fb)
                resolved = _resolve_by_meta(r_issn, r_title, r_srcid, "doi")
                if resolved:
                    return resolved
            if title_fb:
                res = self.search_journal_by_title(title_fb)
                if not res.get("error"):
                    res["_found_via"] = "title"
                    return res
            return {"issn": info.get("value", ""), "error": primary_error}

        def _fetch_journal(key: str, info: dict):
            """Worker: resuelve un journal key a dict de cobertura usando caché + API."""
            # Consultar caché en disco primero
            cached = disk_cache._dcache_get(key)
            if cached is not None:
                logger.info(f"  [disk-cache] {key}")
                return key, cached

            try:
                if info["type"] == "eid":
                    r_issn, r_title, r_srcid = self.get_issn_from_eid(info["value"])
                    result = _resolve_by_meta(r_issn, r_title, r_srcid, "eid") or {}
                    if result.get("error") or not result:
                        result = _try_fallbacks(info, result.get("error", "EID sin cobertura"))

                elif info["type"] == "issn":
                    result = self.get_journal_coverage(info["value"])
                    if not result.get("error"):
                        result["_found_via"] = "issn"
                        src_id = result.get("source_id")
                        if src_id and str(src_id) not in source_id_cache:
                            source_id_cache[str(src_id)] = result
                    else:
                        result = _try_fallbacks(info, result.get("error", "ISSN no encontrado"))

                elif info["type"] == "doi":
                    r_issn, r_title, r_srcid = self.get_issn_from_doi(info["value"])
                    result = _resolve_by_meta(r_issn, r_title, r_srcid, "doi") or {}
                    if result.get("error") or not result:
                        result = _try_fallbacks(
                            {**info, "doi_fallback": ""},
                            result.get("error", "DOI sin cobertura"),
                        )

                elif info["type"] == "title":
                    result = self.search_journal_by_title(info["value"])
                    if not result.get("error"):
                        result["_found_via"] = "title"
                else:
                    result = {"issn": info.get("value", ""), "error": "Sin identificador válido"}

                # Persistir en caché de disco si fue exitoso
                if not result.get("error"):
                    disk_cache._dcache_set(key, result)

                return key, result

            except SerialTitleAPIError as e:
                return key, {"issn": info["value"], "error": str(e)}

        # Ejecutar en paralelo con reporte de progreso
        _t0 = time.time()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_journal, k, v): k
                for k, v in journal_keys.items()
            }
            total = len(futures)
            done = 0
            for future in as_completed(futures):
                key, result = future.result()
                journal_cache[key] = result
                done += 1
                if done % 25 == 0 or done == total:
                    elapsed = time.time() - _t0
                    pct = done / total if total else 1.0
                    eta = (elapsed / pct - elapsed) if pct > 0 else 0
                    bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
                    logger.info(
                        f"  [{bar}] {done}/{total} ({pct:.0%})  "
                        f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d} transcurrido  "
                        f"~{int(eta // 60):02d}:{int(eta % 60):02d} restante"
                    )

        # ── Paso 3: Enriquecer cada publicación ───────────────────────
        return [
            enrich_publication(pub, journal_cache)
            for pub in publications
        ]
