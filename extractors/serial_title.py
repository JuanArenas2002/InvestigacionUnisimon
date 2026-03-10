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

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import scopus_config

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Caché persistente en disco para resultados del Serial Title API
# Sobrevive a reinicios del servidor — evita re-consumir cuota en cada ejecución
# ──────────────────────────────────────────────────────────────────────────────
_CACHE_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".journal_disk_cache.json")
_CACHE_LOCK  = threading.Lock()   # protege lecturas/escrituras al fichero y al dict
_CACHE_TTL   = 7 * 24 * 3600     # 7 días (segundos)
_cache_mem: dict | None = None    # cargado una sola vez; None = aún no cargado


def _dcache_load() -> dict:
    """Carga la caché desde disco (solo la primera vez; después usa la copia en memoria)."""    
    global _cache_mem
    if _cache_mem is not None:
        return _cache_mem
    with _CACHE_LOCK:
        if _cache_mem is not None:   # double-checked locking
            return _cache_mem
        raw: dict = {}
        try:
            if os.path.exists(_CACHE_FILE):
                with open(_CACHE_FILE, "r", encoding="utf-8") as fh:
                    raw = json.load(fh)
                # Podar entradas expiradas al cargar
                now = time.time()
                raw = {k: v for k, v in raw.items()
                       if now - v.get("_t", 0) < _CACHE_TTL}
                logger.info(f"[journal-cache] {len(raw)} journals cargados del disco ({_CACHE_FILE}).")
        except Exception as exc:
            logger.warning(f"[journal-cache] No se pudo leer caché de disco: {exc}")
            raw = {}
        _cache_mem = raw
    return _cache_mem


def _dcache_get(key: str) -> dict | None:
    """Devuelve el dict de cobertura si existe en caché y no expiró; si no, None."""
    cache = _dcache_load()
    entry = cache.get(key)
    if entry and time.time() - entry.get("_t", 0) < _CACHE_TTL:
        return entry["data"]
    return None


def _dcache_set(key: str, data: dict) -> None:
    """Guarda una entrada en la caché en memoria y persiste al disco atómicamente."""
    cache = _dcache_load()
    with _CACHE_LOCK:
        cache[key] = {"data": data, "_t": time.time()}
        try:
            tmp = _CACHE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(cache, fh, ensure_ascii=False)
            os.replace(tmp, _CACHE_FILE)   # escritura atómica
        except Exception as exc:
            logger.warning(f"[journal-cache] No se pudo persistir caché: {exc}")


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


def _split_issns(raw: str) -> list[str]:
    """
    Divide un campo ISSN que puede contener varios valores separados por
    punto y coma ('; '), coma o espacio, y retorna solo los ISSNs válidos.

    Ejemplo: '14220067; 16616596' → ['14220067', '16616596']
    """
    import re
    if not raw:
        return []
    parts = re.split(r"[;,\s]+", str(raw).strip())
    result = []
    seen: set[str] = set()
    for part in parts:
        clean = _clean_issn(part)
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def _title_similarity(a: str, b: str) -> float:
    """
    Jaccard de tokens entre dos títulos normalizados.
    Retorna 0.0-1.0 (1.0 = idénticos).
    """
    import re as _re
    def _tokens(s: str) -> set[str]:
        return set(_re.sub(r'[^\w]', ' ', (s or '').lower()).split())
    t1, t2 = _tokens(a), _tokens(b)
    if not t1 or not t2:
        return 0.0
    return len(t1 & t2) / len(t1 | t2)


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

        # Rate limiter global compartido entre todos los hilos del extractor
        self._rate_lock: threading.Lock = threading.Lock()
        self._last_call_time: float = 0.0

        # Evitar spam de warnings 403 — solo avisamos la primera vez
        self._abstract_api_forbidden: bool = False

        logger.info("SerialTitleExtractor inicializado.")

    # ------------------------------------------------------------------
    # Sesión HTTP con reintentos
    # ------------------------------------------------------------------

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=scopus_config.max_retries,
            backoff_factor=2,                        # esperas: 2, 4, 8 s
            status_forcelist=[500, 502, 503, 504],   # 429 se maneja manualmente
            allowed_methods=["GET"],
            respect_retry_after_header=True,
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

    def _get(self, url: str, params: dict) -> requests.Response:
        """
        GET con reintento manual ante 429 (respeta Retry-After o espera 60 s).
        El rate limiter se aplica antes de CADA intento, incluidos los reintentos
        post-429, para que los hilos no se alineen al despertar tras la espera.
        """
        for attempt in range(4):   # max 4 intentos
            self._rate_limited_sleep()   # ← serializa todos los hilos antes del HTTP
            resp = self.session.get(url, params=params, timeout=scopus_config.timeout)
            if resp.status_code != 429:
                return resp
            wait = int(resp.headers.get("Retry-After", 60))
            wait = max(wait, 10)   # nunca menos de 10 s
            logger.warning(f"  429 en {url} — esperando {wait} s (intento {attempt+1}/4)")
            time.sleep(wait)
        return resp   # devuelve el último 429 para que el caller lo maneje

    def _get_abstract(self, url: str, params: dict) -> requests.Response:
        """
        GET para el Abstract Retrieval API — NO consume el slot del rate limiter.
        El Serial Title API y el Abstract Retrieval API tienen cuotas independientes;
        serializar ambos con el mismo lock innecesariamente ralentiza el proceso.
        El 403 (sin acceso) se devuelve tal cual sin lanzar excepción.
        """
        try:
            return self.session.get(url, params=params, timeout=scopus_config.timeout)
        except requests.exceptions.RequestException as exc:
            # Encapsular en una respuesta falsa con status 0 para que el caller lo maneje
            r = requests.Response()
            r.status_code = 0
            r._content = str(exc).encode()
            return r

    def _rate_limited_sleep(self, min_interval: float = 1.0) -> None:
        """
        Garantiza un intervalo mínimo *entre cualquier par de llamadas HTTP* a la API,
        coordinando todos los hilos del ThreadPoolExecutor.

        Se llama desde _get() antes de cada intento HTTP, por lo que cubre tanto las
        llamadas normales como los reintentos después de un 429. Así, cuando N hilos
        se despiertan a la vez tras su Retry-After, el lock los serializa y los
        re-espacía en lugar de dispararlos simultáneamente.

        Args:
            min_interval: Tiempo mínimo en segundos entre requests (default 1.0 s).
        """
        with self._rate_lock:
            now = time.time()
            elapsed = now - self._last_call_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_call_time = time.time()

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
            resp = self._get(url, {"view": "ENHANCED"})
            if resp.status_code == 404:
                return {"issn": issn, "error": "Revista no encontrada en Scopus."}
            if resp.status_code == 429:
                return {"issn": issn, "error": "Rate limit Scopus (429) — reintenta más tarde."}
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

    def get_journal_coverage_by_source_id(self, source_id: str) -> dict:
        """
        Consulta el Serial Title API de Scopus por Scopus Source ID (srcid).
        Más confiable que buscar por ISSN porque el source_id es el ID interno
        de Scopus para la revista — no tiene problemas de formato ni variantes.

        Args:
            source_id: Scopus Source ID (ej: '21100830991').

        Returns:
            Mismo formato que get_journal_coverage().
        """
        if not self.api_key:
            raise SerialTitleAPIError("SCOPUS_API_KEY no configurada.")
        try:
            resp = self._get(
                self.SEARCH_URL,
                {"srcid": source_id.strip(), "view": "ENHANCED"},
            )
            if resp.status_code == 404:
                return {"issn": source_id, "error": "Revista no encontrada por source_id."}
            if resp.status_code == 429:
                return {"issn": source_id, "error": "Rate limit Scopus (429)."}
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error Serial Title API para source_id {source_id}: {e}")
            raise SerialTitleAPIError(f"Error en Serial Title API: {e}")
        return self._parse_entry(source_id, data)

    def get_issn_from_eid(self, eid: str) -> tuple[str | None, str | None, str | None]:
        """
        Consulta el Abstract Retrieval API de Scopus por EID (2-s2.0-...)
        y extrae el ISSN, el nombre de la fuente y el Scopus Source ID.

        Se usa como fallback cuando no hay ISSN ni DOI disponibles.

        Args:
            eid: EID de Scopus (ej: '2-s2.0-85207865300').

        Returns:
            Tuple (issn, source_title, source_id) — cualquiera puede ser None.
        """
        eid_clean = eid.strip()
        # Si ya sabemos que el Abstract Retrieval API devuelve 403, ir directo al fallback
        if self._abstract_api_forbidden:
            return None, None, None
        try:
            resp = self._get_abstract(
                f"{self.ABSTRACT_URL}/eid/{eid_clean}",
                {"view": "META"},
            )
            if resp.status_code in (404, 400):
                logger.debug(f"EID no encontrado en Scopus: {eid_clean}")
                return None, None, None
            if resp.status_code == 403:
                if not self._abstract_api_forbidden:
                    self._abstract_api_forbidden = True
                    logger.warning(
                        "Abstract Retrieval API devuelve 403 (sin acceso con esta API key). "
                        "Los EIDs y DOIs se resolverán solo por ISSN/título. "
                        "Activa el permiso 'Abstract Retrieval' en dev.elsevier.com para mayor precisión."
                    )
                return None, None, None
            if resp.status_code == 429:
                logger.warning(f"Rate limit al consultar EID {eid_clean}")
                return None, None, None
            if resp.status_code == 0:
                logger.debug(f"Error de red consultando EID '{eid_clean}'")
                return None, None, None
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.debug(f"Error consultando EID '{eid_clean}': {e}")
            return None, None, None

        coredata = (
            data.get("abstracts-retrieval-response", {})
            .get("coredata", {})
        ) or (
            data.get("full-text-retrieval-response", {})
            .get("coredata", {})
        )

        # Intentar todas las variantes de ISSN que devuelve Scopus
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

        issn = _clean_issn(str(issn_raw)) if issn_raw else None

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

        logger.debug(f"EID {eid_clean} → ISSN={issn}, source_id={source_id}, fuente={source_title}")
        return issn, source_title or None, source_id or None

    def get_issn_from_doi(self, doi: str) -> tuple[str | None, str | None, str | None]:
        """
        Consulta el Abstract Retrieval API de Scopus por DOI y extrae
        el ISSN (o ISBN), el nombre de la fuente y el Scopus Source ID.

        Se usa como paso previo cuando una publicación no tiene ISSN
        pero sí tiene DOI, para luego poder consultar el Serial Title API.

        Args:
            doi: DOI del artículo (con o sin prefijo 'https://doi.org/').

        Returns:
            Tuple (issn, source_title, source_id) — cualquiera puede ser None.
        """
        doi_clean = doi.strip().lower()
        # Quitar prefijo de URL si viene con él
        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            if doi_clean.startswith(prefix):
                doi_clean = doi_clean[len(prefix):]
                break

        # Si ya sabemos que el Abstract Retrieval API devuelve 403, ir directo al fallback
        if self._abstract_api_forbidden:
            return None, None, None
        try:
            resp = self._get_abstract(
                f"{self.ABSTRACT_URL}/doi/{doi_clean}",
                {"view": "META"},
            )
            if resp.status_code in (404, 400):
                logger.debug(f"DOI no encontrado en Scopus: {doi_clean}")
                return None, None, None
            if resp.status_code == 403:
                if not self._abstract_api_forbidden:
                    self._abstract_api_forbidden = True
                    logger.warning(
                        "Abstract Retrieval API devuelve 403 (sin acceso con esta API key). "
                        "Los DOIs se resolverán solo por ISSN/título."
                    )
                return None, None, None
            if resp.status_code == 429:
                logger.warning(f"Rate limit al consultar DOI {doi_clean}")
                return None, None, None
            if resp.status_code == 0:
                logger.debug(f"Error de red consultando DOI '{doi_clean}'")
                return None, None, None
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.debug(f"Error consultando DOI '{doi_clean}': {e}")
            return None, None, None

        # La respuesta puede venir en abstracts-retrieval-response o full-text-retrieval-response
        coredata = (
            data.get("abstracts-retrieval-response", {})
            .get("coredata", {})
        ) or (
            data.get("full-text-retrieval-response", {})
            .get("coredata", {})
        )

        # Intentar todas las variantes de ISSN que devuelve Scopus
        issn_raw = (
            coredata.get("prism:issn")
            or coredata.get("prism:eIssn")
            or coredata.get("prism:eissn")
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

        source_id_raw = (
            coredata.get("source-id")
            or coredata.get("sourceid")
            or coredata.get("dc:source")
        )
        if isinstance(source_id_raw, dict):
            source_id_raw = source_id_raw.get("$") or source_id_raw.get("#text")
        source_id = str(source_id_raw).strip() if source_id_raw else None

        logger.debug(f"DOI {doi_clean} → ISSN={issn}, source_id={source_id}, fuente={source_title}")
        return issn, source_title or None, source_id or None

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
            resp = self._get(
                self.SEARCH_URL,
                {"title": title.strip(), "view": "ENHANCED", "count": 1},
            )
            if resp.status_code == 404:
                return {"issn": title, "error": f"Revista '{title}' no encontrada en Scopus."}
            if resp.status_code == 429:
                return {"issn": title, "error": "Rate limit Scopus (429)."}
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error buscando revista por título '{title}': {e}")
            return {"issn": title, "error": str(e)}

        result = self._parse_entry(title, data)
        if result.get("error"):
            return result

        # Validar que el título devuelto por Scopus sea similar al buscado.
        # Umbral 0.30 (Jaccard) evita matches completamente incorrectos sin
        # ser demasiado estricto con variaciones de nombre.
        result_title = str(result.get("title") or "")
        if result_title and _title_similarity(title, result_title) < 0.30:
            logger.warning(
                f"Búsqueda por título: '{title}' → Scopus devolvió "
                f"'{result_title}' (similitud < 0.30). Se descarta la respuesta."
            )
            return {"issn": title, "error": f"Coincidencia débil: Scopus devolvió '{result_title}'"}

        return result

    # ------------------------------------------------------------------
    # Verificación de cobertura para lista de publicaciones
    # ------------------------------------------------------------------

    def check_publications_coverage(
        self,
        publications: list[dict],
        max_workers: int = 1,
        delay: float = 1.0,
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
        # Prioridad de identificadores (de más a menos confiable):
        #   EID  →  ISSN/EISSN  →  ISBN  →  DOI  →  source_title
        #
        # EID es el identificador nativo de Scopus: EID → Abstract Retrieval
        # → source_id → Serial Title API. Ruta más confiable.
        # Deduplicación por source_id evita N llamadas Serial Title cuando N
        # artículos pertenecen a la misma revista (ver source_id_cache en Step 2).
        _SKIP_VALUES = {"" , "sin datos", "no encontrada", "—"}

        def _has_prev_data(pub: dict) -> bool:
            """True si la publicación ya tiene datos de cobertura válidos."""
            in_cov = str(pub.get("_prev_in_coverage") or "").strip().lower()
            found  = str(pub.get("_prev_journal_found") or "").strip().lower()
            return in_cov not in _SKIP_VALUES and found in ("sí", "si", "true", "1")

        journal_keys: dict[str, dict] = {}   # key → {type, value}
        skipped_prev = 0
        for pub in publications:
            if _has_prev_data(pub):
                skipped_prev += 1
                continue   # ya tiene datos buenos — no consultar

            issns = _split_issns(pub.get("issn", ""))   # puede haber varios
            isbn  = _clean_issn(pub.get("isbn", ""))     # ISBN como ISSN de serie
            doi   = (pub.get("doi") or "").strip()
            eid   = (pub.get("eid") or "").strip()
            src   = (pub.get("source_title") or "").strip()

            if eid:
                # EID: identificador nativo Scopus — PRIORIDAD MÁXIMA.
                # EID → Abstract Retrieval → source_id → Serial Title API.
                # El source_id desambigua revistas con ISSN duplicado o con
                # variantes de formato (print/electronic).
                key = f"eid:{eid.lower()}"
                if key not in journal_keys:
                    issn_fb = issns[0] if issns else (isbn or "")
                    journal_keys[key] = {
                        "type": "eid",
                        "value": eid,
                        "issn_fallback": issn_fb,
                        "doi_fallback": doi,
                        "title_fallback": src,
                    }
                    
            elif issns:
                for issn in issns:
                    key = f"issn:{issn}"
                    if key not in journal_keys:
                        journal_keys[key] = {"type": "issn", "value": issn, "doi_fallback": doi, "title_fallback": src}
                    else:
                        entry = journal_keys[key]
                        if not entry.get("doi_fallback") and doi:
                            entry["doi_fallback"] = doi
                        if not entry.get("title_fallback") and src:
                            entry["title_fallback"] = src
            elif isbn:
                # ISBN como ISSN de serie — puede fallar en Serial Title API;
                # guardamos fallbacks para el caso en que falle.
                key = f"issn:{isbn}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "issn", "value": isbn, "doi_fallback": doi, "title_fallback": src}
                else:
                    entry = journal_keys[key]
                    if not entry.get("doi_fallback") and doi:
                        entry["doi_fallback"] = doi
                    if not entry.get("title_fallback") and src:
                        entry["title_fallback"] = src
            elif doi:
                key = f"doi:{doi.lower()}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "doi", "value": doi, "title_fallback": src}
                else:
                    entry = journal_keys[key]
                    if not entry.get("title_fallback") and src:
                        entry["title_fallback"] = src
            elif src:
                # Último recurso: buscar por nombre de revista.
                # search_journal_by_title valida similitud (Jaccard ≥ 0.30)
                # para evitar falsos positivos.
                key = f"title:{src.lower()}"
                if key not in journal_keys:
                    journal_keys[key] = {"type": "title", "value": src}
            # Sin ningún identificador → se omite

        logger.info(
            f"check_publications_coverage: {len(publications)} publicaciones, "
            f"{skipped_prev} ya con datos previos, "
            f"{len(journal_keys)} journals únicos a consultar."
        )

        # ── 2. Consultar Scopus por cada journal único ────────────────
        journal_cache: dict[str, dict] = {}
        # source_id_cache: deduplicación a nivel de revista.
        # Cuando EID_A y EID_B pertenecen a la misma revista (mismo source_id),
        # la segunda consulta al Serial Title API se omite → sirve del caché.
        source_id_cache: dict[str, dict] = {}

        def _resolve_by_abstract_meta(r_issn, r_title, r_srcid, found_via: str) -> dict | None:
            """Intenta resolver cobertura con los metadatos del Abstract Retrieval API.
            Prioridad: source_id → ISSN → título de la API (más confiable que Excel).
            """
            # 1) source_id → Serial Title API  (la vía más directa y confiable)
            if r_srcid:
                # ¿Ya tenemos este source_id en caché?
                if r_srcid in source_id_cache:
                    cached = dict(source_id_cache[r_srcid])
                    cached["_found_via"] = found_via
                    logger.debug(f"  {found_via}→source_id={r_srcid} → HIT caché")
                    return cached
                try:
                    res = self.get_journal_coverage_by_source_id(r_srcid)
                    if not res.get("error"):
                        res["_found_via"] = found_via
                        source_id_cache[r_srcid] = res
                        logger.debug(f"  {found_via}→source_id={r_srcid} → OK (guardado en caché)")
                        return res
                except SerialTitleAPIError:
                    pass
            # 2) ISSN → Serial Title API
            if r_issn:
                res = self.get_journal_coverage(r_issn)
                if not res.get("error"):
                    res["_found_via"] = found_via
                    src_id = res.get("source_id")
                    if src_id and str(src_id) not in source_id_cache:
                        source_id_cache[str(src_id)] = res
                    return res
            # 3) Título devuelto por la propia API de Scopus (Abstract Retrieval).
            #    Este título es más confiable que el "Source title" del Excel:
            #    viene de Scopus mismo, así que hacemos búsqueda sin Jaccard estricto.
            if r_title and r_title.strip():
                res = self.search_journal_by_title(r_title.strip())
                if not res.get("error"):
                    res["_found_via"] = found_via
                    src_id = res.get("source_id")
                    if src_id and str(src_id) not in source_id_cache:
                        source_id_cache[str(src_id)] = res
                    logger.debug(f"  {found_via}→título API='{r_title}' → OK")
                    return res
            return None

        def _try_fallbacks(info: dict, primary_error: str) -> dict:
            """
            Cadena de fallback cuando el identificador primario falló.
            ISSN → DOI → source_title (último recurso con guard de similitud).
            (EID ya es el primario; aquí los fallbacks son para los casos sin EID.)
            """
            issn_fb  = (info.get("issn_fallback")  or "").strip()
            doi_fb   = (info.get("doi_fallback")   or "").strip()
            title_fb = (info.get("title_fallback") or "").strip()

            # 1) ISSN → Serial Title API directo
            if issn_fb:
                res = self.get_journal_coverage(issn_fb)
                if not res.get("error"):
                    res["_found_via"] = "issn_fallback"
                    src_id = res.get("source_id")
                    if src_id and str(src_id) not in source_id_cache:
                        source_id_cache[str(src_id)] = res
                    return res

            # 2) DOI → Abstract Retrieval → source_id / ISSN
            if doi_fb:
                r_issn, r_title, r_srcid = self.get_issn_from_doi(doi_fb)
                resolved = _resolve_by_abstract_meta(r_issn, r_title, r_srcid, "doi")
                if resolved:
                    return resolved

            # 3) Nombre de revista → Serial Title Search (último recurso)
            #    search_journal_by_title ya aplica Jaccard ≥ 0.30 para evitar
            #    falsos positivos.
            if title_fb:
                res = self.search_journal_by_title(title_fb)
                if not res.get("error"):
                    res["_found_via"] = "title"
                    return res

            return {"issn": info.get("value", ""), "error": primary_error}

        def _fetch_journal(key: str, info: dict) -> tuple[str, dict]:
            # 0. Caché persistente en disco — no gastar cuota si ya fue consultado
            _cached = _dcache_get(key)
            if _cached is not None:
                logger.info(f"  [disk-cache] {key}")
                return key, _cached
            try:
                if info["type"] == "eid":
                    # PRIORIDAD 1: EID → Abstract Retrieval → source_id → Serial Title API
                    r_issn, r_title, r_srcid = self.get_issn_from_eid(info["value"])
                    result = _resolve_by_abstract_meta(r_issn, r_title, r_srcid, "eid") or {}
                    if result.get("error") or not result:
                        # Fallback: ISSN directo → DOI → nombre de revista
                        result = _try_fallbacks(
                            info,
                            result.get("error", "EID sin cobertura en Scopus"),
                        )
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
                    # DOI → Abstract Retrieval → source_id / ISSN
                    r_issn, r_title, r_srcid = self.get_issn_from_doi(info["value"])
                    result = _resolve_by_abstract_meta(r_issn, r_title, r_srcid, "doi") or {}
                    if result.get("error") or not result:
                        result = _try_fallbacks(
                            {**info, "doi_fallback": ""},
                            result.get("error", "DOI sin cobertura en Scopus"),
                        )
                elif info["type"] == "title":
                    # Último recurso: búsqueda por nombre de revista
                    result = self.search_journal_by_title(info["value"])
                    if not result.get("error"):
                        result["_found_via"] = "title"
                else:
                    result = {"issn": info.get("value", ""), "error": "Sin identificador válido"}
                # Persistir en caché de disco si la consulta fue exitosa
                if not result.get("error"):
                    _dcache_set(key, result)
                return key, result
            except SerialTitleAPIError as e:
                return key, {"issn": info["value"], "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_journal, k, v): k
                for k, v in journal_keys.items()
            }
            _prog_total = len(futures)
            _prog_done  = 0
            _prog_t0    = time.time()
            for future in as_completed(futures):
                key, result = future.result()
                journal_cache[key] = result
                _prog_done += 1
                if _prog_done % 25 == 0 or _prog_done == _prog_total:
                    _elapsed = time.time() - _prog_t0
                    _pct     = _prog_done / _prog_total if _prog_total else 1.0
                    _eta     = (_elapsed / _pct - _elapsed) if _pct > 0 else 0
                    _bar     = "█" * int(_pct * 20) + "░" * (20 - int(_pct * 20))
                    logger.info(
                        f"  [{_bar}] {_prog_done}/{_prog_total} ({_pct:.0%})  "
                        f"{int(_elapsed // 60):02d}:{int(_elapsed % 60):02d} transcurrido  "
                        f"~{int(_eta // 60):02d}:{int(_eta % 60):02d} restante"
                    )

        # ── 3. Enriquecer cada publicación ────────────────────────────
        enriched = []
        for pub in publications:
            row = dict(pub)

            # Si ya tenía datos válidos del Excel anterior → restaurarlos tal cual
            if _has_prev_data(pub):
                row["scopus_journal_title"]  = pub.get("_prev_scopus_journal_title") or None
                row["scopus_publisher"]      = pub.get("_prev_scopus_publisher") or None
                row["journal_status"]        = pub.get("_prev_journal_status") or "Unknown"
                row["coverage_from"]         = None
                row["coverage_to"]           = None
                row["coverage_periods"]      = []
                row["coverage_periods_str"]  = pub.get("_prev_coverage_periods_str") or "—"
                row["journal_found"]         = True
                row["journal_subject_areas"] = None
                row["in_coverage"]           = pub.get("_prev_in_coverage") or "Sin datos"
                row["coverage_error"]        = None
                enriched.append(row)
                continue

            issns = _split_issns(pub.get("issn", ""))  # lista de ISSNs
            isbn  = _clean_issn(pub.get("isbn", ""))
            doi   = (pub.get("doi") or "").strip()
            eid   = (pub.get("eid") or "").strip()
            src   = (pub.get("source_title") or "").strip()

            # Buscar en caché — misma prioridad del Step 1: eid > issn > isbn > doi > title
            journal_info = None
            cache_key    = None
            if eid:
                cache_key = f"eid:{eid.lower()}"
                journal_info = journal_cache.get(cache_key)
            if journal_info is None and issns:
                for issn in issns:
                    k = f"issn:{issn}"
                    candidate = journal_cache.get(k)
                    if candidate and not candidate.get("error"):
                        journal_info = candidate
                        cache_key = k
                        break
                if journal_info is None and issns:
                    cache_key = cache_key or f"issn:{issns[0]}"
                    journal_info = journal_cache.get(cache_key)
            if journal_info is None and isbn:
                cache_key = f"issn:{isbn}"
                journal_info = journal_cache.get(cache_key)
            if journal_info is None and doi:
                cache_key = f"doi:{doi.lower()}"
                journal_info = journal_cache.get(cache_key)
            if journal_info is None and src:
                cache_key = f"title:{src.lower()}"
                journal_info = journal_cache.get(cache_key)

            if journal_info and not journal_info.get("error"):
                row["scopus_journal_title"]   = journal_info.get("title")
                row["scopus_publisher"]       = journal_info.get("publisher")
                row["journal_status"]         = journal_info.get("status", "Unknown")
                row["coverage_from"]          = journal_info.get("coverage_from")
                row["coverage_to"]            = journal_info.get("coverage_to")
                row["coverage_periods"]       = journal_info.get("coverage_periods", [])
                row["journal_found"]          = True
                row["journal_found_via"]      = journal_info.get("_found_via", "issn")
                row["coverage_error"]         = None
                areas = journal_info.get("subject_areas") or []
                row["journal_subject_areas"]  = " | ".join(areas) if areas else None

                # Consolidar ISSN / E-ISSN si el artículo no lo tenía originalmente
                had_issn = bool(issns or isbn)
                if not had_issn:
                    row["resolved_issn"]  = journal_info.get("resolved_issn")  or ""
                    row["resolved_eissn"] = journal_info.get("resolved_eissn") or ""
                else:
                    row["resolved_issn"]  = ""
                    row["resolved_eissn"] = ""

                # —— Verificación de cobertura contra TODOS los periodos ——
                try:
                    pub_year = int(pub.get("year") or 0)
                except (ValueError, TypeError):
                    pub_year = 0

                periods: list[tuple[int, int]] = journal_info.get("coverage_periods") or []
                cf = journal_info.get("coverage_from")
                ct = journal_info.get("coverage_to")

                _cy = datetime.now().year
                # Si la revista sigue activa (coverage_to reciente), Scopus
                # puede estar 1-2 años atrás. Extender el techo efectivo.
                _ect = max(ct, _cy) if (ct and ct >= _cy - 2) else ct
                if pub_year and periods:
                    _last = periods[-1][1]
                    _eff_last = max(_last, _cy) if _last >= _cy - 2 else _last
                    if any(s <= pub_year <= e for s, e in periods) or (_last < pub_year <= _eff_last):
                        row["in_coverage"] = "Sí"
                    elif pub_year < periods[0][0]:
                        row["in_coverage"] = "No (antes de cobertura)"
                    elif pub_year > _eff_last:
                        row["in_coverage"] = "No (después de cobertura)"
                    else:
                        # Está entre periodos válidos pero en una laguna
                        row["in_coverage"] = "No (laguna de cobertura)"
                elif pub_year and cf and _ect:
                    if cf <= pub_year <= _ect:
                        row["in_coverage"] = "Sí"
                    elif pub_year < cf:
                        row["in_coverage"] = "No (antes de cobertura)"
                    else:
                        row["in_coverage"] = "No (después de cobertura)"
                elif pub_year and cf and not _ect:
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
                row["resolved_issn"]         = ""
                row["resolved_eissn"]        = ""
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

        # coverageEndYear declarado por Scopus puede ser mayor que el último año
        # con publicationCount > 0 en yearly-data (Scopus no carga datos del año
        # en curso hasta meses después). Usarlo como cota superior para no penalizar
        # revistas activas cuyo coverage_to calculado queda rezagado 1-2 años.
        try:
            declared_end = int(entry["coverageEndYear"]) if entry.get("coverageEndYear") else None
        except (ValueError, TypeError):
            declared_end = None
        if declared_end and (coverage_to is None or declared_end > coverage_to):
            coverage_to = declared_end
            # Extender el último periodo hasta el año declarado
            if coverage_periods:
                last_start, last_end = coverage_periods[-1]
                if declared_end > last_end:
                    coverage_periods[-1] = (last_start, declared_end)

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

        # Normalizar códigos de un carácter que devuelve el API de Scopus:
        #   "d" → Discontinued,  "n" / "a" → Active
        _STATUS_MAP: dict[str, str] = {
            "d":              "Discontinued",
            "discontinued":   "Discontinued",
            "inactive":       "Discontinued",
            "n":              "Active",
            "a":              "Active",
            "active":         "Active",
        }

        if explicit_status:
            status = _STATUS_MAP.get(str(explicit_status).strip().lower(), explicit_status)
        elif coverage_to and coverage_to >= current_year - 2:
            # coverage_to >= 2024: Scopus probablemente no ha actualizado todavía → Active.
            # (cubre también proyecciones al futuro)
            status = "Active"
        elif coverage_to:
            # coverage_to < 2024: parada hace más de 2 años sin confirmación de Scopus.
            # → "Inactiva": verificar manualmente. Discontinued solo cuando Scopus lo dice explícito.
            status = "Inactiva"
        else:
            status = "Unknown"

        logger.info(
            f"  _parse_entry ISSN={issn}: explicit_status={explicit_status!r} "
            f"coverage_to={coverage_to} → status={status!r}"
        )

        is_discontinued = status.lower() in ("inactive", "inactiva", "discontinued")

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

        # ── ISSN / E-ISSN reales del entry ────────────────────────────
        # (pueden diferir del ISSN de búsqueda si se llegó aquí via DOI/título)
        def _entry_issn(field: str) -> str | None:
            raw = entry.get(field)
            if isinstance(raw, dict):
                raw = raw.get("$") or raw.get("#text")
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            return _clean_issn(str(raw)) if raw else None

        resolved_issn  = _entry_issn("prism:issn")  or _entry_issn("prism:isbn")
        resolved_eissn = _entry_issn("prism:eIssn") or _entry_issn("prism:e-issn")

        return {
            "issn": issn,
            "resolved_issn":  resolved_issn,
            "resolved_eissn": resolved_eissn,
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
