"""
Cliente HTTP para el Serial Title API y Abstract Retrieval API de Scopus.

Responsabilidades:
  - Crear la sesión HTTP autenticada con API Key e Inst Token.
  - Implementar rate limiting manual ante respuestas 429 (el Serial Title API
    no usa reintentos automáticos de urllib3 para el 429 porque necesita
    respetar el Retry-After header y coordinar threads concurrentes).
  - Serializar llamadas concurrentes desde ThreadPoolExecutor para evitar
    ráfagas simultáneas que disparen el rate limit.

Nota sobre los dos tipos de GET:
  - _get():           Para Serial Title API. Usa el rate limiter compartido.
  - _get_abstract():  Para Abstract Retrieval API. SIN rate limiter porque
                      tiene cuota independiente del Serial Title API.
"""

import logging
import time
import threading
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def create_session(config, api_key: str, inst_token: Optional[str] = None) -> requests.Session:
    """
    Crea una sesión HTTP configurada para el Serial Title API de Scopus.

    La sesión NO incluye 429 en el status_forcelist porque el rate limit
    de Scopus se maneja manualmente en ScopusSerialClient._get() para
    respetar el header Retry-After y coordinar threads concurrentes.

    Args:
        config:     Configuración de Scopus con max_retries.
        api_key:    API key de Elsevier Developer Portal.
        inst_token: Token institucional de Elsevier (opcional).

    Returns:
        Sesión requests lista para el Serial Title API.
    """
    session = requests.Session()

    # backoff_factor=2 → esperas de 2s, 4s, 8s. El 429 se maneja manualmente.
    retry = Retry(
        total=config.max_retries,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],  # 429 excluido — manejo manual
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "X-ELS-APIKey": api_key,
        "Accept":       "application/json",
    })
    if inst_token:
        session.headers["X-ELS-Insttoken"] = inst_token

    return session


class ScopusSerialClient:
    """
    Cliente para el Serial Title API y Abstract Retrieval API de Scopus.

    Encapsula:
    - Rate limiting compartido entre threads (serializa llamadas HTTP).
    - Reintento manual ante 429 respetando Retry-After.
    - GET separado para Abstract Retrieval API (sin rate limiter propio).
    - Estado de flag _abstract_api_forbidden para evitar spam de warnings 403.

    Se instancia una vez por SerialTitleExtractor y se comparte entre
    todos los threads del ThreadPoolExecutor.
    """

    def __init__(self, session: requests.Session, config):
        """
        Args:
            session: Sesión HTTP autenticada (de create_session()).
            config:  Configuración de Scopus (timeout, base_url).
        """
        self.session = session
        self.config  = config

        # Lock y timestamp para rate limiting entre threads
        self._rate_lock:      threading.Lock = threading.Lock()
        self._last_call_time: float          = 0.0

        # Flag para evitar warnings repetidos cuando Abstract API devuelve 403
        self._abstract_api_forbidden: bool = False

    def _rate_limited_sleep(self, min_interval: float = 1.0) -> None:
        """
        Garantiza un intervalo mínimo entre llamadas HTTP, coordinando threads.

        Se llama antes de CADA petición al Serial Title API. Cuando N threads
        se despiertan tras un Retry-After, el lock los serializa y los reespacía
        en lugar de dispararlos simultáneamente (lo que volvería a causar 429).

        Args:
            min_interval: Tiempo mínimo en segundos entre requests (default 1.0 s).
        """
        with self._rate_lock:
            now = time.time()
            elapsed = now - self._last_call_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_call_time = time.time()

    def get(self, url: str, params: dict) -> requests.Response:
        """
        GET con reintento manual ante 429 (respeta Retry-After o espera 60 s).

        El rate limiter se aplica antes de CADA intento, incluyendo los
        reintentos post-429, para que los threads no se alineen al despertar.

        Args:
            url:    URL del endpoint del Serial Title API.
            params: Parámetros de query string.

        Returns:
            Respuesta HTTP. Puede ser 429 si se agotaron los reintentos.
        """
        resp = None
        for attempt in range(4):   # Máximo 4 intentos
            self._rate_limited_sleep()
            resp = self.session.get(url, params=params, timeout=self.config.timeout)
            if resp.status_code != 429:
                return resp
            # Respetar Retry-After del header, mínimo 10 segundos
            wait = max(int(resp.headers.get("Retry-After", 60)), 10)
            logger.warning(
                f"  [Serial Title] 429 en {url} — "
                f"esperando {wait}s (intento {attempt + 1}/4)"
            )
            time.sleep(wait)
        return resp   # Devolver el último 429 para que el caller lo maneje

    def get_abstract(self, url: str, params: dict) -> requests.Response:
        """
        GET para el Abstract Retrieval API de Scopus.

        NO consume el slot del rate limiter del Serial Title API porque
        tienen cuotas independientes. Un 403 se devuelve tal cual sin
        lanzar excepción (la key puede no tener acceso a este endpoint).

        Args:
            url:    URL del Abstract Retrieval API.
            params: Parámetros de query string.

        Returns:
            Respuesta HTTP, o Response falsa con status=0 si hay error de red.
        """
        try:
            return self.session.get(url, params=params, timeout=self.config.timeout)
        except requests.exceptions.RequestException as exc:
            # Devolver respuesta falsa para que el caller pueda manejarla
            r = requests.Response()
            r.status_code = 0
            r._content = str(exc).encode()
            return r
