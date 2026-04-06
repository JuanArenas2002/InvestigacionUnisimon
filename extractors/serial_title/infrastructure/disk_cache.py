"""
Caché persistente en disco para resultados del Serial Title API de Scopus.

Motivación: el Serial Title API consume cuota de la API key de Elsevier.
Esta caché evita re-consultar revistas ya conocidas entre ejecuciones del
servidor, reduciendo el consumo de cuota y acelerando las respuestas.

Características:
  - Persistencia: los datos sobreviven a reinicios del servidor.
  - TTL: las entradas expiran después de 7 días (configurable).
  - Thread safety: un threading.Lock protege lecturas y escrituras.
  - Escritura atómica: usa os.replace() para evitar archivos corruptos.
  - Carga lazy: el archivo se lee solo la primera vez que se necesita.

NOTA: No es process-safe. Si múltiples procesos acceden simultáneamente
al mismo archivo de caché, puede haber race conditions. Para entornos
con múltiples workers de Gunicorn, considerar una caché compartida (Redis).
"""

import json
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

# Ruta del archivo de caché en disco (al lado del paquete serial_title/)
_CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",             # subir un nivel (de infrastructure/ a serial_title/)
    ".journal_disk_cache.json",
)
_CACHE_FILE = os.path.normpath(_CACHE_FILE)

# Lock para proteger acceso concurrente desde múltiples threads
_CACHE_LOCK = threading.Lock()

# TTL de las entradas: 7 días en segundos
_CACHE_TTL = 7 * 24 * 3600

# Caché en memoria: cargada una sola vez desde disco, luego mantenida en RAM
_cache_mem: dict = None   # None = aún no cargado


def _dcache_load() -> dict:
    """
    Carga la caché desde disco la primera vez. Después usa la copia en memoria.

    Aplica double-checked locking para que sea thread-safe sin bloquear
    innecesariamente cuando la caché ya está cargada.

    Al cargar, poda automáticamente las entradas expiradas (TTL vencido)
    para mantener el archivo limpio.

    Returns:
        Dict de caché en memoria, con claves ISSN y valores {data, _t}.
    """
    global _cache_mem
    if _cache_mem is not None:
        return _cache_mem

    with _CACHE_LOCK:
        if _cache_mem is not None:   # Double-checked locking
            return _cache_mem

        raw: dict = {}
        try:
            if os.path.exists(_CACHE_FILE):
                with open(_CACHE_FILE, "r", encoding="utf-8") as fh:
                    raw = json.load(fh)
                # Podar entradas expiradas al cargar
                now = time.time()
                raw = {
                    k: v for k, v in raw.items()
                    if now - v.get("_t", 0) < _CACHE_TTL
                }
                logger.info(
                    f"[journal-cache] {len(raw)} journals cargados del disco "
                    f"({_CACHE_FILE})."
                )
        except Exception as exc:
            logger.warning(f"[journal-cache] No se pudo leer caché de disco: {exc}")
            raw = {}

        _cache_mem = raw

    return _cache_mem


def _dcache_get(key: str) -> dict:
    """
    Retorna los datos de cobertura de una revista si está en caché y no expiró.

    Args:
        key: Clave de búsqueda (ej: 'issn:25953982', 'doi:10.xxx/...').

    Returns:
        Dict de datos de cobertura si está en caché válida, None si no.
    """
    cache = _dcache_load()
    entry = cache.get(key)
    if entry and time.time() - entry.get("_t", 0) < _CACHE_TTL:
        return entry["data"]
    return None


def _dcache_set(key: str, data: dict) -> None:
    """
    Guarda una entrada en la caché en memoria y la persiste al disco atómicamente.

    La escritura atómica usa os.replace() (rename atómico): escribe en un
    archivo temporal y luego lo mueve al archivo final. Esto garantiza que
    el archivo de caché nunca quede en estado inconsistente si el proceso
    se interrumpe durante la escritura.

    Args:
        key:  Clave de la entrada (ej: 'issn:25953982').
        data: Dict de datos de cobertura a almacenar.
    """
    cache = _dcache_load()
    with _CACHE_LOCK:
        cache[key] = {"data": data, "_t": time.time()}
        try:
            tmp = _CACHE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(cache, fh, ensure_ascii=False)
            # Rename atómico: garantiza que el archivo final es siempre consistente
            os.replace(tmp, _CACHE_FILE)
        except Exception as exc:
            logger.warning(f"[journal-cache] No se pudo persistir caché: {exc}")
