"""
Excepciones y utilidades de rate-limit para la API de OpenAlex.
"""

import json as _json


class OpenAlexAPIError(Exception):
    """Excepción genérica para errores de la API de OpenAlex."""
    pass


class OpenAlexRateLimitError(Exception):
    """
    Se agotó el presupuesto/cuota de OpenAlex.

    La cuota se renueva a medianoche (UTC). Esperar ``retry_after`` segundos
    antes de volver a llamar.

    Atributos:
        retry_after: segundos hasta el próximo intento (0 si desconocido).
    """

    def __init__(self, retry_after: int = 0):
        self.retry_after = retry_after
        hours = retry_after // 3600
        mins  = (retry_after % 3600) // 60
        super().__init__(
            f"Cuota de OpenAlex agotada. "
            f"Reintenta en {hours}h {mins}m (Retry-After: {retry_after}s)."
        )


def extract_retry_after(exc: Exception) -> "int | None":
    """
    Intenta extraer el valor ``Retry-After`` de una excepción HTTP.

    Estrategia en cascada:
      1. Leer ``exc.response.status_code == 429`` → cabecera ``Retry-After``
         o campo ``retryAfter`` del body JSON.
      2. Detectar '429', 'rate limit' o 'insufficient budget' en ``str(exc)``.

    Returns:
        Número de segundos hasta el próximo intento, o ``None`` si el error
        no es un 429 de rate-limit.
    """
    try:
        import requests as _req   # noqa: F401 (solo para el tipo de exc)
        resp = getattr(exc, "response", None)
        if resp is not None and resp.status_code == 429:
            retry = resp.headers.get("Retry-After", 0)
            try:
                body = resp.json()
                retry = body.get("retryAfter", retry)
            except Exception:
                pass
            return int(retry)
    except Exception:
        pass

    # Fallback: buscar indicadores de rate-limit en el mensaje de la excepción
    msg = str(exc).lower()
    if "429" in msg or "rate limit" in msg or "insufficient budget" in msg:
        try:
            start = str(exc).find("{")
            if start != -1:
                body = _json.loads(str(exc)[start:])
                return int(body.get("retryAfter", 0))
        except Exception:
            pass
        return 0

    return None
