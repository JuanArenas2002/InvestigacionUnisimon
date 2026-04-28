"""
Blocklist de tokens JWT revocados (in-memory).

Almacena JTIs (JWT IDs) de tokens que han sido explícitamente invalidados
(logout). Los tokens revocados son rechazados en verify_token aunque estén
firmados correctamente y no hayan expirado.

Limitación conocida: la blocklist se pierde si el servidor se reinicia.
Dado que los tokens expiran a los 60 min y un reinicio invalida esa ventana,
es aceptable para este caso de uso. Para persistencia completa, migrar a Redis.
"""

import threading
from datetime import datetime, timezone


class TokenBlocklist:
    def __init__(self) -> None:
        self._revoked: dict[str, datetime] = {}  # jti → expires_at
        self._lock = threading.Lock()

    def revoke(self, jti: str, expires_at: datetime) -> None:
        with self._lock:
            self._revoked[jti] = expires_at
            self._purge_expired()

    def is_revoked(self, jti: str) -> bool:
        with self._lock:
            return jti in self._revoked

    def _purge_expired(self) -> None:
        now = datetime.now(timezone.utc)
        expired = [jti for jti, exp in self._revoked.items() if exp <= now]
        for jti in expired:
            del self._revoked[jti]

    def __len__(self) -> int:
        return len(self._revoked)


# Singleton — importar desde aquí
blocklist = TokenBlocklist()
