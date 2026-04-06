"""
Capa de infraestructura del módulo Serial Title.

Responsabilidades:
  - disk_cache: caché persistente en disco para resultados del Serial Title API.
    Sobrevive a reinicios del servidor y evita re-consumir cuota en cada ejecución.
  - http_client: sesión HTTP autenticada con rate limiting manual ante 429.
"""
