"""
Paquete pipeline: Arquitectura DDD refactorizada.

Estructura:
  domain/                 — Entidades, servicios y interfaces de dominio (lógica pura)
  application/            — Use cases y commands (orquestación)
  infrastructure/         — Implementación técnica (básico, routers originales delegados)
  endpoints/              — Handlers HTTP delgados (requests/responses)
  shared/                 — DTOs compartidos

Archivos antiguos:
  - extraction.py, coverage.py, reconciliation_ops.py, admin.py → migrados a endpoints/
  - _pipeline_helpers.py, _ids.py, _json_loader.py → se mantienen por compat
"""
from fastapi import APIRouter
from .endpoints import router as endpoints_router

router = endpoints_router

__all__ = ["router"]
