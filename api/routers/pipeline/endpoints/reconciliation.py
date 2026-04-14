"""
Endpoints: Reconciliation

endpoints/reconciliation.py - Mantiene 372 líneas de reconciliation_ops.py reducidas a ~200
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.external_records import ReconciliationStatsResponse
from reconciliation.engine import ReconciliationEngine
from api.routers.pipeline.application.sync_service import FullSyncService


logger = logging.getLogger("pipeline")
router = APIRouter(tags=["Reconciliation"])


# ── POST /pipeline/enrich ─────────────────────────────────────────────────────

@router.post(
    "/enrich",
    response_model=dict,
    summary="Enriquecer canónicos con datos de todas las fuentes",
)
def enrich_canonicals(
    batch_size: int = 200,
    db: Session = Depends(get_db),
):
    """
    Recorre **todos** los canónicos existentes y completa sus campos vacíos
    usando los registros de fuente ya vinculados (`*_records` con
    `canonical_publication_id` asignado).

    No importa el `status` del registro de fuente — si está vinculado
    a un canónico, aporta sus datos.

    Útil cuando:
    - Se descargó una fuente nueva y sus registros ya se reconciliaron,
      pero los canónicos todavía no tienen los campos que esa fuente provee.
    - Se agregaron columnas nuevas a las tablas de fuente (ej: `publisher`,
      `journal_coverage`) y se quiere propagar esos valores a los canónicos.
    - Un canónico se creó desde una sola fuente y ahora hay más fuentes vinculadas.

    Respuesta:
    - `canonicals_processed`: total de canónicos revisados
    - `canonicals_enriched`: cuántos recibieron al menos un campo nuevo
    - `fields_filled`: total de campos completados en toda la pasada
    - `errors`: canónicos que fallaron (se omiten y se continúa)
    """
    engine = ReconciliationEngine(session=db)
    try:
        return engine.enrich_all_canonicals(batch_size=batch_size)
    except Exception as e:
        logger.error(f"Error en enriquecimiento masivo: {e}")
        raise HTTPException(500, f"Error en enriquecimiento: {e}")


# ── POST /pipeline/reconcile ──────────────────────────────────────────────

@router.post(
    "/reconcile",
    response_model=ReconciliationStatsResponse,
    summary="Reconciliar lote de pendientes",
)
async def reconcile_pending(batch_size: int = 500):
    """Ejecuta un lote de reconciliación sobre registros pendientes (non-blocking)."""
    from starlette.concurrency import run_in_threadpool

    def _reconcile_sync():
        engine = ReconciliationEngine()
        with engine.session:
            try:
                stats = engine.reconcile_pending(batch_size=batch_size)
                return ReconciliationStatsResponse(**stats.to_dict())
            except Exception as e:
                raise HTTPException(500, f"Error en reconciliación: {e}")

    return await run_in_threadpool(_reconcile_sync)


# ── POST /pipeline/reconcile-all ──────────────────────────────────────────

@router.post(
    "/reconcile-all",
    response_model=ReconciliationStatsResponse,
    summary="Reconciliar todos los pendientes",
)
async def reconcile_all():
    """Reconcilia TODOS los registros pendientes (non-blocking)."""
    from starlette.concurrency import run_in_threadpool

    def _reconcile_all_sync():
        engine = ReconciliationEngine()
        with engine.session:
            try:
                total_stats = ReconciliationStatsResponse()
                while True:
                    stats = engine.reconcile_pending(batch_size=500)
                    if stats.total_processed == 0:
                        break
                    total_stats.total_processed += stats.total_processed
                    total_stats.doi_exact_matches += stats.doi_exact_matches
                    total_stats.fuzzy_high_matches += stats.fuzzy_high_matches
                    total_stats.fuzzy_combined_matches += stats.fuzzy_combined_matches
                    total_stats.manual_review += stats.manual_review
                    total_stats.new_canonical_created += stats.new_canonical
                    total_stats.errors += stats.errors
                return total_stats
            except Exception as e:
                raise HTTPException(500, f"Error: {e}")

    return await run_in_threadpool(_reconcile_all_sync)


# ── POST /pipeline/all-sources ────────────────────────────────────────────────

@router.post(
    "/all-sources",
    response_model=dict,
    summary="Reconciliar todos los registros de todas las fuentes + enriquecimiento Scopus",
)
async def reconcile_all_sources(
    batch_size: int = 50,
    db: Session = Depends(get_db),
):
    """
    Flujo COMPLETO de sincronización (non-blocking).

    Delegado a FullSyncService:
    1. Reconcilia todos los registros de todas las fuentes (Scopus, OpenAlex, WoS, CvLAC, Datos Abiertos)
       contra canonical_publications usando el DOI como clave.
    2. Enriquece canónicos cruzándolos con Scopus por DOI (por lotes de `batch_size`).
    3. Actualiza autores con Scopus Author IDs.
    """
    from starlette.concurrency import run_in_threadpool

    def _full_sync():
        try:
            return FullSyncService().run(db, batch_size=batch_size)
        except Exception as e:
            raise HTTPException(500, f"Error en sincronización: {e}")

    return await run_in_threadpool(_full_sync)
