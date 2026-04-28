"""
Endpoints: Reconciliation

endpoints/reconciliation.py - Mantiene 372 líneas de reconciliation_ops.py reducidas a ~200
"""
import logging
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from api.dependencies import get_db
from api.schemas.external_records import ReconciliationStatsResponse
from reconciliation.engine import ReconciliationEngine
from api.routers.pipeline.application.sync_service import FullSyncService
from db.models import CanonicalPublication, PossibleDuplicatePair
from extractors.base import normalize_doi


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


# ── POST /pipeline/full-enrich ────────────────────────────────────────────────

@router.post(
    "/full-enrich",
    response_model=dict,
    summary="Pipeline completo para todas las publicaciones: buscar en fuentes, vincular y enriquecer",
)
async def full_enrich_all(
    batch_size: int = 50,
    use_openalex: bool = True,
    use_scopus: bool = True,
    use_wos: bool = True,
    scopus_delay: float = 0.5,
    db: Session = Depends(get_db),
):
    """
    Recorre **todos** los canónicos con DOI y ejecuta el pipeline completo:

    1. Consulta fuentes externas (OpenAlex, Scopus, WoS) por DOI.
    2. Ingesta registros nuevos encontrados.
    3. Vincula source records no enlazados (`canonical_publication_id IS NULL`)
       sin importar su status (PENDING, MANUAL_REVIEW, etc.).
    4. Aplica datos frescos de la API al canónico (abstract, citas, keywords…).
    5. Enriquece con todos los source records vinculados en BD.
    6. Extrae autores de cada fuente.

    Parámetros:
    - `batch_size`: canónicos procesados por lote antes de commit (default 50)
    - `use_openalex` / `use_scopus` / `use_wos`: habilitar/deshabilitar fuentes
    - `scopus_delay`: segundos entre llamadas a Scopus (default 0.5)

    Respuesta:
    - `canonicals_processed`: total recorridos
    - `canonicals_enriched`: cuántos recibieron al menos un campo nuevo o autor
    - `records_ingested`: source records nuevos insertados en total
    - `records_linked`: source records vinculados a su canónico
    - `errors`: canónicos que fallaron (se omiten y se continúa)
    """
    from starlette.concurrency import run_in_threadpool
    from extractors.openalex.extractor import OpenAlexExtractor
    from extractors.scopus import ScopusExtractor, ScopusAPIError
    from extractors.wos import WosExtractor
    from db.models import SOURCE_MODELS, CanonicalPublication
    from extractors.base import normalize_doi as _norm_doi
    import time

    def _run():
        engine = ReconciliationEngine(session=db)

        oa_ext  = OpenAlexExtractor() if use_openalex else None
        sc_ext  = ScopusExtractor()   if use_scopus   else None
        wos_ext = WosExtractor()      if use_wos      else None

        stats = {
            "canonicals_processed": 0,
            "canonicals_enriched": 0,
            "records_ingested": 0,
            "records_linked": 0,
            "errors": 0,
        }

        offset = 0
        while True:
            batch = (
                db.query(CanonicalPublication)
                .filter(CanonicalPublication.doi.isnot(None))
                .order_by(CanonicalPublication.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )
            if not batch:
                break

            engine._cache = engine._build_cache()

            for canonical in batch:
                try:
                    ndoi = _norm_doi(canonical.doi) or canonical.doi
                    records_to_ingest = []

                    if oa_ext:
                        try:
                            rec = oa_ext.search_by_doi(ndoi)
                            if rec:
                                records_to_ingest.append(rec)
                        except Exception as e:
                            logger.debug(f"OpenAlex DOI {ndoi}: {e}")

                    if sc_ext:
                        try:
                            rec = sc_ext.search_by_doi(ndoi)
                            if rec:
                                records_to_ingest.append(rec)
                            if scopus_delay > 0:
                                time.sleep(scopus_delay)
                        except ScopusAPIError as e:
                            logger.debug(f"Scopus DOI {ndoi}: {e}")
                        except Exception as e:
                            logger.debug(f"Scopus DOI {ndoi}: {e}")

                    if wos_ext:
                        try:
                            rec = wos_ext.search_by_doi(ndoi)
                            if rec:
                                records_to_ingest.append(rec)
                        except Exception as e:
                            logger.debug(f"WoS DOI {ndoi}: {e}")

                    # Ingestar registros nuevos
                    ingested = engine.ingest_records(records_to_ingest)
                    stats["records_ingested"] += ingested

                    # Vincular source records sin enlazar para este DOI
                    unlinked = []
                    for model_cls in SOURCE_MODELS.values():
                        unlinked.extend(
                            db.query(model_cls)
                            .filter(
                                model_cls.doi == ndoi,
                                model_cls.canonical_publication_id.is_(None),
                            )
                            .all()
                        )

                    if unlinked:
                        for rec in unlinked:
                            try:
                                engine._reconcile_one(rec)
                                stats["records_linked"] += 1
                            except Exception as exc:
                                logger.debug(f"Reconcile {rec}: {exc}")
                        db.flush()

                    # Aplicar datos frescos al canónico
                    changed = False
                    for fresh_rec in records_to_ingest:
                        before_abstract = canonical.abstract
                        engine._enrich_canonical(canonical, fresh_rec)
                        engine._ingest_authors(canonical, fresh_rec)
                        if canonical.abstract != before_abstract:
                            changed = True

                    # Enriquecer desde todos los source records vinculados en BD
                    enrich_result = engine.enrich_canonical(canonical.id)
                    if enrich_result.get("total_changes", 0) > 0 or changed or unlinked:
                        stats["canonicals_enriched"] += 1

                    stats["canonicals_processed"] += 1

                except Exception as e:
                    logger.error(f"full-enrich: error en canonical={canonical.id}: {e}", exc_info=True)
                    stats["errors"] += 1

            db.commit()
            offset += batch_size
            logger.info(
                f"full-enrich lote {offset}: "
                f"procesados={stats['canonicals_processed']}, "
                f"enriquecidos={stats['canonicals_enriched']}, "
                f"vinculados={stats['records_linked']}"
            )

        return stats

    return await run_in_threadpool(_run)


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


# ── POST /pipeline/backfill-authors ──────────────────────────────────────────

@router.post(
    "/backfill-authors",
    response_model=dict,
    summary="Poblar autores desde source records existentes en BD",
)
async def backfill_publication_authors(
    batch_size: int = 500,
    db: Session = Depends(get_db),
):
    """
    Recorre los source records ya reconciliados en BD y ejecuta
    `_ingest_authors()` para cada uno.

    Usa solo datos locales (sin llamadas a APIs externas).
    Para re-consultar fuentes externas usa `/backfill-authors-from-sources`.
    """
    from starlette.concurrency import run_in_threadpool

    def _backfill():
        try:
            engine = ReconciliationEngine(session=db)
            return engine.backfill_publication_authors(batch_size=batch_size)
        except Exception as e:
            logger.error(f"Error en backfill de autores: {e}", exc_info=True)
            raise HTTPException(500, f"Error en backfill de autores: {e}")

    return await run_in_threadpool(_backfill)


# ── POST /pipeline/backfill-authors-from-sources ─────────────────────────────

@router.post(
    "/backfill-authors-from-sources",
    response_model=dict,
    summary="Poblar autores consultando fuentes externas por DOI",
)
async def backfill_publication_authors_from_sources(
    batch_size: int = 100,
    use_openalex: bool = True,
    use_scopus: bool = True,
    use_wos: bool = True,
    scopus_delay: float = 0.3,
    db: Session = Depends(get_db),
):
    """
    Para cada publicación canónica, busca sus autores en este orden:

    1. **BD local** — `raw_data._parsed_authors` / `authors_json` del source record
       (sin API call, instantáneo)
    2. **OpenAlex** — `search_by_doi` (gratis, sin cuota)
    3. **Scopus** — `search_by_doi` (cuota Elsevier, `scopus_delay` seg entre calls)
    4. **WoS** — `search_by_doi` (cuota Clarivate)

    Solo sube al siguiente nivel si el anterior no devuelve autores.
    Publicaciones sin DOI solo usan el nivel 1.

    Parámetros:
    - `batch_size`: canónicos por lote (default 100)
    - `use_openalex` / `use_scopus` / `use_wos`: habilitar/deshabilitar fuentes
    - `scopus_delay`: segundos entre calls a Scopus (default 0.3)

    Respuesta:
    - `canonicals_processed`: total canónicos recorridos
    - `from_db` / `from_openalex` / `from_scopus` / `from_wos`: autores por fuente
    - `no_authors_found`: canónicos sin autores en ninguna fuente
    - `authors_linked`: total vínculos en `publication_authors` al final
    - `errors`: canónicos con error (se omiten y continúa)
    """
    from starlette.concurrency import run_in_threadpool

    def _backfill():
        try:
            engine = ReconciliationEngine(session=db)
            return engine.backfill_publication_authors_from_sources(
                batch_size=batch_size,
                use_openalex=use_openalex,
                use_scopus=use_scopus,
                use_wos=use_wos,
                scopus_delay=scopus_delay,
            )
        except Exception as e:
            logger.error(f"Error en backfill from sources: {e}", exc_info=True)
            raise HTTPException(500, f"Error: {e}")

    return await run_in_threadpool(_backfill)


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


# ── POST /pipeline/backfill-duplicate-pairs ───────────────────────────────────

@router.post(
    "/backfill-duplicate-pairs",
    response_model=dict,
    summary="Escanear canónicos existentes y poblar tabla de pares duplicados",
)
async def backfill_duplicate_pairs(
    min_similarity: float = Query(0.92, ge=0.5, le=1.0, description="Umbral mínimo de similitud de título (0-1)"),
    year_tolerance: int = Query(1, ge=0, le=3, description="Tolerancia en años para comparar"),
    db: Session = Depends(get_db),
):
    """
    Escanea **todos** los canónicos existentes con rapidfuzz y llena
    `possible_duplicate_pairs` con los pares encontrados.

    Útil para poblar la tabla por primera vez (backfill) o después de
    ingestas masivas sin haber llamado antes a `GET /publications/duplicates`.

    - Solo inserta pares **nuevos** (ignora los que ya existen).
    - No modifica pares con `status='merged'` o `'dismissed'`.
    - Usa el mismo algoritmo de agrupación por año que el endpoint
      `GET /publications/duplicates` (O(k²) por bucket, no O(n²) global).

    Respuesta:
    - `canonicals_scanned`: total de canónicos analizados
    - `pairs_found`: pares con similitud >= min_similarity
    - `pairs_inserted`: pares nuevos insertados en la tabla
    - `pairs_already_existed`: pares que ya estaban en la tabla
    """
    from starlette.concurrency import run_in_threadpool
    from rapidfuzz import fuzz as rfuzz, process as rprocess

    def _scan():
        min_score = min_similarity * 100.0

        # Cargar todos los canónicos con título normalizado
        pubs = db.query(
            CanonicalPublication.id,
            CanonicalPublication.doi,
            CanonicalPublication.normalized_title,
            CanonicalPublication.publication_year,
        ).filter(
            CanonicalPublication.normalized_title.isnot(None),
            func.length(CanonicalPublication.normalized_title) > 10,
        ).all()

        n = len(pubs)
        logger.info(f"backfill-duplicate-pairs: escaneando {n} canónicos")

        # Agrupar por año
        year_buckets: dict = defaultdict(list)
        for p in pubs:
            year_buckets[p.publication_year].append(p)

        seen_pairs: set = set()
        raw_pairs: list = []

        def _compare_bucket(bucket):
            titles = [p.normalized_title for p in bucket]
            for i in range(len(bucket)):
                matches = rprocess.extract(
                    titles[i], titles,
                    scorer=rfuzz.token_sort_ratio,
                    score_cutoff=min_score,
                    limit=50,
                )
                for _, score, j in matches:
                    if j <= i:
                        continue
                    key = (min(bucket[i].id, bucket[j].id), max(bucket[i].id, bucket[j].id))
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)
                    raw_pairs.append((bucket[i], bucket[j], score))

        distinct_years = sorted(y for y in year_buckets if y is not None)
        for idx, yr in enumerate(distinct_years):
            _compare_bucket(year_buckets[yr])
            # Años adyacentes dentro de tolerancia
            for delta in range(1, year_tolerance + 1):
                if idx + delta < len(distinct_years) and distinct_years[idx + delta] == yr + delta:
                    adjacent = year_buckets[distinct_years[idx + delta]]
                    titles_adj = [p.normalized_title for p in adjacent]
                    for pub in year_buckets[yr]:
                        matches = rprocess.extract(
                            pub.normalized_title, titles_adj,
                            scorer=rfuzz.token_sort_ratio,
                            score_cutoff=min_score,
                            limit=50,
                        )
                        for _, score, j in matches:
                            key = (min(pub.id, adjacent[j].id), max(pub.id, adjacent[j].id))
                            if key in seen_pairs:
                                continue
                            seen_pairs.add(key)
                            raw_pairs.append((pub, adjacent[j], score))

        # Sin año → compararlos entre sí
        if year_buckets[None]:
            _compare_bucket(year_buckets[None])

        # DOI duplicados
        doi_map: dict = defaultdict(list)
        for p in pubs:
            if p.doi:
                nd = normalize_doi(p.doi)
                if nd:
                    doi_map[nd].append(p)
        for doi_pubs in doi_map.values():
            if len(doi_pubs) < 2:
                continue
            for i in range(len(doi_pubs)):
                for j in range(i + 1, len(doi_pubs)):
                    key = (min(doi_pubs[i].id, doi_pubs[j].id), max(doi_pubs[i].id, doi_pubs[j].id))
                    if key not in seen_pairs:
                        seen_pairs.add(key)
                        raw_pairs.append((doi_pubs[i], doi_pubs[j], 100.0))

        logger.info(f"backfill-duplicate-pairs: {len(raw_pairs)} pares encontrados")

        # Insertar en BD — solo pares nuevos
        inserted = 0
        already_existed = 0

        # Cargar pares existentes de una vez para evitar N queries
        existing_keys = {
            (row.canonical_id_1, row.canonical_id_2)
            for row in db.query(
                PossibleDuplicatePair.canonical_id_1,
                PossibleDuplicatePair.canonical_id_2,
            ).all()
        }

        for p1, p2, score in raw_pairs:
            id1, id2 = min(p1.id, p2.id), max(p1.id, p2.id)
            key = (id1, id2)
            if key in existing_keys:
                already_existed += 1
                continue
            # Determinar método
            if p1.doi and p2.doi and normalize_doi(p1.doi) == normalize_doi(p2.doi):
                method = "doi"
            else:
                method = "title"
            db.add(PossibleDuplicatePair(
                canonical_id_1=id1,
                canonical_id_2=id2,
                similarity_score=round(score, 2),
                match_method=method,
                status="pending",
            ))
            existing_keys.add(key)
            inserted += 1

        try:
            db.commit()
        except Exception as e:
            db.rollback()
            raise HTTPException(500, f"Error al insertar pares: {e}")

        logger.info(
            f"backfill-duplicate-pairs: {inserted} nuevos insertados, "
            f"{already_existed} ya existían"
        )
        return {
            "canonicals_scanned": n,
            "pairs_found": len(raw_pairs),
            "pairs_inserted": inserted,
            "pairs_already_existed": already_existed,
        }

    return await run_in_threadpool(_scan)
