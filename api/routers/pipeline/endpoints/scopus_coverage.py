"""
Endpoints: Coverage / Scopus Journal Coverage

endpoints/scopus_coverage.py - Mantiene 745 líneas de coverage.py reducidas a ~300
"""
import logging
import io
import time as _time
from typing import List

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends
from starlette.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.serial_title import JournalCoverageResponse, BulkCoverageRequest
from api.routers._pipeline_helpers import (
    _enrich_discontinued_with_openalex,
    _rescue_not_found_via_openalex,
)
from .._ids import _build_pub_entry


logger = logging.getLogger("pipeline")
router = APIRouter(tags=["Scopus Coverage"])


# ── GET /pipeline/scopus/journal-coverage ────────────────────────────────────

@router.get(
    "/scopus/journal-coverage",
    response_model=JournalCoverageResponse,
    summary="Cobertura de una revista en Scopus por ISSN",
)
def scopus_journal_coverage(issn: str):
    """Ejemplo: GET /pipeline/scopus/journal-coverage?issn=0028-0836"""
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError

    extractor = SerialTitleExtractor()
    try:
        result = extractor.get_journal_coverage(issn)
    except SerialTitleAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])

    return JournalCoverageResponse(**result)


# ── POST /pipeline/scopus/journal-coverage/bulk ──────────────────────────────

@router.post(
    "/journal-coverage/bulk",
    summary="Cobertura masiva de revistas en Scopus",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        }
    },
)
def scopus_journal_coverage_bulk(body: BulkCoverageRequest):
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import generate_journal_coverage_excel

    extractor = SerialTitleExtractor()
    try:
        results = extractor.get_bulk_coverage(
            issns=body.issns,
            max_workers=body.max_workers,
        )
    except SerialTitleAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    excel_bytes = generate_journal_coverage_excel(results)
    filename = f"journal_coverage_{len(body.issns)}_issns.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── POST /pipeline/scopus/journal-coverage/bulk-from-file ────────────────────

@router.post(
    "/journal-coverage/bulk-from-file",
    summary="Cobertura masiva de revistas — carga un Excel con ISSNs",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}
        }
    },
)
def scopus_journal_coverage_bulk_from_file(
    file: UploadFile = File(..., description="Archivo .xlsx con ISSNs en la columna A"),
    max_workers: int = Query(5, ge=1, le=10),
):
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import generate_journal_coverage_excel, read_issns_from_excel

    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Solo se aceptan archivos Excel (.xlsx)")

    file_bytes = file.file.read()
    try:
        issns = read_issns_from_excel(file_bytes)
    except ValueError as e:
        raise HTTPException(422, str(e))

    extractor = SerialTitleExtractor()
    try:
        results = extractor.get_bulk_coverage(issns=issns, max_workers=max_workers)
    except Exception as e:
        raise HTTPException(502, str(e))

    excel_bytes = generate_journal_coverage_excel(results)
    filename = f"journal_coverage_{len(issns)}_issns.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /pipeline/scopus/journal-coverage/debug ──────────────────────────────

@router.get(
    "/journal-coverage/debug",
    summary="[DEBUG] JSON crudo + resultado parseado del Serial Title API",
)
def scopus_journal_coverage_debug(issn: str):
    from extractors.serial_title import SerialTitleExtractor

    extractor = SerialTitleExtractor()
    clean_issn = issn.strip().replace("-", "")
    url = f"{extractor.BASE_URL}/{clean_issn}"
    try:
        resp = extractor.session.get(url, params={"view": "ENHANCED"}, timeout=20)
        raw = resp.json()
    except Exception as e:
        raise HTTPException(502, str(e))

    try:
        parsed = extractor._parse_entry(clean_issn, raw)
    except Exception as e:
        parsed = {"parse_error": str(e)}

    entry = (raw.get("serial-metadata-response", {}).get("entry") or [{}])[0]
    return {
        "status_code": resp.status_code,
        "url": str(resp.url),
        "coverage_fields": {
            "coverageStartYear": entry.get("coverageStartYear"),
            "coverageEndYear": entry.get("coverageEndYear"),
        },
        "parsed_result": parsed,
        "raw_body": raw,
    }


# ── POST /pipeline/scopus/check-publications-coverage ────────────────────────

@router.post(
    "/check-publications-coverage",
    summary="Verificar cobertura Scopus para publicaciones (Excel)",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}
        }
    },
)
async def scopus_check_publications_coverage(
    file: UploadFile = File(..., description="Excel de exportación Scopus"),
    max_workers: int = Query(1, ge=1, le=5),
):
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import read_publications_from_excel, generate_publications_coverage_excel

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "El archivo está vacío")

    _t_pipeline = _time.time()
    logger.info(f"[check-coverage] Archivo recibido: {file.filename} ({len(raw):,} bytes)")

    # 1. Leer Excel
    _t0 = _time.time()
    try:
        headers, rows = await run_in_threadpool(read_publications_from_excel, raw)
    except ValueError as e:
        raise HTTPException(400, str(e))

    logger.info(
        f"[check-coverage] Leer Excel: {_time.time()-_t0:.1f}s "
        f"— {len(rows)} publicaciones"
    )

    # 2. Construir publicaciones
    for row in rows:
        row["_source"] = "Scopus Export"
    publications = [_build_pub_entry(row, include_prev=True) for row in rows]

    # 3. Consultar Scopus
    logger.info("[check-coverage] Iniciando consulta a Scopus API...")
    _t0 = _time.time()
    extractor = SerialTitleExtractor()
    try:
        enriched = await run_in_threadpool(
            extractor.check_publications_coverage, publications, max_workers
        )
    except SerialTitleAPIError as e:
        raise HTTPException(502, f"Error en Scopus API: {e}")

    logger.info(f"[check-coverage] Scopus API: {_time.time()-_t0:.1f}s")

    # 4. Fusionar resultados
    for row, cov in zip(rows, enriched):
        row.update({
            "journal_found": cov.get("journal_found", False),
            "journal_found_via": cov.get("journal_found_via", ""),
            "scopus_journal_title": cov.get("scopus_journal_title", ""),
            "journal_status": cov.get("journal_status", ""),
            "in_coverage": cov.get("in_coverage", "Sin datos"),
        })

    # 4.5 Enriquecer descontinuadas
    _t0 = _time.time()
    await run_in_threadpool(_enrich_discontinued_with_openalex, rows)
    logger.info(f"[check-coverage] OpenAlex cruce: {_time.time()-_t0:.1f}s")

    # 4.6 Rescate OpenAlex
    n_not_found = sum(1 for r in rows if not r.get("journal_found"))
    if n_not_found > 0:
        _t0 = _time.time()
        await run_in_threadpool(_rescue_not_found_via_openalex, rows, extractor)
        logger.info(f"[check-coverage] Rescate OpenAlex: {_time.time()-_t0:.1f}s")

    # 5. Generar Excel
    logger.info("[check-coverage] Generando Excel...")
    _t0 = _time.time()
    try:
        excel_bytes = await run_in_threadpool(generate_publications_coverage_excel, headers, rows)
    except Exception as e:
        logger.exception("[check-coverage] Error en Excel")
        raise HTTPException(500, str(e))

    logger.info(
        f"[check-coverage] Excel: {_time.time()-_t0:.1f}s | "
        f"Total: {_time.time()-_t_pipeline:.1f}s"
    )

    filename = f"cobertura_scopus_{len(rows)}_pubs.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── POST /pipeline/scopus/reprocess-coverage ─────────────────────────────────

@router.post(
    "/reprocess-coverage",
    summary="Re-procesar publicaciones no resueltas",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}
        }
    },
)
async def scopus_reprocess_coverage(
    file: UploadFile = File(...),
    max_workers: int = Query(1, ge=1, le=5),
):
    from extractors.serial_title import SerialTitleExtractor
    from api.exporters.excel import read_publications_from_excel, generate_publications_coverage_excel

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "El archivo está vacío")

    _t_pipeline = _time.time()
    _t0 = _time.time()
    try:
        headers, rows = await run_in_threadpool(read_publications_from_excel, raw)
    except ValueError as e:
        raise HTTPException(400, str(e))

    logger.info(f"[reprocess] Leer Excel: {_time.time()-_t0:.1f}s")

    # Solo re-procesar no resueltas
    for row in rows:
        row["_source"] = row.get("Fuente", "Scopus Export")

    publications = [_build_pub_entry(row, include_prev=True) for row in rows]

    # Scopus check
    logger.info("[reprocess] Scopus API...")
    _t0 = _time.time()
    extractor = SerialTitleExtractor()
    try:
        enriched = await run_in_threadpool(
            extractor.check_publications_coverage, publications, max_workers
        )
    except Exception as e:
        raise HTTPException(502, str(e))

    logger.info(f"[reprocess] Scopus API: {_time.time()-_t0:.1f}s")

    for row, result in zip(rows, enriched):
        row.update({k: v for k, v in result.items() if not k.startswith("_prev_")})

    # Rescate
    _t0 = _time.time()
    await run_in_threadpool(_rescue_not_found_via_openalex, rows, extractor)
    logger.info(f"[reprocess] Rescate: {_time.time()-_t0:.1f}s")

    # Excel
    logger.info("[reprocess] Excel...")
    _t0 = _time.time()
    try:
        excel_bytes = await run_in_threadpool(generate_publications_coverage_excel, headers, rows)
    except Exception as e:
        raise HTTPException(500, str(e))

    logger.info(
        f"[reprocess] Excel: {_time.time()-_t0:.1f}s | "
        f"Total: {_time.time()-_t_pipeline:.1f}s"
    )

    filename = f"cobertura_reprocesada_{len(rows)}_pubs.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /pipeline/scopus/debug/raw ───────────────────────────────────────────

@router.get("/debug/raw", summary="Respuesta cruda de Scopus")
def scopus_debug_raw():
    from config import institution
    from extractors.scopus import ScopusExtractor

    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    extractor = ScopusExtractor()
    query = extractor._build_query(None, None, affiliation_id)
    params = {
        "query": query,
        "count": extractor.config.max_per_page,
        "sort": "pubyear",
    }
    resp = extractor.session.get(extractor.SEARCH_URL, params=params, timeout=20)
    return resp.json()


# ── GET /pipeline/scopus/by-institution ──────────────────────────────────────

@router.get(
    "/by-institution",
    response_model=list,
    summary="Listar DOIs de Scopus por ID institucional",
)
def list_scopus_dois_by_institution(db: Session = Depends(get_db)):
    from config import institution
    from extractors.scopus import ScopusExtractor

    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    if not affiliation_id:
        return []
    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=1000)
    return [r.doi for r in records if getattr(r, "doi", None)]


# ── POST /pipeline/scopus/by-institution/reconcile ───────────────────────────

@router.post(
    "/by-institution/reconcile",
    response_model=dict,
    summary="Crear y reconciliar publicaciones por ID institucional",
)
def reconcile_scopus_by_institution(db: Session = Depends(get_db)):
    from config import institution
    from extractors.scopus import ScopusExtractor
    from db.models import CanonicalPublication, ScopusRecord

    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    if not affiliation_id:
        return {"created": 0, "reconciled": 0, "duplicates": 0}

    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=1000)

    created, reconciled, duplicates, inserted = 0, 0, 0, 0
    seen_dois = set()

    for r in records:
        if not r.source_id:
            continue
        exists = db.query(ScopusRecord).filter_by(scopus_doc_id=r.source_id).first()
        if not exists:
            rec = ScopusRecord(
                scopus_doc_id=r.source_id,
                doi=r.doi,
                title=r.title,
                publication_year=r.publication_year,
                publication_date=r.publication_date,
                publication_type=r.publication_type,
                source_journal=r.source_journal,
                issn=r.issn,
                is_open_access=r.is_open_access,
                citation_count=r.citation_count,
                status="pending",
            )
            db.add(rec)
            inserted += 1

        doi = getattr(r, "doi", None)
        if not doi:
            continue
        doi = doi.strip().lower()
        if doi in seen_dois:
            duplicates += 1
            continue
        seen_dois.add(doi)

        try:
            db.query(CanonicalPublication).filter_by(doi=doi).one()
            reconciled += 1
        except:
            pub = CanonicalPublication(doi=doi, title=getattr(r, "title", None))
            db.add(pub)
            created += 1

    db.commit()
    return {"created": created, "reconciled": reconciled, "duplicates": duplicates}
