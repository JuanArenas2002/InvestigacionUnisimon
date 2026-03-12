"""
Endpoints de cobertura Scopus por revista y publicación.

Rutas:
  GET  /scopus/journal-coverage              — Cobertura de 1 ISSN.
  POST /scopus/journal-coverage/bulk         — Cobertura masiva (lista ISSNs → Excel).
  POST /scopus/journal-coverage/bulk-from-file — Cobertura masiva (Excel con ISSNs → Excel).
  GET  /scopus/journal-coverage/debug        — JSON crudo + parseado para diagnóstico.
  POST /scopus/check-publications-coverage   — Cobertura por publicación (Excel → Excel).
  POST /scopus/reprocess-coverage            — Re-procesar Excel de resultado previo.
  GET  /scopus/debug/raw                     — Respuesta cruda de Scopus API.
  GET  /scopus/by-institution                — Listar DOIs por ID institucional.
  POST /scopus/by-institution/reconcile      — Reconciliar DOIs por ID institucional.
"""
import logging

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.serial_title import JournalCoverageResponse, BulkCoverageRequest
from api.routers._pipeline_helpers import (
    _enrich_discontinued_with_openalex,
    _rescue_not_found_via_openalex,
)
from ._ids import _build_pub_entry

logger = logging.getLogger("pipeline")
router = APIRouter(tags=["Pipeline"])


# ── GET /pipeline/scopus/journal-coverage ────────────────────────────────────

@router.get(
    "/scopus/journal-coverage",
    response_model=JournalCoverageResponse,
    summary="Cobertura de una revista en Scopus por ISSN",
    description=(
        "Consulta el Serial Title API de Scopus para un ISSN y retorna "
        "los años de cobertura y si la revista está activa o descontinuada."
    ),
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
    "/scopus/journal-coverage/bulk",
    summary="Cobertura masiva de revistas en Scopus — retorna Excel",
    description=(
        "Recibe una lista de ISSNs, los consulta en paralelo al Serial Title API "
        "de Scopus y devuelve un archivo Excel (.xlsx) con los resultados."
    ),
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Archivo Excel con la cobertura de las revistas consultadas.",
        }
    },
)
def scopus_journal_coverage_bulk(body: BulkCoverageRequest):
    import io
    from fastapi.responses import StreamingResponse
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
    "/scopus/journal-coverage/bulk-from-file",
    summary="Cobertura masiva de revistas — carga un Excel con ISSNs",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Excel con resultados de cobertura por cada ISSN.",
        }
    },
)
def scopus_journal_coverage_bulk_from_file(
    file: UploadFile = File(..., description="Archivo .xlsx con ISSNs en la columna A"),
    max_workers: int = Query(5, ge=1, le=10, description="Hilos paralelos (1-10)"),
):
    import io
    from fastapi.responses import StreamingResponse
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import generate_journal_coverage_excel, read_issns_from_excel

    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Solo se aceptan archivos Excel (.xlsx). Recibido: " + file.filename,
        )

    file_bytes = file.file.read()
    try:
        issns = read_issns_from_excel(file_bytes)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    extractor = SerialTitleExtractor()
    try:
        results = extractor.get_bulk_coverage(issns=issns, max_workers=max_workers)
    except SerialTitleAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    excel_bytes = generate_journal_coverage_excel(results)
    filename = f"journal_coverage_{len(issns)}_issns.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /pipeline/scopus/journal-coverage/debug ──────────────────────────────

@router.get(
    "/scopus/journal-coverage/debug",
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
        raise HTTPException(status_code=502, detail=str(e))

    try:
        parsed = extractor._parse_entry(clean_issn, raw)
    except Exception as e:
        parsed = {"parse_error": str(e)}

    entry = (raw.get("serial-metadata-response", {}).get("entry") or [{}])[0]
    coverage_debug = {
        "coverageStartYear_root": entry.get("coverageStartYear"),
        "coverageEndYear_root":   entry.get("coverageEndYear"),
        "covers_raw":             entry.get("covers"),
        "coverageInfo_raw":       entry.get("coverageInfo"),
        "all_entry_keys":         sorted(entry.keys()) if isinstance(entry, dict) else [],
    }

    return {
        "status_code":     resp.status_code,
        "url":             str(resp.url),
        "coverage_fields": coverage_debug,
        "parsed_result":   parsed,
        "raw_body":        raw,
    }


# ── POST /pipeline/scopus/check-publications-coverage ────────────────────────

@router.post(
    "/scopus/check-publications-coverage",
    summary="Verificar cobertura Scopus para publicaciones (Excel)",
    responses={
        200: {
            "description": "Excel con cada publicación enriquecida con datos de cobertura Scopus.",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            },
        }
    },
)
async def scopus_check_publications_coverage(
    file: UploadFile = File(
        ...,
        description="Excel de exportación Scopus (columnas: Title, Year, Source title, ISSN, DOI, …)",
    ),
    max_workers: int = Query(
        1, ge=1, le=5,
        description="Hilos paralelos para consultar la API de Scopus (1=secuencial, evita 429)",
    ),
):
    """
    Acepta un Excel de exportación de Scopus con una publicación por fila.

    Para cada publicación:
    1. Busca la revista por ISSN (fallback: ISBN → DOI → nombre).
    2. Comprueba si el año de publicación cae dentro de algún periodo de cobertura Scopus.

    Devuelve un Excel enriquecido con columnas adicionales:
    - **Revista en Scopus** (Sí/No)
    - **Título oficial (Scopus)**
    - **Editorial (Scopus)**
    - **Estado revista** (Active / Discontinued / Unknown)
    - **Periodos de cobertura** (ej: 2002  |  2006–2026)
    - **¿En cobertura?** ← coloreada
    """
    import io as _io
    import time as _ctime
    from starlette.concurrency import run_in_threadpool
    from fastapi.responses import StreamingResponse
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import read_publications_from_excel, generate_publications_coverage_excel

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "El archivo está vacío.")

    _t_pipeline = _ctime.time()
    logger.info(f"[check-coverage] Archivo recibido: '{file.filename}' ({len(raw):,} bytes)")

    # 1. Leer Excel
    _t0 = _ctime.time()
    try:
        headers, rows = await run_in_threadpool(read_publications_from_excel, raw)
    except ValueError as e:
        raise HTTPException(400, str(e))

    logger.info(
        f"[check-coverage] Paso 1 (Leer Excel): {_ctime.time()-_t0:.1f}s "
        f"— {len(rows)} publicaciones, {len(headers)} columnas"
    )

    # 2. Mapear a publications
    for row in rows:
        row["_source"] = "Scopus Export"

    publications = [_build_pub_entry(row, include_prev=True) for row in rows]

    # 2.5 Incorporar publicaciones de OpenAlex BD que NO están en el Excel
    def _load_openalex_extra(excel_rows: list) -> tuple:
        from db.session import get_session
        from db.models import OpenalexRecord as _OARec

        def _nd(d: str) -> str:
            d = (d or "").strip().lower()
            d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
            return d.split()[0] if d else ""

        existing_dois = {
            _nd(str(r.get("__doi") or ""))
            for r in excel_rows
            if r.get("__doi")
        }

        session = get_session()
        try:
            oa_records = session.query(_OARec).filter(
                _OARec.doi.isnot(None)
            ).order_by(_OARec.publication_year.desc()).all()
        except Exception as exc:
            logger.warning(f"[check-coverage] Error cargando OpenAlex BD: {exc}")
            return [], []
        finally:
            session.close()

        oa_rows: list = []
        oa_pubs: list = []
        seen: set = set()

        for rec in oa_records:
            ndoi = _nd(rec.doi or "")
            if not ndoi or ndoi in existing_dois or ndoi in seen:
                continue
            seen.add(ndoi)

            row = {
                "__title":         rec.title or "",
                "__year":          rec.publication_year,
                "__doi":           rec.doi or "",
                "__issn":          rec.issn or "",
                "__eissn":         "",
                "__isbn":          "",
                "__eid":           "",
                "__link":          rec.url or "",
                "__source_title":  rec.source_journal or "",
                "__document_type": rec.publication_type or "",
                "_source":         "OpenAlex BD",
                "¿En cobertura?": "",
                "Revista en Scopus": "",
                "Estado revista": "",
                "Título oficial (Scopus)": "",
                "Editorial (Scopus)": "",
                "Periodos de cobertura": "",
            }
            pub = {
                "issn":         rec.issn or "",
                "isbn":         "",
                "doi":          rec.doi or "",
                "eid":          "",
                "source_title": rec.source_journal or "",
                "year":         rec.publication_year,
                "title":        rec.title or "",
                "_prev_in_coverage":         "",
                "_prev_journal_found":        "",
                "_prev_journal_status":       "",
                "_prev_scopus_journal_title": "",
                "_prev_scopus_publisher":     "",
                "_prev_coverage_periods_str": "",
            }
            oa_rows.append(row)
            oa_pubs.append(pub)

        return oa_rows, oa_pubs

    _t0 = _ctime.time()
    oa_extra_rows, oa_extra_pubs = await run_in_threadpool(_load_openalex_extra, rows)
    logger.info(f"[check-coverage] Paso 2.5 (OpenAlex BD extra): {_ctime.time()-_t0:.1f}s")
    if oa_extra_rows:
        logger.info(
            f"[check-coverage] OpenAlex BD: añadiendo {len(oa_extra_rows)} publicaciones "
            f"exclusivas (no están en el Excel subido)."
        )
        rows.extend(oa_extra_rows)
        publications.extend(oa_extra_pubs)
    else:
        logger.info("[check-coverage] OpenAlex BD: sin publicaciones adicionales que agregar.")

    # Diagnóstico
    n_issn  = sum(1 for p in publications if p["issn"])
    n_isbn  = sum(1 for p in publications if p["isbn"] and not p["issn"])
    n_doi   = sum(1 for p in publications if p["doi"] and not p["issn"] and not p["isbn"])
    n_eid   = sum(1 for p in publications if p["eid"] and not p["issn"] and not p["isbn"] and not p["doi"])
    n_title = sum(1 for p in publications if p["source_title"] and not p["issn"] and not p["isbn"] and not p["doi"] and not p["eid"])
    n_none  = sum(1 for p in publications if not any([p["issn"], p["isbn"], p["doi"], p["eid"], p["source_title"]]))
    logger.info(
        f"[check-coverage] Identificadores: ISSN={n_issn}  ISBN={n_isbn}  "
        f"DOI={n_doi}  EID={n_eid}  Solo-título={n_title}  Sin-id={n_none}  "
        f"|| workers={max_workers}"
    )
    for i, p in enumerate(publications[:3]):
        logger.info(
            f"[check-coverage][diag] Pub#{i+1}: "
            f"issn={p['issn']!r}  isbn={p['isbn']!r}  doi={p['doi']!r}  "
            f"eid={p['eid']!r}  src={p['source_title']!r}  "
            f"prev_found={p['_prev_journal_found']!r}  prev_cov={p['_prev_in_coverage']!r}"
        )

    # 3. Consultar Scopus
    logger.info(f"[check-coverage] Paso 3 (Scopus API): iniciando con {max_workers} worker(s)...")
    _t0 = _ctime.time()
    extractor = SerialTitleExtractor()
    try:
        enriched = await run_in_threadpool(
            extractor.check_publications_coverage, publications, max_workers,
        )
    except SerialTitleAPIError as e:
        raise HTTPException(502, f"Error consultando Scopus Serial Title API: {e}")

    logger.info(
        f"[check-coverage] Paso 3 (Scopus API): {_ctime.time()-_t0:.1f}s "
        f"— {len(enriched)} resultados recibidos."
    )

    # 4. Fusionar resultados
    for row, cov in zip(rows, enriched):
        row.update({
            "journal_found":         cov.get("journal_found", False),
            "journal_found_via":     cov.get("journal_found_via", ""),
            "scopus_journal_title":  cov.get("scopus_journal_title", ""),
            "scopus_publisher":      cov.get("scopus_publisher", ""),
            "journal_status":        cov.get("journal_status", ""),
            "coverage_from":         cov.get("coverage_from"),
            "coverage_to":           cov.get("coverage_to"),
            "coverage_periods":      cov.get("coverage_periods", []),
            "in_coverage":           cov.get("in_coverage", "Sin datos"),
            "journal_subject_areas": cov.get("journal_subject_areas", ""),
            "resolved_issn":         cov.get("resolved_issn", ""),
            "resolved_eissn":        cov.get("resolved_eissn", ""),
        })

    n_found     = sum(1 for r in enriched if r.get("journal_found"))
    n_in_cov    = sum(1 for r in enriched if r.get("in_coverage") == "Sí")
    n_discont   = sum(1 for r in enriched if str(r.get("journal_status", "")).strip().lower() in ("discontinued", "inactive", "inactiva"))
    n_sin_datos = sum(1 for r in enriched if r.get("in_coverage") == "Sin datos")
    n_from_scopus = sum(1 for r in rows if r.get("_source") == "Scopus Export")
    n_from_oa     = sum(1 for r in rows if r.get("_source") == "OpenAlex BD")
    logger.info(
        f"[check-coverage] Resultados: encontradas={n_found}/{len(enriched)}  "
        f"en-cobertura={n_in_cov}  descontinuadas={n_discont}  sin-datos={n_sin_datos}  "
        f"fuente=Scopus:{n_from_scopus} OA:{n_from_oa}"
    )

    # 4.5 Cruce con OpenAlex BD para publicaciones en revistas descontinuadas
    if n_discont > 0:
        _t0 = _ctime.time()
        await run_in_threadpool(_enrich_discontinued_with_openalex, rows)
        n_oa = sum(1 for r in rows if r.get("_openalex"))
        logger.info(
            f"[check-coverage] Paso 4.5 (OpenAlex cruce descontinuadas): "
            f"{_ctime.time()-_t0:.1f}s — {n_oa} coincidencias"
        )
        logger.info(
            f"[check-coverage] Publicaciones descontinuadas con datos OpenAlex: {n_oa}/{n_discont}"
        )

    # 4.6 Rescate OpenAlex→Scopus para publicaciones sin resolver
    n_not_found = sum(1 for r in rows if not r.get("journal_found"))
    if n_not_found > 0:
        logger.info(
            f"[check-coverage] Paso 4.6 (Rescate OpenAlex→Scopus): "
            f"{n_not_found} publicaciones sin resolver..."
        )
        _t0 = _ctime.time()
        await run_in_threadpool(_rescue_not_found_via_openalex, rows, extractor)
        _elapsed_rescue = _ctime.time() - _t0
        n_rescued = sum(
            1 for r in rows
            if r.get("journal_found") and "openalex" in str(r.get("journal_found_via", ""))
        )
        n_found   = sum(1 for r in rows if r.get("journal_found"))
        n_in_cov  = sum(1 for r in rows if r.get("in_coverage") == "Sí")
        n_discont = sum(1 for r in rows if str(r.get("journal_status", "")).strip().lower() in ("discontinued", "inactive"))
        logger.info(
            f"[check-coverage] Paso 4.6 completado en {_elapsed_rescue:.1f}s: rescatadas={n_rescued}  "
            f"total_encontradas={n_found}/{len(rows)}  en-cobertura={n_in_cov}  descontinuadas={n_discont}"
        )

    # 5. Generar Excel de salida
    logger.info("[check-coverage] Paso 5 (Generar Excel): iniciando...")
    _t0 = _ctime.time()
    try:
        excel_bytes = await run_in_threadpool(generate_publications_coverage_excel, headers, rows)
    except Exception as e:
        logger.exception("[check-coverage] Error generando Excel")
        raise HTTPException(500, f"Error generando el archivo Excel de salida: {e}")

    logger.info(
        f"[check-coverage] Paso 5 (Generar Excel): {_ctime.time()-_t0:.1f}s "
        f"— {len(excel_bytes):,} bytes\n"
        f"[check-coverage] ✓ Pipeline completo en {_ctime.time()-_t_pipeline:.1f}s total."
    )

    filename = f"cobertura_scopus_{len(rows)}_pubs.xlsx"
    return StreamingResponse(
        _io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── POST /pipeline/scopus/reprocess-coverage ─────────────────────────────────

@router.post(
    "/scopus/reprocess-coverage",
    summary="Re-procesar publicaciones no resueltas (Excel de resultado previo)",
    responses={
        200: {
            "description": "Excel actualizado con las publicaciones re-procesadas.",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            },
        }
    },
)
async def scopus_reprocess_coverage(
    file: UploadFile = File(
        ...,
        description="Excel de salida generado por /scopus/check-publications-coverage",
    ),
    max_workers: int = Query(
        1, ge=1, le=5,
        description="Hilos paralelos para consultar la API de Scopus",
    ),
):
    """
    Re-procesa únicamente las publicaciones cuya revista **no se encontró** en
    Scopus o que quedaron como **Sin datos** en el primer pase.

    Las filas ya resueltas (`En Scopus = Sí`) se conservan intactas.

    Flujo:
    1. Lee el Excel de resultado.
    2. Separa filas resueltas (fast-path) de filas pendientes (re-consulta).
    3. Ejecuta `check_publications_coverage`.
    4. Rescate OpenAlex → Scopus para las que siguen sin resolver.
    5. Genera el Excel de salida actualizado.
    """
    import io as _io
    import time as _rtime
    from starlette.concurrency import run_in_threadpool
    from fastapi.responses import StreamingResponse
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import read_publications_from_excel, generate_publications_coverage_excel

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "El archivo está vacío.")

    _t_pipeline = _rtime.time()
    logger.info(f"[reprocess] Archivo recibido: '{file.filename}' ({len(raw):,} bytes)")

    # 1. Leer Excel
    _t0 = _rtime.time()
    try:
        headers, rows = await run_in_threadpool(read_publications_from_excel, raw)
    except ValueError as e:
        raise HTTPException(400, str(e))

    logger.info(
        f"[reprocess] Paso 1 (Leer Excel): {_rtime.time()-_t0:.1f}s "
        f"— {len(rows)} publicaciones, {len(headers)} columnas"
    )

    # 2. Clasificar filas
    def _is_resolved(row: dict) -> bool:
        found = str(row.get("En Scopus") or row.get("Revista en Scopus") or "").strip().lower()
        cov   = str(row.get("¿En cobertura?") or "").strip().lower()
        return found == "sí" and bool(cov) and cov != "sin datos"

    for row in rows:
        row["_source"] = str(row.get("Fuente") or "Scopus Export").strip() or "Scopus Export"

    n_resolved = sum(1 for r in rows if _is_resolved(r))
    n_pending  = len(rows) - n_resolved
    logger.info(f"[reprocess] Filas resueltas={n_resolved}  pendientes={n_pending}")

    publications = [
        _build_pub_entry(row, include_prev=_is_resolved(row))
        for row in rows
    ]

    n_eid  = sum(1 for p in publications if p.get("eid"))
    n_issn = sum(1 for p in publications if p.get("issn"))
    n_doi  = sum(1 for p in publications if p.get("doi"))
    n_none = sum(
        1 for p in publications
        if not any([p.get("eid"), p.get("issn"), p.get("doi"), p.get("source_title")])
    )
    logger.info(
        f"[reprocess] IDs disponibles: EID={n_eid}  ISSN={n_issn}  DOI={n_doi}  sin-id={n_none}"
    )

    # 3. Scopus check de cobertura
    logger.info(f"[reprocess] Paso 3 (Scopus API): iniciando con {max_workers} worker(s)...")
    _t0 = _rtime.time()
    extractor = SerialTitleExtractor()
    try:
        enriched = await run_in_threadpool(
            extractor.check_publications_coverage, publications, max_workers
        )
    except SerialTitleAPIError as e:
        raise HTTPException(502, f"Error en la API de Scopus: {e}")
    except Exception as e:
        logger.exception("[reprocess] Error en check_publications_coverage")
        raise HTTPException(500, f"Error interno al verificar cobertura: {e}")

    logger.info(
        f"[reprocess] Paso 3 (Scopus API): {_rtime.time()-_t0:.1f}s — {len(enriched)} resultados"
    )

    # 4. Fusionar resultados
    for row, result in zip(rows, enriched):
        row.update({k: v for k, v in result.items() if not k.startswith("_prev_")})

    # 4.5 Enriquecer descontinuadas con OpenAlex BD
    _t0 = _rtime.time()
    await run_in_threadpool(_enrich_discontinued_with_openalex, rows)
    logger.info(f"[reprocess] Paso 4.5 (OpenAlex cruce descontinuadas): {_rtime.time()-_t0:.1f}s")

    # 4.6 Rescate OpenAlex→Scopus
    n_not_found = sum(1 for r in rows if not r.get("journal_found"))
    if n_not_found > 0:
        logger.info(
            f"[reprocess] Paso 4.6 (Rescate OpenAlex→Scopus): {n_not_found} publicaciones..."
        )
        _t0 = _rtime.time()
        await run_in_threadpool(_rescue_not_found_via_openalex, rows, extractor)
        n_rescued = sum(
            1 for r in rows
            if r.get("journal_found") and "openalex" in str(r.get("journal_found_via", ""))
        )
        logger.info(
            f"[reprocess] Paso 4.6 completado en {_rtime.time()-_t0:.1f}s: rescatadas={n_rescued}"
        )

    n_found  = sum(1 for r in rows if r.get("journal_found"))
    n_in_cov = sum(1 for r in rows if r.get("in_coverage") == "Sí")
    logger.info(
        f"[reprocess] Resultado final: encontradas={n_found}/{len(rows)}  en-cobertura={n_in_cov}"
    )

    # 5. Generar Excel
    logger.info("[reprocess] Paso 5 (Generar Excel): iniciando...")
    _t0 = _rtime.time()
    try:
        excel_bytes = await run_in_threadpool(generate_publications_coverage_excel, headers, rows)
    except Exception as e:
        logger.exception("[reprocess] Error generando Excel")
        raise HTTPException(500, f"Error generando el archivo Excel de salida: {e}")

    logger.info(
        f"[reprocess] Paso 5 (Generar Excel): {_rtime.time()-_t0:.1f}s "
        f"— {len(excel_bytes):,} bytes\n"
        f"[reprocess] ✓ Pipeline completo en {_rtime.time()-_t_pipeline:.1f}s total."
    )

    filename = f"cobertura_reprocesada_{len(rows)}_pubs.xlsx"
    return StreamingResponse(
        _io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── GET /pipeline/scopus/debug/raw ───────────────────────────────────────────

@router.get("/scopus/debug/raw", summary="Depuración: respuesta cruda de Scopus")
def scopus_debug_raw():
    from config import institution
    from extractors.scopus import ScopusExtractor

    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    extractor = ScopusExtractor()
    query = extractor._build_query(None, None, affiliation_id)
    params = {
        "query": query,
        "count": extractor.config.max_per_page,
        "sort":  "pubyear",
        "field": (
            "dc:identifier,doi,dc:title,prism:publicationName,"
            "prism:coverDate,subtypeDescription,citedby-count,"
            "author,prism:issn,openaccess,openaccessFlag,"
            "dc:description,authkeywords,prism:volume,prism:issueIdentifier,"
            "prism:pageRange,afid,affiliation"
        ),
    }
    resp = extractor.session.get(extractor.SEARCH_URL, params=params, timeout=extractor.config.timeout)
    return resp.json()


# ── GET /pipeline/scopus/by-institution ──────────────────────────────────────

@router.get(
    "/scopus/by-institution",
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
    "/scopus/by-institution/reconcile",
    response_model=dict,
    summary="Crear y reconciliar publicaciones por ID institucional",
)
def reconcile_scopus_by_institution(db: Session = Depends(get_db)):
    from config import institution
    from extractors.scopus import ScopusExtractor
    from db.models import CanonicalPublication, ScopusRecord
    from sqlalchemy.orm.exc import NoResultFound

    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    if not affiliation_id:
        return {"created": 0, "reconciled": 0, "duplicates": 0}

    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=1000)
    logger.info(f"Registros obtenidos de Scopus: {len(records)}")

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
                raw_data=None,
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
        except NoResultFound:
            pub = CanonicalPublication(doi=doi, title=getattr(r, "title", None))
            db.add(pub)
            created += 1

    db.commit()
    return {"created": created, "reconciled": reconciled, "duplicates": duplicates, "inserted": inserted}
