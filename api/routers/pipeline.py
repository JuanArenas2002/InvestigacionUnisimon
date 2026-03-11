import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Body, UploadFile, File, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from api.dependencies import get_db
from api.schemas.external_records import (
    ExtractionRequest,
    ScopusExtractionRequest,
    JsonLoadRequest,
    IngestRequest,
    IngestResponse,
    ExtractionResponse,
    ReconciliationStatsResponse,
    CrossrefScopusResponse,
    EnrichedFieldDetail,
)
from api.schemas.serial_title import (
    JournalCoverageResponse,
    BulkCoverageRequest,
    BulkCoverageResponse,
)
from api.schemas.common import MessageResponse
from config import DATA_DIR
from extractors.base import StandardRecord
from reconciliation.engine import ReconciliationEngine

import re as _re

logger = logging.getLogger("pipeline")
router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

# Funciones auxiliares compartidas entre los dos endpoints de cobertura
from api.routers._pipeline_helpers import (      # noqa: E402
    _enrich_discontinued_with_openalex,
    _rescue_not_found_via_openalex,
)

# ── Helpers compartidos ───────────────────────────────────────────────────────
_EID_FROM_URL    = _re.compile(r"[?&]eid=(2-s2\.0-[^&\s]+)", _re.IGNORECASE)
_EID_FROM_PATH   = _re.compile(r"/pages/publications/(\d+)", _re.IGNORECASE)
_EID_FROM_RECORD = _re.compile(r"[?&]eid=([^&\s]+)", _re.IGNORECASE)


def _resolve_eid(row: dict) -> str:
    """Devuelve el EID directo o lo extrae del Link si la columna EID está vacía.

    Maneja tres formatos de URL de Scopus:
      - ?eid=2-s2.0-XXXXX          → EID completo en query param
      - /pages/publications/NNNNN  → número puro, se prefija con '2-s2.0-'
      - Otros ?eid=XXX             → se usa tal cual
    Ignora valores placeholder del Excel de salida ('—', etc.).
    """
    eid = str(row.get("__eid", "") or "").strip()
    if eid and eid.lower() not in _EMPTY_PLACEHOLDERS:
        return eid
    link = str(row.get("__link", "") or "").strip()
    if link:
        m = _EID_FROM_PATH.search(link)
        if m:
            return f"2-s2.0-{m.group(1)}"
        m = _EID_FROM_RECORD.search(link)
        if m:
            return m.group(1).strip()
    return ""


# Valores "vacíos" que el Excel de salida escribe como placeholder
_EMPTY_PLACEHOLDERS = frozenset({"—", "-", "–", "n/a", "na", "none", "null", "sin datos", "no encontrada"})


def _clean_id(v) -> str:
    """Normaliza un valor de identificador: elimina placeholders del Excel de salida."""
    s = str(v or "").strip()
    return "" if s.lower() in _EMPTY_PLACEHOLDERS else s


def _build_pub_entry(row: dict, *, include_prev: bool = True) -> dict:
    """
    Construye el dict de publicación para check_publications_coverage.
    Si include_prev=False se omiten los _prev_* (fuerza re-consulta).
    Lee nombres de columna tanto del formato Scopus original como del
    formato de salida del propio reporte (para re-procesamiento).
    Normaliza los valores '—' que el Excel de salida escribe en celdas vacías.
    """
    def _g(*keys) -> str:
        for k in keys:
            v = row.get(k)
            s = str(v or "").strip()
            if s and s.lower() not in _EMPTY_PLACEHOLDERS:
                return s
        return ""

    pub = {
        "issn":         ";".join(filter(None, [
            _clean_id(row.get("__issn",  "")),
            _clean_id(row.get("__eissn", "")),
        ])),
        "isbn":         _clean_id(row.get("__isbn", "")),
        "doi":          _clean_id(row.get("__doi", "")),
        "eid":          _resolve_eid(row),
        "source_title": _g("__source_title"),
        "year":         row.get("__year"),
        "title":        _g("__title"),
    }
    if include_prev:
        pub.update({
            # Formato antiguo primero, luego nuevo formato de salida
            "_prev_in_coverage":         _g("¿En cobertura?"),
            "_prev_journal_found":        _g("Revista en Scopus", "En Scopus"),
            "_prev_journal_status":       _g("Estado revista"),
            "_prev_scopus_journal_title": _g("Título oficial (Scopus)", "Revista (Scopus)"),
            "_prev_scopus_publisher":     _g("Editorial (Scopus)", "Editorial"),
            "_prev_coverage_periods_str": _g("Periodos de cobertura", "Periodos cobertura"),
        })
    else:
        pub.update({
            "_prev_in_coverage":         "",
            "_prev_journal_found":        "",
            "_prev_journal_status":       "",
            "_prev_scopus_journal_title": "",
            "_prev_scopus_publisher":     "",
            "_prev_coverage_periods_str": "",
        })
    return pub

# --- ENDPOINTS ---

@router.post("/extract/openalex", response_model=ExtractionResponse, summary="Extraer de OpenAlex por ROR", tags=["OpenAlex"])
def extract_openalex(body: ExtractionRequest):
    """
    Extrae publicaciones de OpenAlex usando el ROR id de la institución (por defecto) o el proporcionado.
    Guarda los registros en openalex_records y reconcilia.
    """
    from config import institution
    from extractors.openalex import OpenAlexExtractor
    from db.session import get_engine
    from sqlalchemy.orm import sessionmaker
    from db.models import OpenalexRecord

    ror_id = body.affiliation_id or institution.ror_id
    extractor = OpenAlexExtractor(ror_id=ror_id)
    records = extractor.extract(
        year_from=body.year_from,
        year_to=body.year_to,
        max_results=body.max_results,
    )
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    inserted = 0
    for r in records:
        # Evitar duplicados por openalex_id
        if not r.source_id:
            continue
        exists = session.query(OpenalexRecord).filter_by(openalex_id=r.source_id).first()
        if exists:
            continue
        rec = OpenalexRecord(
            openalex_id=r.source_id,
            doi=r.doi,
            title=r.title,
            publication_year=r.publication_year,
            publication_date=r.publication_date,
            publication_type=r.publication_type,
            source_journal=r.source_journal,
            issn=r.issn,
            is_open_access=r.is_open_access,
            citation_count=r.citation_count,
            status='pending',
            raw_data=None,
        )
        session.add(rec)
        inserted += 1
    session.commit()
    session.close()
    # Reconciliar
    from reconciliation.engine import ReconciliationEngine
    engine = ReconciliationEngine()
    stats = engine.reconcile_pending(batch_size=500)
    return ExtractionResponse(
        extracted=len(records),
        inserted=inserted,
        message=f"Extraídos {len(records)}, insertados {inserted}",
        reconciliation=ReconciliationStatsResponse(**stats.to_dict()),
    )


# --- RECONCILIACIÓN GLOBAL TODOS CONTRA TODOS ---
@router.post("/reconcile/all-sources", response_model=dict, summary="Reconciliar todos los registros de todas las fuentes")
def reconcile_all_sources(db: Session = Depends(get_db)):
    """
    Recorre todos los registros de todas las fuentes, busca por DOI en las demás fuentes y reconcilia en publicaciones canónicas.
    """
    from db.models import CanonicalPublication, ScopusRecord, OpenalexRecord, WosRecord, CvlacRecord, DatosAbiertosRecord
    from sqlalchemy.orm.exc import NoResultFound
    sources = [ScopusRecord, OpenalexRecord, WosRecord, CvlacRecord, DatosAbiertosRecord]
    created, reconciled, duplicates, enriched = 0, 0, 0, 0
    seen_dois = set()
    import re
    from unidecode import unidecode
    def normalize_doi(doi):
        if not doi:
            return None
        doi = doi.strip().lower()
        doi = doi.replace('https://doi.org/', '').replace('http://doi.org/', '')
        doi = doi.split()[0]  # Quitar espacios extra
        # Validar formato DOI
        if not re.match(r'^10\.\d{4,9}/[-._;()/:a-z0-9]+$', doi):
            return None
        return doi
    def normalize_title(title):
        if not title:
            return None
        return unidecode(title.strip().lower())

    campos = [
        "title", "publication_year", "publication_date", "publication_type", "source_journal", "issn", "is_open_access", "citation_count"
    ]
    # Recorre cada fuente
    for SourceModel in sources:
        records = db.query(SourceModel).all()
        for r in records:
            doi = normalize_doi(getattr(r, "doi", None))
            if not doi:
                continue
            if doi in seen_dois:
                duplicates += 1
                continue
            seen_dois.add(doi)
            try:
                pub = db.query(CanonicalPublication).filter_by(doi=doi).one()
                enriched_this = False
                prov = dict(pub.field_provenance or {})
                for campo in campos:
                    valor_canonico = getattr(pub, campo, None)
                    valor_fuente = getattr(r, campo, None)
                    # Normalizar título antes de comparar
                    if campo == "title":
                        valor_canonico = normalize_title(valor_canonico)
                        valor_fuente = normalize_title(valor_fuente)
                    if (valor_canonico is None or valor_canonico == "") and valor_fuente not in (None, ""):
                        # Guardar valor original (no normalizado) en el canónico
                        if campo == "title":
                            setattr(pub, campo, getattr(r, campo, None))
                        else:
                            setattr(pub, campo, valor_fuente)
                        prov[campo] = r.source_name if hasattr(r, "source_name") else SourceModel.__tablename__
                        enriched_this = True
                if enriched_this:
                    pub.field_provenance = prov
                    enriched += 1
                reconciled += 1
            except NoResultFound:
                pub = CanonicalPublication(doi=doi, title=getattr(r, "title", None))
                # Inicializar provenance
                prov = {"title": r.source_name if hasattr(r, "source_name") else SourceModel.__tablename__}
                pub.field_provenance = prov
                db.add(pub)
                db.commit()
                created += 1
    db.commit()
    return {"created": created, "reconciled": reconciled, "duplicates": duplicates, "enriched": enriched, "total_processed": len(seen_dois)}

@router.get("/scopus/test-extract", summary="Test extracción Scopus (guardar en scopus_records)")
def scopus_test_extract():
    from config import institution
    from extractors.scopus import ScopusExtractor
    from db.session import get_engine
    from sqlalchemy.orm import sessionmaker
    from db.models import ScopusRecord
    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=10)
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    inserted = 0
    for r in records:
        # Evitar duplicados por scopus_doc_id
        if not r.source_id:
            continue
        exists = session.query(ScopusRecord).filter_by(scopus_doc_id=r.source_id).first()
        if exists:
            continue
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
            # Puedes agregar más campos si lo deseas
            status='pending',
            raw_data=None,
        )
        session.add(rec)
        inserted += 1
    session.commit()
    session.close()
    return {"inserted": inserted, "total": len(records)}


# --- ENDPOINTS: Cobertura de revistas (Serial Title API) --- Módulo desacoplado
# Extractor: extractors/serial_title.py
# Exportador: api/exporters/excel.py
# Schemas:    api/schemas/serial_title.py

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
    """
    Ejemplo: GET /pipeline/scopus/journal-coverage?issn=0028-0836
    """
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError

    extractor = SerialTitleExtractor()
    try:
        result = extractor.get_journal_coverage(issn)
    except SerialTitleAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])

    return JournalCoverageResponse(**result)


@router.post(
    "/scopus/journal-coverage/bulk",
    summary="Cobertura masiva de revistas en Scopus — retorna Excel",
    description=(
        "Recibe una lista de ISSNs, los consulta en paralelo al Serial Title API "
        "de Scopus y devuelve un archivo Excel (.xlsx) con los resultados, "
        "incluyendo años de cobertura, estado y editorial de cada revista."
    ),
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Archivo Excel con la cobertura de las revistas consultadas.",
        }
    },
)
def scopus_journal_coverage_bulk(body: BulkCoverageRequest):
    """
    Ejemplo de body:
    ```json
    {
      "issns": ["2595-3982", "0028-0836", "1234-5678"],
      "max_workers": 5
    }
    ```
    Retorna un archivo `journal_coverage.xlsx` para descargar.
    """
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

    # Convertir subject_areas (lista) a str para serializar correctamente
    serialized = []
    for r in results:
        row = dict(r)
        if isinstance(row.get("subject_areas"), list):
            row["subject_areas"] = " | ".join(row["subject_areas"])
        serialized.append(row)

    excel_bytes = generate_journal_coverage_excel(results)

    filename = f"journal_coverage_{len(body.issns)}_issns.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post(
    "/scopus/journal-coverage/bulk-from-file",
    summary="Cobertura masiva de revistas — carga un Excel con ISSNs",
    description=(
        "Sube un archivo Excel (.xlsx) con una columna de ISSNs (columna A). "
        "El sistema extrae los ISSNs, los consulta en paralelo en Scopus y "
        "devuelve un nuevo Excel con los resultados de cobertura. "
        "La primera fila puede ser encabezado; se detecta y omite automáticamente."
    ),
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
    """
    Formato del Excel de entrada:
    | ISSN        |       ← encabezado opcional
    |-------------|  
    | 2595-3982   |
    | 0028-0836   |
    | 25953982    |  ← con o sin guion
    """
    import io
    from fastapi.responses import StreamingResponse
    from extractors.serial_title import SerialTitleExtractor, SerialTitleAPIError
    from api.exporters.excel import generate_journal_coverage_excel, read_issns_from_excel

    # Validar extensión
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Solo se aceptan archivos Excel (.xlsx). Recibido: " + file.filename,
        )

    # Leer archivo
    file_bytes = file.file.read()
    try:
        issns = read_issns_from_excel(file_bytes)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # Consultar Scopus
    extractor = SerialTitleExtractor()
    try:
        results = extractor.get_bulk_coverage(issns=issns, max_workers=max_workers)
    except SerialTitleAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Generar Excel de salida
    excel_bytes = generate_journal_coverage_excel(results)
    filename = f"journal_coverage_{len(issns)}_issns.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/scopus/journal-coverage/debug",
    summary="[DEBUG] JSON crudo + resultado parseado del Serial Title API",
)
def scopus_journal_coverage_debug(issn: str):
    """
    Retorna el JSON sin procesar del Serial Title API **y** el resultado
    del parser interno, para comparar ambos y detectar discrepancias.
    Ejemplo: GET /pipeline/scopus/journal-coverage/debug?issn=1473-2130
    """
    from extractors.serial_title import SerialTitleExtractor

    extractor = SerialTitleExtractor()
    clean_issn = issn.strip().replace("-", "")
    url = f"{extractor.BASE_URL}/{clean_issn}"
    try:
        resp = extractor.session.get(
            url,
            params={"view": "ENHANCED"},
            timeout=20,
        )
        raw = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Parsed result
    try:
        parsed = extractor._parse_entry(clean_issn, raw)
    except Exception as e:
        parsed = {"parse_error": str(e)}

    # Extraer solo los campos de cobertura del raw para comparar fácilmente
    entry = (raw.get("serial-metadata-response", {}).get("entry") or [{}])[0]
    coverage_debug = {
        "coverageStartYear_root": entry.get("coverageStartYear"),
        "coverageEndYear_root":   entry.get("coverageEndYear"),
        "covers_raw":             entry.get("covers"),
        "coverageInfo_raw":       entry.get("coverageInfo"),
        "all_entry_keys":         sorted(entry.keys()) if isinstance(entry, dict) else [],
    }

    return {
        "status_code": resp.status_code,
        "url":         str(resp.url),
        "coverage_fields": coverage_debug,
        "parsed_result":   parsed,
        "raw_body":        raw,
    }


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
    file: UploadFile = File(..., description="Excel de exportación Scopus (columnas: Title, Year, Source title, ISSN, DOI, …)"),
    max_workers: int = Query(1, ge=1, le=5, description="Hilos paralelos para consultar la API de Scopus (1=secuencial, evita 429)"),
):
    """
    Acepta un Excel de exportación de Scopus (o similar) con una publicación por fila.

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

    # 1. Leer Excel (bloqueante — openpyxl)
    _t0 = _ctime.time()
    try:
        headers, rows = await run_in_threadpool(read_publications_from_excel, raw)
    except ValueError as e:
        raise HTTPException(400, str(e))

    logger.info(f"[check-coverage] Paso 1 (Leer Excel): {_ctime.time()-_t0:.1f}s — {len(rows)} publicaciones, {len(headers)} columnas")

    # 2. Mapear a formato que acepta check_publications_coverage
    # Marcar filas del Excel con su fuente
    for row in rows:
        row["_source"] = "Scopus Export"

    publications = [_build_pub_entry(row, include_prev=True) for row in rows]

    # 2.5 Incorporar publicaciones de OpenAlex BD que NO están en el Excel
    #     → análisis unificado Scopus Export + OpenAlex BD
    def _load_openalex_extra(excel_rows: list[dict]) -> tuple[list[dict], list[dict]]:
        """
        Consulta openalex_records y devuelve (oa_rows, oa_pubs):
          - oa_rows  : dicts con claves __* para el Excel (una fila por pub)
          - oa_pubs  : dicts con claves que acepta check_publications_coverage
        Solo incluye registros cuyo DOI NO está ya en el Excel subido.
        """
        from db.session import get_session
        from db.models import OpenalexRecord as _OARec

        def _nd(d: str) -> str:
            d = (d or "").strip().lower()
            d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
            return d.split()[0] if d else ""

        # DOIs ya presentes en el Excel (para deduplicar)
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

        oa_rows: list[dict] = []
        oa_pubs: list[dict] = []
        seen: set[str] = set()

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
                # Sin valores previos de cobertura
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

    # Resumen de identifiers disponibles
    n_issn  = sum(1 for p in publications if p["issn"])
    n_isbn  = sum(1 for p in publications if p["isbn"] and not p["issn"])
    n_doi   = sum(1 for p in publications if p["doi"] and not p["issn"] and not p["isbn"])
    n_eid   = sum(1 for p in publications if p["eid"] and not p["issn"] and not p["isbn"] and not p["doi"])
    n_title = sum(1 for p in publications if p["source_title"] and not p["issn"] and not p["isbn"] and not p["doi"] and not p["eid"])
    n_none  = sum(1 for p in publications if not p["issn"] and not p["isbn"] and not p["doi"] and not p["eid"] and not p["source_title"])
    logger.info(
        f"[check-coverage] Identificadores: ISSN={n_issn}  ISBN={n_isbn}  "
        f"DOI={n_doi}  EID={n_eid}  Solo-título={n_title}  Sin-id={n_none}  "
        f"|| workers={max_workers}"
    )
    # Diagnóstico: mostrar primeros 3 registros con sus identificadores extraidos
    for i, p in enumerate(publications[:3]):
        logger.info(
            f"[check-coverage][diag] Pub#{i+1}: "
            f"issn={p['issn']!r}  isbn={p['isbn']!r}  doi={p['doi']!r}  "
            f"eid={p['eid']!r}  src={p['source_title']!r}  "
            f"prev_found={p['_prev_journal_found']!r}  prev_cov={p['_prev_in_coverage']!r}"
        )

    # 3. Consultar Scopus en paralelo (bloqueante — ThreadPoolExecutor interno)
    logger.info(f"[check-coverage] Paso 3 (Scopus API): iniciando consultas con {max_workers} worker(s)...")
    _t0 = _ctime.time()
    extractor = SerialTitleExtractor()
    try:
        enriched = await run_in_threadpool(
            extractor.check_publications_coverage,
            publications,
            max_workers,
        )
    except SerialTitleAPIError as e:
        raise HTTPException(502, f"Error consultando Scopus Serial Title API: {e}")

    logger.info(f"[check-coverage] Paso 3 (Scopus API): {_ctime.time()-_t0:.1f}s — {len(enriched)} resultados recibidos.")

    # 4. Fusionar resultados con filas originales
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

    # Resumen de resultados
    n_found       = sum(1 for r in enriched if r.get("journal_found"))
    n_in_cov      = sum(1 for r in enriched if r.get("in_coverage") == "Sí")
    n_discont     = sum(1 for r in enriched if str(r.get("journal_status","")).strip().lower() in ("discontinued", "inactive", "inactiva"))
    n_sin_datos   = sum(1 for r in enriched if r.get("in_coverage") == "Sin datos")
    n_from_scopus = sum(1 for r in rows if r.get("_source") == "Scopus Export")
    n_from_oa     = sum(1 for r in rows if r.get("_source") == "OpenAlex BD")
    logger.info(
        f"[check-coverage] Resultados: encontradas={n_found}/{len(enriched)}  "
        f"en-cobertura={n_in_cov}  descontinuadas={n_discont}  sin-datos={n_sin_datos}  "
        f"fuente=Scopus:{n_from_scopus} OA:{n_from_oa}"
    )

    # 4.5 Cruce con OpenAlex DB: enriquecer publicaciones en revistas descontinuadas
    # Se consulta openalex_records por DOI para mostrar detalle adicional en el Excel.
    def _enrich_discontinued_with_openalex(rows_local: list[dict]) -> None:
        """Adjunta datos de openalex_records (por DOI) a filas con revistas descontinuadas."""
        from db.session import get_session
        from db.models import OpenalexRecord as _OARecord

        _DISC = {"discontinued", "inactive", "inactiva"}

        def _ndoi(d: str) -> str:
            d = (d or "").strip().lower()
            d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
            return d.split()[0] if d else ""

        disc_dois = list({
            _ndoi(str(row.get("__doi") or ""))
            for row in rows_local
            if str(row.get("journal_status", "")).strip().lower() in _DISC
            and row.get("__doi")
        })
        disc_dois = [d for d in disc_dois if d]

        if not disc_dois:
            logger.info("[check-coverage] OpenAlex cruce: ningún DOI descontinuado para consultar.")
            return

        session = get_session()
        try:
            oa_records = session.query(_OARecord).filter(
                _OARecord.doi.in_(disc_dois)
            ).all()
            openalex_map: dict[str, dict] = {}
            for rec in oa_records:
                key = _ndoi(rec.doi or "")
                if key:
                    openalex_map[key] = {
                        "oa_work_id":    rec.openalex_work_id or "",
                        "oa_title":      rec.title or "",
                        "oa_year":       rec.publication_year,
                        "oa_authors":    rec.authors_text or "",
                        "oa_journal":    rec.source_journal or "",
                        "oa_issn":       rec.issn or "",
                        "oa_open_access": (
                            "Sí" if rec.is_open_access is True
                            else ("No" if rec.is_open_access is False else "")
                        ),
                        "oa_oa_status":  rec.oa_status or "",
                        "oa_citations":  rec.citation_count if rec.citation_count is not None else 0,
                        "oa_url":        rec.url or "",
                    }
            logger.info(
                f"[check-coverage] OpenAlex cruce: {len(disc_dois)} DOIs descontinuados "
                f"→ {len(openalex_map)} coincidencias en BD"
            )
            for row in rows_local:
                key = _ndoi(str(row.get("__doi") or ""))
                if key in openalex_map:
                    row["_openalex"] = openalex_map[key]
        except Exception as exc:
            logger.warning(f"[check-coverage] Cruce OpenAlex BD falló: {exc}")
        finally:
            session.close()

    if n_discont > 0:
        _t0 = _ctime.time()
        await run_in_threadpool(_enrich_discontinued_with_openalex, rows)
        n_oa = sum(1 for r in rows if r.get("_openalex"))
        logger.info(f"[check-coverage] Paso 4.5 (OpenAlex cruce descontinuadas): {_ctime.time()-_t0:.1f}s — {n_oa} coincidencias")
        logger.info(f"[check-coverage] Publicaciones descontinuadas con datos OpenAlex: {n_oa}/{n_discont}")

    # 4.6 Rescate OpenAlex→Scopus: para publicaciones NO encontradas en Scopus,
    # buscar su ISSN/título de revista en openalex_records y re-consultar Serial Title API.
    def _rescue_not_found_via_openalex(rows_local: list[dict], extractor_inst) -> None:
        """
        Para cada fila con journal_found=False y DOI:
          1. Busca el registro en openalex_records (BD local) por DOI.
          2. Para DOIs que la BD no tiene → llama OpenAlex API directamente por DOI
             (GET /works/https://doi.org/{doi}) sin guardar en BD.
          3. Usa el ISSN o source_journal que OpenAlex tiene registrado.
          4. Re-consulta Scopus Serial Title API con ese dato.
          5. Si lo encuentra, actualiza la fila con toda la info de cobertura.
        """
        from db.session import get_session
        from db.models import OpenalexRecord as _OARecord
        from extractors.serial_title import SerialTitleAPIError as _STErr
        from config import institution as _inst
        import re as _re
        import pyalex as _pyalex
        from pyalex import Works as _Works
        _pyalex.config.email           = getattr(_inst, "contact_email", "") or "api@openalex.org"
        _pyalex.config.max_retries     = 3
        _pyalex.config.retry_backoff_factor = 0.5

        def _ndoi(d: str) -> str:
            d = (d or "").strip().lower()
            d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
            return d.split()[0] if d else ""

        def _clean_issn(s: str) -> str:
            return _re.sub(r"[^0-9Xx]", "", (s or "")).upper()

        def _compute_in_coverage(pub_year: int, journal_info: dict) -> str:
            """Replica la lógica de check_publications_coverage para ¿En cobertura?"""
            import datetime as _dt
            _cy = _dt.datetime.now().year
            periods: list = journal_info.get("coverage_periods") or []
            cf = journal_info.get("coverage_from")
            ct = journal_info.get("coverage_to")
            # Si la revista sigue activa (coverage_to reciente), Scopus puede
            # estar 1-2 años atrás en sus datos. Extender el techo efectivo.
            _ect = max(ct, _cy) if (ct and ct >= _cy - 2) else ct
            if not pub_year:
                return "Sin datos"
            if pub_year and periods:
                _last = periods[-1][1]
                _eff_last = max(_last, _cy) if _last >= _cy - 2 else _last
                if any(s <= pub_year <= e for s, e in periods) or (_last < pub_year <= _eff_last):
                    return "Sí"
                elif pub_year < periods[0][0]:
                    return "No (antes de cobertura)"
                elif pub_year > _eff_last:
                    return "No (después de cobertura)"
                else:
                    return "No (laguna de cobertura)"
            elif cf and _ect:
                if cf <= pub_year <= _ect:
                    return "Sí"
                elif pub_year < cf:
                    return "No (antes de cobertura)"
                return "No (después de cobertura)"
            elif cf:
                return "Sí" if pub_year >= cf else "No (antes de cobertura)"
            return "Sin datos"

        # 1. Recoger DOIs de filas no encontradas
        not_found_rows = [
            r for r in rows_local
            if not r.get("journal_found") and r.get("__doi")
        ]
        if not not_found_rows:
            logger.info("[rescue-oa] No hay filas sin encontrar con DOI. Nada que rescatar.")
            return

        dois_needed = list({_ndoi(str(r.get("__doi") or "")) for r in not_found_rows})
        dois_needed = [d for d in dois_needed if d]
        logger.info(f"[rescue-oa] {len(not_found_rows)} filas sin encontrar → consultando {len(dois_needed)} DOIs en OpenAlex BD...")

        # 2. Consultar BD OpenAlex
        session = get_session()
        try:
            oa_records = session.query(_OARecord).filter(
                _OARecord.doi.in_(dois_needed)
            ).all()
        except Exception as exc:
            logger.warning(f"[rescue-oa] Error consultando BD OpenAlex: {exc}")
            return
        finally:
            session.close()

        # Construir mapa doi → (issn, source_journal)
        oa_map: dict[str, tuple[str, str]] = {}
        for rec in oa_records:
            key = _ndoi(rec.doi or "")
            if key:
                oa_map[key] = (rec.issn or "", rec.source_journal or "")

        logger.info(f"[rescue-oa] OpenAlex BD devolvió {len(oa_map)} registros de {len(dois_needed)} buscados.")

        # 2b. Para DOIs que la BD no tiene → consultar OpenAlex API vía PyAlex.
        #     Works()[doi_url] hace GET /works/{doi_url} y maneja retry/rate-limit.
        dois_missing_from_db = [d for d in dois_needed if d not in oa_map]

        if dois_missing_from_db:
            logger.info(
                f"[rescue-oa] {len(dois_missing_from_db)} DOIs no están en BD → "
                f"consultando OpenAlex API (PyAlex)..."
            )
            for doi_key in dois_missing_from_db:
                try:
                    doi_url = f"https://doi.org/{doi_key}"
                    work = _Works()[doi_url]
                    primary_loc = work.get("primary_location") or {}
                    source      = primary_loc.get("source") or {}
                    oa_issn     = source.get("issn_l") or ""
                    oa_journal  = source.get("display_name") or ""
                    if not oa_issn:
                        issn_list = source.get("issn") or []
                        oa_issn = issn_list[0] if issn_list else ""
                    oa_map[doi_key] = (oa_issn, oa_journal)
                    logger.debug(
                        f"[rescue-oa] DOI {doi_key} → issn={oa_issn!r} revista={oa_journal!r}"
                    )
                except Exception as exc:
                    logger.debug(f"[rescue-oa] DOI {doi_key} → no encontrado en OpenAlex: {exc}")
                    continue

            n_api_hits = sum(1 for d in dois_missing_from_db if d in oa_map)
            logger.info(
                f"[rescue-oa] OpenAlex API: {n_api_hits}/{len(dois_missing_from_db)} DOIs resueltos."
            )

        # 3. Pre-resolver journals únicos con deduplicación y caché de disco
        from extractors.serial_title import _dcache_get as _st_dcache_get, _dcache_set as _st_dcache_set

        _rescue_jcache: dict[str, dict | None] = {}

        def _rescue_resolve_issn(issn: str):
            key = f"issn:{issn}"
            if key in _rescue_jcache:
                return _rescue_jcache[key]
            cached = _st_dcache_get(key)
            if cached is not None:
                _rescue_jcache[key] = cached
                return cached
            try:
                res = extractor_inst.get_journal_coverage(issn)
                if not res.get("error"):
                    _st_dcache_set(key, res)
                    _rescue_jcache[key] = res
                    return res
            except _STErr:
                pass
            _rescue_jcache[key] = None
            return None

        def _rescue_resolve_title(title: str):
            key = f"title:{title.lower()}"
            if key in _rescue_jcache:
                return _rescue_jcache[key]
            cached = _st_dcache_get(key)
            if cached is not None:
                _rescue_jcache[key] = cached
                return cached
            try:
                res = extractor_inst.search_journal_by_title(title)
                if not res.get("error"):
                    _st_dcache_set(key, res)
                    _rescue_jcache[key] = res
                    return res
            except _STErr:
                pass
            _rescue_jcache[key] = None
            return None

        # Deduplicar ISSNs y títulos únicos antes del bucle por filas
        _unique_issns: set[str] = set()
        _unique_titles: set[str] = set()
        # _issn_to_titles: para ISSNs que fallen, saber qué títulos usar como fallback
        _issn_to_titles: dict[str, set[str]] = {}
        for _r in not_found_rows:
            _dk = _ndoi(str(_r.get("__doi") or ""))
            if _dk not in oa_map:
                continue
            _ui = _clean_issn(oa_map[_dk][0])
            _ut = (oa_map[_dk][1] or "").strip()
            if _ui and len(_ui) >= 7:
                _unique_issns.add(_ui)
                if _ut:
                    _issn_to_titles.setdefault(_ui, set()).add(_ut)
            elif _ut:
                _unique_titles.add(_ut)

        logger.info(
            f"[rescue-oa] Pre-resolviendo {len(_unique_issns)} ISSNs únicos "
            f"(de {len(not_found_rows)} filas)..."
        )
        _done = 0
        for _ui in _unique_issns:
            _rescue_resolve_issn(_ui)
            _done += 1
            if _done % 10 == 0 or _done == len(_unique_issns):
                logger.info(f"[rescue-oa] Pre-resolución ISSNs: {_done}/{len(_unique_issns)}")
            # Si el ISSN falló → añadir sus títulos para pre-resolver como fallback
            if _rescue_jcache.get(f"issn:{_ui}") is None:
                for _ft in _issn_to_titles.get(_ui, set()):
                    _unique_titles.add(_ft)

        if _unique_titles:
            logger.info(f"[rescue-oa] Pre-resolviendo {len(_unique_titles)} títulos fallback...")
            for _ut in _unique_titles:
                _rescue_resolve_title(_ut)
        logger.info(f"[rescue-oa] Pre-resolución completa. Actualizando filas...")

        rescued = 0
        for row in not_found_rows:
            doi_key = _ndoi(str(row.get("__doi") or ""))
            if doi_key not in oa_map:
                continue

            oa_issn, oa_src_journal = oa_map[doi_key]
            journal_info: dict | None = None
            used_via = ""

            # Intento A: ISSN de OpenAlex (desde caché pre-construida)
            issn_clean = _clean_issn(oa_issn)
            if issn_clean and len(issn_clean) >= 7:
                res = _rescue_resolve_issn(issn_clean)
                if res:
                    journal_info = res
                    used_via = "openalex→scopus(issn)"

            # Intento B: nombre de revista de OpenAlex (desde caché pre-construida)
            if journal_info is None and oa_src_journal.strip():
                res = _rescue_resolve_title(oa_src_journal.strip())
                if res:
                    journal_info = res
                    used_via = "openalex→scopus(título)"

            if journal_info is None:
                continue  # ni ISSN ni título funcionaron

            # 4. Actualizar la fila con la info de cobertura recién obtenida
            try:
                pub_year = int(row.get("__year") or 0)
            except (ValueError, TypeError):
                pub_year = 0

            _areas = journal_info.get("subject_areas") or []
            _areas_str = " | ".join(_areas) if isinstance(_areas, list) else str(_areas or "")

            row["journal_found"]         = True
            row["journal_found_via"]     = used_via
            row["scopus_journal_title"]  = journal_info.get("title") or oa_src_journal
            row["scopus_publisher"]      = journal_info.get("publisher") or ""
            row["journal_status"]        = journal_info.get("status") or ""
            row["coverage_from"]         = journal_info.get("coverage_from")
            row["coverage_to"]           = journal_info.get("coverage_to")
            row["coverage_periods"]      = journal_info.get("coverage_periods") or []
            row["journal_subject_areas"] = _areas_str
            row["resolved_issn"]         = journal_info.get("resolved_issn") or oa_issn
            row["resolved_eissn"]        = journal_info.get("resolved_eissn") or ""
            row["in_coverage"]           = _compute_in_coverage(pub_year, journal_info)
            # Adjuntar datos OpenAlex para la hoja "Descont. OpenAlex"
            row["_openalex"] = {
                "oa_work_id":     "",
                "oa_title":       row.get("__title") or "",
                "oa_year":        pub_year,
                "oa_authors":     "",
                "oa_journal":     oa_src_journal,
                "oa_issn":        oa_issn,
                "oa_open_access": "",
                "oa_oa_status":   "",
                "oa_citations":   0,
                "oa_url":         "",
            }
            rescued += 1

        logger.info(
            f"[rescue-oa] Rescatadas {rescued}/{len(not_found_rows)} publicaciones "
            f"usando datos OpenAlex → Scopus Serial Title."
        )

    n_not_found = sum(1 for r in rows if not r.get("journal_found"))
    if n_not_found > 0:
        logger.info(f"[check-coverage] Paso 4.6 (Rescate OpenAlex→Scopus): {n_not_found} publicaciones sin resolver...")
        _t0 = _ctime.time()
        await run_in_threadpool(_rescue_not_found_via_openalex, rows, extractor)
        _elapsed_rescue = _ctime.time() - _t0
        n_rescued = sum(1 for r in rows if r.get("journal_found") and "openalex" in str(r.get("journal_found_via", "")))
        # Recalcular contadores para el log final
        n_found    = sum(1 for r in rows if r.get("journal_found"))
        n_in_cov   = sum(1 for r in rows if r.get("in_coverage") == "Sí")
        n_discont  = sum(1 for r in rows if str(r.get("journal_status","")).strip().lower() in ("discontinued", "inactive"))
        logger.info(
            f"[check-coverage] Paso 4.6 completado en {_elapsed_rescue:.1f}s: rescatadas={n_rescued}  "
            f"total_encontradas={n_found}/{len(rows)}  en-cobertura={n_in_cov}  descontinuadas={n_discont}"
        )

    # 5. Generar Excel de salida (bloqueante — openpyxl)
    logger.info(f"[check-coverage] Paso 5 (Generar Excel): iniciando...")
    _t0 = _ctime.time()
    try:
        excel_bytes = await run_in_threadpool(generate_publications_coverage_excel, headers, rows)
    except Exception as e:
        logger.exception("[check-coverage] Error generando Excel")
        raise HTTPException(500, f"Error generando el archivo Excel de salida: {e}")

    logger.info(
        f"[check-coverage] Paso 5 (Generar Excel): {_ctime.time()-_t0:.1f}s — {len(excel_bytes):,} bytes\n"
        f"[check-coverage] ✓ Pipeline completo en {_ctime.time()-_t_pipeline:.1f}s total."
    )

    filename = f"cobertura_scopus_{len(rows)}_pubs.xlsx"
    return StreamingResponse(
        _io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: Re-procesar publicaciones fallidas del Excel de salida
# • Recibe el Excel generado por /scopus/check-publications-coverage.
# • Preserva las filas ya resueltas (En Scopus=Sí) sin llamar a la API.
# • Re-consulta únicamente las que fallaron (No / Sin datos).
# • No mezcla OpenAlex BD (ya se hizo en el primer pase).
# • Sí ejecuta el rescate OpenAlex→Scopus para las que siguen sin resolver.
# ─────────────────────────────────────────────────────────────────────────────
@router.post(
    "/scopus/reprocess-coverage",
    summary="Re-procesar publicaciones no resueltas (Excel de resultado previo)",
    responses={
        200: {
            "description": "Excel actualizado con las publicaciones anteriormente no resueltas re-procesadas.",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            },
        }
    },
)
async def scopus_reprocess_coverage(
    file: UploadFile = File(
        ...,
        description="Excel de salida generado por /scopus/check-publications-coverage"
    ),
    max_workers: int = Query(
        1, ge=1, le=5,
        description="Hilos paralelos para consultar la API de Scopus"
    ),
):
    """
    Re-procesa únicamente las publicaciones cuya revista **no se encontró** en
    Scopus o que quedaron como **Sin datos** en el primer pase.

    Las filas ya resueltas (`En Scopus = Sí`) se conservan intactas sin
    consumir cuota de la API.

    Flujo:
    1. Lee el Excel de resultado (hoja 'Cobertura' o 'Datos originales').
    2. Separa filas **resueltas** (fast-path) de filas **pendientes** (re-consulta).
    3. Ejecuta `check_publications_coverage` sobre todas (las resueltas son
       un pass-through instantáneo gracías a `_prev_*`).
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

    logger.info(f"[reprocess] Paso 1 (Leer Excel): {_rtime.time()-_t0:.1f}s — {len(rows)} publicaciones, {len(headers)} columnas")

    # 2. Clasificar filas y construir publications
    #    - Resuelta: En Scopus=Sí  → include_prev=True  (fast-path en check_publications_coverage)
    #    - Pendiente: En Scopus=No o Sin datos → include_prev=False (fuerza re-consulta)
    def _is_resolved(row: dict) -> bool:
        found = str(row.get("En Scopus") or row.get("Revista en Scopus") or "").strip().lower()
        cov   = str(row.get("¿En cobertura?") or "").strip().lower()
        # Consideramos resuelta si se encontró en Scopus Y tiene dato de cobertura distinto de 'sin datos'
        return found == "sí" and bool(cov) and cov != "sin datos"

    for row in rows:
        # Preservar fuente original del Excel de resultado (columna Fuente)
        row["_source"] = str(row.get("Fuente") or "Scopus Export").strip() or "Scopus Export"

    n_resolved = sum(1 for r in rows if _is_resolved(r))
    n_pending  = len(rows) - n_resolved
    logger.info(f"[reprocess] Filas resueltas={n_resolved}  pendientes={n_pending}")

    publications = [
        _build_pub_entry(row, include_prev=_is_resolved(row))
        for row in rows
    ]

    # Diagnóstico: distribución de identificadores disponibles
    n_eid   = sum(1 for p in publications if p.get("eid"))
    n_issn  = sum(1 for p in publications if p.get("issn"))
    n_doi   = sum(1 for p in publications if p.get("doi"))
    n_none  = sum(1 for p in publications if not p.get("eid") and not p.get("issn") and not p.get("doi") and not p.get("source_title"))
    logger.info(
        f"[reprocess] IDs disponibles: EID={n_eid}  ISSN={n_issn}  DOI={n_doi}  sin-id={n_none}"
    )
    for i, p in enumerate(publications[:3]):
        logger.info(
            f"[reprocess][diag] Pub#{i+1}: "
            f"eid={p.get('eid')!r}  issn={p.get('issn')!r}  doi={p.get('doi')!r}  "
            f"year={p.get('year')!r}  src={p.get('source_title')!r}  "
            f"prev_found={p.get('_prev_journal_found')!r}"
        )

    # 3. Ejecutar check de cobertura (filas resueltas son pass-through instantáneo)
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

    logger.info(f"[reprocess] Paso 3 (Scopus API): {_rtime.time()-_t0:.1f}s — {len(enriched)} resultados")

    # 4. Fusionar resultados en rows
    for row, result in zip(rows, enriched):
        row.update({
            k: v for k, v in result.items()
            if not k.startswith("_prev_")
        })

    # 4.5 Enriquecer descontinuadas con OpenAlex BD
    _t0 = _rtime.time()
    await run_in_threadpool(_enrich_discontinued_with_openalex, rows)
    logger.info(f"[reprocess] Paso 4.5 (OpenAlex cruce descontinuadas): {_rtime.time()-_t0:.1f}s")

    # 4.6 Rescate OpenAlex→Scopus para las que siguen sin resolver
    n_not_found = sum(1 for r in rows if not r.get("journal_found"))
    if n_not_found > 0:
        logger.info(f"[reprocess] Paso 4.6 (Rescate OpenAlex→Scopus): {n_not_found} publicaciones...")
        _t0 = _rtime.time()
        await run_in_threadpool(_rescue_not_found_via_openalex, rows, extractor)
        n_rescued = sum(
            1 for r in rows
            if r.get("journal_found") and "openalex" in str(r.get("journal_found_via", ""))
        )
        logger.info(f"[reprocess] Paso 4.6 completado en {_rtime.time()-_t0:.1f}s: rescatadas={n_rescued}")

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
        f"[reprocess] Paso 5 (Generar Excel): {_rtime.time()-_t0:.1f}s — {len(excel_bytes):,} bytes\n"
        f"[reprocess] ✓ Pipeline completo en {_rtime.time()-_t_pipeline:.1f}s total."
    )

    filename = f"cobertura_reprocesada_{len(rows)}_pubs.xlsx"
    return StreamingResponse(
        _io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
        "sort": "pubyear",
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


# ── MODELOS PARA BÚSQUEDA DE DOI ──
class DoiSearchRequest(BaseModel):
    doi: str

class DoiSourceResult(BaseModel):
    source: str
    record: dict | None

class DoiSearchResponse(BaseModel):
    results: list[DoiSourceResult]

# ── POST /pipeline/search-doi-in-sources ────────────────
@router.post("/search-doi-in-sources", response_model=DoiSearchResponse, summary="Buscar DOI en todas las fuentes")
def search_doi_in_sources(body: DoiSearchRequest = Body(...)):
    """
    Busca un DOI en todas las fuentes externas (APIs OpenAlex, Scopus, WoS, CvLAC, Datos Abiertos) y retorna el registro encontrado por fuente.
    Utiliza búsqueda directa por DOI si el extractor la soporta, para máxima eficiencia.
    """
    doi = body.doi.strip().lower()
    results = []

    # OpenAlex
    try:
        from extractors.openalex import OpenAlexExtractor
        extractor = OpenAlexExtractor()
        # OpenAlex soporta búsqueda directa por DOI
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        if record:
            results.append(DoiSourceResult(source="openalex", record=record.to_dict()))
        else:
            results.append(DoiSourceResult(source="openalex", record=None))
    except Exception as e:
        results.append(DoiSourceResult(source="openalex", record=None))

    # Scopus
    try:
        from extractors.scopus import ScopusExtractor
        extractor = ScopusExtractor()
        # Scopus soporta búsqueda directa por DOI
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        if record:
            results.append(DoiSourceResult(source="scopus", record=record.to_dict()))
        else:
            results.append(DoiSourceResult(source="scopus", record=None))
    except Exception as e:
        results.append(DoiSourceResult(source="scopus", record=None))

    # WoS
    try:
        from extractors.wos import WosExtractor
        extractor = WosExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        if record:
            results.append(DoiSourceResult(source="wos", record=record.to_dict()))
        else:
            results.append(DoiSourceResult(source="wos", record=None))
    except Exception as e:
        results.append(DoiSourceResult(source="wos", record=None))

    # CvLAC
    try:
        from extractors.cvlac import CvlacExtractor
        extractor = CvlacExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        if record:
            results.append(DoiSourceResult(source="cvlac", record=record.to_dict()))
        else:
            results.append(DoiSourceResult(source="cvlac", record=None))
    except Exception as e:
        results.append(DoiSourceResult(source="cvlac", record=None))

    # Datos Abiertos
    try:
        from extractors.datos_abiertos import DatosAbiertosExtractor
        extractor = DatosAbiertosExtractor()
        record = extractor.search_by_doi(doi) if hasattr(extractor, "search_by_doi") else None
        if record:
            results.append(DoiSourceResult(source="datos_abiertos", record=record.to_dict()))
        else:
            results.append(DoiSourceResult(source="datos_abiertos", record=None))
    except Exception as e:
        results.append(DoiSourceResult(source="datos_abiertos", record=None))

    return DoiSearchResponse(results=results)
# router = APIRouter(prefix="/pipeline", tags=["Pipeline"])  # Eliminada para evitar sobrescribir el router original



# ── POST /pipeline/extract/scopus ────────────────────────────

@router.post("/extract/scopus", response_model=ExtractionResponse, summary="Extraer de Scopus")
def extract_scopus(body: ScopusExtractionRequest):
    """Extrae publicaciones de Scopus, ingesta y reconcilia."""
    from config import institution
    from extractors.scopus import ScopusExtractor
    from reconciliation.engine import ReconciliationEngine

    affiliation_id = body.affiliation_id or institution.scopus_affiliation_id
    extractor = ScopusExtractor()
    records = extractor.extract(
        year_from=body.year_from,
        year_to=body.year_to,
        max_results=body.max_results,
        affiliation_id=affiliation_id,
    )
    engine = ReconciliationEngine()
    stats = engine.reconcile_batch(records)
    return ExtractionResponse(
        extracted=len(records),
        inserted=stats.total_processed,
        message=f"Extraídos {len(records)}, reconciliados {stats.total_processed}",
        reconciliation=ReconciliationStatsResponse(**stats.to_dict()),
    )


# ── POST /pipeline/load-json ─────────────────────────────────

def _detect_json_source(data) -> str:
    """
    Auto-detecta la fuente de un JSON por su estructura.
    Retorna: 'openalex', 'scopus', 'wos', 'cvlac', 'datos_abiertos'
    """
    items = data if isinstance(data, list) else data.get("results", data.get("works", data.get("search-results", {}).get("entry", [])))
    if not items:
        if isinstance(data, dict) and "search-results" in data:
            return "scopus"
        return "openalex"

    sample = items[0] if items else {}

    # Scopus: tiene dc:identifier, prism:publicationName
    if "dc:identifier" in sample or "prism:publicationName" in sample or "dc:title" in sample:
        return "scopus"

    # OpenAlex: tiene 'authorships', 'primary_location'
    if "authorships" in sample or (isinstance(sample.get("id", ""), str) and "openalex.org" in sample.get("id", "")):
        return "openalex"

    # WoS: tiene 'uid' con WOS:
    if "uid" in sample or (isinstance(sample.get("title"), dict) and "value" in sample.get("title", {})):
        return "wos"

    # Datos Abiertos: tiene 'cod_producto', 'nme_tipologia_producto'
    if "cod_producto" in sample or "nme_tipologia_producto" in sample:
        return "datos_abiertos"

    # CvLAC: tiene 'grupo' o 'cod_rh'
    if "cod_rh" in sample or "grupo" in sample:
        return "cvlac"

    return "openalex"


def _parse_json_records(raw_data, source: str) -> list:
    """
    Parsea un JSON usando el extractor correcto según la fuente.
    Retorna lista de StandardRecord.
    """
    records = []

    if source == "openalex":
        from extractors.openalex import OpenAlexExtractor
        extractor = OpenAlexExtractor()
        items = raw_data if isinstance(raw_data, list) else raw_data.get("results", raw_data.get("works", []))
        for item in items:
            try:
                records.append(extractor._parse_record(item))
            except Exception:
                continue
        records = extractor._post_process(records)

    elif source == "scopus":
        from extractors.scopus import ScopusExtractor
        extractor = ScopusExtractor()
        if isinstance(raw_data, dict) and "search-results" in raw_data:
            items = raw_data["search-results"].get("entry", [])
        elif isinstance(raw_data, list):
            items = raw_data
        else:
            items = raw_data.get("results", raw_data.get("entry", []))
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    elif source == "wos":
        from extractors.wos import WoSExtractor
        extractor = WoSExtractor()
        items = raw_data if isinstance(raw_data, list) else raw_data.get("hits", raw_data.get("records", []))
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    elif source == "datos_abiertos":
        from extractors.datos_abiertos import DatosAbiertosExtractor
        extractor = DatosAbiertosExtractor()
        items = raw_data if isinstance(raw_data, list) else raw_data.get("results", [])
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    elif source == "cvlac":
        from extractors.cvlac import CvLACExtractor
        extractor = CvLACExtractor()
        items = raw_data if isinstance(raw_data, list) else raw_data.get("results", [])
        for item in items:
            try:
                rec = extractor._parse_record(item)
                rec.compute_normalized_fields()
                records.append(rec)
            except Exception:
                continue

    else:
        raise ValueError(f"Fuente no soportada: {source}")

    return records


@router.post("/load-json", response_model=ExtractionResponse, summary="Cargar archivo JSON")
def load_json_file(body: JsonLoadRequest):
    """
    Carga un archivo JSON e ingesta los registros + reconcilia.

    - **Auto-detecta** la fuente (OpenAlex, Scopus, WoS, CvLAC, Datos Abiertos)
      por la estructura del JSON.
    - **Previene duplicados**: si cargas el mismo JSON (o dos JSON con registros
      repetidos), los duplicados se detectan y omiten.
    - **Reconcilia** automáticamente: vincula los registros nuevos a publicaciones
      canónicas existentes por DOI o fuzzy matching.

    Deduplicación en 4 niveles:
      1. Hash determinista (source + ID + DOI + título + año)
      2. source_name + source_id
      3. source_name + DOI normalizado
      4. source_name + título normalizado + año
    """
    filepath = Path(DATA_DIR) / body.filename
    if not filepath.exists():
        raise HTTPException(404, f"Archivo no encontrado: {body.filename}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except Exception as e:
        raise HTTPException(400, f"Error leyendo JSON: {e}")

    # Detectar o usar la fuente indicada
    source = body.source or _detect_json_source(raw_data)
    logger.info(f"Cargando JSON '{body.filename}' como fuente: {source}")

    try:
        records = _parse_json_records(raw_data, source)
    except Exception as e:
        raise HTTPException(500, f"Error parseando JSON como {source}: {e}")

    if not records:
        return ExtractionResponse(
            extracted=0, inserted=0,
            message=f"No se encontraron registros válidos en '{body.filename}' (fuente detectada: {source})",
        )

    engine = ReconciliationEngine()
    try:
        stats = engine.reconcile_batch(records)
        return ExtractionResponse(
            extracted=len(records),
            inserted=stats.total_processed,
            message=(
                f"JSON '{body.filename}' (fuente: {source}): "
                f"{len(records)} leídos, reconciliados {stats.total_processed}."
            ),
            reconciliation=ReconciliationStatsResponse(**stats.to_dict()),
        )
    except Exception as e:
        raise HTTPException(500, f"Error en ingesta/reconciliación: {e}")
    finally:
        engine.session.close()


# ── POST /pipeline/ingest ────────────────────────────────────



# ── POST /pipeline/reconcile ─────────────────────────────────

@router.post("/reconcile", response_model=ReconciliationStatsResponse, summary="Reconciliar pendientes")
def reconcile_pending(batch_size: int = 500):
    """Ejecuta un lote de reconciliación sobre registros pendientes."""
    engine = ReconciliationEngine()
    try:
        stats = engine.reconcile_pending(batch_size=batch_size)
        return ReconciliationStatsResponse(**stats.to_dict())
    except Exception as e:
        raise HTTPException(500, f"Error en reconciliación: {e}")
    finally:
        engine.session.close()


# ── POST /pipeline/reconcile-all ─────────────────────────────

@router.post("/reconcile-all", response_model=ReconciliationStatsResponse, summary="Reconciliar todos")
def reconcile_all():
    """Reconcilia TODOS los registros pendientes (puede tardar)."""
    engine = ReconciliationEngine()
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
        raise HTTPException(500, f"Error en reconciliación: {e}")
    finally:
        engine.session.close()


# ── POST /pipeline/crossref-scopus ────────────────────────────

@router.post(
    "/crossref-scopus",
    response_model=CrossrefScopusResponse,
    summary="Cruzar inventario con Scopus por DOI (por lotes)",
)
def crossref_scopus(
    batch_size: int = 50,
    db: Session = Depends(get_db),
):
    """
    Cruza las publicaciones canónicas con Scopus y **enriquece** datos faltantes.

    Trabaja **por lotes**: cada llamada procesa hasta `batch_size` DOIs (default 50).
    Llámalo varias veces hasta que `pending` llegue a 0.

    Cada llamada:
    1. Toma los próximos N canónicos con DOI que NO tengan registro Scopus.
    2. Busca cada DOI en la API de Scopus.
    3. Si lo encuentra → rellena campos vacíos (revista, ISSN, tipo, citas, etc.).
    4. Actualiza autores con Scopus Author ID.
    5. Ingesta el registro Scopus y reconcilia.

    **Parámetro**: `batch_size` (query param, default 50, max 200)
    """
    import time
    from db.models import (
        CanonicalPublication,
        ScopusRecord,
        Author,
        PublicationAuthor,
    )
    from extractors.scopus import ScopusExtractor
    from extractors.base import normalize_author_name
    from config import SourceName

    # Limitar batch
    batch_size = min(max(batch_size, 1), 200)

    # ── 1. Obtener canónicos con DOI ──
    all_with_doi = (
        db.query(CanonicalPublication.id)
        .filter(CanonicalPublication.doi.isnot(None))
        .filter(CanonicalPublication.doi != "")
        .count()
    )

    if all_with_doi == 0:
        return CrossrefScopusResponse(
            total_canonical_with_doi=0,
            already_in_scopus=0,
            dois_consulted=0,
            found_in_scopus=0,
            not_found=0,
            inserted=0,
            enriched_publications=0,
            fields_filled=0,
            authors_enriched=0,
            errors=0,
            message="No hay publicaciones canónicas con DOI para cruzar.",
            enrichment_detail=None,
            reconciliation=None
        )

    # ── 2. DOIs que ya tienen registro Scopus ──
    existing_scopus_dois = set(
        row[0].strip().lower() for row in
        db.query(ScopusRecord.doi)
        .filter(ScopusRecord.doi.isnot(None))
        .all()
    )


    already_in_scopus = len(existing_scopus_dois)

    # ── 3. Seleccionar lote de canónicos con DOI que NO están en Scopus ──
    batch = (
        db.query(CanonicalPublication)
        .filter(CanonicalPublication.doi.isnot(None))
        .filter(CanonicalPublication.doi != "")
        .filter(~CanonicalPublication.doi.in_(existing_scopus_dois))
        .order_by(CanonicalPublication.id.asc())
        .limit(batch_size)
        .all()
    )

    dois_consulted = 0
    found_in_scopus = 0
    not_found = 0
    inserted = 0
    enriched_publications = 0
    fields_filled_count = 0
    authors_enriched_count = 0
    errors = 0
    enrichment_detail = []
    engine = ReconciliationEngine()
    extractor = ScopusExtractor()

    for canon in batch:
        doi = canon.doi.strip().lower()
        dois_consulted += 1
        try:
            record = extractor.search_by_doi(doi)
        except Exception as e:
            logger.error(f"Error consultando Scopus para DOI {doi}: {e}")
            errors += 1
            continue
        if record:
            found_in_scopus += 1
            # Insertar registro en external_records y scopus_records
            try:
                inserted += engine.ingest_records([record])
            except Exception as e:
                logger.error(f"Error insertando registro Scopus: {e}")
                errors += 1
            # Enriquecer campos de la publicación
            fields_updated = []
            prov = dict(getattr(canon, "field_provenance", {}) or {})
            if not canon.issn and getattr(record, "issn", None):
                old = canon.issn
                canon.issn = record.issn
                fields_updated.append(("issn", old, record.issn))
                prov["issn"] = "scopus"
            if not canon.publication_type and getattr(record, "publication_type", None):
                old = canon.publication_type
                canon.publication_type = record.publication_type
                fields_updated.append(("publication_type", old, record.publication_type))
                prov["publication_type"] = "scopus"
            if not canon.publication_date and getattr(record, "publication_date", None):
                old = canon.publication_date
                canon.publication_date = record.publication_date
                fields_updated.append(("publication_date", old, record.publication_date))
                prov["publication_date"] = "scopus"
            if canon.is_open_access is None and getattr(record, "is_open_access", None) is not None:
                old = str(canon.is_open_access)
                canon.is_open_access = record.is_open_access
                fields_updated.append(("is_open_access", old, str(record.is_open_access)))
                prov["is_open_access"] = "scopus"
            if getattr(record, "citation_count", None) and (record.citation_count > (canon.citation_count or 0)):
                old = str(canon.citation_count)
                canon.citation_count = record.citation_count
                fields_updated.append(("citation_count", old, str(record.citation_count)))
                prov["citation_count"] = "scopus"
            if fields_updated:
                canon.field_provenance = prov
                enriched_publications += 1
                fields_filled_count += len(fields_updated)
                for field_name, old_val, new_val in fields_updated:
                    if len(enrichment_detail) < 100:
                        enrichment_detail.append(EnrichedFieldDetail(
                            canonical_id=canon.id,
                            doi=canon.doi,
                            field=field_name,
                            old_value=old_val,
                            new_value=new_val,
                        ))
            # Enriquecer autores
            if getattr(record, "authors", None):
                pub_authors = (
                    db.query(Author)
                    .join(PublicationAuthor, PublicationAuthor.author_id == Author.id)
                    .filter(PublicationAuthor.publication_id == canon.id)
                    .all()
                )
                scopus_author_map = {}
                for sa in record.authors:
                    if sa.get("scopus_id") and sa.get("name"):
                        norm = normalize_author_name(sa["name"])
                        if norm:
                            scopus_author_map[norm] = sa["scopus_id"]
                for author in pub_authors:
                    if not author.scopus_id and author.normalized_name:
                        sid = scopus_author_map.get(author.normalized_name)
                        if sid:
                            author.scopus_id = sid
                            a_prov = dict(author.field_provenance or {})
                            a_prov["scopus_id"] = "scopus"
                            author.field_provenance = a_prov
                            authors_enriched_count += 1
        else:
            not_found += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error haciendo commit de enriquecimientos: {e}")

    # Reconciliar
    total_stats = ReconciliationStatsResponse()
    try:
        stats = engine.reconcile_pending(batch_size=500)
        total_stats = ReconciliationStatsResponse(**stats.to_dict())
    except Exception as e:
        logger.error(f"Error en reconciliación: {e}")

    return CrossrefScopusResponse(
        total_canonical_with_doi=all_with_doi,
        already_in_scopus=already_in_scopus,
        dois_consulted=dois_consulted,
        found_in_scopus=found_in_scopus,
        not_found=not_found,
        inserted=inserted,
        enriched_publications=enriched_publications,
        fields_filled=fields_filled_count,
        authors_enriched=authors_enriched_count,
        errors=errors,
        message=f"Lote de {len(batch)} procesado. {found_in_scopus} encontrados en Scopus, {enriched_publications} enriquecidos.",
        enrichment_detail=enrichment_detail if enrichment_detail else None,
        reconciliation=total_stats if total_stats.total_processed > 0 else None,
    )


# ── DELETE /pipeline/truncate-all ─────────────────────────────

@router.delete("/truncate-all", response_model=MessageResponse, summary="Eliminar todos los registros")
def truncate_all(db: Session = Depends(get_db)):
    """
    Vacía **todas** las tablas de la base de datos y reinicia los contadores de PK.

    ⚠️ OPERACIÓN DESTRUCTIVA: elimina publicaciones canónicas, registros externos,
    autores, relaciones, logs de reconciliación, revistas e instituciones.
    """
    from sqlalchemy import text

    tables = [
        "reconciliation_log",
        "publication_authors",
        "author_institutions",
        "openalex_records",
        "scopus_records",
        "wos_records",
        "cvlac_records",
        "datos_abiertos_records",
        "canonical_publications",
        "authors",
        "journals",
        "institutions",
    ]

    try:
        for table in tables:
            db.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        db.commit()

        # Verificar
        counts = {}
        for table in tables:
            row = db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            counts[table] = row

        total = sum(counts.values())
        return MessageResponse(
            message=f"Todas las tablas vaciadas correctamente. Registros restantes: {total}",
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Error vaciando tablas: {e}")
        raise HTTPException(500, f"Error vaciando tablas: {e}")


# ── POST /pipeline/init-db ──────────────────────────────────

@router.post("/init-db", response_model=MessageResponse, summary="Inicializar base de datos")
def init_database():
    """Inicializa las tablas de la base de datos."""
    try:
        from db.session import create_all_tables
        create_all_tables()
        return MessageResponse(message="Tablas creadas/verificadas exitosamente")
    except Exception as e:
        raise HTTPException(500, f"Error inicializando BD: {e}")


@router.get("/scopus/by-institution", response_model=list, summary="Listar DOIs de Scopus por ID institucional")
def list_scopus_dois_by_institution(db: Session = Depends(get_db)):
    """
    Devuelve los DOIs encontrados en Scopus usando el identificador institucional configurado.
    """
    from config import institution
    from extractors.scopus import ScopusExtractor
    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    if not affiliation_id:
        return []
    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=1000)
    return [r.doi for r in records if getattr(r, "doi", None)]


@router.post("/scopus/by-institution/reconcile", response_model=dict, summary="Crear y reconciliar publicaciones por ID institucional")
def reconcile_scopus_by_institution(db: Session = Depends(get_db)):
    """
    Extrae DOIs de Scopus por el identificador institucional, crea registros canónicos si no existen y reconcilia con otras bases, evitando duplicados.
    """
    from config import institution
    from extractors.scopus import ScopusExtractor
    from db.models import CanonicalPublication
    from sqlalchemy.orm.exc import NoResultFound
    affiliation_id = getattr(institution, "scopus_affiliation_id", None)
    if not affiliation_id:
        return {"created": 0, "reconciled": 0, "duplicates": 0}
    extractor = ScopusExtractor()
    records = extractor.extract(affiliation_id=affiliation_id, max_results=1000)
    print("Llamando extractor.extract para Scopus...")
    print(f"Registros obtenidos de Scopus: {len(records)}")
    from db.models import ScopusRecord
    created, reconciled, duplicates, inserted = 0, 0, 0, 0
    seen_dois = set()
    for r in records:
        # Guardar en scopus_records
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
                status='pending',
                raw_data=None,
            )
            db.add(rec)
            inserted += 1
        doi = getattr(r, "doi", None)
        print(f"Registro Scopus: {r}")
        if not doi or doi in seen_dois:
            duplicates += 1
            continue
        seen_dois.add(doi)
        try:
            pub = db.query(CanonicalPublication).filter_by(doi=doi).one()
            print(f"DOI ya existe: {doi}")
            reconciled += 1
        except NoResultFound:
            print(f"Creando nuevo registro canónico para DOI: {doi}")
            pub = CanonicalPublication(doi=doi, title=getattr(r, "title", None))
            db.add(pub)
            db.commit()
            created += 1
            # Aquí puedes llamar a reconciliación con otras fuentes
            # reconcile_with_sources(pub, db)
    db.commit()
    print(f"Resultado: created={created}, reconciled={reconciled}, duplicates={duplicates}, inserted={inserted}")
    return {"created": created, "reconciled": reconciled, "duplicates": duplicates, "inserted": inserted}
