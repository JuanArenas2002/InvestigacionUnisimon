# --- ENDPOINT: Extraer y guardar productos de OpenAlex por ROR ---

# Endpoint temporal para probar extracción de Scopus
import json
import logging
from pathlib import Path
from typing import List, Dict, Any


from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Body
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
from api.schemas.common import MessageResponse
from config import DATA_DIR
from extractors.base import StandardRecord
from reconciliation.engine import ReconciliationEngine

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Body
router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

# --- ENDPOINT: Extraer y guardar productos de OpenAlex por ROR ---

# --- ENDPOINT: Extraer y guardar productos de OpenAlex por ROR ---
from api.schemas.external_records import ExtractionRequest

@router.post("/extract/openalex", response_model=ExtractionResponse, summary="Extraer de OpenAlex por ROR")
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
from fastapi import HTTPException
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
import logging
logger = logging.getLogger("pipeline")
from pydantic import BaseModel
# --- IMPORTS Y ROUTER ---
# --- LOGGING ---
import logging
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
# --- IMPORTS Y ROUTER ---
# --- LOGGING ---

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
