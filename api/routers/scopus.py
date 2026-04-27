"""
Router de Scopus Insights.
Dashboard completo sobre registros, contribuciones y cobertura de Scopus.
Ahora usa directamente la tabla scopus_records.
"""

import io
import logging
import time
from typing import Optional, List

from fastapi import APIRouter, Depends, Query, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from api.dependencies import get_db
from api.schemas.scopus import (
    ScopusInsightsResponse,
    ScopusRecordSummary,
    ScopusCoverageVsTotal,
    ScopusFieldContribution,
    ScopusAuthorStats,
    ScopusCitationStats,
    ScopusTopJournal,
    ScopusYearDistribution,
    ScopusEnrichedPublicationSample,
    ScopusSearchResponse,
)
from api.schemas.common import PaginatedResponse
from api.schemas.external_records import ExternalRecordRead, ExternalRecordDetail
from db.models import (
    CanonicalPublication,
    ScopusRecord,
    Author,
)
from api.exporters.excel.scopus_search import generate_scopus_search_excel
from api.services.scopus_search_service import ScopusSearchService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/scopus", tags=["Scopus Dashboard"])


# ══════════════════════════════════════════════════════════════
# GET /scopus/doc — Referencia de búsqueda avanzada
# ══════════════════════════════════════════════════════════════

_ADVANCED_QUERY_DOC = {
    "title": "Referencia de Búsqueda Avanzada — Scopus API",
    "description": (
        "Documentación de los operadores de campo disponibles en build_advanced_query() "
        "y extract_advanced(). Equivalente a la pestaña 'Advanced Search' de la web de Scopus."
    ),
    "reference_url": "https://dev.elsevier.com/sc_search_tips.html",
    "parameters": [
        {
            "param": "title_abs_key",
            "scopus_operator": "TITLE-ABS-KEY(...)",
            "description": "Busca el término en título, resumen Y palabras clave a la vez. Es el operador más amplio y el recomendado para búsquedas temáticas generales.",
            "example": 'title_abs_key="inteligencia artificial"  →  TITLE-ABS-KEY("inteligencia artificial")',
        },
        {
            "param": "title",
            "scopus_operator": "TITLE(...)",
            "description": "Busca únicamente en el título del documento.",
            "example": 'title="deep learning"  →  TITLE("deep learning")',
        },
        {
            "param": "abstract",
            "scopus_operator": "ABS(...)",
            "description": "Busca únicamente en el resumen del documento.",
            "example": 'abstract="climate change"  →  ABS("climate change")',
        },
        {
            "param": "keywords",
            "scopus_operator": "KEY(...)",
            "description": "Busca únicamente en las palabras clave del documento.",
            "example": 'keywords="machine learning"  →  KEY("machine learning")',
        },
        {
            "param": "author",
            "scopus_operator": "AUTH(...)",
            "description": "Busca por nombre de autor. Formato recomendado: 'Apellido, Inicial' o solo el apellido.",
            "example": 'author="García, J."  →  AUTH("García, J.")',
        },
        {
            "param": "first_author",
            "scopus_operator": "AUTHFIRST(...)",
            "description": "Busca solo en el primer autor del documento.",
            "example": 'first_author="Martínez"  →  AUTHFIRST("Martínez")',
        },
        {
            "param": "author_id",
            "scopus_operator": "AU-ID(...)",
            "description": "Busca por Scopus Author ID numérico. Es el identificador más preciso para un autor.",
            "example": 'author_id="57208979556"  →  AU-ID(57208979556)',
        },
        {
            "param": "orcid",
            "scopus_operator": "ORCID(...)",
            "description": "Busca por ORCID del autor. Se acepta con o sin el prefijo https://orcid.org/.",
            "example": 'orcid="0000-0002-2096-7900"  →  ORCID(0000-0002-2096-7900)',
        },
        {
            "param": "affiliation_id",
            "scopus_operator": "AF-ID(...)",
            "description": "Busca documentos afiliados a una institución por su AF-ID de Scopus. Acepta múltiples IDs separados por coma, generando una cláusula OR entre ellos.",
            "example": 'affiliation_id="60106970,60112687"  →  (AF-ID(60106970) OR AF-ID(60112687))',
        },
        {
            "param": "affiliation_name",
            "scopus_operator": "AFFIL(...)",
            "description": "Busca por nombre textual de afiliación. Menos preciso que AF-ID; se recomienda como fallback.",
            "example": 'affiliation_name="Universidad de Antioquia"  →  AFFIL("Universidad de Antioquia")',
        },
        {
            "param": "source_title",
            "scopus_operator": "SRCTITLE(...)",
            "description": "Filtra por nombre de revista, libro o serie donde fue publicado el documento.",
            "example": 'source_title="Biomédica"  →  SRCTITLE("Biomédica")',
        },
        {
            "param": "issn",
            "scopus_operator": "ISSN(...)",
            "description": "Filtra por ISSN de la fuente. Los guiones se eliminan automáticamente.",
            "example": 'issn="0120-4157"  →  ISSN(01204157)',
        },
        {
            "param": "doi_filter",
            "scopus_operator": "DOI(...)",
            "description": "Busca un documento por DOI exacto. El prefijo https://doi.org/ se elimina automáticamente.",
            "example": 'doi_filter="10.1016/j.jhydrol.2020.125741"  →  DOI(10.1016/j.jhydrol.2020.125741)',
        },
        {
            "param": "publisher",
            "scopus_operator": "PUBLISHER(...)",
            "description": "Filtra por nombre de editorial.",
            "example": 'publisher="Elsevier"  →  PUBLISHER("Elsevier")',
        },
        {
            "param": "year_from",
            "scopus_operator": "PUBYEAR > (year-1)",
            "description": "Año mínimo de publicación (inclusive). Se convierte a PUBYEAR > año-1.",
            "example": "year_from=2020  →  PUBYEAR > 2019",
        },
        {
            "param": "year_to",
            "scopus_operator": "PUBYEAR < (year+1)",
            "description": "Año máximo de publicación (inclusive). Se convierte a PUBYEAR < año+1.",
            "example": "year_to=2024  →  PUBYEAR < 2025",
        },
        {
            "param": "year_exact",
            "scopus_operator": "PUBYEAR = year",
            "description": "Año exacto de publicación. Si se especifica, anula year_from y year_to.",
            "example": "year_exact=2023  →  PUBYEAR = 2023",
        },
        {
            "param": "document_type",
            "scopus_operator": "DOCTYPE(código)",
            "description": "Tipo de documento. Acepta tanto el nombre legible como el código corto de Scopus.",
            "example": 'document_type="article"  →  DOCTYPE(ar)',
            "accepted_values": {
                "article": "ar",
                "review": "re",
                "conference paper": "cp",
                "book": "bk",
                "book chapter": "ch",
                "editorial": "ed",
                "letter": "le",
                "note": "no",
                "short survey": "sh",
                "erratum": "er",
                "report": "rp",
                "abstract report": "ab",
            },
        },
        {
            "param": "subject_area",
            "scopus_operator": "SUBJAREA(...)",
            "description": "Área temática ASJC de Scopus. Usar el código de 4 letras mayúsculas.",
            "example": 'subject_area="MEDI"  →  SUBJAREA(MEDI)',
            "accepted_values": {
                "AGRI": "Agricultura y Ciencias Biológicas",
                "ARTS": "Artes y Humanidades",
                "BIOC": "Bioquímica, Genética y Biología Molecular",
                "BUSI": "Negocios, Gestión y Contabilidad",
                "CENG": "Ingeniería Química",
                "CHEM": "Química",
                "COMP": "Ciencias de la Computación",
                "DECI": "Ciencias de la Decisión",
                "DENT": "Odontología",
                "EART": "Ciencias de la Tierra y Planetarias",
                "ECON": "Economía, Econometría y Finanzas",
                "ENER": "Energía",
                "ENGI": "Ingeniería",
                "ENVI": "Ciencias Ambientales",
                "IMMU": "Inmunología y Microbiología",
                "MATE": "Ciencia de Materiales",
                "MATH": "Matemáticas",
                "MEDI": "Medicina",
                "MULT": "Multidisciplinar",
                "NEUR": "Neurociencia",
                "NURS": "Enfermería",
                "PHAR": "Farmacología y Farmacia",
                "PHYS": "Física y Astronomía",
                "PSYC": "Psicología",
                "SOCI": "Ciencias Sociales",
                "VETE": "Medicina Veterinaria",
            },
        },
        {
            "param": "language",
            "scopus_operator": "LANGUAGE(...)",
            "description": "Filtra por idioma de publicación.",
            "example": 'language="Spanish"  →  LANGUAGE(Spanish)',
            "accepted_values": ["English", "Spanish", "French", "German", "Portuguese", "Chinese", "Japanese"],
        },
        {
            "param": "open_access",
            "scopus_operator": "OPENACCESS(1)",
            "description": "Si es True, filtra solo publicaciones en acceso abierto.",
            "example": "open_access=True  →  OPENACCESS(1)",
        },
        {
            "param": "funder",
            "scopus_operator": "FUND-SPONSOR(...)",
            "description": "Filtra documentos financiados por una entidad específica.",
            "example": 'funder="Minciencias"  →  FUND-SPONSOR("Minciencias")',
        },
        {
            "param": "grant_number",
            "scopus_operator": "FUND-NO(...)",
            "description": "Filtra por número de contrato o grant de financiación.",
            "example": 'grant_number="2021-1001"  →  FUND-NO("2021-1001")',
        },
        {
            "param": "extra",
            "scopus_operator": "(libre)",
            "description": "Cláusula Scopus adicional en formato libre. Se añade al final con el operador configurado.",
            "example": 'extra="AND NOT DOCTYPE(ed)"',
        },
        {
            "param": "operator",
            "scopus_operator": "AND | OR",
            "description": "Operador lógico entre todas las cláusulas generadas. Por defecto es AND.",
            "example": 'operator="AND"  (por defecto)',
        },
    ],
    "examples": [
        {
            "description": "Artículos de dos instituciones colombianas entre 2020 y 2024",
            "python": (
                "extract_advanced(\n"
                "    affiliation_id='60106970,60112687',\n"
                "    year_from=2020, year_to=2024,\n"
                "    document_type='article',\n"
                ")"
            ),
            "scopus_query": "(AF-ID(60106970) OR AF-ID(60112687)) AND PUBYEAR > 2019 AND PUBYEAR < 2025 AND DOCTYPE(ar)",
        },
        {
            "description": "Publicaciones OA de un autor por ORCID sobre machine learning",
            "python": (
                "extract_advanced(\n"
                "    orcid='0000-0002-2096-7900',\n"
                "    title_abs_key='machine learning',\n"
                "    open_access=True,\n"
                ")"
            ),
            "scopus_query": 'TITLE-ABS-KEY("machine learning") AND ORCID(0000-0002-2096-7900) AND OPENACCESS(1)',
        },
        {
            "description": "Publicaciones de Minciencias en una revista específica desde 2018",
            "python": (
                "extract_advanced(\n"
                "    source_title='Biomédica',\n"
                "    funder='Minciencias',\n"
                "    year_from=2018,\n"
                "    language='Spanish',\n"
                ")"
            ),
            "scopus_query": 'SRCTITLE("Biomédica") AND PUBYEAR > 2017 AND LANGUAGE(Spanish) AND FUND-SPONSOR("Minciencias")',
        },
        {
            "description": "Reviews de medicina publicadas en 2023 en acceso abierto",
            "python": (
                "extract_advanced(\n"
                "    subject_area='MEDI',\n"
                "    document_type='review',\n"
                "    year_exact=2023,\n"
                "    open_access=True,\n"
                ")"
            ),
            "scopus_query": "SUBJAREA(MEDI) AND PUBYEAR = 2023 AND DOCTYPE(re) AND OPENACCESS(1)",
        },
        {
            "description": "Producción de un autor específico por Scopus Author ID",
            "python": (
                "extract_advanced(\n"
                "    author_id='57208979556',\n"
                "    year_from=2015,\n"
                ")"
            ),
            "scopus_query": "AU-ID(57208979556) AND PUBYEAR > 2014",
        },
    ],
    "build_query_example": {
        "description": "También puedes construir el string de query sin ejecutar la extracción",
        "python": (
            "from extractors.scopus import ScopusExtractor\n\n"
            "query = ScopusExtractor.build_advanced_query(\n"
            "    affiliation_id='60106970',\n"
            "    title_abs_key='biodiversidad',\n"
            "    year_from=2019,\n"
            "    document_type='article',\n"
            "    open_access=True,\n"
            ")\n"
            "# query == 'TITLE-ABS-KEY(\"biodiversidad\") AND AF-ID(60106970) AND PUBYEAR > 2018 AND DOCTYPE(ar) AND OPENACCESS(1)'"
        ),
    },
}


@router.get(
    "/doc",
    summary="Referencia de búsqueda avanzada Scopus",
    tags=["Scopus Dashboard"],
)
def scopus_advanced_query_doc():
    """
    Referencia completa de parámetros para **búsqueda avanzada de Scopus**.

    Documenta todos los operadores de campo disponibles en:
    - `ScopusExtractor.build_advanced_query()` — construye el string de query
    - `ScopusExtractor.extract_advanced()` — construye y ejecuta la extracción

    Equivalente a la pestaña **Advanced Search** de [scopus.com](https://www.scopus.com/search/form.uri#advanced).
    """
    return _ADVANCED_QUERY_DOC


def _scopus_to_read(er: ScopusRecord) -> ExternalRecordRead:
    """Convierte ScopusRecord a ExternalRecordRead."""
    return ExternalRecordRead(
        id=er.id,
        source_name=er.source_name,
        source_id=er.source_id,
        doi=er.doi,
        title=er.title,
        publication_year=er.publication_year,
        authors_text=er.authors_text,
        status=er.status,
        canonical_publication_id=er.canonical_publication_id,
        match_type=er.match_type,
        match_score=er.match_score,
        reconciled_at=er.reconciled_at,
        created_at=er.created_at,
        updated_at=er.updated_at,
    )


def _scopus_to_detail(er: ScopusRecord) -> ExternalRecordDetail:
    """Convierte ScopusRecord a ExternalRecordDetail."""
    return ExternalRecordDetail(
        id=er.id,
        source_name=er.source_name,
        source_id=er.source_id,
        doi=er.doi,
        title=er.title,
        publication_year=er.publication_year,
        authors_text=er.authors_text,
        status=er.status,
        canonical_publication_id=er.canonical_publication_id,
        match_type=er.match_type,
        match_score=er.match_score,
        reconciled_at=er.reconciled_at,
        created_at=er.created_at,
        updated_at=er.updated_at,
        raw_data=er.raw_data,
        normalized_title=er.normalized_title,
        normalized_authors=er.normalized_authors,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/insights — Dashboard completo
# ══════════════════════════════════════════════════════════════

@router.get(
    "/insights",
    response_model=ScopusInsightsResponse,
    summary="Dashboard completo de Scopus",
)
def scopus_insights(db: Session = Depends(get_db)):
    """
    Retorna **todo** lo que tiene que ver con Scopus en un solo endpoint.
    """
    records = _build_record_summary(db)
    coverage = _build_coverage(db)
    field_contributions = _build_field_contributions(db)
    authors = _build_author_stats(db)
    citations = _build_citation_stats(db)
    top_journals = _build_top_journals(db)
    year_dist = _build_year_distribution(db)
    samples = _build_enrichment_samples(db)

    return ScopusInsightsResponse(
        records=records,
        coverage=coverage,
        field_contributions=field_contributions,
        authors=authors,
        citations=citations,
        top_journals=top_journals,
        year_distribution=year_dist,
        enrichment_samples=samples,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/records/by-eid/{eid} — Buscar por EID de Scopus
# ══════════════════════════════════════════════════════════════

@router.get(
    "/records/by-eid/{eid:path}",
    response_model=ExternalRecordDetail,
    summary="Buscar registro Scopus por EID",
)
def get_scopus_record_by_eid(eid: str, db: Session = Depends(get_db)):
    """
    Retorna el detalle completo de un registro Scopus buscando por su **EID**.

    Formatos aceptados:
    - `2-s2.0-105016707528`  (EID completo)
    - `105016707528`          (solo la parte numérica)

    El EID se almacena en la BD sin el prefijo `2-s2.0-`, por lo que
    ambos formatos son equivalentes.
    """
    # Normalizar: quitar prefijo si viene con él
    doc_id = eid.strip()
    if doc_id.startswith("2-s2.0-"):
        doc_id = doc_id[len("2-s2.0-"):]

    er = db.query(ScopusRecord).filter(ScopusRecord.scopus_doc_id == doc_id).first()
    if not er:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontró ningún registro Scopus con EID '{eid}' (doc_id buscado: '{doc_id}')",
        )
    return _scopus_to_detail(er)


# ══════════════════════════════════════════════════════════════
# GET /scopus/not-found — DOIs no encontrados en Scopus
# ══════════════════════════════════════════════════════════════

@router.get(
    "/not-found",
    response_model=PaginatedResponse[ExternalRecordRead],
    summary="DOIs no encontrados en Scopus",
)
def scopus_not_found(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Lista de DOIs que se buscaron en Scopus pero **no se encontraron**.
    Son los registros placeholder con scopus_doc_id='not-found-{doi}'.
    """
    q = db.query(ScopusRecord).filter(
        ScopusRecord.scopus_doc_id.like("not-found-%"),
    )

    total = q.count()
    items = (
        q.order_by(ScopusRecord.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[_scopus_to_read(er) for er in items],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/enriched-fields
# ══════════════════════════════════════════════════════════════

@router.get(
    "/enriched-fields",
    summary="Publicaciones donde Scopus aportó un campo específico",
)
def scopus_enriched_by_field(
    field: str = Query(
        ...,
        description="Campo a consultar (doi, source_journal, issn, citation_count, publication_type, is_open_access, publication_date)",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Retorna las publicaciones canónicas cuyo campo indicado
    fue aportado por Scopus según `field_provenance`.
    """
    q = (
        db.query(CanonicalPublication)
        .filter(
            CanonicalPublication.field_provenance.isnot(None),
            text(f"field_provenance->>'{field}' = 'scopus'"),
        )
    )

    total = q.count()
    items = (
        q.order_by(CanonicalPublication.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[
            {
                "id": p.id,
                "doi": p.doi,
                "title": p.title,
                "publication_year": p.publication_year,
                field: getattr(p, field, None),
                "field_provenance": p.field_provenance,
            }
            for p in items
        ],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# GET /scopus/authors — Autores con Scopus ID
# ══════════════════════════════════════════════════════════════

@router.get(
    "/authors",
    summary="Autores con Scopus Author ID",
)
def scopus_authors(
    only_scopus: bool = Query(False, description="Solo autores cuyo único ID externo es Scopus"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Lista autores que tienen Scopus Author ID."""
    q = db.query(Author).filter(
        Author.external_ids.has_key("scopus"),
    )

    if only_scopus:
        q = q.filter(
            or_(Author.orcid.is_(None), Author.orcid == ""),
            ~Author.external_ids.has_key("openalex"),
        )

    total = q.count()
    items = (
        q.order_by(Author.name.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse.create(
        items=[
            {
                "id": a.id,
                "name": a.name,
                "scopus_id": a.scopus_id,
                "orcid": a.orcid,
                "openalex_id": a.openalex_id,
                "is_institutional": a.is_institutional,
                "scopus_profile_url": f"https://www.scopus.com/authid/detail.uri?authorId={a.scopus_id}",
            }
            for a in items
        ],
        total=total, page=page, page_size=page_size,
    )


# ══════════════════════════════════════════════════════════════
# FUNCIONES INTERNAS (helpers para /insights)
# ══════════════════════════════════════════════════════════════

def _build_record_summary(db: Session) -> ScopusRecordSummary:
    """Conteos de registros Scopus por estado."""
    total = db.query(func.count(ScopusRecord.id)).scalar() or 0

    statuses = (
        db.query(ScopusRecord.status, func.count(ScopusRecord.id))
        .group_by(ScopusRecord.status)
        .all()
    )
    status_map = {s: c for s, c in statuses}

    not_found = db.query(func.count(ScopusRecord.id)).filter(
        ScopusRecord.scopus_doc_id.like("not-found-%"),
    ).scalar() or 0

    return ScopusRecordSummary(
        total=total,
        matched=status_map.get("matched", 0),
        new_canonical=status_map.get("new_canonical", 0),
        pending=status_map.get("pending", 0),
        manual_review=status_map.get("manual_review", 0),
        rejected=status_map.get("rejected", 0),
        not_found_placeholders=not_found,
    )


def _build_coverage(db: Session) -> ScopusCoverageVsTotal:
    """Cobertura de Scopus respecto al inventario."""
    total_canon = db.query(func.count(CanonicalPublication.id)).scalar() or 0

    with_scopus = (
        db.query(func.count(func.distinct(ScopusRecord.canonical_publication_id)))
        .filter(
            ScopusRecord.canonical_publication_id.isnot(None),
            ~ScopusRecord.scopus_doc_id.like("not-found-%"),
        )
        .scalar() or 0
    )

    pct = round(with_scopus / total_canon * 100, 1) if total_canon else 0.0

    only_scopus = (
        db.query(func.count(CanonicalPublication.id))
        .filter(
            CanonicalPublication.sources_count == 1,
            CanonicalPublication.id.in_(
                db.query(ScopusRecord.canonical_publication_id)
                .filter(
                    ScopusRecord.canonical_publication_id.isnot(None),
                    ~ScopusRecord.scopus_doc_id.like("not-found-%"),
                )
            ),
        )
        .scalar() or 0
    )

    multi = with_scopus - only_scopus if with_scopus > only_scopus else 0

    return ScopusCoverageVsTotal(
        total_canonical=total_canon,
        with_scopus_record=with_scopus,
        pct_coverage=pct,
        only_in_scopus=only_scopus,
        multi_source_with_scopus=multi,
    )


def _build_field_contributions(db: Session) -> List[ScopusFieldContribution]:
    """Cuántos campos de las canónicas fueron aportados por Scopus."""
    fields_to_check = [
        "doi", "title", "publication_year", "source_journal", "issn",
        "publication_type", "is_open_access", "citation_count",
        "publication_date",
    ]

    total_canon = db.query(func.count(CanonicalPublication.id)).scalar() or 0
    results = []

    for field in fields_to_check:
        count = (
            db.query(func.count(CanonicalPublication.id))
            .filter(
                CanonicalPublication.field_provenance.isnot(None),
                text(f"field_provenance->>'{field}' = 'scopus'"),
            )
            .scalar() or 0
        )
        pct = round(count / total_canon * 100, 1) if total_canon else 0.0
        results.append(ScopusFieldContribution(
            field=field, count=count, percentage=pct,
        ))

    results.sort(key=lambda x: x.count, reverse=True)
    return results


def _build_author_stats(db: Session) -> ScopusAuthorStats:
    """Estadísticas de autores con Scopus ID."""
    total = db.query(func.count(Author.id)).scalar() or 0

    with_sid = db.query(func.count(Author.id)).filter(
        Author.external_ids.has_key("scopus")
    ).scalar() or 0

    only_scopus = db.query(func.count(Author.id)).filter(
        Author.external_ids.has_key("scopus"),
        or_(Author.orcid.is_(None), Author.orcid == ""),
        ~Author.external_ids.has_key("openalex"),
    ).scalar() or 0

    pct = round(with_sid / total * 100, 1) if total else 0.0

    return ScopusAuthorStats(
        total_authors=total,
        with_scopus_id=with_sid,
        pct_with_scopus_id=pct,
        only_scopus=only_scopus,
    )


def _build_citation_stats(db: Session) -> ScopusCitationStats:
    """Métricas de citas donde Scopus es la fuente."""
    q = (
        db.query(CanonicalPublication)
        .filter(
            CanonicalPublication.field_provenance.isnot(None),
            text("field_provenance->>'citation_count' = 'scopus'"),
            CanonicalPublication.citation_count > 0,
        )
    )

    pubs = q.all()
    count = len(pubs)

    if not pubs:
        return ScopusCitationStats()

    total = sum(p.citation_count for p in pubs if p.citation_count)
    max_pub = max(pubs, key=lambda p: p.citation_count or 0)

    return ScopusCitationStats(
        publications_with_citations_from_scopus=count,
        total_citations_from_scopus=total,
        max_citation_count=max_pub.citation_count or 0,
        max_citation_doi=max_pub.doi,
        max_citation_title=max_pub.title,
        avg_citations=round(total / count, 1) if count else 0.0,
    )


# ══════════════════════════════════════════════════════════════
# POST /scopus/search-products
# Búsqueda masiva de productos en Scopus desde Excel
# ══════════════════════════════════════════════════════════════

@router.post(
    "/search-products",
    summary="Buscar productos en Scopus desde Excel",
    description=(
        "Carga un archivo Excel con publicaciones y busca cada una en Scopus.\n\n"
        "El Excel debe contener las siguientes columnas:\n"
        "- **Título** (requerido): Título de la publicación\n"
        "- **Año**: Año de publicación\n"
        "- **DOI**: Identificador digital\n"
        "- **ISSN**: ISSN de la revista\n"
        "- **Revista**: Nombre de la revista\n\n"
        "Devuelve un Excel con dos hojas:\n"
        "1. **Encontrados**: Productos hallados en Scopus con sus IDs y detalles\n"
        "2. **No Encontrados**: Productos no encontrados con los parámetros de búsqueda\n"
        "3. **Resumen**: Estadísticas de la búsqueda"
    ),
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Excel con resultados de búsqueda"
        },
        400: {"description": "Archivo vacío o formato inválido"},
        425: {"description": "Error al procesar el archivo"},
    },
)
async def search_products_in_scopus(
    file: UploadFile = File(..., description="Archivo Excel (.xlsx) con publicaciones"),
    max_workers: int = Query(5, ge=1, le=10, description="Búsquedas simultáneas (1-10)"),
    max_delay: float = Query(0.1, ge=0.05, le=1.0, description="Delay por slot concurrente (seg)"),
):
    """
    Endpoint para búsqueda masiva de productos en Scopus.

    Procesa cada fila del Excel, busca la publicación en Scopus y devuelve
    un Excel con productos encontrados y no encontrados.
    Usa hasta `max_workers` búsquedas simultáneas para reducir el tiempo total.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Solo se aceptan archivos Excel (.xlsx o .xls)")

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(400, "El archivo está vacío")

    logger.info(
        f"[search-products] Archivo recibido: {file.filename} "
        f"({len(file_bytes):,} bytes) — workers={max_workers}, delay={max_delay}s"
    )

    # Crear servicio de búsqueda con concurrencia configurada
    service = ScopusSearchService(max_workers=max_workers)
    service.delay_between_requests = max_delay
    
    try:
        # Ejecutar búsqueda masiva
        _t0 = time.time()
        found_results, not_found_results = await service.search_publications_batch(file_bytes)
        elapsed = time.time() - _t0
        
        logger.info(
            f"[search-products] Búsqueda completada en {elapsed:.1f}s\n"
            f"  - Encontrados: {len(found_results)}\n"
            f"  - No encontrados: {len(not_found_results)}"
        )
        
        # Convertir a dicts para exportar
        found_dicts = [r.dict() for r in found_results]
        not_found_dicts = [r.dict() for r in not_found_results]
        
        # Generar Excel
        _t1 = time.time()
        excel_bytes = await run_in_threadpool(
            generate_scopus_search_excel,
            found_dicts,
            not_found_dicts,
        )
        logger.info(f"[search-products] Excel generado en {time.time() - _t1:.1f}s")
        
        # Retornar como descarga
        filename = f"scopus_search_{len(found_results) + len(not_found_results)}_productos.xlsx"
        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
        
    except ValueError as e:
        logger.error(f"[search-products] Error de validación: {e}")
        raise HTTPException(422, f"Error procesando archivo: {str(e)}")
    except Exception as e:
        logger.error(f"[search-products] Error inesperado: {e}", exc_info=True)
        raise HTTPException(
            500,
            f"Error durante la búsqueda en Scopus: {str(e)}"
        )


# ══════════════════════════════════════════════════════════════
# POST /scopus/author-production
# Extrae la producción de autores por Scopus Author ID desde Excel
# ══════════════════════════════════════════════════════════════

@router.post(
    "/author-production",
    summary="Extraer producción de autores desde Scopus",
    description=(
        "Carga un archivo Excel con IDs de autores de Scopus y extrae su producción.\n\n"
        "El Excel debe contener las siguientes columnas:\n"
        "- **author_id** (requerido): Scopus Author ID\n"
        "- **author_name** (opcional): Nombre del autor\n"
        "- **affiliation** (opcional): Afiliación del autor\n\n"
        "Devuelve un Excel con:\n"
        "1. **Summary**: Resumen de autores y cantidad de publicaciones\n"
        "2. **Por autor**: Una hoja por cada autor con todas sus publicaciones y metadatos"
    ),
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Excel con producción de autores"
        },
        400: {"description": "Archivo vacío o formato inválido"},
        422: {"description": "Error al procesar el archivo"},
    },
)
async def extract_author_production(
    file: UploadFile = File(..., description="Archivo Excel (.xlsx) con IDs de autores"),
    max_workers: int = Query(3, ge=1, le=10, description="Extracciones simultáneas (1-10)"),
):
    """
    Endpoint para extraer la producción académica de múltiples autores desde Scopus.

    Procesa cada autor del Excel, busca todas sus publicaciones en Scopus 
    y devuelve un Excel con la producción completa incluídos todos los metadatos.
    
    Usa hasta `max_workers` extracciones simultáneas para reducir el tiempo total.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Solo se aceptan archivos Excel (.xlsx o .xls)")

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(400, "El archivo está vacío")

    logger.info(
        f"[author-production] Archivo recibido: {file.filename} "
        f"({len(file_bytes):,} bytes) — workers={max_workers}"
    )

    # Importar servicio
    from api.services.scopus_author_production_service import ScopusAuthorProductionService
    from api.exporters.excel.scopus_author_production import generate_author_production_excel

    service = ScopusAuthorProductionService(max_workers=max_workers)
    
    try:
        # Ejecutar extracción masiva
        _t0 = time.time()
        author_results = await service.process_author_ids(file_bytes)
        elapsed = time.time() - _t0
        
        if not author_results:
            raise HTTPException(422, "No se encontraron autores en el archivo")
        
        logger.info(
            f"[author-production] Extracción completada en {elapsed:.1f}s\n"
            f"  - Total autores: {len(author_results)}\n"
            f"  - Exitosos: {sum(1 for r in author_results if r['status'] == 'success')}\n"
            f"  - Con errores: {sum(1 for r in author_results if r['status'] == 'error')}"
        )
        
        # Generar Excel
        _t1 = time.time()
        excel_bytes = await run_in_threadpool(
            generate_author_production_excel,
            author_results,
        )
        logger.info(f"[author-production] Excel generado en {time.time() - _t1:.1f}s")
        
        # Retornar como descarga
        num_authors = len(author_results)
        total_pubs = sum(r.get("publications_count", 0) for r in author_results)
        filename = f"scopus_autores_{num_authors}_produccion_{total_pubs}.xlsx"
        
        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
        
    except ValueError as e:
        logger.error(f"[author-production] Error de validación: {e}")
        raise HTTPException(422, f"Error procesando archivo: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[author-production] Error inesperado: {e}", exc_info=True)
        raise HTTPException(
            500,
            f"Error durante la extracción de Scopus: {str(e)}"
        )


def _build_top_journals(db: Session, limit: int = 20) -> List[ScopusTopJournal]:
    """Revistas más frecuentes en registros Scopus (columna tipada)."""
    rows = (
        db.query(
            ScopusRecord.source_journal,
            func.count(ScopusRecord.id),
        )
        .filter(
            ~ScopusRecord.scopus_doc_id.like("not-found-%"),
            ScopusRecord.source_journal.isnot(None),
            ScopusRecord.source_journal != "",
        )
        .group_by(ScopusRecord.source_journal)
        .order_by(func.count(ScopusRecord.id).desc())
        .limit(limit)
        .all()
    )
    return [ScopusTopJournal(journal_name=r[0], count=r[1]) for r in rows if r[0]]


def _build_year_distribution(db: Session) -> List[ScopusYearDistribution]:
    """Registros Scopus por año de publicación."""
    rows = (
        db.query(
            ScopusRecord.publication_year,
            func.count(ScopusRecord.id),
        )
        .filter(
            ScopusRecord.publication_year.isnot(None),
            ~ScopusRecord.scopus_doc_id.like("not-found-%"),
        )
        .group_by(ScopusRecord.publication_year)
        .order_by(ScopusRecord.publication_year)
        .all()
    )
    return [ScopusYearDistribution(year=r[0], count=r[1]) for r in rows]


def _build_enrichment_samples(db: Session, limit: int = 10) -> List[ScopusEnrichedPublicationSample]:
    """Ejemplos de publicaciones donde Scopus aportó campos."""
    pubs = (
        db.query(CanonicalPublication)
        .filter(
            CanonicalPublication.field_provenance.isnot(None),
            text("field_provenance::text LIKE '%scopus%'"),
        )
        .limit(limit)
        .all()
    )

    samples = []
    for p in pubs:
        prov = p.field_provenance or {}
        fields = [k for k, v in prov.items() if v == "scopus"]
        if fields:
            samples.append(ScopusEnrichedPublicationSample(
                canonical_id=p.id,
                doi=p.doi,
                title=p.title[:200] if p.title else "",
                fields_from_scopus=fields,
            ))
    return samples


# ══════════════════════════════════════════════════════════════
# POST /scopus/author-h-index
# Extrae el h-index de autores por Scopus Author ID desde Excel
# ══════════════════════════════════════════════════════════════

@router.post(
    "/author-h-index",
    summary="Extraer H-Index de autores desde Scopus",
    description=(
        "Carga un archivo Excel con IDs de autores de Scopus y extrae su H-Index.\n\n"
        "El Excel debe contener una columna con IDs de autores de Scopus. "
        "Las columnas soportadas son:\n"
        "- **author_id** (recomendado)\n"
        "- **scopus_id**\n"
        "- **scopus_author_id**\n"
        "- **id**\n\n"
        "Devuelve un Excel con:\n"
        "1. **H-Index Autores**: Tabla con H-Index y otras métricas\n"
        "2. **Errores**: Autores que no pudieron procesarse\n"
        "3. **Resumen**: Estadísticas generales"
    ),
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Excel con H-Index de autores"
        },
        400: {"description": "Archivo vacío o formato inválido"},
        422: {"description": "Error al procesar el archivo"},
    },
)
async def extract_author_h_index(
    file: UploadFile = File(..., description="Archivo Excel (.xlsx) con IDs de autores"),
    max_workers: int = Query(3, ge=1, le=10, description="Consultas simultáneas (1-10)"),
):
    """
    Endpoint para extraer el H-Index de múltiples autores desde Scopus.

    Procesa cada autor del Excel, consulta su H-Index en Scopus 
    y devuelve un Excel con los resultados.
    
    Usa hasta `max_workers` consultas simultáneas para reducir el tiempo total.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Solo se aceptan archivos Excel (.xlsx o .xls)")

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(400, "El archivo está vacío")

    logger.info(
        f"[author-h-index] Archivo recibido: {file.filename} "
        f"({len(file_bytes):,} bytes) — workers={max_workers}"
    )

    # Importar servicio
    from api.services.scopus_h_index_service import ScopusHIndexService
    from api.exporters.excel.scopus_h_index import generate_h_index_excel
    from datetime import datetime

    service = ScopusHIndexService(max_workers=max_workers)
    
    try:
        # Ejecutar extracción de h-index
        _t0 = time.time()
        author_results = await run_in_threadpool(service.process_author_ids, file_bytes)
        elapsed = time.time() - _t0
        
        if not author_results:
            raise HTTPException(422, "No se encontraron autores en el archivo")
        
        successful = sum(1 for r in author_results if r['status'] == 'success')
        failed = sum(1 for r in author_results if r['status'] == 'error')
        
        logger.info(
            f"[author-h-index] Extracción completada en {elapsed:.1f}s\n"
            f"  - Total autores: {len(author_results)}\n"
            f"  - Exitosos: {successful}\n"
            f"  - Con errores: {failed}"
        )
        
        # Generar Excel
        excel_bytes = await run_in_threadpool(generate_h_index_excel, author_results)
        
        filename = f"h_index_scopus_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[author-h-index] Error: {str(e)}", exc_info=True)
        raise HTTPException(422, f"Error al procesar: {str(e)}")
