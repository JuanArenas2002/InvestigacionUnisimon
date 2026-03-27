"""
Schemas Pydantic para registros externos y reconciliación.
"""

from typing import Optional, List, Any
from datetime import datetime
from pydantic import BaseModel, Field


# ── Registros Externos ───────────────────────────────────────

class ExternalRecordRead(BaseModel):
    id: int
    source_name: str
    source_id: Optional[str] = None
    doi: Optional[str] = None
    title: Optional[str] = None
    publication_year: Optional[int] = None
    authors_text: Optional[str] = None
    status: str
    canonical_publication_id: Optional[int] = None
    match_type: Optional[str] = None
    match_score: Optional[float] = None
    reconciled_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    model_config = {"from_attributes": True}


class ExternalRecordDetail(ExternalRecordRead):
    raw_data: Optional[dict] = None
    normalized_title: Optional[str] = None
    normalized_authors: Optional[str] = None


class SourceStatusCount(BaseModel):
    source_name: str
    status: str
    count: int


class MatchTypeDistribution(BaseModel):
    match_type: str
    count: int
    avg_score: Optional[float] = None


class ManualReviewItem(BaseModel):
    id: int
    source_name: str
    title: Optional[str] = None
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    match_score: Optional[float] = None
    candidate_title: Optional[str] = None
    candidate_id: Optional[int] = None


class ResolveReviewRequest(BaseModel):
    action: str = Field(..., pattern="^(link|reject)$", description="'link' o 'reject'")
    canonical_id: Optional[int] = Field(
        None, description="ID canónico al que vincular (requerido si action='link')"
    )


# ── Reconciliación ───────────────────────────────────────────

class ReconciliationStatsResponse(BaseModel):
    total_processed: int = 0
    doi_exact_matches: int = 0
    fuzzy_high_matches: int = 0
    fuzzy_combined_matches: int = 0
    manual_review: int = 0
    new_canonical_created: int = 0
    errors: int = 0


class ReconciliationLogRead(BaseModel):
    id: int
    source_name: str
    source_record_id: int
    canonical_publication_id: Optional[int] = None
    match_type: str
    match_score: Optional[float] = None
    action: str
    created_at: datetime
    match_details: Optional[dict] = None

    model_config = {"from_attributes": True}


# ── Extracción ───────────────────────────────────────────────

class ExtractionRequest(BaseModel):
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    max_results: Optional[int] = None
    affiliation_id: Optional[str] = None


class ScopusExtractionRequest(ExtractionRequest):
    affiliation_id: Optional[str] = None


class JsonLoadRequest(BaseModel):
    filename: str
    source: Optional[str] = Field(
        None,
        description=(
            "Fuente del JSON: openalex, scopus, wos, cvlac, datos_abiertos. "
            "Si no se indica, se auto-detecta por la estructura del JSON."
        ),
    )


class IngestRequest(BaseModel):
    """Registros estándar para ingesta directa."""
    records: List[dict] = Field(
        ..., description="Lista de registros estandarizados como diccionarios"
    )


class IngestResponse(BaseModel):
    inserted: int
    skipped: int = 0
    message: str = ""


class ExtractionResponse(BaseModel):
    extracted: int
    inserted: int
    message: str = ""
    reconciliation: Optional[ReconciliationStatsResponse] = None


class EnrichedFieldDetail(BaseModel):
    """Detalle de un campo enriquecido para una publicación."""
    canonical_id: int
    doi: str
    field: str = Field(..., description="Campo actualizado")
    old_value: Optional[str] = None
    new_value: Optional[str] = None


# Re-exportar desde serial_title para compatibilidad con imports existentes
from api.schemas.serial_title import JournalCoverageResponse  # noqa: F401


class CrossrefScopusResponse(BaseModel):
    """Respuesta del cruce/enriquecimiento de inventario con Scopus por DOI."""
    total_canonical_with_doi: int = Field(
        ..., description="Total de publicaciones canónicas con DOI"
    )
    already_in_scopus: int = Field(
        0, description="Ya tenían registro Scopus, se omitieron"
    )
    dois_consulted: int = Field(
        0, description="DOIs efectivamente consultados en Scopus"
    )
    found_in_scopus: int = Field(
        0, description="Encontrados en Scopus"
    )
    not_found: int = Field(
        0, description="No encontrados en Scopus"
    )
    inserted: int = Field(
        0, description="Registros nuevos insertados en external_records"
    )
    enriched_publications: int = Field(
        0, description="Publicaciones canónicas enriquecidas con datos de Scopus"
    )
    fields_filled: int = Field(
        0, description="Total de campos individuales rellenados"
    )
    authors_enriched: int = Field(
        0, description="Autores actualizados con Scopus Author ID"
    )
    errors: int = Field(
        0, description="Errores durante la consulta"
    )
    message: str = ""
    enrichment_detail: Optional[List[EnrichedFieldDetail]] = Field(
        None, description="Detalle de cada campo enriquecido (primeros 100)"
    )
    reconciliation: Optional[ReconciliationStatsResponse] = None
