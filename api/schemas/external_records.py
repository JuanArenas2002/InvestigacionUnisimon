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


class StatusSummaryItem(BaseModel):
    status: str
    total: int
    by_source: dict  # {source_name: count}


class ResetRejectedRequest(BaseModel):
    match_type: Optional[str] = Field(
        None,
        description=(
            "Filtrar por tipo de rechazo. "
            "Ej: 'invalid_title_blacklisted', 'invalid_title_too_short'. "
            "Si se omite, resetea TODOS los registros rechazados."
        ),
    )
    source: Optional[str] = Field(
        None, description="Limitar a una fuente específica (openalex, scopus, …)"
    )
    dry_run: bool = Field(
        False, description="Si True, solo cuenta sin modificar nada."
    )


class ResetRejectedResponse(BaseModel):
    dry_run: bool
    match_type_filter: Optional[str]
    source_filter: Optional[str]
    reset: dict   # {source_name: count}
    total_reset: int


# ── Action Queue ─────────────────────────────────────────────

class ActionItem(BaseModel):
    """Un registro que requiere acción humana."""
    id: int
    source_name: str
    title: Optional[str] = None
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    match_score: Optional[float] = None
    match_type: Optional[str] = None
    # Para manual_review: canónico candidato sugerido por el motor
    suggested_canonical_id: Optional[int] = None
    suggested_canonical_title: Optional[str] = None
    suggested_canonical_doi: Optional[str] = None
    # Instrucción clara de qué hacer
    recommended_action: str
    resolve_hint: str


class ActionGroup(BaseModel):
    status: str
    count: int
    description: str
    bulk_action_available: bool
    items: List[ActionItem]


class ActionQueueResponse(BaseModel):
    total_pending_action: int
    groups: List[ActionGroup]


# ── Bulk Resolve ─────────────────────────────────────────────

class BulkResolveRequest(BaseModel):
    action: str = Field(
        ...,
        pattern="^(link_suggested|reject_all|reset_pending)$",
        description=(
            "link_suggested: vincula cada registro a su canónico sugerido "
            "(solo los que tengan candidato y score >= min_score). "
            "reject_all: rechaza todos los de manual_review. "
            "reset_pending: devuelve a pending para re-reconciliar."
        ),
    )
    source: Optional[str] = Field(None, description="Limitar a una fuente")
    min_score: float = Field(
        0.90,
        ge=0.0, le=1.0,
        description="Score mínimo para link_suggested (default 0.90 = 90%)",
    )
    dry_run: bool = Field(False, description="Si True, solo cuenta sin modificar")


class BulkResolveResponse(BaseModel):
    dry_run: bool
    action: str
    source_filter: Optional[str]
    min_score: Optional[float]
    linked: int = 0
    rejected: int = 0
    reset: int = 0
    skipped: int = 0
    total_affected: int


class PromoteToCanonicalResponse(BaseModel):
    """Resultado de promover un registro de fuente a canónico nuevo."""
    source_name: str
    source_record_id: int
    canonical_id: int
    title: str
    doi: Optional[str] = None
    message: str


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
