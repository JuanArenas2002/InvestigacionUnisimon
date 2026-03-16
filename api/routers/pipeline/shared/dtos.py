"""
Shared DTOs: Data Transfer Objects para requests/responses.

DTOs son usados por Endpoints y Application Layer para
comunica información entre capas.
"""
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


# ── Coverage DTOs ────────────────────────────────────────────────────────────

class PublicationIn(BaseModel):
    """DTO de entrada: Publicación para verificar cobertura."""
    issn: Optional[str] = None
    isbn: Optional[str] = None
    doi: Optional[str] = None
    eid: Optional[str] = None
    source_title: Optional[str] = None
    title: Optional[str] = None
    year: Optional[int] = None
    document_type: Optional[str] = None
    
    _prev_in_coverage: Optional[str] = None
    _prev_journal_found: Optional[bool] = None
    _prev_journal_status: Optional[str] = None
    _prev_scopus_journal_title: Optional[str] = None
    _prev_scopus_publisher: Optional[str] = None
    _prev_coverage_periods_str: Optional[str] = None
    _source: Optional[str] = "Scopus Export"


class CoverageResultOut(BaseModel):
    """DTO de salida: Resultado de cobertura de una publicación."""
    journal_found: bool
    journal_found_via: Optional[str] = None
    scopus_journal_title: Optional[str] = None
    scopus_publisher: Optional[str] = None
    journal_status: Optional[str] = None
    coverage_from: Optional[int] = None
    coverage_to: Optional[int] = None
    coverage_periods: List[str] = []
    in_coverage: str = "Sin datos"  # "Sí", "No", "Sin datos"
    journal_subject_areas: Optional[str] = None
    resolved_issn: Optional[str] = None
    resolved_eissn: Optional[str] = None


# ── Extraction DTOs ──────────────────────────────────────────────────────────

class ExtractionRequest(BaseModel):
    """DTO: Request de extracción."""
    affiliation_id: Optional[str] = None
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    max_results: int = 1000


class ReconciliationStats(BaseModel):
    """DTO: Estadísticas de reconciliación."""
    total_processed: int = 0
    doi_exact_matches: int = 0
    fuzzy_high_matches: int = 0
    fuzzy_combined_matches: int = 0
    manual_review: int = 0
    new_canonical_created: int = 0
    errors: int = 0


class ExtractionResponse(BaseModel):
    """DTO: Response de extracción."""
    extracted: int
    inserted: int
    message: str
    reconciliation: Optional[ReconciliationStats] = None


# ── Enrichment DTOs ──────────────────────────────────────────────────────────

class EnrichedFieldDetail(BaseModel):
    """DTO: Detalle de campo enriquecido."""
    canonical_id: int
    doi: str
    field: str
    old_value: str
    new_value: str


class CrossrefScopusResponse(BaseModel):
    """DTO: Response de cruce Crossref-Scopus."""
    total_canonical_with_doi: int
    already_in_scopus: int
    dois_consulted: int
    found_in_scopus: int
    not_found: int
    inserted: int
    enriched_publications: int
    fields_filled: int
    authors_enriched: int
    errors: int
    message: str
    enrichment_detail: Optional[List[EnrichedFieldDetail]] = None
    reconciliation: Optional[ReconciliationStats] = None
