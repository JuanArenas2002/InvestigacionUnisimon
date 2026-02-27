"""
Schemas Pydantic para estadísticas, dashboards, revistas e instituciones.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel


# ── Revistas ─────────────────────────────────────────────────

class JournalRead(BaseModel):
    id: int
    issn: Optional[str] = None
    name: str
    publisher: Optional[str] = None
    country: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class JournalCreate(BaseModel):
    issn: Optional[str] = None
    name: str
    publisher: Optional[str] = None
    country: Optional[str] = None


# ── Instituciones ────────────────────────────────────────────

class InstitutionRead(BaseModel):
    id: int
    ror_id: Optional[str] = None
    name: str
    country: Optional[str] = None
    type: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class InstitutionCreate(BaseModel):
    ror_id: Optional[str] = None
    name: str
    country: Optional[str] = None
    type: Optional[str] = None


# ── Estadísticas ─────────────────────────────────────────────

class SystemStats(BaseModel):
    """KPIs rápidos del sistema"""
    canonical_publications: int = 0
    external_records: int = 0
    authors: int = 0
    journals: int = 0
    institutions: int = 0
    reconciliation_log: int = 0
    pending: int = 0
    manual_review: int = 0
    matched: int = 0


class OverviewStats(BaseModel):
    """Estadísticas panorámicas del inventario"""
    total_canonical: int = 0
    total_external: int = 0
    total_authors: int = 0
    institutional_authors: int = 0
    status_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    multi_source_count: int = 0
    cross_source_pct: float = 0.0


class ReconciliationTimelineItem(BaseModel):
    date: str
    status: str
    count: int


class YearSourceItem(BaseModel):
    source_name: str
    year: int
    count: int


class QualityProblemsOverview(BaseModel):
    """Resumen de problemas de calidad"""
    total_canonical: int = 0
    missing_doi_count: int = 0
    missing_year_count: int = 0
    missing_title_count: int = 0
    missing_authors_count: int = 0
    pending_count: int = 0
    manual_review_count: int = 0
    external_no_title_count: int = 0
    missing_orcid_count: int = 0


class QualityProblemDetail(BaseModel):
    """Registro problemático"""
    id: int
    title: Optional[str] = None
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    source_journal: Optional[str] = None
    category: str


class HealthResponse(BaseModel):
    status: str = "ok"
    database: bool = False
    message: str = ""


class JsonFileInfo(BaseModel):
    filename: str
    size_mb: float
    modified: str
