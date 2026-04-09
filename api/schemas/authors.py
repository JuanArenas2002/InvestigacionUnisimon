"""
Schemas Pydantic para autores.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class AuthorBase(BaseModel):
    name: str
    normalized_name: Optional[str] = None
    cedula: Optional[str] = None
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scopus_id: Optional[str] = None
    wos_id: Optional[str] = None
    cvlac_id: Optional[str] = None
    google_scholar_id: Optional[str] = None
    is_institutional: bool = False
    field_provenance: Optional[dict] = Field(
        None, description="{campo: fuente} indica qué fuente aportó cada dato del autor"
    )
    verification_status: str = Field(
        "auto_detected",
        description="auto_detected | verified | needs_review | flagged",
    )


class AuthorRead(AuthorBase):
    id: int
    created_at: datetime
    updated_at: datetime
    pub_count: int = 0
    possible_duplicate_of: Optional[int] = None

    model_config = {"from_attributes": True}


class AuthorDetail(AuthorRead):
    """Autor con lista de publicaciones"""
    publications: List["AuthorPublicationRead"] = []


class AuthorPublicationRead(BaseModel):
    id: int
    title: str
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    publication_type: Optional[str] = None
    source_journal: Optional[str] = None
    citation_count: int = 0
    is_open_access: Optional[bool] = None
    sources: List[str] = Field(default_factory=list, description="Nombres de las fuentes")
    source_links: dict = Field(
        default_factory=dict,
        description="ID por fuente: {openalex: id, scopus: id, ...}. El frontend construye la URL completa.",
    )

    model_config = {"from_attributes": True}


class CoauthorRead(BaseModel):
    id: int
    name: str
    is_institutional: bool
    shared_pubs: int

    model_config = {"from_attributes": True}


class AuthorGlobalStats(BaseModel):
    total_authors: int
    total_institutional: int
    total_with_orcid: int
    total_publications: int
    avg_pubs_per_author: float


class AuthorIdsCoverage(BaseModel):
    total: int
    institutional: int
    with_orcid: int
    with_openalex: int
    with_scopus: int
    with_wos: int
    with_cvlac: int
    with_google_scholar: int
    with_cedula: int


# ── Duplicados ───────────────────────────────────────────────

class DuplicateAuthorMatch(BaseModel):
    """Un autor dentro de un grupo de posibles duplicados."""
    id: int
    name: str
    normalized_name: Optional[str] = None
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scopus_id: Optional[str] = None
    is_institutional: bool = False
    field_provenance: Optional[dict] = None
    pub_count: int = 0
    created_at: datetime


class DuplicateAuthorGroup(BaseModel):
    """Grupo de autores que podrían ser la misma persona."""
    normalized_name: str = Field(..., description="Nombre normalizado compartido")
    count: int = Field(..., description="Número de autores con este nombre")
    authors: List[DuplicateAuthorMatch] = []


class DuplicateSummary(BaseModel):
    """Resumen de duplicados detectados."""
    total_groups: int = Field(0, description="Grupos de posibles duplicados")
    total_duplicate_authors: int = Field(0, description="Autores involucrados en duplicados")
    groups: List[DuplicateAuthorGroup] = []


class MergeAuthorsRequest(BaseModel):
    """Solicitud para fusionar autores duplicados."""
    keep_id: int = Field(..., description="ID del autor a conservar (el principal)")
    merge_ids: List[int] = Field(..., description="IDs de los autores a absorber/eliminar")


class MergeAuthorsResponse(BaseModel):
    """Resultado de la fusión de autores."""
    kept_author_id: int
    merged_count: int
    publications_reassigned: int
    ids_inherited: dict = Field(
        default_factory=dict,
        description="IDs heredados: {orcid: '...', scopus_id: '...', ...}",
    )
    message: str = ""


# ── Inventario de autor ──────────────────────────────────────

class InventoryProductRead(BaseModel):
    """Producto dentro del inventario de un autor."""
    id: int
    title: str
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    publication_type: Optional[str] = None
    source_journal: Optional[str] = None
    issn: Optional[str] = None
    citation_count: int = 0
    is_open_access: Optional[bool] = None
    field_provenance: Optional[dict] = Field(
        None, description="Qué fuente aportó cada campo de esta publicación"
    )
    sources: List[str] = Field(default_factory=list, description="Fuentes que contienen este producto")
    source_links: dict = Field(
        default_factory=dict,
        description="ID por fuente: {openalex: id, scopus: id, ...}. El frontend construye la URL completa.",
    )

    model_config = {"from_attributes": True}


class InventoryTypeSummary(BaseModel):
    """Conteo por tipo de publicación."""
    publication_type: str
    count: int


class InventorySourceSummary(BaseModel):
    """Conteo por fuente."""
    source: str
    count: int


class InventoryYearSummary(BaseModel):
    """Conteo por año."""
    year: Optional[int] = None
    count: int


class InventorySummary(BaseModel):
    """Resumen estadístico del inventario."""
    total_products: int = 0
    total_citations: int = 0
    by_type: List[InventoryTypeSummary] = []
    by_source: List[InventorySourceSummary] = []
    by_year: List[InventoryYearSummary] = []
    sources_coverage: dict = Field(
        default_factory=dict,
        description="Cuántos productos tiene cada fuente: {openalex: 12, scopus: 8, ...}",
    )


class AuthorInventoryResponse(BaseModel):
    """Inventario completo de un autor: info personal + todos sus productos."""
    author: AuthorRead
    summary: InventorySummary
    products: List[InventoryProductRead] = []


# ── Audit log ────────────────────────────────────────────────

class AuthorAuditLogRead(BaseModel):
    """Entrada del historial de cambios de un autor."""
    id: int
    author_id: Optional[int] = None
    change_type: str
    before_data: Optional[dict] = None
    after_data: Optional[dict] = None
    field_changes: Optional[dict] = None
    source: Optional[str] = None
    changed_by: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Conflictos ───────────────────────────────────────────────

class AuthorConflictRead(BaseModel):
    """Conflicto entre fuentes para un campo de autor."""
    id: int
    author_id: int
    field_name: str
    existing_value: Optional[str] = None
    new_value: Optional[str] = None
    existing_source: Optional[str] = None
    new_source: Optional[str] = None
    resolved: bool = False
    resolution: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class ResolveConflictRequest(BaseModel):
    """Solicitud para resolver un conflicto."""
    resolution: str = Field(
        ..., description="kept_existing | used_new | manual | ignored"
    )
    resolved_by: Optional[str] = None


# ── Verificación ─────────────────────────────────────────────

class VerifyAuthorRequest(BaseModel):
    """Solicitud para cambiar el estado de verificación de un autor."""
    verification_status: str = Field(
        ..., description="verified | needs_review | flagged | auto_detected"
    )
    changed_by: Optional[str] = None


# ── Importación masiva ───────────────────────────────────────

class BatchAuthorItem(BaseModel):
    """Un autor dentro de una importación masiva."""
    name: str
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scopus_id: Optional[str] = None
    wos_id: Optional[str] = None
    cvlac_id: Optional[str] = None
    is_institutional: bool = False


class BatchImportRequest(BaseModel):
    """Cuerpo de una importación masiva de autores."""
    authors: List[BatchAuthorItem] = Field(..., min_length=1, max_length=500)
    source: str = Field("manual", description="Nombre de la fuente/importador")


class BatchImportResponse(BaseModel):
    """Resultado de la importación masiva."""
    total_received: int
    created: int
    updated: int
    skipped: int
    conflicts: int
    details: List[dict] = Field(default_factory=list)


# ── Autores similares (posibles duplicados fuzzy) ────────────

class SimilarAuthorRead(BaseModel):
    """Autor con puntuación de similitud respecto al consultado."""
    id: int
    name: str
    normalized_name: Optional[str] = None
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scopus_id: Optional[str] = None
    is_institutional: bool = False
    verification_status: str = "auto_detected"
    pub_count: int = 0
    similarity_score: float = Field(..., description="Similitud pg_trgm 0-1")

    model_config = {"from_attributes": True}
