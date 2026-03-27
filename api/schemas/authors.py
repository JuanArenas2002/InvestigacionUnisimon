"""
Schemas Pydantic para autores.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class AuthorBase(BaseModel):
    name: str
    normalized_name: Optional[str] = None
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scopus_id: Optional[str] = None
    wos_id: Optional[str] = None
    cvlac_id: Optional[str] = None
    is_institutional: bool = False
    field_provenance: Optional[dict] = Field(
        None, description="{campo: fuente} indica qué fuente aportó cada dato del autor"
    )


class AuthorRead(AuthorBase):
    id: int
    created_at: datetime
    updated_at: datetime
    pub_count: int = 0

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
