"""
Schemas Pydantic para publicaciones canónicas.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


# ── Lectura ─────────────────────────────────────────────────

class PublicationBase(BaseModel):
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    title: str
    normalized_title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    publication_type: Optional[str] = None
    language: Optional[str] = None
    journal_id: Optional[int] = None
    source_journal: Optional[str] = None
    issn: Optional[str] = None
    is_open_access: Optional[bool] = None
    oa_status: Optional[str] = None
    citation_count: int = 0
    institutional_authors_count: int = 0
    sources_count: int = 1
    field_provenance: Optional[dict] = Field(
        None,
        description="Procedencia de cada campo: {campo: fuente_que_lo_aportó}",
    )


class PublicationRead(PublicationBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class PublicationDetail(PublicationRead):
    """Publicación con registros externos y autores"""
    external_records: List["ExternalRecordBrief"] = []
    authors: List["PublicationAuthorRead"] = []
    source_links: dict = Field(
        default_factory=dict,
        description="Enlaces por fuente: {openalex: url, scopus: url, ...}",
    )
    field_provenance: Optional[dict] = Field(
        None,
        description="Procedencia de cada campo: indica qué fuente (openalex, scopus, wos, cvlac, datos_abiertos) aportó cada dato al registro canónico.",
    )


class PublicationAuthorRead(BaseModel):
    author_id: int
    author_name: str
    is_institutional: bool
    author_position: Optional[int] = None
    orcid: Optional[str] = None

    model_config = {"from_attributes": True}


class ExternalRecordBrief(BaseModel):
    id: int
    source_name: str
    source_id: Optional[str] = None
    doi: Optional[str] = None
    status: str
    match_type: Optional[str] = None
    match_score: Optional[float] = None
    source_url: str = ""

    model_config = {"from_attributes": True}


class PublicationExistsResponse(BaseModel):
    exists: bool
    canonical_id: Optional[int] = None
    source: Optional[str] = None
    match_method: Optional[str] = None


class FieldCoverageResponse(BaseModel):
    total: int
    with_doi: int
    with_journal: int
    with_issn: int
    with_year: int
    with_type: int
    with_language: int
    with_oa_info: int

    @property
    def pct_doi(self) -> float:
        return (self.with_doi / self.total * 100) if self.total else 0.0


class YearDistribution(BaseModel):
    year: int
    count: int


# ── Publicaciones duplicadas ─────────────────────────────────

class DuplicatePublicationPair(BaseModel):
    """Par de publicaciones candidatas a ser duplicadas."""
    canonical_id_1: int
    canonical_id_2: int
    doi_1: Optional[str] = None
    doi_2: Optional[str] = None
    title_1: str
    title_2: str
    type_1: Optional[str] = Field(None, description="Tipo de publicación 1")
    type_2: Optional[str] = Field(None, description="Tipo de publicación 2")
    year_1: Optional[int] = None
    year_2: Optional[int] = None
    sources_1: List[str] = Field(default_factory=list)
    sources_2: List[str] = Field(default_factory=list)
    similarity_score: float = Field(..., description="Similitud del título (0-1)")
    same_doi: bool = Field(False, description="Ambas tienen el mismo DOI")
    same_year: bool = Field(False, description="Mismo año de publicación")
    recommendation: str = Field("", description="Recomendación: merge, review, o keep_both")
    author_similarity: Optional[float] = Field(None, description="Similitud de autores entre ambas publicaciones (0-1)")
    author_diff_1: List[int] = Field(default_factory=list, description="IDs de autores que están en la publicación 1 pero no en la 2")
    author_diff_2: List[int] = Field(default_factory=list, description="IDs de autores que están en la publicación 2 pero no en la 1")
    authors_1: List[PublicationAuthorRead] = Field(default_factory=list, description="Autores de la publicación 1")
    authors_2: List[PublicationAuthorRead] = Field(default_factory=list, description="Autores de la publicación 2")


class DuplicatePublicationsSummary(BaseModel):
    """Resumen del análisis de duplicados."""
    total_pairs: int = 0
    high_confidence: int = Field(0, description="Pares con similitud >= 0.95 (casi seguros)")
    medium_confidence: int = Field(0, description="Pares con similitud 0.85-0.95")
    low_confidence: int = Field(0, description="Pares con similitud 0.80-0.85")
    same_doi_different_id: int = Field(0, description="Pares con mismo DOI pero diferente ID canónico")
    pairs: List[DuplicatePublicationPair] = []
