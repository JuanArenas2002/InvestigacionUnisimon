"""
Schemas para el Portal del Investigador (/api/me).

Solo lectura — expuestos al investigador autenticado sobre sus propios datos.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


# ── Afiliaciones ──────────────────────────────────────────────────────────────

class ResearcherAffiliation(BaseModel):
    """Afiliación institucional del investigador."""
    institution_id: int
    institution_name: str
    ror_id: Optional[str] = None
    country: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    is_current: bool = False


# ── Perfil ────────────────────────────────────────────────────────────────────

class ResearcherProfile(BaseModel):
    """
    Perfil completo del investigador autenticado.
    Incluye identificadores en todas las fuentes, estado de verificación
    y afiliaciones institucionales.
    """
    id: int
    name: str
    normalized_name: Optional[str] = None
    cedula: Optional[str] = None
    orcid: Optional[str] = None
    external_ids: Optional[dict] = Field(
        None,
        description="IDs por fuente: {openalex, scopus, wos, cvlac, google_scholar}",
    )
    is_institutional: bool = False
    verification_status: str = Field(
        "auto_detected",
        description="auto_detected | verified | needs_review | flagged",
    )
    field_provenance: Optional[dict] = Field(
        None,
        description="Qué fuente aportó cada campo del perfil",
    )
    affiliations: List[ResearcherAffiliation] = []
    pub_count: int = 0

    model_config = {"from_attributes": True}


# ── Publicaciones ─────────────────────────────────────────────────────────────

class ResearcherPublicationRead(BaseModel):
    """
    Publicación del investigador con estado y fuentes de origen.
    Versión extendida de AuthorPublicationRead con `estado_publicacion`.
    """
    id: int
    title: str
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    publication_type: Optional[str] = None
    source_journal: Optional[str] = None
    issn: Optional[str] = None
    citation_count: int = 0
    is_open_access: Optional[bool] = None
    oa_status: Optional[str] = None
    estado_publicacion: Optional[str] = Field(
        None,
        description="Avalado | Revisión | Rechazado",
    )
    sources: List[str] = Field(
        default_factory=list,
        description="Fuentes que reportan esta publicación",
    )
    source_links: dict = Field(
        default_factory=dict,
        description="ID por fuente: {openalex: id, scopus: id, ...}",
    )

    model_config = {"from_attributes": True}
