"""
Schemas y utilidades compartidas para todos los routers de fuentes.

Cada plataforma (OpenAlex, Scopus, WoS, CvLAC, Datos Abiertos) sigue
el mismo contrato:
  POST /sources/{platform}/search/by-institution  → extrae y almacena
  POST /sources/{platform}/search/by-author       → extrae y almacena
  GET  /sources/{platform}/records                → lista lo almacenado
  GET  /sources/{platform}/records/{id}           → detalle de un registro
"""

from typing import Optional, List, Any
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────
# REQUEST SCHEMAS
# ─────────────────────────────────────────────────────────────

class SearchByInstitutionRequest(BaseModel):
    """Parámetros para búsqueda por institución en cualquier fuente."""
    ror_id: Optional[str] = Field(
        None, description="ROR ID de la institución (ej: https://ror.org/02njbw696)"
    )
    affiliation_id: Optional[str] = Field(
        None, description="ID de afiliación en la plataforma (ej: Scopus affiliation ID)"
    )
    institution_name: Optional[str] = Field(
        None, description="Nombre de la institución como texto libre"
    )
    year_from: Optional[int] = Field(None, ge=1900, le=2099, description="Año inicial (inclusive)")
    year_to:   Optional[int] = Field(None, ge=1900, le=2099, description="Año final (inclusive)")
    max_results: int = Field(1000, ge=1, le=10000, description="Límite de registros a descargar")


class SearchByAuthorRequest(BaseModel):
    """Parámetros para búsqueda por autor en cualquier fuente."""
    orcid: Optional[str] = Field(
        None, description="ORCID del autor (ej: 0000-0002-1234-5678)"
    )
    source_author_id: Optional[str] = Field(
        None,
        description=(
            "ID del autor en la plataforma destino: "
            "OpenAlex author ID, Scopus AU-ID, WoS RID, CvLAC code, etc."
        ),
    )
    author_name: Optional[str] = Field(
        None, description="Nombre del autor como texto libre (fallback)"
    )
    year_from: Optional[int] = Field(None, ge=1900, le=2099)
    year_to:   Optional[int] = Field(None, ge=1900, le=2099)
    max_results: int = Field(200, ge=1, le=2000)


# ─────────────────────────────────────────────────────────────
# RESPONSE SCHEMAS
# ─────────────────────────────────────────────────────────────

class SearchResult(BaseModel):
    """Respuesta estándar después de una búsqueda/descarga."""
    source: str = Field(..., description="Nombre de la plataforma")
    inserted: int = Field(..., description="Registros nuevos insertados en la tabla")
    skipped:  int = Field(..., description="Registros omitidos por ser duplicados")
    errors:   int = Field(..., description="Registros con error durante la ingesta")
    message:  str = Field(..., description="Mensaje descriptivo del resultado")


class SourceRecordSummary(BaseModel):
    """Campos comunes a todos los registros de fuente — para listados."""
    id:                     int
    doi:                    Optional[str] = None
    title:                  Optional[str] = None
    publication_year:       Optional[int] = None
    publication_type:       Optional[str] = None
    source_journal:         Optional[str] = None
    citation_count:         int = 0
    status:                 str
    match_type:             Optional[str] = None
    match_score:            Optional[float] = None
    canonical_publication_id: Optional[int] = None
    created_at:             Any = None

    model_config = {"from_attributes": True}
