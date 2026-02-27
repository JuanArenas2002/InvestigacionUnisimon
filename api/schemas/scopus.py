"""
Schemas Pydantic para el dashboard de Scopus.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


# ── Resumen general ──────────────────────────────────────────

class ScopusRecordSummary(BaseModel):
    """Conteos de registros Scopus por estado."""
    total: int = Field(0, description="Total de external_records de Scopus")
    matched: int = Field(0, description="Vinculados a una publicación canónica existente")
    new_canonical: int = Field(0, description="Crearon una nueva publicación canónica")
    pending: int = Field(0, description="Pendientes de reconciliar")
    manual_review: int = Field(0, description="En revisión manual")
    rejected: int = Field(0, description="Rechazados")
    not_found_placeholders: int = Field(
        0, description="Placeholders (DOI buscado en Scopus y no encontrado)"
    )


class ScopusFieldContribution(BaseModel):
    """Cuántos campos de publicaciones canónicas fueron aportados por Scopus."""
    field: str = Field(..., description="Nombre del campo")
    count: int = Field(0, description="Nº de canónicas donde Scopus aportó este campo")
    percentage: float = Field(0.0, description="Porcentaje sobre total de canónicas con ese campo relleno")


class ScopusAuthorStats(BaseModel):
    """Estadísticas de autores con datos de Scopus."""
    total_authors: int = Field(0, description="Total de autores en BD")
    with_scopus_id: int = Field(0, description="Autores que tienen Scopus Author ID")
    pct_with_scopus_id: float = Field(0.0, description="% de autores con Scopus ID")
    only_scopus: int = Field(
        0, description="Autores que SOLO tienen Scopus ID (no ORCID ni OpenAlex)"
    )


class ScopusCitationStats(BaseModel):
    """Métricas de citas aportadas por Scopus."""
    publications_with_citations_from_scopus: int = 0
    total_citations_from_scopus: int = 0
    max_citation_count: int = 0
    max_citation_doi: Optional[str] = None
    max_citation_title: Optional[str] = None
    avg_citations: float = 0.0


class ScopusTopJournal(BaseModel):
    """Revista más frecuente en registros Scopus."""
    journal_name: str
    count: int


class ScopusYearDistribution(BaseModel):
    """Distribución por año de registros Scopus."""
    year: int
    count: int


class ScopusEnrichedPublicationSample(BaseModel):
    """Ejemplo de publicación enriquecida por Scopus."""
    canonical_id: int
    doi: Optional[str] = None
    title: str
    fields_from_scopus: List[str] = Field(
        default_factory=list,
        description="Campos cuyo valor fue aportado por Scopus",
    )


class ScopusCoverageVsTotal(BaseModel):
    """Cobertura de Scopus respecto al inventario total."""
    total_canonical: int = Field(0, description="Total de publicaciones canónicas")
    with_scopus_record: int = Field(0, description="Canónicas con al menos 1 registro Scopus")
    pct_coverage: float = Field(0.0, description="Porcentaje de cobertura")
    only_in_scopus: int = Field(
        0, description="Canónicas donde Scopus es la ÚNICA fuente (sources_count=1, fuente=scopus)"
    )
    multi_source_with_scopus: int = Field(
        0, description="Canónicas con Scopus + al menos otra fuente"
    )


# ── Respuesta completa ───────────────────────────────────────

class ScopusInsightsResponse(BaseModel):
    """Dashboard completo de registros y contribuciones de Scopus."""

    # Registros
    records: ScopusRecordSummary
    coverage: ScopusCoverageVsTotal

    # Contribución de campos
    field_contributions: List[ScopusFieldContribution] = Field(
        default_factory=list,
        description="Campos de canónicas aportados por Scopus (basado en field_provenance)",
    )

    # Autores
    authors: ScopusAuthorStats

    # Citas
    citations: ScopusCitationStats

    # Top revistas
    top_journals: List[ScopusTopJournal] = Field(
        default_factory=list,
        description="Top 20 revistas más frecuentes en registros Scopus",
    )

    # Distribución por año
    year_distribution: List[ScopusYearDistribution] = Field(
        default_factory=list,
        description="Registros Scopus por año de publicación",
    )

    # Muestras de enriquecimiento
    enrichment_samples: List[ScopusEnrichedPublicationSample] = Field(
        default_factory=list,
        description="Hasta 10 ejemplos de publicaciones enriquecidas por Scopus",
    )
