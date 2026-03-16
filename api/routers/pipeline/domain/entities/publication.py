"""
Domain Entity: Publication (Value Objects and Aggregate Root)
"""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class CoveragePeriod:
    """Value Object: Período de cobertura en una revista."""
    start_year: int
    end_year: int
    
    def contains_year(self, year: int) -> bool:
        return self.start_year <= year <= self.end_year
    
    def __str__(self) -> str:
        if self.start_year == self.end_year:
            return str(self.start_year)
        return f"{self.start_year}–{self.end_year}"


@dataclass
class Journal:
    """Value Object: Información de revista."""
    issn: Optional[str] = None
    eissn: Optional[str] = None
    title: Optional[str] = None
    publisher: Optional[str] = None
    status: Optional[str] = None  # "Active", "Discontinued", etc.
    coverage_periods: List[CoveragePeriod] = None
    
    def __post_init__(self):
        if self.coverage_periods is None:
            self.coverage_periods = []
    
    def is_active(self) -> bool:
        return self.status and "active" in self.status.lower()
    
    def is_discontin(self) -> bool:
        return self.status and ("discontin" in self.status.lower() or "inactive" in self.status.lower())
    
    def has_coverage_for_year(self, year: int) -> bool:
        return any(period.contains_year(year) for period in self.coverage_periods)


@dataclass
class Publication:
    """Aggregate Root: Publicación con todos sus datos relacionados."""
    
    # Identificadores
    doi: Optional[str] = None
    issn: Optional[str] = None
    isbn: Optional[str] = None
    eid: Optional[str] = None  # Scopus EID
    
    # Metadata básica
    title: Optional[str] = None
    source_title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[datetime] = None
    publication_type: Optional[str] = None
    
    # Detalles
    is_open_access: Optional[bool] = None
    citation_count: Optional[int] = None
    
    # Información de revista (journálistica)
    journal: Optional[Journal] = None
    
    # Metadatos de búsqueda (para diagnóstico)
    _prev_in_coverage: Optional[str] = None
    _prev_journal_found: Optional[bool] = None
    _prev_journal_status: Optional[str] = None
    _prev_scopus_journal_title: Optional[str] = None
    _prev_scopus_publisher: Optional[str] = None
    _prev_coverage_periods_str: Optional[str] = None
    _source: Optional[str] = None  # "Scopus Export", "OpenAlex BD", etc.
    
    # Resultados de búsqueda
    journal_found: bool = False
    journal_found_via: Optional[str] = None
    in_coverage: Optional[str] = None  # "Sí", "No", "Sin datos"
    
    # Enriquecimiento OpenAlex
    _openalex: Optional[Dict[str, Any]] = None
    
    def can_search_by_identifier(self) -> bool:
        """Verifica si hay un identificador para buscar."""
        return bool(self.doi or self.issn or self.isbn or self.eid or self.source_title)
    
    def get_primary_identifier(self) -> Optional[str]:
        """Retorna el identificador primario en orden de preferencia."""
        return self.issn or self.isbn or self.doi or self.eid or self.source_title
    
    def needs_reconciliation(self) -> bool:
        """Verifica si la publicación necesita reconciliación."""
        return not self.journal_found or self.in_coverage == "Sin datos"
    
    def is_in_coverage(self) -> bool:
        """Verifica si la publicación está en cobertura."""
        return self.in_coverage == "Sí"
    
    def is_discontinued(self) -> bool:
        """Verifica si la revista está descontinuada."""
        return self.journal and self.journal.is_discontin()
