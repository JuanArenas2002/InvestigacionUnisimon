"""
Domain Repository Interfaces: Abstracciones para acceso a datos.

Los repositorios definidos aquí son interfaces abstractas.
Las implementaciones están en infrastructure/repositories/.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from ..entities.publication import Publication


class PublicationRepository(ABC):
    """Interfaz: Repositorio de publicaciones canónicas."""
    
    @abstractmethod
    def find_by_doi(self, doi: str) -> Optional[Publication]:
        """Busca una publicación por DOI."""
        pass
    
    @abstractmethod
    def find_by_criteria(self, **kwargs) -> List[Publication]:
        """Busca publicaciones por criterios (ISSN, título, año, etc.)."""
        pass
    
    @abstractmethod
    def save(self, publication: Publication) -> Publication:
        """Guarda o actualiza una publicación."""
        pass
    
    @abstractmethod
    def find_pending(self, limit: int = 100) -> List[Publication]:
        """Retorna publicaciones que necesitan reconciliación."""
        pass
    
    @abstractmethod
    def count_with_doi(self) -> int:
        """Cuenta publicaciones con DOI."""
        pass
    
    @abstractmethod
    def count_all(self) -> int:
        """Cuenta todas las publicaciones."""
        pass


class ExternalRecordRepository(ABC):
    """Interfaz: Repositorio de registros externos (Scopus, OpenAlex, etc.)."""
    
    @abstractmethod
    def find_by_source_id(self, source_name: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Busca un registro por fuente e ID."""
        pass
    
    @abstractmethod
    def find_by_doi(self, source_name: str, doi: str) -> Optional[Dict[str, Any]]:
        """Busca registros por fuente y DOI."""
        pass
    
    @abstractmethod
    def save(self, source_name: str, record: Dict[str, Any]) -> None:
        """Guarda un registro externo."""
        pass
    
    @abstractmethod
    def find_all_by_source(self, source_name: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Retorna todos los registros de una fuente."""
        pass


class CoverageRepository(ABC):
    """Interfaz: Repositorio de información de cobertura (Scopus journales, etc.)."""
    
    @abstractmethod
    def cache_coverage(self, issn: str, coverage_data: Dict[str, Any]) -> None:
        """Cachea información de cobertura de una revista."""
        pass
    
    @abstractmethod
    def get_cached_coverage(self, issn: str) -> Optional[Dict[str, Any]]:
        """Retrieves cached coverage information for a journal."""
        pass
