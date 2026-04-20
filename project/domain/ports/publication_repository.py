from abc import ABC, abstractmethod
from typing import Dict, List

from project.domain.models.publication import Publication


class PublicationRepositoryPort(ABC):
    """Puerto de salida — operaciones sobre publicaciones y fuentes."""

    @abstractmethod
    def save_authors(self, publications: List[Publication]) -> int:
        """
        Persiste todos los autores de una lista de publicaciones.

        Cascada de identificacion:
          1. ORCID exacto
          2. external_ids por fuente (scopus_id, openalex_id, etc.)
          3. Nombre normalizado canonico (tokens ordenados)

        Idempotente: si el autor ya existe lo actualiza/enriquece.
        Retorna el numero de autores procesados.
        """
        ...

    @abstractmethod
    def save_source_records(self, records_by_source: Dict[str, List[Publication]]) -> int:
        """Persiste registros crudos por fuente. Retorna total insertado/actualizado."""
        ...

    @abstractmethod
    def upsert_canonical_publications(self, publications: List[Publication]) -> int:
        """Upsert en canonical_publications. Retorna total procesado."""
        ...

    @abstractmethod
    def list_publications(self, limit: int = 100, offset: int = 0) -> List[dict]:
        """Lista publicaciones canonicas paginadas."""
        ...
