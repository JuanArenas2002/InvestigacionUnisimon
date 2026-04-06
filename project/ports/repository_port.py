from abc import ABC, abstractmethod
from typing import Dict, List

from project.domain.models.publication import Publication


class RepositoryPort(ABC):
    """Puerto de salida para persistencia y consulta."""

    @abstractmethod
    def save_source_records(self, records_by_source: Dict[str, List[Publication]]) -> int:
        ...

    @abstractmethod
    def upsert_canonical_publications(self, publications: List[Publication]) -> int:
        ...

    @abstractmethod
    def list_publications(self, limit: int = 100, offset: int = 0) -> List[dict]:
        ...
