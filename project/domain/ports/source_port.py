from abc import ABC, abstractmethod
from typing import List, Optional

from project.domain.models.publication import Publication


class SourcePort(ABC):
    """Puerto de entrada para cualquier fuente bibliografica."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        ...

    @abstractmethod
    def fetch_records(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        **kwargs,
    ) -> List[Publication]:
        ...
