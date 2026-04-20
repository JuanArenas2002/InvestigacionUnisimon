from abc import ABC, abstractmethod
from typing import List, Optional


class AuthorRepositoryPort(ABC):
    """Puerto de salida — operaciones de consulta y edicion de perfil de autor."""

    @abstractmethod
    def get_author_by_id(self, author_id: int) -> Optional[dict]:
        """Retorna dict con campos basicos del autor o None si no existe."""
        ...

    @abstractmethod
    def get_author_name_options(self, author_id: int) -> List[dict]:
        """
        Retorna lista de {source, name, profile_url} con los nombres
        disponibles en cada fuente vinculada al autor.
        """
        ...

    @abstractmethod
    def update_author_name(self, author_id: int, name: str, source: str) -> dict:
        """Actualiza name + normalized_name del autor. Registra audit log."""
        ...

    @abstractmethod
    def get_author_source_links(self, author_id: int) -> List[dict]:
        """Retorna lista de {source, external_id, profile_url, linked} para todas las fuentes."""
        ...

    @abstractmethod
    def update_author_source_link(self, author_id: int, source: str, external_id: str) -> dict:
        """Vincula un ID externo al autor. Registra audit log."""
        ...

    @abstractmethod
    def remove_author_source_link(self, author_id: int, source: str) -> dict:
        """Desvincula una fuente del autor. Registra audit log."""
        ...

    @abstractmethod
    def update_author_orcid(self, author_id: int, orcid: str) -> dict:
        """Actualiza el ORCID del autor. Registra audit log."""
        ...

    @abstractmethod
    def check_source_id_conflict(
        self, source: str, external_id: str, exclude_author_id: int
    ) -> Optional[int]:
        """Retorna author_id del autor que ya tiene ese external_id, o None si libre."""
        ...
