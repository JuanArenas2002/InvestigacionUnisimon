from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from project.domain.models.publication import Publication


class RepositoryPort(ABC):
    """Puerto de salida para persistencia y consulta."""

    @abstractmethod
    def save_authors(self, publications: List[Publication]) -> int:
        """
        Persiste todos los autores de una lista de publicaciones.

        Usa cascada de identificacion:
          1. ORCID exacto
          2. external_ids por fuente (scopus_id, openalex_id, etc.)
          3. Nombre normalizado canonico (tokens ordenados)

        Es idempotente: si el autor ya existe lo actualiza/enriquece.
        Retorna el numero de autores procesados.
        """
        ...

    @abstractmethod
    def save_source_records(self, records_by_source: Dict[str, List[Publication]]) -> int:
        ...

    @abstractmethod
    def upsert_canonical_publications(self, publications: List[Publication]) -> int:
        ...

    @abstractmethod
    def list_publications(self, limit: int = 100, offset: int = 0) -> List[dict]:
        ...

    # ── Edición controlada de perfil de autor ────────────────────────

    @abstractmethod
    def get_author_by_id(self, author_id: int) -> Optional[dict]:
        """Retorna dict con campos básicos del autor o None si no existe."""
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
