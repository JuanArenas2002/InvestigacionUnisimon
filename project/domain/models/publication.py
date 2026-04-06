from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from project.domain.models.author import Author


@dataclass(slots=True)
class Publication:
    """Entidad de dominio para una publicacion bibliografica."""

    source_name: str
    source_id: Optional[str] = None
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    publication_type: Optional[str] = None
    language: Optional[str] = None
    source_journal: Optional[str] = None
    issn: Optional[str] = None
    is_open_access: Optional[bool] = None
    oa_status: Optional[str] = None
    authors: List[Author] = field(default_factory=list)
    citation_count: int = 0
    citations_by_year: Dict[int, int] = field(default_factory=dict)
    url: Optional[str] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)
    normalized_title: Optional[str] = None
    authors_text: Optional[str] = None
    normalized_authors: Optional[str] = None
    canonical_key: Optional[str] = None
    match_score: Optional[float] = None
    match_type: Optional[str] = None
    extracted_at: Optional[str] = None

    def identity_key(self) -> str:
        if self.doi:
            return f"doi:{self.doi}"
        return f"title:{self.normalized_title or self.title}|year:{self.publication_year or ''}"
