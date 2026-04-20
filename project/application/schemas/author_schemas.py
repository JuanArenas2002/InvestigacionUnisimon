"""DTOs for author use cases (plain Python — no FastAPI/SQLAlchemy dependency)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class AuthorSnapshot:
    """Minimal author data needed for merge decisions."""
    id: int
    name: str
    orcid: Optional[str]
    openalex_id: Optional[str]
    scopus_id: Optional[str]
    wos_id: Optional[str]
    cvlac_id: Optional[str]
    is_institutional: bool
    field_provenance: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MergeAuthorsCommand:
    keep_id: int
    merge_ids: List[int]


@dataclass
class MergeAuthorsResult:
    kept_author_id: int
    merged_count: int
    publications_reassigned: int
    ids_inherited: Dict[str, Any]
    message: str


@dataclass
class ExternalIdValidationResult:
    source: str
    candidate_id: str
    validated: bool
    matched: int = 0
    total_from_source: int = 0
    author_pubs_in_db: int = 0
    match_rate: float = 0.0
    message: str = ""
