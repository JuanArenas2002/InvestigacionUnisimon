"""DTOs for publication use cases (plain Python — no FastAPI/SQLAlchemy dependency)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class PublicationSnapshot:
    """Minimal publication data needed for merge decisions."""
    id: int
    title: Optional[str]
    doi: Optional[str]
    publication_year: Optional[int]
    publication_type: Optional[str]
    sources_count: int
    field_provenance: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MergePublicationsCommand:
    keep_id: int
    merge_id: int


@dataclass
class MergePublicationsResult:
    kept_id: int
    merged_id: int
    fields_inherited: List[str]
    message: str


@dataclass(frozen=True)
class AutoMergeFilters:
    min_similarity: float = 0.95
    only_same_year: bool = False
    skip_doi_conflicts: bool = True
    skip_type_conflicts: bool = True
    require_shared_author: bool = False
    dry_run: bool = False


@dataclass
class AutoMergeResult:
    dry_run: bool
    pairs_evaluated: int
    pairs_merged: int
    pairs_skipped: int
    merged_pairs: List[Dict[str, Any]] = field(default_factory=list)
    skipped_pairs: List[Dict[str, Any]] = field(default_factory=list)
