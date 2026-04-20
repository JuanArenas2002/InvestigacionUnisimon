"""
Use case: Merge canonical publications.

Business rules live here. Database orchestration stays in the infrastructure
layer (repository / router) and calls back into these functions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from project.application.schemas.publication_schemas import (
    AutoMergeFilters,
    MergePublicationsCommand,
    MergePublicationsResult,
    PublicationSnapshot,
)
from project.domain.value_objects.doi import DOI

# ── Domain constants ─────────────────────────────────────────────────────────

#: Fields that can be inherited from the removed publication to fill gaps in
#: the keeper.  Order matters: listed in priority of "importance to show".
MERGE_FIELDS: List[str] = [
    "title", "normalized_title", "publication_year", "publication_date",
    "publication_type", "language", "source_journal", "issn", "abstract",
    "keywords", "source_url", "page_range", "publisher", "journal_coverage",
    "knowledge_area", "cine_code", "first_author", "corresponding_author",
    "is_open_access", "oa_status", "citation_count", "doi", "pmid", "pmcid",
]


# ── Pure domain functions ────────────────────────────────────────────────────

def pick_keeper(
    p1: PublicationSnapshot,
    p2: PublicationSnapshot,
) -> Tuple[PublicationSnapshot, PublicationSnapshot]:
    """
    Returns (keeper, removable).

    Decision cascade:
      1. More sources linked  (sources_count).
      2. More fields filled.
      3. Lower ID (older record, tie-break).
    """
    def _filled(p: PublicationSnapshot) -> int:
        return sum(1 for f in MERGE_FIELDS if getattr(p, f, None))

    if p1.sources_count != p2.sources_count:
        return (p1, p2) if p1.sources_count > p2.sources_count else (p2, p1)
    f1, f2 = _filled(p1), _filled(p2)
    if f1 != f2:
        return (p1, p2) if f1 > f2 else (p2, p1)
    return (p1, p2) if p1.id < p2.id else (p2, p1)


def compute_field_inheritance(
    keeper_data: Dict[str, Any],
    removable_data: Dict[str, Any],
    keeper_provenance: Dict[str, Any],
    removable_provenance: Dict[str, Any],
    merge_label: str = "merged",
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Computes which fields from removable should fill gaps in keeper.

    Returns:
        (updated_fields, inherited_field_names)
        updated_fields: dict of {field: value} for fields to set on keeper.
    """
    updates: Dict[str, Any] = {}
    inherited: List[str] = []
    new_provenance = dict(keeper_provenance)

    for f in MERGE_FIELDS:
        if not keeper_data.get(f) and removable_data.get(f):
            updates[f] = removable_data[f]
            new_provenance[f] = removable_provenance.get(f, merge_label)
            inherited.append(f)

    updates["field_provenance"] = new_provenance
    return updates, inherited


def should_skip_pair(
    p1_doi: Optional[str],
    p2_doi: Optional[str],
    p1_type: Optional[str],
    p2_type: Optional[str],
    p1_year: Optional[int],
    p2_year: Optional[int],
    filters: AutoMergeFilters,
) -> Optional[str]:
    """
    Returns a skip reason string if the pair should NOT be auto-merged,
    or None if the merge should proceed.
    """
    if filters.only_same_year and p1_year and p2_year and p1_year != p2_year:
        return f"años distintos ({p1_year} vs {p2_year})"

    if filters.skip_doi_conflicts and p1_doi and p2_doi:
        d1 = DOI.parse(p1_doi)
        d2 = DOI.parse(p2_doi)
        if d1 and d2 and d1 != d2:
            return f"DOIs distintos ({p1_doi} vs {p2_doi})"

    if filters.skip_type_conflicts and p1_type and p2_type:
        if p1_type.upper() != p2_type.upper():
            return f"tipos distintos ({p1_type} vs {p2_type})"

    return None


# ── Validation ───────────────────────────────────────────────────────────────

def validate_merge_command(cmd: MergePublicationsCommand) -> None:
    """Raises ValueError if the command is invalid."""
    if cmd.keep_id == cmd.merge_id:
        raise ValueError("keep_id y merge_id deben ser diferentes")
    if cmd.keep_id <= 0 or cmd.merge_id <= 0:
        raise ValueError("Los IDs deben ser enteros positivos")
