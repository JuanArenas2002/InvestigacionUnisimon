"""
Use case: Merge duplicate authors.

Business rules live here. Database orchestration (bulk updates, deletes)
stays in the infrastructure layer.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from project.application.schemas.author_schemas import (
    AuthorSnapshot,
    MergeAuthorsCommand,
)

# ── Domain constants ─────────────────────────────────────────────────────────

#: External ID attributes that can be inherited from donor to keeper.
INHERITABLE_IDS: List[str] = [
    "orcid",
    "openalex_id",
    "scopus_id",
    "wos_id",
    "cvlac_id",
]


# ── Pure domain functions ────────────────────────────────────────────────────

def compute_id_inheritance(
    keeper: AuthorSnapshot,
    donor: AuthorSnapshot,
) -> Dict[str, Any]:
    """
    Returns {attr: value} for each external ID that the donor has and the
    keeper lacks. Also includes is_institutional if donor is institutional
    and keeper is not.
    """
    inherited: Dict[str, Any] = {}

    for attr in INHERITABLE_IDS:
        donor_val = getattr(donor, attr, None)
        if donor_val and not getattr(keeper, attr, None):
            inherited[attr] = donor_val

    if donor.is_institutional and not keeper.is_institutional:
        inherited["is_institutional"] = True

    return inherited


def merge_field_provenance(
    keeper_prov: Dict[str, Any],
    donor_prov: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merges donor provenance into keeper, keeping keeper values where they exist.
    (Keeper's provenance takes precedence.)
    """
    merged = dict(donor_prov)
    merged.update(keeper_prov)
    return merged


# ── Validation ───────────────────────────────────────────────────────────────

def validate_merge_command(cmd: MergeAuthorsCommand) -> None:
    """Raises ValueError if the command is invalid."""
    if not cmd.merge_ids:
        raise ValueError("merge_ids no puede estar vacío")
    if cmd.keep_id in cmd.merge_ids:
        raise ValueError("keep_id no puede estar en merge_ids")
    if cmd.keep_id <= 0:
        raise ValueError("keep_id debe ser un entero positivo")
    bad = [i for i in cmd.merge_ids if i <= 0]
    if bad:
        raise ValueError(f"merge_ids contiene IDs inválidos: {bad}")
