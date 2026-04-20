from __future__ import annotations

import re
from typing import Optional


_DOI_STRIP = re.compile(
    r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)",
    re.IGNORECASE,
)
_DOI_VALID = re.compile(r"^10\.\d{4,}/\S+$")


class DOI:
    """Value object for a normalized DOI identifier."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        normalized = _DOI_STRIP.sub("", value.strip()).strip()
        if not _DOI_VALID.match(normalized):
            raise ValueError(f"DOI inválido: '{value}'")
        self._value = normalized.lower()

    @classmethod
    def parse(cls, raw: Optional[str]) -> Optional["DOI"]:
        """Returns a DOI instance or None if raw is empty/invalid."""
        if not raw or not raw.strip():
            return None
        try:
            return cls(raw)
        except ValueError:
            return None

    @property
    def value(self) -> str:
        return self._value

    def __str__(self) -> str:
        return self._value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DOI):
            return self._value == other._value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return f"DOI({self._value!r})"
