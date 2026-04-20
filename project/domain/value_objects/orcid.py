from __future__ import annotations

import re
from typing import Optional


_ORCID_FORMAT = re.compile(r"^\d{4}-\d{4}-\d{4}-[\dX]{4}$")
_ORCID_EXTRACT = re.compile(r"(\d{4}-\d{4}-\d{4}-[\dX]{4})", re.IGNORECASE)


class ORCID:
    """Value object for an ORCID identifier (0000-0001-2345-6789)."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        clean = value.strip().upper()
        if not _ORCID_FORMAT.match(clean):
            raise ValueError(
                f"ORCID '{value}' no tiene formato válido. "
                "Debe ser: 0000-0001-2345-6789 (cuatro grupos de 4 dígitos/X)"
            )
        self._value = clean

    @classmethod
    def parse(cls, raw: Optional[str]) -> Optional["ORCID"]:
        """Returns an ORCID from a string or URL, or None if not found/invalid."""
        if not raw or not raw.strip():
            return None
        m = _ORCID_EXTRACT.search(raw.strip())
        if not m:
            return None
        try:
            return cls(m.group(1))
        except ValueError:
            return None

    @classmethod
    def validate(cls, raw: str) -> bool:
        """True if raw is a valid ORCID string."""
        return bool(_ORCID_FORMAT.match(raw.strip().upper()))

    @property
    def value(self) -> str:
        return self._value

    def __str__(self) -> str:
        return self._value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ORCID):
            return self._value == other._value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return f"ORCID({self._value!r})"
