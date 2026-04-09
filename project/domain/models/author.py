from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(slots=True)
class Author:
    """Entidad de dominio para un autor."""

    name: str
    orcid: Optional[str] = None
    is_institutional: bool = False
    external_ids: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
