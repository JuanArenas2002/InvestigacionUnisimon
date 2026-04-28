from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class ImpactLevel(IntEnum):
    LOW = 0     # <= 5 citas
    MEDIUM = 1  # 6–20 citas
    HIGH = 2    # > 20 citas

    @classmethod
    def from_citation_count(cls, count: int) -> "ImpactLevel":
        if count <= 5:
            return cls.LOW
        if count <= 20:
            return cls.MEDIUM
        return cls.HIGH

    def label(self) -> str:
        return {self.LOW: "bajo", self.MEDIUM: "medio", self.HIGH: "alto"}[self]


@dataclass(frozen=True)
class ImpactPrediction:
    publication_id: int
    predicted_level: ImpactLevel
    probabilities: dict[str, float]  # {"bajo": 0.1, "medio": 0.3, "alto": 0.6}
    model_version: str

    @property
    def label(self) -> str:
        return self.predicted_level.label()
