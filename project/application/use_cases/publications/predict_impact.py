"""
Use case: predecir el nivel de impacto de una o varias publicaciones.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from project.domain.models.impact_prediction import ImpactPrediction
from project.domain.ports.impact_predictor_port import ImpactPredictorPort


@dataclass(frozen=True)
class PredictImpactCommand:
    publication_ids: List[int]


@dataclass
class PredictImpactResult:
    predictions: List[ImpactPrediction]
    model_version: str


class PredictImpactUseCase:
    def __init__(self, predictor: ImpactPredictorPort) -> None:
        self._predictor = predictor

    def execute(self, command: PredictImpactCommand, session) -> PredictImpactResult:
        if not self._predictor.is_trained():
            raise RuntimeError(
                "El modelo no está entrenado. Ejecuta TrainImpactModelUseCase primero."
            )

        predictions = self._predictor.predict_batch(command.publication_ids, session)
        return PredictImpactResult(
            predictions=predictions,
            model_version=self._predictor.version,
        )
