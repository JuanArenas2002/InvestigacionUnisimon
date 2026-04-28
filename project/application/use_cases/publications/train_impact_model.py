"""
Use case: entrenar el modelo predictor de impacto de publicaciones.

Orquesta la carga de datos (a través del port) y devuelve métricas.
No conoce XGBoost ni SQLAlchemy directamente.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from project.domain.ports.impact_predictor_port import ImpactPredictorPort


@dataclass
class TrainImpactModelResult:
    success: bool
    metrics: Dict[str, Any]
    message: str


class TrainImpactModelUseCase:
    def __init__(self, predictor: ImpactPredictorPort) -> None:
        self._predictor = predictor

    def execute(self, session) -> TrainImpactModelResult:
        try:
            metrics = self._predictor.train(session)
            return TrainImpactModelResult(
                success=True,
                metrics=metrics,
                message=(
                    f"Modelo entrenado correctamente. "
                    f"Accuracy: {metrics['accuracy']:.1%} sobre {metrics['samples_test']} muestras de prueba. "
                    f"Versión: {metrics['model_version']}"
                ),
            )
        except ValueError as exc:
            return TrainImpactModelResult(success=False, metrics={}, message=str(exc))
