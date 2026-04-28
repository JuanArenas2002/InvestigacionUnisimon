from abc import ABC, abstractmethod
from typing import List

from project.domain.models.impact_prediction import ImpactPrediction


class ImpactPredictorPort(ABC):
    """Puerto para predecir el nivel de impacto de una publicación."""

    @abstractmethod
    def train(self, session) -> dict:
        """
        Entrena el modelo con datos de canonical_publications.
        Retorna métricas: accuracy, samples, etc.
        """

    @abstractmethod
    def predict(self, publication_id: int, session) -> ImpactPrediction:
        """Predice el nivel de impacto de una publicación canónica por ID."""

    @abstractmethod
    def predict_batch(self, publication_ids: List[int], session) -> List[ImpactPrediction]:
        """Predice en lote. Más eficiente que llamar predict() N veces."""

    @abstractmethod
    def is_trained(self) -> bool:
        """True si el modelo está listo para predecir."""

    @property
    @abstractmethod
    def version(self) -> str:
        """Identificador de versión del modelo (ej: hash del dataset o fecha)."""
