from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class ImpactProbabilities(BaseModel):
    bajo: float = Field(..., description="Probabilidad de impacto bajo (<=5 citas)")
    medio: float = Field(..., description="Probabilidad de impacto medio (6-20 citas)")
    alto: float = Field(..., description="Probabilidad de impacto alto (>20 citas)")


class ImpactPredictionResponse(BaseModel):
    publication_id: int
    predicted_level: str = Field(..., description="bajo | medio | alto")
    probabilities: ImpactProbabilities
    model_version: str


class ImpactBatchRequest(BaseModel):
    publication_ids: List[int] = Field(..., min_length=1, max_length=500)


class ImpactBatchResponse(BaseModel):
    predictions: List[ImpactPredictionResponse]
    model_version: str
    count: int


class TrainResponse(BaseModel):
    success: bool
    message: str
    accuracy: Optional[float] = None
    samples_train: Optional[int] = None
    samples_test: Optional[int] = None
    class_distribution: Optional[Dict[str, int]] = None
    model_version: Optional[str] = None
