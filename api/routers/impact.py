"""
Router: Predicción de impacto de publicaciones (XGBoost).

Endpoints:
  GET  /publications/{id}/impact      → predice una publicación
  POST /publications/impact/batch     → predice en lote (hasta 500)
  POST /publications/impact/train     → re-entrena el modelo con datos actuales
"""
import logging
from functools import lru_cache
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.impact import (
    ImpactBatchRequest,
    ImpactBatchResponse,
    ImpactPredictionResponse,
    ImpactProbabilities,
    TrainResponse,
)
from project.application.use_cases.publications.predict_impact import (
    PredictImpactCommand,
    PredictImpactUseCase,
)
from project.application.use_cases.publications.train_impact_model import TrainImpactModelUseCase
from project.domain.models.impact_prediction import ImpactPrediction
from project.infrastructure.ml.xgboost_predictor import XGBoostImpactPredictor

logger = logging.getLogger("api")

router = APIRouter(prefix="/publications", tags=["Predicción de Impacto"])


# ── Singleton del predictor (cargado una sola vez al importar el router) ──────

@lru_cache(maxsize=1)
def _get_predictor() -> XGBoostImpactPredictor:
    return XGBoostImpactPredictor()


def _to_response(pred: ImpactPrediction) -> ImpactPredictionResponse:
    return ImpactPredictionResponse(
        publication_id=pred.publication_id,
        predicted_level=pred.label,
        probabilities=ImpactProbabilities(**pred.probabilities),
        model_version=pred.model_version,
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/{publication_id}/impact",
    response_model=ImpactPredictionResponse,
    summary="Predecir impacto de una publicación",
    description=(
        "Devuelve el nivel de impacto predicho (`bajo` / `medio` / `alto`) "
        "junto con las probabilidades para cada clase.\n\n"
        "- **bajo**: <= 5 citas\n"
        "- **medio**: 6–20 citas\n"
        "- **alto**: > 20 citas\n\n"
        "Requiere que el modelo esté entrenado (`POST /publications/impact/train`)."
    ),
)
def predict_impact(
    publication_id: int,
    db: Session = Depends(get_db),
):
    predictor = _get_predictor()
    if not predictor.is_trained():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="El modelo de impacto no está entrenado. Ejecuta POST /publications/impact/train.",
        )

    use_case = PredictImpactUseCase(predictor)
    try:
        result = use_case.execute(PredictImpactCommand(publication_ids=[publication_id]), db)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    if not result.predictions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Publicación {publication_id} no encontrada.",
        )
    return _to_response(result.predictions[0])


@router.post(
    "/impact/batch",
    response_model=ImpactBatchResponse,
    summary="Predecir impacto en lote",
    description=(
        "Predice el nivel de impacto para múltiples publicaciones en una sola llamada "
        "(máximo 500 IDs por request)."
    ),
)
def predict_impact_batch(
    body: ImpactBatchRequest,
    db: Session = Depends(get_db),
):
    predictor = _get_predictor()
    if not predictor.is_trained():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="El modelo de impacto no está entrenado. Ejecuta POST /publications/impact/train.",
        )

    use_case = PredictImpactUseCase(predictor)
    try:
        result = use_case.execute(PredictImpactCommand(publication_ids=body.publication_ids), db)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

    return ImpactBatchResponse(
        predictions=[_to_response(p) for p in result.predictions],
        model_version=result.model_version,
        count=len(result.predictions),
    )


@router.post(
    "/impact/train",
    response_model=TrainResponse,
    summary="Entrenar (o re-entrenar) el modelo de impacto",
    description=(
        "Re-entrena el modelo XGBoost con todos los datos actuales de "
        "`canonical_publications`. Persiste el modelo en disco para que "
        "las predicciones siguientes lo usen automáticamente.\n\n"
        "Esta operación puede tardar unos segundos según el tamaño de la BD."
    ),
)
def train_impact_model(db: Session = Depends(get_db)):
    predictor = _get_predictor()
    use_case = TrainImpactModelUseCase(predictor)

    try:
        result = use_case.execute(db)
    except Exception as exc:
        logger.exception("Error entrenando modelo de impacto")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

    if not result.success:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=result.message)

    return TrainResponse(
        success=result.success,
        message=result.message,
        accuracy=result.metrics.get("accuracy"),
        samples_train=result.metrics.get("samples_train"),
        samples_test=result.metrics.get("samples_test"),
        class_distribution=result.metrics.get("class_distribution"),
        model_version=result.metrics.get("model_version"),
    )
