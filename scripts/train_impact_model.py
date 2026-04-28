"""
CLI: entrenar el modelo predictor de impacto de publicaciones.

Uso:
    python scripts/train_impact_model.py
    python scripts/train_impact_model.py --predict 123 456 789
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Asegura que el raíz del proyecto esté en el path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from project.infrastructure.ml.xgboost_predictor import XGBoostImpactPredictor
from project.application.use_cases.publications.train_impact_model import TrainImpactModelUseCase
from project.application.use_cases.publications.predict_impact import (
    PredictImpactCommand,
    PredictImpactUseCase,
)
from db.session import get_session


def cmd_train(args) -> None:
    predictor = XGBoostImpactPredictor()
    use_case = TrainImpactModelUseCase(predictor)

    session = get_session()
    try:
        result = use_case.execute(session)
    finally:
        session.close()

    if result.success:
        print(result.message)
        print("\nDistribución de clases:")
        for label, count in result.metrics.get("class_distribution", {}).items():
            print(f"  {label}: {count} publicaciones")
        print("\nReporte completo:")
        print(json.dumps(result.metrics.get("classification_report", {}), indent=2, ensure_ascii=False))
    else:
        print(f"Error: {result.message}", file=sys.stderr)
        sys.exit(1)


def cmd_predict(args) -> None:
    pub_ids = [int(i) for i in args.ids]
    predictor = XGBoostImpactPredictor()
    use_case = PredictImpactUseCase(predictor)
    command = PredictImpactCommand(publication_ids=pub_ids)

    session = get_session()
    try:
        result = use_case.execute(command, session)
    finally:
        session.close()

    print(f"Modelo versión: {result.model_version}\n")
    for pred in result.predictions:
        probs = pred.probabilities
        print(
            f"  pub_id={pred.publication_id} → impacto {pred.label.upper()} "
            f"(bajo={probs['bajo']:.1%} medio={probs['medio']:.1%} alto={probs['alto']:.1%})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Predictor de impacto de publicaciones (XGBoost)")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("train", help="Entrenar el modelo con datos actuales de la DB")

    pred_parser = sub.add_parser("predict", help="Predecir impacto de publicaciones por ID")
    pred_parser.add_argument("ids", nargs="+", help="IDs de canonical_publications")

    args = parser.parse_args()

    if args.cmd == "train":
        cmd_train(args)
    elif args.cmd == "predict":
        cmd_predict(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
