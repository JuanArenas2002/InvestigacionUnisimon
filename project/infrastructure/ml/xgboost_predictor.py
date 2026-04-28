from __future__ import annotations

import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

from project.domain.models.impact_prediction import ImpactLevel, ImpactPrediction
from project.domain.ports.impact_predictor_port import ImpactPredictorPort

# Directorio donde se persiste el modelo entrenado
_MODEL_DIR = Path(__file__).resolve().parents[3] / "models"
_MODEL_PATH = _MODEL_DIR / "xgb_impact.joblib"
_VERSION_PATH = _MODEL_DIR / "xgb_impact.version"

# Columnas numéricas usadas como features
FEATURES = [
    "coauthorships_count",
    "institutional_authors_count",
    "sources_count",
]


class XGBoostImpactPredictor(ImpactPredictorPort):
    """
    Adaptador XGBoost que implementa ImpactPredictorPort.

    Persistencia: el modelo entrenado se guarda en models/xgb_impact.joblib
    y puede recargarse sin necesidad de re-entrenar.
    """

    def __init__(self, model_path: Optional[Path] = None) -> None:
        self._model_path = model_path or _MODEL_PATH
        self._model: Optional[xgb.XGBClassifier] = None
        self._version: str = "untrained"
        self._load_if_exists()

    # ── Port implementation ─────────────────────────────────────────────────

    def train(self, session) -> dict:
        df = self._load_training_data(session)
        if len(df) < 10:
            raise ValueError(
                f"Datos insuficientes para entrenar: {len(df)} registros "
                "(mínimo 10). Ejecuta la reconciliación primero."
            )

        df["impact_level"] = df["citation_count"].apply(ImpactLevel.from_citation_count)

        X = df[FEATURES].fillna(0).astype(float)
        y = df["impact_level"].astype(int)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y if y.nunique() > 1 else None
        )

        model = xgb.XGBClassifier(
            objective="multi:softprob",
            num_class=3,
            n_estimators=200,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric="mlogloss",
            random_state=42,
            verbosity=0,
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        y_pred = model.predict(X_test)
        accuracy = float(accuracy_score(y_test, y_pred))
        report = classification_report(y_test, y_pred, target_names=["bajo", "medio", "alto"], output_dict=True)

        self._model = model
        self._version = self._compute_version(df)
        self._persist()

        class_dist = df["impact_level"].value_counts().to_dict()
        return {
            "accuracy": round(accuracy, 4),
            "samples_train": len(X_train),
            "samples_test": len(X_test),
            "class_distribution": {ImpactLevel(k).label(): int(v) for k, v in class_dist.items()},
            "classification_report": report,
            "model_version": self._version,
            "trained_at": datetime.utcnow().isoformat(),
        }

    def predict(self, publication_id: int, session) -> ImpactPrediction:
        results = self.predict_batch([publication_id], session)
        if not results:
            raise ValueError(f"Publicación {publication_id} no encontrada o sin datos.")
        return results[0]

    def predict_batch(self, publication_ids: List[int], session) -> List[ImpactPrediction]:
        self._assert_trained()
        df = self._load_features_for_ids(publication_ids, session)
        if df.empty:
            return []

        X = df[FEATURES].fillna(0).astype(float)
        proba_matrix = self._model.predict_proba(X)

        results = []
        for i, pub_id in enumerate(df["id"]):
            probs = proba_matrix[i]
            predicted_idx = int(np.argmax(probs))
            results.append(ImpactPrediction(
                publication_id=int(pub_id),
                predicted_level=ImpactLevel(predicted_idx),
                probabilities={
                    "bajo": round(float(probs[0]), 4),
                    "medio": round(float(probs[1]), 4),
                    "alto": round(float(probs[2]), 4),
                },
                model_version=self._version,
            ))
        return results

    def is_trained(self) -> bool:
        return self._model is not None

    @property
    def version(self) -> str:
        return self._version

    # ── Internal helpers ────────────────────────────────────────────────────

    def _assert_trained(self) -> None:
        if not self.is_trained():
            raise RuntimeError(
                "El modelo no está entrenado. Ejecuta train() o "
                "asegúrate de que models/xgb_impact.joblib exista."
            )

    def _load_training_data(self, session) -> pd.DataFrame:
        sql = """
            SELECT id, citation_count,
                   COALESCE(coauthorships_count, 0)        AS coauthorships_count,
                   COALESCE(institutional_authors_count, 0) AS institutional_authors_count,
                   COALESCE(sources_count, 1)               AS sources_count
            FROM canonical_publications
            WHERE citation_count IS NOT NULL
        """
        from sqlalchemy import text
        rows = session.execute(text(sql)).mappings().all()
        return pd.DataFrame(rows)

    def _load_features_for_ids(self, publication_ids: List[int], session) -> pd.DataFrame:
        from sqlalchemy import text
        ids_str = ",".join(str(i) for i in publication_ids)
        sql = f"""
            SELECT id,
                   COALESCE(coauthorships_count, 0)        AS coauthorships_count,
                   COALESCE(institutional_authors_count, 0) AS institutional_authors_count,
                   COALESCE(sources_count, 1)               AS sources_count
            FROM canonical_publications
            WHERE id IN ({ids_str})
        """
        rows = session.execute(text(sql)).mappings().all()
        return pd.DataFrame(rows)

    def _persist(self) -> None:
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._model, self._model_path)
        _VERSION_PATH.write_text(self._version)

    def _load_if_exists(self) -> None:
        if self._model_path.exists():
            self._model = joblib.load(self._model_path)
            if _VERSION_PATH.exists():
                self._version = _VERSION_PATH.read_text().strip()
            else:
                self._version = "loaded"

    @staticmethod
    def _compute_version(df: pd.DataFrame) -> str:
        fingerprint = f"{len(df)}-{int(df['citation_count'].sum())}-{datetime.utcnow().date()}"
        return hashlib.sha1(fingerprint.encode()).hexdigest()[:8]
