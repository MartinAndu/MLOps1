from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Tuple, Dict, Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

import mlflow
import mlflow.sklearn


CAT_COLS = ["id_bandera", "productos_marca"]
NUM_COLS = ["productos_precio_lista"]
TARGET = "descuento"


def _build_pipeline() -> Pipeline:
    """Pipeline de preprocesamiento + modelo."""
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), CAT_COLS),
            ("num", "passthrough", NUM_COLS),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    model = RandomForestRegressor(
        random_state=42,
        n_jobs=-1,
    )

    pipe = Pipeline([
        ("pre", pre),
        ("clf", model),
    ])
    return pipe


def _compute_metrics(y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    return {"mae": mae, "rmse": rmse, "r2": r2}


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def train_and_evaluate(base_dir: str) -> str:
    """
    Entrena el modelo a partir de los splits en base_dir/processed/splits.joblib,
    registra un GridSearch en MLflow y guarda artefactos en base_dir/models/.

    Returns
    -------
    str
        Ruta al modelo entrenado (model.joblib).
    """
    base = Path(base_dir)
    splits_path = base / "processed" / "splits.joblib"
    models_dir = base / "models"
    _safe_mkdir(models_dir)

    if not splits_path.exists():
        raise FileNotFoundError(
            f"[TRAIN] No existe {splits_path}. Asegúrese de correr primero el split."
        )

    # Cargar splits
    Xtr, Xte, ytr, yte = joblib.load(splits_path)
    # Sanitizar dtypes: que las columnas existan y estén en el orden esperado
    for c in CAT_COLS + NUM_COLS:
        if c not in Xtr.columns:
            Xtr[c] = pd.NA
        if c not in Xte.columns:
            Xte[c] = pd.NA
    Xtr = Xtr[CAT_COLS + NUM_COLS].copy()
    Xte = Xte[CAT_COLS + NUM_COLS].copy()

    # Casting opcional para robustez
    Xtr["id_bandera"] = Xtr["id_bandera"].astype("string")
    Xte["id_bandera"] = Xte["id_bandera"].astype("string")
    Xtr["productos_marca"] = Xtr["productos_marca"].astype("string")
    Xte["productos_marca"] = Xte["productos_marca"].astype("string")
    Xtr["productos_precio_lista"] = pd.to_numeric(Xtr["productos_precio_lista"], errors="coerce")
    Xte["productos_precio_lista"] = pd.to_numeric(Xte["productos_precio_lista"], errors="coerce")

    # Configurar MLflow
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("TPFinal-AMq1")

    pipe = _build_pipeline()

    # Búsqueda simple (rápida) para cumplir el requisito de hyperparam search
    param_grid = {
        "clf__n_estimators": [120, 200],
        "clf__max_depth": [None, 20],
        "clf__min_samples_leaf": [1, 2],
        "clf__max_features": ["sqrt"],
        "clf__bootstrap": [True],
        "clf__max_samples": [0.7, 0.9],
    }

    # Submuestra para CV (acelera la búsqueda si hay muchos datos)
    n = min(3000, len(Xtr))
    if n < len(Xtr):
        rng = np.random.default_rng(42)
        idx = rng.choice(len(Xtr), size=n, replace=False)
        Xcv, ycv = Xtr.iloc[idx], ytr.iloc[idx]
    else:
        Xcv, ycv = Xtr, ytr

    cv = KFold(n_splits=2, shuffle=True, random_state=42)

    # Autolog: registra params, métricas, y modelo del GridSearch
    mlflow.sklearn.autolog(log_models=True, registered_model_name=None)

    with mlflow.start_run(run_name="gridsearch"):
        gs = GridSearchCV(
            estimator=pipe,
            param_grid=param_grid,
            scoring="r2",
            cv=cv,
            n_jobs=-1,
            refit=True,
            verbose=0,
        )
        gs.fit(Xcv, ycv)

        # Reentrenar best_estimator_ con TODO el train
        best = gs.best_estimator_
        best.fit(Xtr, ytr)

        # Evaluación en test
        yhat = best.predict(Xte)
        metrics = _compute_metrics(yte, yhat)

        # Persistencia local
        model_path = models_dir / "model.joblib"
        joblib.dump(best, model_path)

        # Guardar métricas formales del TP
        metrics_payload: Dict[str, Any] = {
            "features": CAT_COLS + NUM_COLS,
            "target": TARGET,
            "metrics": metrics,
            "best_params": gs.best_params_,
        }
        (models_dir / "metrics.json").write_text(json.dumps(metrics_payload, indent=2))

        # Log explícito de métricas finales en MLflow
        mlflow.log_metrics({
            "test_mae": metrics["mae"],
            "test_rmse": metrics["rmse"],
            "test_r2": metrics["r2"],
        })

    print(f"[TRAIN] Modelo guardado en: {model_path}")
    print(f"[TRAIN] Métricas guardadas en: {models_dir / 'metrics.json'}")
    return str(model_path)
