from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Dict, Any

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
from mlflow import MlflowClient

CAT_COLS = ["id_bandera", "productos_marca"]
NUM_COLS = ["productos_precio_lista"]
TARGET = "descuento"

def _setup_mlflow() -> None:
    import time
    from mlflow.tracking import MlflowClient

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)

    last_err = None
    for i in range(10):
        try:
            client = MlflowClient()

            # MLflow 2.x
            try:
                client.search_experiments(max_results=1)
            except TypeError:
                # MLflow 1.x
                _ = client.list_experiments()

            print(f"[MLflow] Conectado a {tracking_uri}")
            return
        except Exception as e:
            last_err = e
            print(f"[MLflow] Intento {i+1}/10: todavía no responde ({e}). Retrying...")
            time.sleep(3)

    # Fallback local tras 10 intentos
    local_uri = "file:///opt/airflow/mlruns"
    mlflow.set_tracking_uri(local_uri)
    print(f"[MLflow][WARN] No se pudo conectar a {tracking_uri} tras 10 intentos: {last_err}")
    print(f"[MLflow] Fallback a store local: {local_uri}")


def _build_pipeline() -> Pipeline:
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), CAT_COLS),
            ("num", "passthrough", NUM_COLS),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )
    model = RandomForestRegressor(random_state=42, n_jobs=-1)
    return Pipeline([("pre", pre), ("clf", model)])

def _compute_metrics(y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    return {"mae": mae, "rmse": rmse, "r2": r2}

def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def train_and_evaluate(base_dir: str) -> str:
    """
    Entrena el modelo a partir de base_dir/processed/splits.joblib,
    registra un GridSearch en MLflow (con fallback local) y guarda artefactos en base_dir/models/.
    """
    base = Path(base_dir)
    splits_path = base / "processed" / "splits.joblib"
    models_dir = base / "models"
    _safe_mkdir(models_dir)

    if not splits_path.exists():
        raise FileNotFoundError(f"[TRAIN] No existe {splits_path}. Corra primero el split.")

    # Cargar splits
    Xtr, Xte, ytr, yte = joblib.load(splits_path)

    # Asegurar columnas
    for c in CAT_COLS + NUM_COLS:
        if c not in Xtr.columns:
            Xtr[c] = pd.NA
        if c not in Xte.columns:
            Xte[c] = pd.NA
    Xtr = Xtr[CAT_COLS + NUM_COLS].copy()
    Xte = Xte[CAT_COLS + NUM_COLS].copy()

    # Casts
    Xtr["id_bandera"] = Xtr["id_bandera"].astype("string")
    Xte["id_bandera"] = Xte["id_bandera"].astype("string")
    Xtr["productos_marca"] = Xtr["productos_marca"].astype("string")
    Xte["productos_marca"] = Xte["productos_marca"].astype("string")
    Xtr["productos_precio_lista"] = pd.to_numeric(Xtr["productos_precio_lista"], errors="coerce")
    Xte["productos_precio_lista"] = pd.to_numeric(Xte["productos_precio_lista"], errors="coerce")

    # MLflow
    _setup_mlflow()
    mlflow.set_experiment("TPFinal-AMq1")
    mlflow.sklearn.autolog(log_models=True)

    pipe = _build_pipeline()

    param_grid = {
        "clf__n_estimators": [120, 200],
        "clf__max_depth": [None, 20],
        "clf__min_samples_leaf": [1, 2],
        "clf__max_features": ["sqrt"],
        "clf__bootstrap": [True],
        "clf__max_samples": [0.7, 0.9],
    }

    # Submuestra para CV si hay muchos datos
    n = min(3000, len(Xtr))
    if n < len(Xtr):
        rng = np.random.default_rng(42)
        idx = rng.choice(len(Xtr), size=n, replace=False)
        Xcv, ycv = Xtr.iloc[idx], ytr.iloc[idx]
    else:
        Xcv, ycv = Xtr, ytr

    cv = KFold(n_splits=2, shuffle=True, random_state=42)

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

        best = gs.best_estimator_
        best.fit(Xtr, ytr)

        yhat = best.predict(Xte)
        metrics = _compute_metrics(yte, yhat)

        model_path = models_dir / "model.joblib"
        joblib.dump(best, model_path)

        payload: Dict[str, Any] = {
            "features": CAT_COLS + NUM_COLS,
            "target": TARGET,
            "metrics": metrics,
            "best_params": gs.best_params_,
        }
        (models_dir / "metrics.json").write_text(json.dumps(payload, indent=2))

        mlflow.log_metrics({
            "test_mae": metrics["mae"],
            "test_rmse": metrics["rmse"],
            "test_r2": metrics["r2"],
        })

    print(f"[TRAIN] Modelo guardado en: {model_path}")
    print(f"[TRAIN] Métricas guardadas en: {models_dir / 'metrics.json'}")
    return str(model_path)
