# src/train.py
from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Dict, Any, Tuple

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
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature

# ----------------------------
# Configuración del modelo
# ----------------------------
CAT_COLS = ["id_bandera", "productos_marca"]
NUM_COLS = ["productos_precio_lista"]
TARGET = "descuento"
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT", "TPFinal-AMq1")
REGISTERED_MODEL_NAME = os.getenv("REGISTERED_MODEL_NAME", "descuento-predictor")


# ----------------------------
# Utilidades
# ----------------------------
def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _compute_metrics(y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    return {"mae": mae, "rmse": rmse, "r2": r2}


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


def _setup_mlflow() -> None:
    """
    Configura el tracking de MLflow con reintentos y compatibilidad 1.x → 3.x.
    Evita falsos negativos por cambios de API (`list_experiments` vs `search_experiments`).
    """
    import time

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)

    # Asegurar que el registry use el mismo servidor
    mlflow.set_registry_uri(os.getenv("MLFLOW_REGISTRY_URI", mlflow.get_tracking_uri()))

    last_err = None
    for i in range(10):
        try:
            client = MlflowClient()
            # Ping cross-version (MLflow 2/3 tienen search_experiments; 1.x tenía list_experiments)
            if hasattr(client, "search_experiments"):
                client.search_experiments(max_results=1)
            else:
                client.list_experiments()

            print(f"[MLflow] Conectado a {tracking_uri}")
            return
        except Exception as e:
            last_err = e
            print(f"[MLflow] Intento {i+1}/10: todavía no responde ({e}). Reintentando...")
            time.sleep(3)

    # Fallback local si no conecta
    local_uri = "file:///opt/airflow/mlruns"
    mlflow.set_tracking_uri(local_uri)
    print(f"[MLflow][WARN] No se pudo conectar a {tracking_uri} tras 10 intentos: {last_err}")
    print(f"[MLflow] Fallback a store local: {local_uri}")


# ----------------------------
# Entrenamiento + registro
# ----------------------------
def _load_splits(splits_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    if not splits_path.exists():
        raise FileNotFoundError(f"[TRAIN] No existe {splits_path}. Corra primero el split.")
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
    Xtr["productos_precio_lista"] = _to_numeric(Xtr["productos_precio_lista"])
    Xte["productos_precio_lista"] = _to_numeric(Xte["productos_precio_lista"])
    return Xtr, Xte, ytr, yte


def train_and_evaluate(base_dir: str) -> str:
    """
    Entrena el modelo a partir de base_dir/processed/splits.joblib,
    registra un GridSearch en MLflow y publica el mejor modelo en el Model Registry.
    Además, guarda artefactos locales en base_dir/models/.
    """
    base = Path(base_dir)
    splits_path = base / "processed" / "splits.joblib"
    models_dir = base / "models"
    _safe_mkdir(models_dir)

    Xtr, Xte, ytr, yte = _load_splits(splits_path)

    # MLflow (tracking + experimento)
    _setup_mlflow()
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Pipeline y espacio de búsqueda
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

    # --- Run de MLflow
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

        # Métricas en test
        yhat = best.predict(Xte)
        metrics = _compute_metrics(yte, yhat)

        # Log de hiperparámetros y métricas del test
        mlflow.log_params(gs.best_params_)
        mlflow.log_metrics({
            "test_mae": metrics["mae"],
            "test_rmse": metrics["rmse"],
            "test_r2": metrics["r2"],
        })

        # Firma del modelo y ejemplo de entrada (para UI/serving)
        signature = infer_signature(
            Xtr[CAT_COLS + NUM_COLS],
            best.predict(Xtr[CAT_COLS + NUM_COLS])
        )
        input_example = Xtr[CAT_COLS + NUM_COLS].head(1)

        # Log del modelo como artefacto del run
        mlflow.sklearn.log_model(
            sk_model=best,
            artifact_path="model",
            signature=signature,
            input_example=input_example,
        )

        # Registro explícito en el Model Registry
        run_id = mlflow.active_run().info.run_id
        model_uri = f"runs:/{run_id}/model"
        registered = mlflow.register_model(model_uri=model_uri, name=REGISTERED_MODEL_NAME)
        print(f"[TRAIN] Modelo registrado en MLflow: name={REGISTERED_MODEL_NAME}, version={registered.version}")

        # Artefactos locales (para la API y trazabilidad fuera de MLflow)
        model_path = models_dir / "model.joblib"
        joblib.dump(best, model_path)

        payload: Dict[str, Any] = {
            "features": CAT_COLS + NUM_COLS,
            "target": TARGET,
            "metrics": metrics,
            "best_params": gs.best_params_,
            "registered_model": REGISTERED_MODEL_NAME,
            "registered_version": int(registered.version),
            "run_id": run_id,
        }
        (models_dir / "metrics.json").write_text(json.dumps(payload, indent=2))

    print(f"[TRAIN] Modelo local guardado en: {models_dir / 'model.joblib'}")
    print(f"[TRAIN] Métricas guardadas en: {models_dir / 'metrics.json'}")
    return str(models_dir / "model.joblib")
