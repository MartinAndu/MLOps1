"""
Módulo para el entrenamiento, evaluación y registro del modelo de ML.

Este script contiene la lógica completa para el pipeline de entrenamiento:
1.  Carga los datos preprocesados (splits de entrenamiento y prueba).
2.  Define y construye un pipeline de Scikit-learn para el preprocesamiento
    y el modelado (RandomForestRegressor).
3.  Configura la conexión con el servidor de MLflow.
4.  Ejecuta un GridSearch con validación cruzada para encontrar los mejores
    hiperparámetros.
5.  Registra el experimento, los parámetros, las métricas y el modelo final
    en MLflow.
6.  Publica el mejor modelo en el Model Registry de MLflow.
7.  Guarda los artefactos del modelo (modelo y métricas) localmente para
    su uso por otros servicios como la API.
"""


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
    """Crea un directorio de forma segura, incluyendo directorios padres."""
    p.mkdir(parents=True, exist_ok=True)


def _to_numeric(s: pd.Series) -> pd.Series:
    """Convierte una Serie de Pandas a tipo numérico, forzando errores a NaN."""
    return pd.to_numeric(s, errors="coerce")


def _compute_metrics(y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
    """
    Calcula un conjunto de métricas de regresión.

    Args:
        y_true: Valores reales del objetivo.
        y_pred: Valores predichos por el modelo.

    Returns:
        Un diccionario con las métricas: MAE, RMSE y R2.
    """
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    return {"mae": mae, "rmse": rmse, "r2": r2}


def _build_pipeline() -> Pipeline:
    """
    Construye el pipeline de preprocesamiento y modelado de Scikit-learn.

    El pipeline consiste en:
    1.  Un preprocesador que aplica One-Hot Encoding a las columnas
        categóricas y deja pasar las numéricas.
    2.  Un regresor RandomForest como modelo predictivo.

    Returns:
        El pipeline de Scikit-learn sin entrenar.
    """
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
    Configura y verifica la conexión con el servidor de tracking de MLflow.

    Intenta conectarse al URI de tracking especificado en las variables de
    entorno con varios reintentos. Si la conexión falla, revierte a un
    directorio de tracking local como fallback. Esto asegura robustez en
    entornos donde el servidor MLflow puede tardar en iniciarse.
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
    """
    Carga los conjuntos de datos de entrenamiento y prueba desde un archivo.

    También se asegura de que las columnas esperadas existan y tengan los
    tipos de datos correctos antes de devolverlos.

    Args:
        splits_path: La ruta al archivo `splits.joblib`.

    Returns:
        Una tupla con los cuatro DataFrames/Series: X_train, X_test, y_train, y_test.

    Raises:
        FileNotFoundError: Si el archivo `splits.joblib` no existe.
    """
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
    Orquesta el ciclo completo de entrenamiento, evaluación y registro del modelo.

    Esta función principal ejecuta los siguientes pasos:
    1.  Carga los datos divididos.
    2.  Configura MLflow.
    3.  Realiza una búsqueda de hiperparámetros con GridSearchCV.
    4.  Entrena el mejor modelo con todos los datos de entrenamiento.
    5.  Evalúa el modelo en el conjunto de prueba.
    6.  Registra todo el proceso en un run de MLflow (parámetros, métricas, modelo).
    7.  Registra el modelo en el Model Registry de MLflow para versionado.
    8.  Guarda los artefactos (modelo y métricas) en el sistema de archivos local.

    Args:
        base_dir: La ruta al directorio de datos base (ej. '/opt/airflow/data').

    Returns:
        La ruta completa al archivo del modelo serializado (`model.joblib`).
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
