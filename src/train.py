import os, json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib

import mlflow
import mlflow.sklearn

from .data_utils import FEAT_COLS

ART_DIR = Path("/opt/airflow/data/models")
ART_DIR.mkdir(parents=True, exist_ok=True)

CATEGORICAL = ["id_bandera", "productos_marca"]
NUMERIC = ["productos_precio_lista"]

def build_pipeline():
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), CATEGORICAL),
            ("num", "passthrough", NUMERIC),
        ],
        remainder="drop",
    )
    model = RandomForestRegressor(random_state=42, n_jobs=-1)
    pipe = Pipeline([("pre", pre), ("clf", model)])
    return pipe

def train_and_evaluate(Xtr: pd.DataFrame, ytr: pd.Series, Xte: pd.DataFrame, yte: pd.Series):
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("TPFinal-AMq1")

    pipe = build_pipeline()
    grid = {
        "clf__n_estimators": [120, 200],
        "clf__max_depth": [None, 20],
        "clf__max_features": ["sqrt"],
        "clf__min_samples_leaf": [1, 2],
        "clf__bootstrap": [True],
        "clf__max_samples": [0.7, 0.9],
    }
    cv = KFold(n_splits=2, shuffle=True, random_state=42)

    # Submuestra para CV (rápido)
    n = min(3000, len(Xtr))
    rng = np.random.default_rng(42)
    idx = rng.choice(len(Xtr), size=n, replace=False)
    Xcv, ycv = Xtr.iloc[idx], ytr.iloc[idx]

    mlflow.sklearn.autolog(log_models=True, registered_model_name=None)

    with mlflow.start_run(run_name="gridsearch"):
        gs = GridSearchCV(pipe, grid, scoring="r2", cv=cv, n_jobs=-1, refit=True, verbose=0)
        gs.fit(Xcv, ycv)

        best = gs.best_estimator_
        best.fit(Xtr, ytr)
        yhat = best.predict(Xte)
        mae = float(mean_absolute_error(yte, yhat))
        rmse = float(np.sqrt(mean_squared_error(yte, yhat)))
        r2 = float(r2_score(yte, yhat))

        # Persistencia local
        model_path = ART_DIR / "model.joblib"
        joblib.dump(best, model_path)

        # Log explícito de métricas finales
        mlflow.log_metric("test_mae", mae)
        mlflow.log_metric("test_rmse", rmse)
        mlflow.log_metric("test_r2", r2)

        metrics = {"mae": mae, "rmse": rmse, "r2": r2, "features": FEAT_COLS}
        (ART_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2))

    return str(model_path), metrics
