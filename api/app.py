from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator

# === Config ===
DEFAULT_MODEL_PATH = "/mnt/shared/models/model.joblib"
MODEL_PATH = os.getenv("MODEL_PATH", DEFAULT_MODEL_PATH)

EXPECTED_COLS = ["id_bandera", "productos_marca", "productos_precio_lista"]

app = FastAPI(
    title="TP Final - Predicción de Descuento",
    description="Servicio REST para predecir porcentaje de descuento.",
    version="1.0.0",
)

_model = None  # cache en memoria


# ====== Schemas ======
class PredictRequest(BaseModel):
    id_bandera: Optional[int | str]
    productos_marca: Optional[str]
    productos_precio_lista: Optional[float]

    # Asegurar tipos/normalización como en entrenamiento
    @field_validator("id_bandera")
    @classmethod
    def _id_bandera_to_string(cls, v):
        if v is None:
            return None
        return str(v)

    @field_validator("productos_marca")
    @classmethod
    def _marca_strip(cls, v):
        if v is None:
            return None
        return str(v).strip()

    @field_validator("productos_precio_lista")
    @classmethod
    def _precio_to_float(cls, v):
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            raise ValueError("productos_precio_lista debe ser numérico")


class PredictResponse(BaseModel):
    descuento_pred: float


# ====== Helpers ======
def _load_model() -> None:
    global _model
    if _model is not None:
        return

    path = Path(MODEL_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"Modelo no encontrado en {path}. "
            "Ejecute el DAG hasta 'train_eval' para generar data/models/model.joblib."
        )
    _model = joblib.load(path)


def _build_frame(req: PredictRequest) -> pd.DataFrame:
    # Crear DataFrame con columnas EXACTAS y en orden
    row = {
        "id_bandera": req.id_bandera,
        "productos_marca": req.productos_marca,
        "productos_precio_lista": req.productos_precio_lista,
    }
    df = pd.DataFrame([row], columns=EXPECTED_COLS)

    # Cast igual que en entrenamiento
    df["id_bandera"] = df["id_bandera"].astype("string")
    df["productos_marca"] = df["productos_marca"].astype("string")
    df["productos_precio_lista"] = pd.to_numeric(df["productos_precio_lista"], errors="coerce")
    return df


# ====== Endpoints ======
@app.get("/health")
def health():
    try:
        _load_model()
        return {"status": "ok", "model_path": MODEL_PATH}
    except FileNotFoundError as e:
        # 503 para indicar dependencia no lista (modelo)
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    try:
        _load_model()
    except FileNotFoundError as e:
        # Devuelve 503 más descriptivo (en vez de un 500 genérico)
        raise HTTPException(status_code=503, detail=str(e))

    df = _build_frame(req)

    # Validaciones mínimas
    if df["productos_precio_lista"].isna().any():
        raise HTTPException(status_code=422, detail="productos_precio_lista es requerido y debe ser numérico")

    # Predicción
    try:
        pred = float(_model.predict(df)[0])
    except Exception as e:
        # Si hay mismatch de columnas/OneHot, lo vas a ver acá
        raise HTTPException(status_code=500, detail=f"Error al predecir: {e}")

    return PredictResponse(descuento_pred=pred)
