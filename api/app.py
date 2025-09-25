import os
import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel, Field

MODEL_PATH = os.getenv("MODEL_PATH", "/mnt/shared/models/model.joblib")

app = FastAPI(title="TP Final ML - Predicción de Descuento")

class Item(BaseModel):
    id_bandera: int = Field(..., description="ID de bandera/supermercado")
    productos_marca: str = Field(..., description="Marca del producto")
    productos_precio_lista: float = Field(..., description="Precio de lista")

_model = None
_pipe_cols = ["id_bandera", "productos_marca", "productos_precio_lista"]

def load_model():
    global _model
    if _model is None:
        _model = joblib.load(MODEL_PATH)
    return _model

@app.get("/")
def root():
    return {"status": "ok", "model_path": MODEL_PATH}

@app.post("/predict")
def predict(item: Item):
    model = load_model()
    X = pd.DataFrame([{
        "id_bandera": item.id_bandera,
        "productos_marca": item.productos_marca,
        "productos_precio_lista": item.productos_precio_lista,
    }], columns=_pipe_cols)
    yhat = model.predict(X)
    return {"descuento_pred": float(yhat[0])}
