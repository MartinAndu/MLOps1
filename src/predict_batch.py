"""
Módulo para realizar predicciones batch con un modelo entrenado.

Este script carga el modelo de machine learning serializado (generado en la
etapa de entrenamiento) y lo utiliza para generar predicciones sobre un
conjunto de datos de entrada. Las predicciones resultantes se guardan en un
archivo CSV en una ubicación predefinida.
"""
from pathlib import Path
import pandas as pd
import joblib

MODEL_PATH = "/opt/airflow/data/models/model.joblib"

def predict_on_df(df: pd.DataFrame):
    """
    Carga el modelo entrenado, realiza predicciones y guarda los resultados.

    Esta función toma un DataFrame de entrada, carga el modelo de machine
    learning desde la ruta `MODEL_PATH`, aplica el modelo para generar
    predicciones y guarda estas predicciones en un archivo CSV llamado
    'preds.csv' dentro del directorio '/opt/airflow/data/predictions/'.

    Args:
        df: Un DataFrame de Pandas que contiene los datos de entrada sobre los
            cuales se realizarán las predicciones. Debe tener las mismas
            columnas y preprocesamiento que los datos de entrenamiento.

    Side Effects:
        - Crea el directorio '/opt/airflow/data/predictions/' si no existe.
        - Guarda un archivo 'preds.csv' en el directorio anterior con las
          predicciones.
    """
    model = joblib.load(MODEL_PATH)
    preds = model.predict(df)
    out = Path("/opt/airflow/data/predictions")
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"pred_descuento": preds}).to_csv(out / "preds.csv", index=False)
