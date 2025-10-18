"""
Módulo de utilidades para la carga de datos y definición de constantes.

Este script centraliza las configuraciones y funciones comunes relacionadas con
el manejo de datos del proyecto, como la ubicación del directorio de datos,
las columnas de features y la columna objetivo.

Constantes:
- SHARED (Path): Ruta al directorio de datos compartidos.
- FEAT_COLS (list): Lista de nombres de las columnas usadas como features.
- TARGET (str): Nombre de la columna objetivo para la predicción.
"""

from pathlib import Path
import pandas as pd

SHARED = Path("/opt/airflow/data")

FEAT_COLS = [
    "id_bandera",
    "productos_marca",
    "productos_precio_lista",
]
TARGET = "descuento"

def load_df():
    df_pkl = SHARED / "df.pkl"
    if df_pkl.exists():
        return pd.read_pickle(df_pkl)
    # fallback vacío si alguien ejecuta split sin etl
    return pd.DataFrame(columns=FEAT_COLS + [TARGET])
