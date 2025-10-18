"""
Módulo para el preprocesamiento y la división de datos.

Este script contiene las funciones necesarias para preparar el dataset final
para el entrenamiento del modelo. La función principal, `split_dataset`, se
encarga de cargar el dataset procesado, dividirlo en conjuntos de entrenamiento
y prueba, y guardar estos conjuntos para su uso en etapas posteriores del
pipeline de ML.
"""

import numpy as np
import pandas as pd
from pathlib import Path
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from .data_utils import FEAT_COLS, TARGET

def split_train_test(df: pd.DataFrame, test_size: float = 0.23, seed: int = 42):
    """
    Divide un DataFrame en conjuntos de entrenamiento y prueba para X e y.

    Esta función toma un DataFrame, lo separa en características (X) y
    objetivo (y), y luego lo divide en subconjuntos de entrenamiento y prueba.
    Maneja los valores nulos en la columna objetivo excluyendo esas filas
    de la división. La división se realiza sobre los índices para mantener la
    integridad de los datos.

    Args:
        df: El DataFrame de entrada que contiene tanto las características
            como la columna objetivo.
        test_size: La proporción del dataset que se asignará al conjunto de
                   prueba.
        seed: La semilla para el generador de números aleatorios para
              garantizar la reproducibilidad.

    Returns:
        Una tupla de 6 elementos que contiene:
        - X_train (pd.DataFrame): Características de entrenamiento.
        - X_test (pd.DataFrame): Características de prueba.
        - y_train (pd.Series): Objetivo de entrenamiento.
        - y_test (pd.Series): Objetivo de prueba.
        - train_idx (pd.Index): Índices de las filas de entrenamiento.
        - test_idx (pd.Index): Índices de las filas de prueba.

    Raises:
        AssertionError: Si la columna objetivo no se encuentra en el DataFrame.
    """
    assert TARGET in df.columns, f"No existe la columna target '{TARGET}'"
    X = df[FEAT_COLS].copy()
    y = df[TARGET].astype(float).copy()
    mask = y.notna()
    X, y = X.loc[mask], y.loc[mask]
    train_idx, test_idx = train_test_split(X.index, test_size=test_size, random_state=seed)
    return X.loc[train_idx], X.loc[test_idx], y.loc[train_idx], y.loc[test_idx], train_idx, test_idx

def split_dataset(base_dir: str, test_size: float = 0.2, random_state: int = 42) -> str:
    """
    Carga, divide y guarda los conjuntos de datos de entrenamiento y prueba.

    Esta función orquesta el proceso de división de datos como un paso del
    pipeline. Carga el dataset completo desde un archivo pickle, lo divide
    en características (X) y objetivo (y), realiza la división en entrenamiento
    y prueba, y finalmente guarda los cuatro DataFrames resultantes en un
    único archivo `splits.joblib` para su uso posterior en el entrenamiento.

    Args:
        base_dir: La ruta al directorio de datos base (ej. '/opt/airflow/data').
        test_size: La proporción del dataset para el conjunto de prueba.
        random_state: La semilla aleatoria para la reproducibilidad de la
                      división.

    Returns:
        La ruta completa al archivo `splits.joblib` que contiene los datos
        divididos.

    Side Effects:
        - Crea el directorio 'data/processed/' si no existe.
        - Guarda un archivo 'splits.joblib' en el directorio anterior.
    """
    base = Path(base_dir)
    processed = base / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    df_path = base / "df.pkl"            # ✅ leer el pkl desde la carpeta base
    df = pd.read_pickle(df_path)

    X = df.drop(columns=["descuento"])
    y = df["descuento"]

    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=test_size, random_state=random_state)

    out = processed / "splits.joblib"
    joblib.dump((Xtr, Xte, ytr, yte), out)   # ✅ guarda en data/processed/splits.joblib
    print(f"[PREPROCESS] Splits guardados en {out}")
    return str(out)
