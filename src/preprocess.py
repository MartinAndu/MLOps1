import numpy as np
import pandas as pd
from pathlib import Path
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from .data_utils import FEAT_COLS, TARGET

def split_train_test(df: pd.DataFrame, test_size: float = 0.23, seed: int = 42):
    assert TARGET in df.columns, f"No existe la columna target '{TARGET}'"
    X = df[FEAT_COLS].copy()
    y = df[TARGET].astype(float).copy()
    mask = y.notna()
    X, y = X.loc[mask], y.loc[mask]
    train_idx, test_idx = train_test_split(X.index, test_size=test_size, random_state=seed)
    return X.loc[train_idx], X.loc[test_idx], y.loc[train_idx], y.loc[test_idx], train_idx, test_idx

def split_dataset(base_dir: str, test_size: float = 0.2, random_state: int = 42) -> str:
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
