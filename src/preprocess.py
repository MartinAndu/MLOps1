import numpy as np
import pandas as pd
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
