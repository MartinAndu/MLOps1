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
        df = pd.read_pickle(df_pkl)
    else:
        # Fallback mínimo: armar desde productos.csv si existiera
        prod_csv = SHARED / "raw" / "productos.csv"
        if prod_csv.exists():
            prod = pd.read_csv(prod_csv, sep="|", encoding="utf-8", dtype=str)
        else:
            prod = pd.DataFrame()
        if TARGET not in prod.columns:
            prod[TARGET] = pd.NA
        for col in FEAT_COLS:
            if col not in prod.columns:
                prod[col] = pd.NA
        df = prod[FEAT_COLS + [TARGET]].copy()
    return df
