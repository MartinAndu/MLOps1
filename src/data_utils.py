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
