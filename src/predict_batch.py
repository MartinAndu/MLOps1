from pathlib import Path
import pandas as pd
import joblib

MODEL_PATH = "/opt/airflow/data/models/model.joblib"

def predict_on_df(df: pd.DataFrame):
    model = joblib.load(MODEL_PATH)
    preds = model.predict(df)
    out = Path("/opt/airflow/data/predictions")
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"pred_descuento": preds}).to_csv(out / "preds.csv", index=False)
