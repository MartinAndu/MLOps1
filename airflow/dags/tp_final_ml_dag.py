from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="tp_final_ml_pipeline",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp-final", "ml"],
)
def pipeline():

    @task()
    def load_paths():
        from pathlib import Path
        base = Path("/opt/airflow/data")
        (base / "processed").mkdir(parents=True, exist_ok=True)
        (base / "models").mkdir(parents=True, exist_ok=True)
        (base / "predictions").mkdir(parents=True, exist_ok=True)
        return str(base)

    @task()
    def split(base_dir: str):
        from src.data_utils import load_df
        from src.preprocess import split_train_test
        import joblib

        df = load_df()
        Xtr, Xte, ytr, yte, train_idx, test_idx = split_train_test(df)
        joblib.dump((Xtr, Xte, ytr, yte), f"{base_dir}/processed/splits.joblib")
        return f"{base_dir}/processed/splits.joblib"

    @task()
    def train_eval(splits_path: str):
        import joblib
        from src.train import train_and_evaluate
        Xtr, Xte, ytr, yte = joblib.load(splits_path)
        model_path, metrics = train_and_evaluate(Xtr, ytr, Xte, yte)
        return {"model_path": model_path, "metrics": metrics}

    @task()
    def report(artifacts: dict):
        from src.evaluate import dump_report
        dump_report(artifacts["metrics"])
        return artifacts["model_path"]

    report(train_eval(split(load_paths())))

pipeline()
