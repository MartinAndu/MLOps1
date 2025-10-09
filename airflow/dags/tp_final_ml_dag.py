# airflow/dags/tp_final_ml_dag.py
from datetime import datetime
from airflow.decorators import dag, task

# Asegura que /opt/airflow/src está en el path por si el env no lo toma
import sys
sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/src")

BASE_DIR = "/opt/airflow/data"  # carpeta compartida por volumen

@dag(
    dag_id="tp_final_ml_pipeline",
    schedule="@daily",           # o None si querés solo manual
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp-final", "ml"],
)
def pipeline():

    @task()
    def etl(base_dir: str) -> str:
        from src.etl import build_dataset
        # build_dataset guarda df.pkl en base_dir y ⬇️ devuelve base_dir (no el pkl)
        return build_dataset(base_dir)

    @task()
    def split(base_dir: str) -> str:
        from src.preprocess import split_dataset
        # split_dataset lee base_dir/df.pkl y escribe base_dir/processed/splits.joblib
        return split_dataset(base_dir)

    @task()
    def train_eval(base_dir: str) -> str:
        from src.train import train_and_evaluate
        # asegurate que train_and_evaluate lea processed/splits.joblib y guarde en data/models/
        return train_and_evaluate(base_dir)

    @task()
    def report(base_dir: str) -> str:
        from src.evaluate import export_metrics
        return export_metrics(base_dir)

    b = etl(BASE_DIR)
    s = split(b)
    t = train_eval(b)
    r = report(b)
    # Dependencias (si querés que report dependa de train):
    t >> r

dag = pipeline()
