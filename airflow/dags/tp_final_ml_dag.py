"""
DAG de Pipeline de ML para Predicción de Descuentos

Este DAG orquesta el pipeline de machine learning de principio a fin para predecir 
el porcentaje de descuento en productos de supermercado.

Flujo del Pipeline:
1.  ETL: Extrae los datos crudos, los procesa y construye un dataset limpio 
    (guardado como `data/df.pkl`).
2.  Split: Divide el dataset limpio en conjuntos de entrenamiento y prueba.
3.  Train & Evaluate: Entrena un modelo de machine learning con el conjunto de 
    entrenamiento, lo evalúa y guarda tanto el modelo como sus métricas.
4.  Report: Genera y exporta un reporte final con las métricas de evaluación 
    del modelo.

Dependencias entre tareas:
- `etl` -> `split`
- `etl` -> `train_eval` -> `report`

El DAG está configurado para ejecutarse diariamente.
"""


from datetime import datetime
from airflow.decorators import dag, task

# Fallback por si el PYTHONPATH no entra por compose
import sys
sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/src")

BASE_DIR = "/opt/airflow/data"

@dag(
    dag_id="tp_final_ml_pipeline",
    schedule="@daily",    # usar None si solo querés manual
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp-final", "ml"],
)
def pipeline():

    @task()
    def etl(base_dir: str) -> str:
        from src.etl import build_dataset
        # build_dataset guarda df.pkl y devuelve base_dir (no el pkl)
        return build_dataset(base_dir)

    @task()
    def split(base_dir: str) -> str:
        from src.preprocess import split_dataset
        return split_dataset(base_dir)

    @task()
    def train_eval(base_dir: str) -> str:
        from src.train import train_and_evaluate
        return train_and_evaluate(base_dir)

    @task()
    def report(base_dir: str) -> str:
        from src.evaluate import export_metrics
        return export_metrics(base_dir)

    b = etl(BASE_DIR)
    s = split(b)
    t = train_eval(b)
    r = report(b)
    t >> r

dag = pipeline()
