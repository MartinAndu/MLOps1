"""
Módulo para la evaluación y exportación de métricas del modelo.

Este script se encarga de la etapa final del pipeline de evaluación, donde las
métricas generadas durante el entrenamiento y la validación se publican en una
ubicación designada para que otros servicios, como la API de predicción o los
dashboards de monitoreo, puedan consumirlas fácilmente.
"""


from __future__ import annotations

from pathlib import Path
import shutil

def export_metrics(base_dir: str) -> str:
    """
    Copia el archivo de métricas desde la carpeta de modelos a la de procesados.

    Esta función actúa como el paso final para "publicar" las métricas del
    modelo. Toma el archivo `metrics.json` generado por la tarea de entrenamiento
    (ubicado en `data/models/`) y lo copia a un directorio estable
    (`data/processed/`), haciéndolo accesible para su consumo externo.

    Args:
        base_dir: La ruta al directorio de datos base (ej. '/opt/airflow/data').

    Returns:
        La ruta completa al archivo de métricas recién copiado.

    Raises:
        FileNotFoundError: Si el archivo de métricas de origen
                           (`models/metrics.json`) no se encuentra.
    """
    base = Path(base_dir)
    models_metrics = base / "models" / "metrics.json"
    out_dir = base / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "metrics.json"

    if not models_metrics.exists():
        raise FileNotFoundError(
            f"[EVALUATE] No se encontró {models_metrics}. "
            f"Ejecute train_and_evaluate primero."
        )

    shutil.copyfile(models_metrics, out_file)
    print(f"[EVALUATE] Métricas copiadas a: {out_file}")
    return str(out_file)
