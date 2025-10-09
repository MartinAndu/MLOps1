from __future__ import annotations

from pathlib import Path
import shutil


def export_metrics(base_dir: str) -> str:
    """
    Copia las métricas finales a processed/ para facilitar su consumo por otras etapas
    o por la documentación del TP.

    Parameters
    ----------
    base_dir : str
        Carpeta raíz de datos compartida entre servicios (p.ej. /opt/airflow/data).

    Returns
    -------
    str
        Ruta del archivo de métricas final en processed/metrics.json
    """
    base = Path(base_dir)
    models_metrics = base / "models" / "metrics.json"
    out_dir = base / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "metrics.json"

    if not models_metrics.exists():
        raise FileNotFoundError(
            f"[EVALUATE] No se encontró {models_metrics}. "
            "Asegúrese de que train_and_evaluate haya corrido y generado las métricas."
        )

    shutil.copyfile(models_metrics, out_file)
    print(f"[EVALUATE] Métricas copiadas a: {out_file}")
    return str(out_file)
