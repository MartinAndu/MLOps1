from __future__ import annotations

from pathlib import Path
import shutil

def export_metrics(base_dir: str) -> str:
    """
    Copia las métricas finales a processed/ para su consumo por otras etapas.
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
