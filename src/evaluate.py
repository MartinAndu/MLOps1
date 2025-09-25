from pathlib import Path
import json

def dump_report(metrics: dict, out_dir: str = "/opt/airflow/data/processed"):
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    (p / "metrics.json").write_text(json.dumps(metrics, indent=2))
