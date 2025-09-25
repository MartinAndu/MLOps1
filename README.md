# TP Final ML + Airflow + FastAPI

Pipeline de ML entrenado desde **Apache Airflow** (Docker) y servicio de predicción con **FastAPI**.

## Estructura clave
- `airflow/`: imagen + DAG `tp_final_ml_pipeline`
- `src/`: código de carga, preprocesamiento, entrenamiento, evaluación y batch predict
- `data/`: volúmenes compartidos (raw, processed, models, predictions)
- `api/`: servicio FastAPI que carga `data/models/model.joblib` y expone `/predict`
- `reference_notebook/`: tu notebook original solo para consulta

## Datos requeridos
Colocá **una** de estas opciones:
1. `data/df.pkl` con columnas: `id_bandera`, `productos_marca`, `productos_precio_lista`, `descuento`
2. O los CSV crudos en `data/raw/` (`comercio.csv`, `productos.csv`, `sucursales.csv`) y adaptá merges en `src/data_utils.py` (ya hay fallback simple).

## Levantar el entorno
```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

- Airflow UI: http://localhost:8080 (user: `admin`, pass: `admin`)
- API FastAPI: http://localhost:8000

## Ejecutar el pipeline
1. Entrá a Airflow, activá el DAG **`tp_final_ml_pipeline`** y ejecutalo.
2. El modelo entrenado se guarda en `data/models/model.joblib` y métricas en `data/models/metrics.json`.

## Probar la API
```bash
curl -X POST http://localhost:8000/predict   -H "Content-Type: application/json"   -d '{
    "id_bandera": 12,
    "productos_marca": "ALA",
    "productos_precio_lista": 5999.0
  }'
```
Respuesta esperada:
```json
{"descuento_pred": 0.1234}
```

## Notas
- Ajustá `FEAT_COLS` y listas `CATEGORICAL/NUMERIC` si cambiás features.
- Evitamos XCom pesados guardando datasets intermedios en `/opt/airflow/data/processed/`.
- Si tenés problemas de permisos en `data/` o `airflow/logs/`: 
  ```bash
  sudo chown -R 50000:0 airflow/logs data
  ```
