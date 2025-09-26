# Trabajo Práctico Final de Aprendizaje de Máquina
**Pipeline de Machine Learning con Apache Airflow, MLflow y FastAPI**

Este repositorio contiene la migración del Trabajo Práctico Final de Aprendizaje de Máquina a un ambiente modular y productivo.  
Incluye un pipeline de datos y entrenamiento orquestado con **Apache Airflow**, un servicio de predicción mediante **FastAPI**, y experimentos de búsqueda de hiperparámetros registrados en **MLflow**.  
Todo se despliega con **Docker Compose**.

---

## Estructura del repositorio

```
.
├── airflow/                     # Configuración e imagen de Airflow
│   ├── dags/
│   │   └── tp_final_ml_dag.py   # DAG principal (etl → split → train_eval → report)
│   ├── dockerfiles/
│   │   └── airflow.Dockerfile   # Imagen extendida de Airflow
│   ├── logs/                    # Logs de Airflow
│   ├── plugins/                 # Plugins opcionales
│   └── requirements.txt         # Dependencias adicionales de Airflow
│
├── api/                         # Servicio FastAPI para predicción
│   ├── app.py                   # API con endpoint POST /predict
│   ├── requirements.txt         # Dependencias del servicio API
│   └── Dockerfile               # Imagen de FastAPI
│
├── src/                         # Código fuente del pipeline
│   ├── etl.py                   # ETL exacto migrado desde la notebook
│   ├── data_utils.py            # Utilidades de carga de df.pkl y features
│   ├── preprocess.py            # Split train/test
│   ├── train.py                 # Entrenamiento y logging en MLflow
│   ├── evaluate.py              # Reporte de métricas
│   └── predict_batch.py         # Predicción batch opcional
│
├── data/                        # Carpeta compartida entre servicios
│   ├── raw/                     # CSV crudos (entrada: productos.csv, sucursales.csv, comercio.csv)
│   ├── processed/               # Datos procesados (splits, métricas)
│   ├── models/                  # Modelos entrenados y métricas finales
│   └── predictions/             # Predicciones batch
│
├── mlruns/                      # Experimentos registrados en MLflow
│
├── reference_notebook/          # Notebook original como referencia
│   └── Aprendizaje_de_maquina_Proyecto_Final.ipynb
│
├── docker-compose.yml           # Orquestación de Airflow, API y MLflow
├── .env                         # Variables de entorno (UID, MLFLOW_TRACKING_URI, etc.)
└── README.md                    # Documentación del proyecto
```

---

## Flujo de trabajo (Airflow)

1. **ETL (`etl.py`)**:
    - Lee los CSV crudos desde `data/raw/`.
    - Construye `df.pkl` replicando las transformaciones de la notebook:
        - Filtrado de promociones.
        - Cálculo de `descuento`.
        - Preparación de `sucursales` y `comercio`.
        - Merge final y mapeo de provincias.

2. **Split (`preprocess.py`)**:
    - Genera `data/processed/splits.joblib` con train/test.

3. **Entrenamiento y evaluación (`train.py`)**:
    - Entrena el modelo con GridSearchCV.
    - Registra parámetros, métricas y artefactos en **MLflow**.
    - Persiste el modelo en `data/models/model.joblib` y métricas en `data/models/metrics.json`.

4. **Reporte (`evaluate.py`)**:
    - Copia métricas a `data/processed/metrics.json`.

---

## Despliegue

1. Construir imágenes:
   ```bash
   docker compose build
   ```

2. Inicializar Airflow:
   ```bash
   docker compose up airflow-init
   ```

3. Levantar todos los servicios:
   ```bash
   docker compose up -d
   ```

---


## Uso con Makefile

Para simplificar el ciclo de vida de los contenedores, este proyecto incluye un `Makefile` con los siguientes comandos:

```bash
# Construye las imágenes de Airflow (sin usar caché)
make build

# Inicializa la base de datos de Airflow y crea usuario admin/admin
make init

# Levanta todos los servicios en segundo plano
make up

# Reinicia solo los contenedores de Airflow (webserver y scheduler)
make restart



## Servicios y puertos

- **Airflow Webserver:** [http://localhost:8080](http://localhost:8080)  
  Usuario: `admin` — Contraseña: `admin`

- **MLflow Tracking UI:** [http://localhost:5001](http://localhost:5001)

- **FastAPI (servicio de predicción):** [http://localhost:8000](http://localhost:8000)

---

## Modificar algo de forma local

Despues de corregir correr

   ```bash
   docker compose restart airflow-webserver airflow-scheduler 
   ```


## Ejecución del pipeline

1. Ingresar a la interfaz de Airflow.
2. Activar el DAG `tp_final_ml_pipeline`.
3. Ejecutarlo manualmente o esperar la ejecución programada.

Los artefactos generados incluyen:
- `data/df.pkl` (dataset procesado)
- `data/models/model.joblib` (modelo entrenado)
- `data/models/metrics.json` (métricas del modelo)
- Registro completo en **MLflow** (`./mlruns` + UI en puerto 5001)

---

## Uso de la API

La API expone un endpoint `POST /predict` que recibe los atributos principales y devuelve la predicción de descuento.

### Ejemplo

```bash
curl -X POST http://localhost:8000/predict   -H "Content-Type: application/json"   -d '{
    "id_bandera": 12,
    "productos_marca": "ALA",
    "productos_precio_lista": 5999.0
  }'
```

Respuesta esperada:

```json
{
  "descuento_pred": 15.23
}
```

---

## Notas adicionales

- La lógica del ETL replica fielmente la notebook entregada.
- El modelo se entrena y evalúa dentro de un Pipeline de `scikit-learn` para garantizar consistencia entre entrenamiento y predicción.
- Si los CSV no están presentes en `data/raw/`, el ETL genera un dataset vacío para que el DAG no falle.
- Para entornos productivos se recomienda asegurar persistencia de `mlruns/` en un almacenamiento remoto y configurar seguridad en Airflow y FastAPI.