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
├── streamlit/                   # Dashboard interactivo para generar las predicciones
│   ├── app.py                   # Aplicacion que genera el dashboard interactivo
│   ├── requirements.txt         # Dependencias del modulo
│   └── Dockerfile               # Imagen de Streamlit
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

## Descripción del modelo

El modelo implementado en este trabajo utiliza datos de **productos, sucursales y comercios** para predecir el **porcentaje de descuento aplicado a un producto en promoción**.

A partir de información como la marca del producto, el precio de lista, la sucursal y el tipo de comercio, el modelo estima cuánto representa el descuento sobre el precio final.  
Esta predicción permite analizar patrones de promociones y entender cómo varían los descuentos según diferentes características del producto y del punto de venta.

---

## 🚀 Cómo levantar el proyecto correctamente

### 1️⃣ Requisitos previos

- **Docker** y **Docker Compose** instalados.
- Archivo `.env` en la raíz con:
  ```bash
  MLFLOW_TRACKING_URI=http://mlflow:5000
  ```

---

### 2️⃣ Pasos para levantar los servicios

**Requisito**: tener instalado **Docker Desktop**

Ejecutar en el directorio raíz del proyecto:

```bash
# 1. Apagar cualquier instancia previa y eliminar volúmenes
docker compose down -v

# 2. Reconstruir las imágenes
docker compose build

# 3. Inicializar la base de datos y el usuario admin de Airflow
docker compose up airflow-init
# Esperar a que termine con "exit code 0" (puede tardar unos segundos)

# 4. Levantar todos los servicios
docker compose up -d
```

Una vez levantado, verificar con:
```bash
docker compose ps
```
Deberías ver `airflow-webserver`, `airflow-scheduler`, `mlflow`, `api` y `postgres` en estado “Up”.

---

### ⚠️ Si aparece el error
> `ERROR: You need to initialize the database. Please run airflow db init`

Entonces ejecutar nuevamente los pasos de inicialización completa:

```bash
# 1. Apagar todo y eliminar volúmenes viejos
docker compose down -v

# 2. Eliminar el volumen pgdata si quedó colgado
docker volume ls | grep pgdata
docker volume rm <nombre-del-volumen>

# 3. Volver a construir e inicializar
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

Esto garantiza que Airflow inicialice correctamente su base de datos antes de levantar el scheduler y el webserver.  
En macOS es común que quede un volumen previo de Postgres; estos pasos lo resuelven.

---

## Uso con Makefile

Para simplificar los comandos:

```bash
# Construir imágenes sin caché
make build

# Inicializar base de datos de Airflow
make init

# Levantar todos los servicios
make start

# Reiniciar solo los contenedores de Airflow
make restart
```

---

## Servicios y puertos

| Servicio | URL | Descripción |
|-----------|-----|-------------|
| **Airflow Webserver** | [http://localhost:8080](http://localhost:8080) | Orquestador de tareas (DAGs) |
| **MLflow Tracking UI** | [http://localhost:5001](http://localhost:5001) | Registro de experimentos |
| **FastAPI (servicio de predicción)** | [http://localhost:8000](http://localhost:8000) | API REST del modelo |
| **Streamlit** | [http://localhost:8501](http://localhost:8501) | Dashboard interactivo |
| **PostgreSQL** | Interno | Base de datos del orquestador |

**Credenciales de Airflow:**  
Usuario: `admin`  
Contraseña: `admin`

---

## Ejecución del pipeline

1. Ingresar a [http://localhost:8080](http://localhost:8080).
2. Activar el DAG `tp_final_ml_pipeline`.
3. Ejecutarlo manualmente (Trigger DAG).
4. Verificar en **MLflow UI** las métricas y artefactos generados.
5. Consumir la api mediante la interfaz disponibilizada en [http://localhost:8501](http://localhost:8501).

**Artefactos esperados:**
- `data/df.pkl` – dataset procesado.
- `data/processed/splits.joblib` – particiones de entrenamiento/prueba.
- `data/models/model.joblib` – modelo entrenado.
- `data/models/metrics.json` – métricas del modelo.
- `data/processed/metrics.json` – métricas copiadas para reporte.

---

## Uso de la API

La API expone un endpoint `POST /predict` que recibe los atributos principales y devuelve la predicción de descuento:

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

## Notas técnicas

- El ETL replica la notebook original con la misma lógica de transformaciones.
- Los datos crudos se pueden obtener desde el Drive compartido o copiar a `data/raw/`.
- Airflow coordina la ejecución de todas las etapas: `etl → split → train_eval → report`.
- Si MLflow no está disponible, el sistema registra localmente los experimentos en `/opt/airflow/mlruns`.

---

## 👥 Integrantes

- **a2110 – Ceballos, Luciano**
- **a2102 – Andújar, Martín Rodrigo**