# Trabajo Práctico Final de Aprendizaje de Máquina  
**Pipeline de Machine Learning con Apache Airflow y FastAPI**

Este repositorio contiene la implementación del Trabajo Práctico Final de Aprendizaje de Máquina.  
El proyecto integra un pipeline de Machine Learning orquestado con **Apache Airflow** y expone un servicio de predicción a través de **FastAPI**, todo desplegado en contenedores mediante **Docker Compose**.

---

## Estructura del repositorio

```
.
├── airflow/                 # Configuración e imagen de Airflow
│   ├── dags/                # Definición de DAGs
│   │   └── tp_final_ml_dag.py
│   ├── dockerfiles/         # Dockerfile extendido de Airflow
│   ├── logs/                # Volumen de logs
│   ├── plugins/             # Plugins opcionales
│   └── requirements.txt     # Dependencias adicionales de Python
│
├── api/                     # Servicio FastAPI para predicciones
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── src/                     # Código fuente del pipeline
│   ├── data_utils.py        # Carga de datos
│   ├── preprocess.py        # Preprocesamiento y división en train/test
│   ├── train.py             # Entrenamiento y evaluación del modelo
│   ├── evaluate.py          # Reporte de métricas
│   └── predict_batch.py     # Predicciones batch
│
├── data/                    # Datos y artefactos
│   ├── raw/                 # Datos de entrada
│   ├── processed/           # Datos intermedios
│   ├── models/              # Modelos entrenados y métricas
│   └── predictions/         # Resultados de predicciones batch
│
├── reference_notebook/      # Notebook original de referencia
│   └── Aprendizaje_Maquina.ipynb
│
├── docker-compose.yml       # Orquestación de servicios
├── .env                     # Variables de entorno
└── README.md                # Documentación del proyecto
```

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/)  
- [Docker Compose](https://docs.docker.com/compose/install/)  

---

## Preparación de los datos

El pipeline espera **una de las siguientes opciones** en el directorio `data/`:

1. Un archivo **`df.pkl`** que contenga las columnas:  
   - `id_bandera`  
   - `productos_marca`  
   - `productos_precio_lista`  
   - `descuento`  

2. Los archivos crudos en `data/raw/`:  
   - `comercio.csv`  
   - `productos.csv`  
   - `sucursales.csv`  

El script `src/data_utils.py` intentará cargar `df.pkl` en primer lugar. Si no está disponible, construirá un dataset mínimo a partir de los CSV.

---

## Instrucciones de despliegue

1. Construir las imágenes:
   ```bash
   docker compose build
   ```

2. Inicializar la base de datos y crear el usuario administrador:
   ```bash
   docker compose up airflow-init
   ```

3. Levantar todos los servicios en segundo plano:
   ```bash
   docker compose up -d
   ```

---

## Acceso a los servicios

- **Airflow Webserver:** [http://localhost:8080](http://localhost:8080)  
  Usuario: `admin`  
  Contraseña: `admin`

- **API de predicción (FastAPI):** [http://localhost:8000](http://localhost:8000)  

---

## Ejecución del pipeline

1. Ingresar a la interfaz de Airflow.  
2. Activar el DAG **`tp_final_ml_pipeline`**.  
3. Ejecutar el DAG de manera manual o esperar a la programación diaria (`@daily`).  

El modelo entrenado se almacenará en:  
- `data/models/model.joblib`  
- Métricas en `data/models/metrics.json`  

---

## Uso de la API de predicción

La API carga el modelo entrenado y expone un endpoint `POST /predict`.

### Ejemplo de solicitud:

```bash
curl -X POST http://localhost:8000/predict   -H "Content-Type: application/json"   -d '{
    "id_bandera": 12,
    "productos_marca": "ALA",
    "productos_precio_lista": 5999.0
  }'
```

### Respuesta esperada:

```json
{
  "descuento_pred": 0.1234
}
```

---

## Notas adicionales

- Si se cambian las características utilizadas, deben actualizarse en `src/data_utils.py` y `src/train.py` (listas `FEAT_COLS`, `CATEGORICAL`, `NUMERIC`).  
- Los datasets intermedios se guardan en `/opt/airflow/data/processed/` para evitar transferencias grandes vía XCom.  
- En caso de problemas de permisos en `airflow/logs` o `data/`, ejecutar:
  ```bash
  sudo chown -R 50000:0 airflow/logs data
  ```
- El proyecto está diseñado para entornos de desarrollo. Para entornos productivos se recomienda revisar la documentación oficial de Airflow y FastAPI, así como implementar medidas adicionales de seguridad y monitoreo.

---
