"""
Dashboard interactivo para el monitoreo del pipeline de predicción de descuentos.

Esta aplicación, desarrollada con Streamlit, sirve como interfaz de usuario para
interactuar con el modelo de machine learning y visualizar los resultados de los
experimentos.

Funcionalidades Principales:
-   Página de 'Predicciones': Permite a los usuarios ingresar datos de un producto
    (ID de bandera, marca, precio) para obtener una predicción de descuento en
    tiempo real. Esta página se comunica con una API REST para realizar la
    predicción.
-   Página de 'Experimentos MLflow': Se conecta a un servidor de MLflow para
    visualizar los experimentos de entrenamiento. Muestra una lista de los
    experimentos y, al seleccionar uno, presenta los 'runs' asociados con
    sus métricas y parámetros, facilitando el seguimiento del rendimiento.

Configuración (Variables de Entorno):
-   API_URL: La dirección del endpoint de la API de predicción.
    (default: http://localhost:8000)
-   MLFLOW_TRACKING_URI: La dirección del servidor de tracking de MLflow.
    (default: http://localhost:5000)
"""
import streamlit as st
import requests
import pandas as pd
import os
import mlflow

# Configuración
API_URL = os.getenv("API_URL", "http://localhost:8000")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

# Configurar MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

st.set_page_config(
    page_title="ML Pipeline Dashboard",
    layout="wide"
)

st.title("Dashboard del modelo generado")

st.markdown("""
El objetivo de este proyecto es **predecir el porcentaje de descuento (descuento)** en productos de supermercado utilizando datos de **Precios Claros - Base SEPA** (Sistema Electrónico de Publicidad de Precios Argentinos).
""")
# Link a MLflow UI
st .markdown(f"Fuente de datos [SEPA](https://datos.gob.ar/dataset/produccion-precios-claros---base-sepa)")


# Sidebar
with st.sidebar:
    st.header("Navegación")
    page = st.radio(
        "Selecciona una página",
        ["Predicciones", "Experimentos MLflow"]
    )

# Página de Predicciones
if page == "Predicciones":
    st.header("Realizar Predicciones")
    
    st.subheader("Ingresa los datos para predicción")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        id_bandera = st.number_input("ID Bandera", value=0, step=1)
    
    with col2:
        productos_marca = st.text_input("Productos Marca", value="ALA")
    
    with col3:
        productos_precio_lista = st.number_input("Productos Precio Lista", value=0.0, step=0.01)
    
    if st.button("Predecir", type="primary"):
        try:
            # Crear payload para la API
            data = {
                "id_bandera": id_bandera,
                "productos_marca": productos_marca,
                "productos_precio_lista": productos_precio_lista
            }
            
            # Hacer request a la API
            response = requests.post(f"{API_URL}/predict", json=data)
            
            if response.status_code == 200:
                result = response.json()
                
                # Extraer el valor de descuento predicho
                descuento_pred = result.get('descuento_pred', result)
                
                st.success("✅ Predicción realizada con éxito")
                
                # Mostrar la predicción en un formato más visible
                st.metric("Descuento Predicho", f"{descuento_pred:.2f}%")
            else:
                st.error(f"Error: {response.status_code} - {response.text}")
        except Exception as e:
            st.error(f"Error al conectar con la API: {str(e)}")

# Página de Experimentos MLflow
elif page == "Experimentos MLflow":
    st.header("Experimentos MLflow")
    
    try:
        # Obtener experimentos de MLflow
        client = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
        experiments = client.search_experiments()
        
        if experiments:
            # Selector de experimento
            exp_names = [exp.name for exp in experiments]
            selected_exp = st.selectbox("Selecciona un experimento", exp_names)
            
            # Obtener el experimento seleccionado
            experiment = next(exp for exp in experiments if exp.name == selected_exp)
            
            # Obtener runs del experimento
            runs = client.search_runs(experiment_ids=[experiment.experiment_id])
            
            if runs:
                # Crear DataFrame con información de los runs
                runs_data = []
                for run in runs:
                    runs_data.append({
                        "Run ID": run.info.run_id[:8],
                        "Start Time": pd.to_datetime(run.info.start_time, unit='ms'),
                        "Status": run.info.status,
                        **run.data.metrics
                    })
                
                df_runs = pd.DataFrame(runs_data)
                st.dataframe(df_runs, use_container_width=True)
                
                # Mostrar detalles del último run
                if st.checkbox("Mostrar detalles del último run"):
                    latest_run = runs[0]
                    st.subheader("Parámetros")
                    st.json(latest_run.data.params)
                    
                    st.subheader("Métricas")
                    st.json(latest_run.data.metrics)
            else:
                st.info("No hay runs en este experimento")
        else:
            st.info("No hay experimentos disponibles")
            
        # Link a MLflow UI
        st.markdown(f"[Abrir MLflow UI]({MLFLOW_TRACKING_URI})")
        
    except Exception as e:
        st.error(f"Error al conectar con MLflow: {str(e)}")



# Footer
st.markdown("---")
st.markdown("Dashboard creado con Streamlit | Conectado a Airflow, MLflow y API")