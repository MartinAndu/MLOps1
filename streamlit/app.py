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
    page_icon="🤖",
    layout="wide"
)

st.title("🤖 ML Pipeline Dashboard")

# Sidebar
with st.sidebar:
    st.header("Navegación")
    page = st.radio(
        "Selecciona una página",
        ["Predicciones", "Métricas del Modelo", "Experimentos MLflow"]
    )

# Página de Predicciones
if page == "Predicciones":
    st.header("📊 Realizar Predicciones")
    
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

# Página de Métricas del Modelo
elif page == "Métricas del Modelo":
    st.header("📈 Métricas del Modelo")
    
    try:
        # Obtener métricas de la API
        response = requests.get(f"{API_URL}/metrics")
        
        if response.status_code == 200:
            metrics = response.json()
            
            # Mostrar métricas en columnas
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Accuracy", f"{metrics.get('accuracy', 0):.4f}")
            with col2:
                st.metric("Precision", f"{metrics.get('precision', 0):.4f}")
            with col3:
                st.metric("Recall", f"{metrics.get('recall', 0):.4f}")
            
            # Si hay más métricas, mostrarlas en una tabla
            st.subheader("Todas las Métricas")
            df_metrics = pd.DataFrame([metrics])
            st.dataframe(df_metrics, use_container_width=True)
        else:
            st.warning("No se pudieron obtener las métricas")
    except Exception as e:
        st.error(f"Error al obtener métricas: {str(e)}")

# Página de Experimentos MLflow
elif page == "Experimentos MLflow":
    st.header("🔬 Experimentos MLflow")
    
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