"""
Módulo ETL para la construcción del dataset de predicción de descuentos.

Este script maneja el proceso completo de Extracción, Transformación y Carga:
1.  Extracción (Extract):
    - Verifica la existencia de archivos CSV en el directorio 'data/raw'.
    - Si no existen, descarga automáticamente los datos desde una carpeta
      pública de Google Drive utilizando `gdown`.
    - Mapea los nombres de archivo descargados (ej. 'producto_....csv') a
      nombres estandarizados ('productos.csv', 'sucursales.csv', etc.).

2.  Transformación (Transform):
    - Lee los archivos CSV de forma robusta, probando múltiples separadores
      y codificaciones.
    - Calcula la columna objetivo `descuento` a partir de los precios de lista
      y los precios promocionales.
    - Combina (merge) los datos de productos, sucursales y comercios.
    - Realiza limpieza de datos y mapea códigos de provincia a nombres completos.

3.  Carga (Load):
    - Guarda el DataFrame final y limpio en formato pickle (`data/df.pkl`),
      listo para ser utilizado en las siguientes etapas del pipeline de ML.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import re
import time

# --- CONFIG ---
DRIVE_FOLDER_ID = "1lG8QrERAQPhw-iBLtAvkQGS5dZl-V_px"  # carpeta compartida
EXPECTED_FILES = {
    "productos": re.compile(r"producto", re.I),
    "sucursales": re.compile(r"sucursal", re.I),
    "comercio": re.compile(r"comercio", re.I),
}

ISO_3166_2_AR = {
    "AR-C": "Ciudad Autónoma de Buenos Aires",
    "AR-B": "Buenos Aires",
    "AR-K": "Catamarca",
    "AR-H": "Chaco",
    "AR-U": "Chubut",
    "AR-X": "Córdoba",
    "AR-W": "Corrientes",
    "AR-E": "Entre Ríos",
    "AR-P": "Formosa",
    "AR-Y": "Jujuy",
    "AR-L": "La Pampa",
    "AR-F": "La Rioja",
    "AR-M": "Mendoza",
    "AR-N": "Misiones",
    "AR-Q": "Neuquén",
    "AR-R": "Río Negro",
    "AR-A": "Salta",
    "AR-J": "San Juan",
    "AR-D": "San Luis",
    "AR-Z": "Santa Cruz",
    "AR-S": "Santa Fe",
    "AR-G": "Santiago del Estero",
    "AR-V": "Tierra del Fuego",
    "AR-T": "Tucumán",
}

KEEP_COLS = [
    "id_bandera",
    "productos_marca",
    "productos_precio_lista",
    "descuento",
    "comercio_bandera_nombre",
    "sucursales_tipo",
    "sucursales_codigo_postal",
    "sucursales_provincia",
    "nombre_provincia_completo",
]

REQUIRED_PRODUCT_COLS = [
    "id_producto",
    "id_sucursal",
    "id_bandera",
    "productos_marca",
    "productos_precio_lista",
]

PROMO_PRICE_CANDIDATES = [
    "productos_precio_unitario_promo1",
    "precio_promo",
    "precio_promocional",
    "precio_unitario_promocion",
]

SUCUR_COLS = [
    "id_bandera",
    "id_sucursal",
    "sucursales_tipo",
    "sucursales_codigo_postal",
    "sucursales_provincia",
]

COMERCIO_COLS = ["id_bandera", "comercio_bandera_nombre"]


def _read_csv_robust(path: Path) -> pd.DataFrame:
    """
    Lee un archivo CSV de forma robusta probando diferentes separadores y codificaciones.

    Itera sobre una lista predefinida de configuraciones comunes (separadores
    '|', ',', ';' y codificaciones 'utf-8', 'latin-1') para encontrar una
    que permita leer el archivo correctamente.

    Args:
        path: La ruta al archivo CSV que se va a leer.

    Returns:
        Un DataFrame de Pandas con los datos del archivo.

    Raises:
        RuntimeError: Si no se puede leer el archivo con ninguna de las
                      configuraciones probadas.
    """
    
    trials = [
        dict(sep="|", encoding="utf-8"),
        dict(sep=",", encoding="utf-8"),
        dict(sep=";", encoding="utf-8"),
        dict(sep=",", encoding="latin-1"),
        dict(sep=";", encoding="latin-1"),
    ]
    last_err = None
    for opts in trials:
        try:
            print(f"[ETL] Leyendo {path.name} con {opts}")
            return pd.read_csv(path, dtype=str, **opts)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No se pudo leer {path}. Último error: {last_err}")


def _to_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _pick_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Busca en una lista de nombres de columnas y devuelve la primera que exista.

    Esta función es útil cuando un campo puede tener diferentes nombres en
    distintas versiones de un dataset (ej. 'precio_promo', 'precio_promocional').
    Itera sobre `candidates` y retorna el primer nombre que se encuentre en
    las columnas del DataFrame.

    Args:
        df: El DataFrame en el que se buscarán las columnas.
        candidates: Una lista de nombres de columnas candidatas, en orden
                    de prioridad.

    Returns:
        El nombre de la primera columna candidata encontrada, o None si ninguna
        de las candidatas existe en el DataFrame.
    """
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _download_drive_folder(folder_id: str, dest_dir: Path, retries: int = 3) -> None:
    """
    Descarga el contenido de una carpeta pública de Google Drive de forma robusta.

    Utiliza la biblioteca `gdown` para descargar todos los archivos de la carpeta
    especificada. La descarga se realiza en un directorio temporal y luego los
    archivos CSV encontrados se mueven al directorio de destino final.
    Implementa un mecanismo de reintentos con espera exponencial para manejar
    errores transitorios de red.

    Args:
        folder_id: El ID de la carpeta de Google Drive a descargar.
        dest_dir: La ruta del directorio de destino donde se guardarán los
                  archivos CSV.
        retries: El número máximo de intentos de descarga en caso de error.

    Raises:
        RuntimeError: Si la descarga falla después de todos los reintentos.
    """
    import gdown  # instalado por airflow/requirements.txt

    tmp = dest_dir.parent / (dest_dir.name + "_tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)

    url = f"https://drive.google.com/drive/folders/{folder_id}"
    err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[ETL] Descargando carpeta de Drive (intento {attempt}) → {url}")
            gdown.download_folder(url=url, output=str(tmp), quiet=False, use_cookies=False)
            err = None
            break
        except Exception as e:
            err = e
            time.sleep(2 * attempt)
    if err:
        raise RuntimeError(f"No se pudo descargar la carpeta de Drive: {err}")

    # mover/renombrar CSVs encontrados a dest_dir
    dest_dir.mkdir(parents=True, exist_ok=True)
    found = list(tmp.rglob("*.csv"))
    if not found:
        print("[ETL][WARN] No se encontraron CSV en la carpeta descargada.")
    for p in found:
        target = dest_dir / p.name
        # si ya existe, sobreescribimos
        shutil.move(str(p), str(target))

    # limpiar tmp
    shutil.rmtree(tmp, ignore_errors=True)


def _ensure_raw_from_drive(raw: Path) -> None:
    """
    Asegura que los archivos CSV sin procesar estén disponibles y estandarizados.

    Verifica si existen archivos CSV en el directorio `raw`. Si no, los descarga
    desde Google Drive. Luego, estandariza los nombres de los archivos encontrados
    (ej. 'producto_2023.csv' -> 'productos.csv') para que las etapas posteriores
    puedan encontrarlos de manera predecible.

    Args:
        raw: La ruta al directorio de datos sin procesar (ej. 'data/raw').

    Raises:
        FileNotFoundError: Si no se encuentran archivos CSV ni localmente ni
                           después de intentar la descarga desde Drive.
    """
    need_download = not any(raw.glob("*.csv"))
    if need_download:
        _download_drive_folder(DRIVE_FOLDER_ID, raw)

    # Intentar mapear nombres: producto*, sucursal*, comercio*
    files = list(raw.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No hay CSV en data/raw/ ni en la carpeta de Drive descargada.")

    def pick(regex: re.Pattern):
        for f in files:
            if regex.search(f.name):
                return f
        return None

    mapping = {
        "productos.csv": pick(EXPECTED_FILES["productos"]),
        "sucursales.csv": pick(EXPECTED_FILES["sucursales"]),
        "comercio.csv": pick(EXPECTED_FILES["comercio"]),
    }

    for std_name, found_path in mapping.items():
        if found_path and found_path.name != std_name:
            target = raw / std_name
            # si ya existe, lo reemplazamos por el detectado
            if target.exists():
                target.unlink()
            found_path.rename(target)
            print(f"[ETL] Mapeado {found_path.name} → {std_name}")


def build_dataset(base_dir: str) -> str:
    """
    Orquesta el proceso ETL completo para construir y guardar el dataset final.

    Esta es la función principal del módulo. Ejecuta la secuencia completa de
    pasos de ETL:
    1.  Asegura la disponibilidad de los datos de origen (`_ensure_raw_from_drive`).
    2.  Lee los archivos CSV de productos, sucursales y comercios.
    3.  Calcula la columna objetivo `descuento`.
    4.  Combina los DataFrames en un único dataset.
    5.  Realiza transformaciones finales (ej. mapeo de provincias).
    6.  Guarda el DataFrame procesado como un archivo pickle (`df.pkl`).

    Args:
        base_dir: La ruta al directorio de datos base (ej. '/opt/airflow/data'),
                  que contiene la subcarpeta 'raw'.

    Returns:
        La misma ruta del directorio base, para permitir el encadenamiento de
        tareas en un pipeline como Airflow.
    """
    base = Path(base_dir)
    raw = base / "raw"
    out_pkl = base / "df.pkl"

    raw.mkdir(parents=True, exist_ok=True)
    _ensure_raw_from_drive(raw)

    prod_path = raw / "productos.csv"
    suc_path = raw / "sucursales.csv"
    com_path = raw / "comercio.csv"

    if not prod_path.exists():
        print("[ETL][WARN] No se encontró productos.csv tras la descarga. Generando df vacío.")
        df_empty = pd.DataFrame(
            {k: pd.Series(dtype="float64") if k == "descuento" else pd.Series(dtype="object") for k in KEEP_COLS}
        )
        df_empty.to_pickle(out_pkl)
        return str(out_pkl)

    # 1) Productos
    producto = _read_csv_robust(prod_path)
    for c in REQUIRED_PRODUCT_COLS:
        if c not in producto.columns:
            print(f"[ETL][WARN] Falta columna en productos: {c}. Se creará vacía.")
            producto[c] = pd.NA

    promo_col = _pick_existing_column(producto, PROMO_PRICE_CANDIDATES)
    if promo_col is None:
        print("[ETL][WARN] No se encontró columna de precio promocional. 'descuento' quedará NA.")

    producto["productos_precio_lista"] = _to_float(producto["productos_precio_lista"])
    if promo_col:
        producto[promo_col] = _to_float(producto[promo_col])

    if promo_col:
        promociones = producto[producto[promo_col].notna()].copy()
    else:
        promociones = producto.copy()
        promociones["descuento"] = np.nan

    promociones = promociones[promociones["productos_precio_lista"] > 0]

    if promo_col:
        desc = (
                (promociones["productos_precio_lista"] - promociones[promo_col])
                / promociones["productos_precio_lista"]
                * 100.0
        )
        promociones["descuento"] = desc.clip(lower=0.0, upper=100.0)
    else:
        promociones["descuento"] = np.nan

    # 4) SUCUR
    if suc_path.exists():
        suc = _read_csv_robust(suc_path)
        for c in SUCUR_COLS:
            if c not in suc.columns:
                suc[c] = pd.NA
        sucur = suc[SUCUR_COLS].copy()
        for col, dtype in sucur.dtypes.items():
            if dtype == "object":
                sucur[col] = sucur[col].astype("string")
    else:
        sucur = pd.DataFrame(columns=SUCUR_COLS)

    # 5) COMERCIO
    if com_path.exists():
        com = _read_csv_robust(com_path)
        for c in COMERCIO_COLS:
            if c not in com.columns:
                com[c] = pd.NA
        comercio = com[COMERCIO_COLS].copy()
        comercio["id_bandera"] = pd.to_numeric(comercio["id_bandera"], errors="coerce").astype("Int64")
    else:
        comercio = pd.DataFrame(columns=COMERCIO_COLS)
        comercio["id_bandera"] = comercio.get("id_bandera", pd.Series(dtype="Int64"))

    for col in ["id_sucursal", "id_bandera", "productos_marca", "productos_precio_lista"]:
        if col not in promociones.columns:
            print(f"[ETL][WARN] Falta {col} en promociones. Se rellena con NA.")
            promociones[col] = pd.NA

    if "id_bandera" in promociones.columns:
        promociones = promociones.drop(columns=["id_bandera"], errors="ignore")

    df = promociones.merge(sucur, how="left", on="id_sucursal")
    df = df.merge(comercio, how="left", on="id_bandera")

    if "sucursales_provincia" in df.columns:
        df["sucursales_provincia"] = df["sucursales_provincia"].astype("string").str.strip()
        df["nombre_provincia_completo"] = df["sucursales_provincia"].map(ISO_3166_2_AR)
    else:
        df["nombre_provincia_completo"] = pd.NA

    for k in KEEP_COLS:
        if k not in df.columns:
            df[k] = pd.NA
    df = df[KEEP_COLS].copy()

    out_pkl.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(out_pkl)
    print(f"[ETL] Dataset final guardado en {out_pkl} con shape={df.shape}")
    return str(base_dir)


if __name__ == "__main__":
    print(build_dataset("/opt/airflow/data"))
