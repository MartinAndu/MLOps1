from pathlib import Path
import pandas as pd
import numpy as np

"""
ETL exacto migrado desde la notebook 'Aprendizaje_de_maquina_Proyecto_Final.ipynb'.

Resumen del flujo (según la notebook):
1) Leer CSV crudos: productos.csv, sucursales.csv, comercio.csv (sep="|", utf-8, dtype=str).
2) Construir 'promociones' como subset de 'producto' donde existe precio promocional:
   promociones = producto[producto['productos_precio_unitario_promo1'].notnull()].copy()
3) Calcular 'descuento' como porcentaje:
   descuento = (productos_precio_lista - productos_precio_unitario_promo1) / productos_precio_lista * 100
   (se clippea a [0, 100] y se ignoran filas con lista<=0).
4) Armar 'sucur' con columnas relevantes y tipificar a string.
5) Armar 'comercio' con ['id_bandera','comercio_bandera_nombre'] y tipificar id_bandera a Int64.
6) Merge final:
   df = promociones.merge(sucur, how='left', on='id_sucursal')
   df = df.merge(comercio, how='left', on='id_bandera')
   (antes del primer merge, se hace drop de 'id_bandera' en 'promociones' para evitar duplicados).
7) Guardar df.pkl en la raíz de /opt/airflow/data.
Opcional: mapear 'sucursales_provincia' (ISO 3166-2 AR-*) a 'nombre_provincia_completo'.
"""

ISO_3166_2_AR = {
    'AR-C': 'Ciudad Autónoma de Buenos Aires',
    'AR-B': 'Buenos Aires',
    'AR-K': 'Catamarca',
    'AR-H': 'Chaco',
    'AR-U': 'Chubut',
    'AR-X': 'Córdoba',
    'AR-W': 'Corrientes',
    'AR-E': 'Entre Ríos',
    'AR-P': 'Formosa',
    'AR-Y': 'Jujuy',
    'AR-L': 'La Pampa',
    'AR-F': 'La Rioja',
    'AR-M': 'Mendoza',
    'AR-N': 'Misiones',
    'AR-Q': 'Neuquén',
    'AR-R': 'Río Negro',
    'AR-A': 'Salta',
    'AR-J': 'San Juan',
    'AR-D': 'San Luis',
    'AR-Z': 'Santa Cruz',
    'AR-S': 'Santa Fe',
    'AR-G': 'Santiago del Estero',
    'AR-V': 'Tierra del Fuego',
    'AR-T': 'Tucumán',
}

REQUIRED_PRODUCT_COLS = [
    'id_producto',
    'id_sucursal',
    'id_bandera',
    'productos_marca',
    'productos_precio_lista',
    'productos_precio_unitario_promo1',  # usada para calcular descuento
]

SUCUR_COLS = [
    'id_bandera', 'id_sucursal',
    'sucursales_tipo',
    'sucursales_codigo_postal',
    'sucursales_provincia',
]

COMERCIO_COLS = ['id_bandera', 'comercio_bandera_nombre']

def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep="|", encoding="utf-8", dtype=str)

def _to_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def build_dataset(base_dir: str) -> str:
    base = Path(base_dir)
    raw = base / "raw"
    out_pkl = base / "df.pkl"

    # 1) Leer CSVs
    prod_path = raw / "productos.csv"
    suc_path  = raw / "sucursales.csv"
    com_path  = raw / "comercio.csv"

    if not prod_path.exists():
        # Dataset vacío compatible para no romper pipeline si falta fuente
        df_empty = pd.DataFrame(columns=['id_bandera','productos_marca','productos_precio_lista','descuento'])
        df_empty.to_pickle(out_pkl)
        return str(out_pkl)

    producto = _read_csv(prod_path)

    # Asegurar columnas mínimas (si faltan, crearlas vacías)
    for c in REQUIRED_PRODUCT_COLS:
        if c not in producto.columns:
            producto[c] = pd.NA

    # 2) Subset de promociones (con precio promo1 no nulo)
    promociones = producto[producto['productos_precio_unitario_promo1'].notna()].copy()

    # Tipos numéricos seguros
    promociones['productos_precio_lista'] = _to_float(promociones['productos_precio_lista'])
    promociones['productos_precio_unitario_promo1'] = _to_float(promociones['productos_precio_unitario_promo1'])

    # Filtrar precios lista válidos
    promociones = promociones[promociones['productos_precio_lista'] > 0]

    # 3) Calcular descuento en porcentaje (0–100)
    desc = (promociones['productos_precio_lista'] - promociones['productos_precio_unitario_promo1']) / promociones['productos_precio_lista'] * 100.0
    promociones['descuento'] = desc.clip(lower=0.0, upper=100.0)

    # 4) SUCUR: seleccionar columnas y tipificar a string (como en la nb)
    if suc_path.exists():
        sucursales = _read_csv(suc_path)
        for c in SUCUR_COLS:
            if c not in sucursales.columns:
                sucursales[c] = pd.NA
        sucur = sucursales[SUCUR_COLS].copy()
        # cast a string dtype para columnas object, como se hizo en la nb
        for col, dtype in sucur.dtypes.items():
            if dtype == 'object':
                sucur[col] = sucur[col].astype('string')
    else:
        sucur = pd.DataFrame(columns=SUCUR_COLS)

    # 5) COMERCIO: seleccionar y tipificar id_bandera a Int64
    if com_path.exists():
        comercio = _read_csv(com_path)
        for c in COMERCIO_COLS:
            if c not in comercio.columns:
                comercio[c] = pd.NA
        comercio = comercio[COMERCIO_COLS].copy()
        comercio['id_bandera'] = pd.to_numeric(comercio['id_bandera'], errors='coerce').astype('Int64')
    else:
        comercio = pd.DataFrame(columns=COMERCIO_COLS)
        comercio['id_bandera'] = comercio.get('id_bandera', pd.Series(dtype='Int64'))

    # 6) Merge final: drop id_bandera desde promociones antes del primer merge (como en la nb)
    if 'id_bandera' in promociones.columns:
        promociones = promociones.drop(columns=['id_bandera'])

    # Merge por id_sucursal y luego por id_bandera
    df = promociones.merge(sucur, how='left', on='id_sucursal')
    df = df.merge(comercio, how='left', on='id_bandera')

    # 6b) Mapear provincias (opcional, si existe la columna)
    if 'sucursales_provincia' in df.columns:
        df['sucursales_provincia'] = df['sucursales_provincia'].astype('string').str.strip()
        df['nombre_provincia_completo'] = df['sucursales_provincia'].map(ISO_3166_2_AR)

    # 7) Selección final: mantener al menos las columnas usadas por el modelo y target
    keep = ['id_bandera', 'productos_marca', 'productos_precio_lista', 'descuento',
            'comercio_bandera_nombre', 'sucursales_tipo', 'sucursales_codigo_postal', 'sucursales_provincia',
            'nombre_provincia_completo']
    # Garantizar presencia de columnas
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA
    df = df[keep].copy()

    # Guardar dataset final
    df.to_pickle(out_pkl)
    return str(out_pkl)
