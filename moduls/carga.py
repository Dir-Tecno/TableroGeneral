import pandas as pd
import geopandas as gpd
import streamlit as st
import io
import datetime
import numpy as np
from minio import Minio
import os 

def convert_numpy_types(df):
    if df is None or df.empty:
        return df

    def convert_value(val):
        if isinstance(val, np.integer):
            return int(val)
        elif isinstance(val, np.floating):
            return float(val)
        elif isinstance(val, np.ndarray):
            return val.tolist()
        elif isinstance(val, np.bool_):
            return bool(val)
        else:
            return val

    for col in df.columns:
        if df[col].dtype.kind in 'iufc':
            df[col] = df[col].apply(convert_value)
    return df

class ParquetLoader:
    @staticmethod
    def load(buffer):
        try:
            df, error = safe_read_parquet(io.BytesIO(buffer), is_buffer=True)
            if df is not None:
                df = convert_numpy_types(df)
                return df
            else:
                st.warning(f"Error al cargar archivo: {error}")
                return None
        except Exception as e:
            st.warning(f"Error al cargar archivo: {str(e)}")
            return None

def safe_read_parquet(file_path_or_buffer, is_buffer=False):
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa

        if is_buffer:
            table = pq.read_table(file_path_or_buffer)
        else:
            table = pq.read_table(file_path_or_buffer)

        try:
            df = table.to_pandas()
        except pa.ArrowInvalid as e:
            if "out of bounds timestamp" in str(e):
                df = table.to_pandas(timestamp_as_object=True)
            else:
                raise
    except (ImportError, Exception):
        try:
            if is_buffer:
                df = pd.read_parquet(file_path_or_buffer, timestamp_as_object=True)
            else:
                df = pd.read_parquet(file_path_or_buffer, timestamp_as_object=True)
        except TypeError:
            if is_buffer:
                df = pd.read_parquet(file_path_or_buffer)
            else:
                df = pd.read_parquet(file_path_or_buffer)
        except Exception as e:
            if "out of bounds timestamp" in str(e):
                if is_buffer:
                    df = pd.read_parquet(file_path_or_buffer, engine='python')
                else:
                    df = pd.read_parquet(file_path_or_buffer, engine='python')
            else:
                raise

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                df[col] = df[col].astype(str)
    return df, None

def procesar_archivo(nombre, contenido, es_buffer):
    try:
        if nombre.endswith('.parquet'):
            if es_buffer:
                df = ParquetLoader.load(contenido)
                fecha = datetime.datetime.now()
            else:
                df, error = safe_read_parquet(contenido)
                fecha = datetime.datetime.now()
            return df, fecha
        elif nombre.endswith('.xlsx'):
            if es_buffer:
                df = pd.read_excel(io.BytesIO(contenido), engine='openpyxl', header=header)
                fecha = datetime.datetime.now()
            else:
                df = pd.read_excel(contenido, engine='openpyxl')
                fecha = datetime.datetime.now()
            return df, fecha
        elif nombre.endswith('.csv') or nombre.endswith('.txt'):
            if es_buffer:
                df = pd.read_csv(io.BytesIO(contenido), sep=';')
                fecha = datetime.datetime.now()
            else:
                df = pd.read_csv(contenido, sep=';')
                fecha = datetime.datetime.now()
            return df, fecha
        elif nombre.endswith('.geojson'):
            if es_buffer:
                gdf = gpd.read_file(io.BytesIO(contenido))
                fecha = datetime.datetime.now()
            else:
                gdf = gpd.read_file(contenido)
                fecha = datetime.datetime.now()
            return gdf, fecha
        else:
            return None, None
    except Exception as e:
        st.warning(f"Error al procesar {nombre}: {str(e)}")
        return None, None

# --- NUEVA FUNCIÓN PARA CARGA LOCAL ---
def load_data_from_local(local_path, modules):
    all_data = {}
    all_dates = {}

    if not os.path.exists(local_path):
        st.error(f"Error Crítico: La ruta para desarrollo local no existe -> {local_path}")
        return all_data, all_dates

    st.info(f"Cargando archivos desde la carpeta local: {local_path}")

    all_files_to_load = [file for files in modules.values() for file in files]
    progress = st.progress(0)
    total = len(all_files_to_load)

    for i, archivo in enumerate(all_files_to_load):
        progress.progress((i + 1) / total)
        file_path = os.path.join(local_path, archivo)
        if os.path.exists(file_path):
            df, fecha = procesar_archivo(archivo, file_path, es_buffer=False)
            if df is not None:
                all_data[archivo] = df
                all_dates[archivo] = fecha
        else:
            st.warning(f"Archivo no encontrado en la ruta local: {file_path}")
            
    progress.empty()
    st.write("Archivos cargados localmente:", list(all_data.keys()))
    return all_data, all_dates


def obtener_archivo_minio(minio_client, bucket, file_name):
    try:
        response = minio_client.get_object(bucket, file_name)
        content = response.read()
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        st.error(f"Error al obtener {file_name} de MinIO: {str(e)}")
        return None

def obtener_lista_archivos_minio(minio_client, bucket):
    try:
        archivos = [obj.object_name for obj in minio_client.list_objects(bucket, recursive=True)]
        st.write(f"Archivos encontrados en MinIO ({bucket}):", archivos)  # Línea de depuración
        return archivos
    except Exception as e:
        st.error(f"Error al listar archivos en MinIO: {str(e)}")
        return []

def load_data_from_minio(minio_client, bucket, modules):
    all_data = {}
    all_dates = {}
    archivos = obtener_lista_archivos_minio(minio_client, bucket)
    extensiones = ['.parquet', '.csv', '.geojson', '.txt', '.xlsx']
    archivos_filtrados = [a for a in archivos if any(a.endswith(ext) for ext in extensiones)]
    st.write("Archivos filtrados:", archivos_filtrados)  # Línea de depuración

    progress = st.progress(0)
    total = len(archivos_filtrados)
    for i, archivo in enumerate(archivos_filtrados):
        progress.progress((i + 1) / total)
        contenido = obtener_archivo_minio(minio_client, bucket, archivo)
        if contenido is None:
            continue
        nombre = archivo.split('/')[-1]
        df, fecha = procesar_archivo(nombre, contenido, es_buffer=True)
        if df is not None:
            all_data[nombre] = df
            all_dates[nombre] = fecha
    progress.empty()
    st.write("Archivos cargados:", list(all_data.keys()))  # Línea de depuración
    return all_data, all_dates  # <-- AGREGA ESTA LÍNEA