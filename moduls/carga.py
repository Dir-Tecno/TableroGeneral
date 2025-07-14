import pandas as pd
import geopandas as gpd
import streamlit as st
import io
import datetime
import numpy as np
from minio import Minio

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
                return None
        except Exception as e:
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

def procesar_archivo(nombre, contenido, es_buffer, logs=None):
    if logs is None:
        logs = {"warnings": [], "info": []}
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
                df = pd.read_excel(io.BytesIO(contenido), engine='openpyxl')
                fecha = datetime.datetime.now()
            else:
                df = pd.read_excel(contenido, engine='openpyxl')
                fecha = datetime.datetime.now()
            return df, fecha
        elif nombre.endswith('.csv') or nombre.endswith('.txt'):
            if es_buffer:
                df = pd.read_csv(io.BytesIO(contenido))
                fecha = datetime.datetime.now()
            else:
                df = pd.read_csv(contenido)
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
        logs["warnings"].append(f"Error al procesar {nombre}: {str(e)}")
        return None, None

def obtener_archivo_minio(minio_client, bucket, file_name, logs=None):
    if logs is None:
        logs = {"warnings": [], "info": []}
    try:
        response = minio_client.get_object(bucket, file_name)
        content = response.read()
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        logs["warnings"].append(f"Error al obtener {file_name} de MinIO: {str(e)}")
        return None

def obtener_lista_archivos_minio(minio_client, bucket, logs=None):
    if logs is None:
        logs = {"warnings": [], "info": []}
    try:
        archivos = [obj.object_name for obj in minio_client.list_objects(bucket, recursive=True)]
        logs["info"].append(f"Archivos encontrados en MinIO ({bucket}): {archivos}")
        return archivos
    except Exception as e:
        logs["warnings"].append(f"Error al listar archivos en MinIO: {str(e)}")
        return []

def load_data_from_local(local_path, modules):
    """
    Carga datos desde la ruta local en modo desarrollo.
    
    Args:
        local_path (str): Ruta local donde se encuentran los archivos.
        modules (dict): Diccionario con los módulos y sus archivos.
        
    Returns:
        tuple: (all_data, all_dates, logs) con los datos, fechas de actualización y logs.
    """
    import os
    from pathlib import Path
    
    all_data = {}
    all_dates = {}
    logs = {"warnings": [], "info": []}
    
    # Obtener lista de todos los archivos necesarios para todos los módulos
    all_files = []
    for module_files in modules.values():
        all_files.extend(module_files)
    all_files = list(set(all_files))  # Eliminar duplicados
    
    total = len(all_files)
    
    for i, nombre in enumerate(all_files):
        file_path = os.path.join(local_path, nombre)
        
        if not os.path.exists(file_path):
            logs["warnings"].append(f"Archivo no encontrado en ruta local: {file_path}")
            continue
            
        try:
            df, fecha = procesar_archivo(nombre, file_path, es_buffer=False, logs=logs)
            if df is not None:
                all_data[nombre] = df
                all_dates[nombre] = fecha
        except Exception as e:
            logs["warnings"].append(f"Error al cargar archivo local {nombre}: {str(e)}")
    
    logs["info"].append(f"Archivos cargados desde local: {list(all_data.keys())}")
    return all_data, all_dates, logs

def load_data_from_minio(minio_client, bucket, modules):
    """
    Carga datos desde MinIO en modo producción.
    
    Args:
        minio_client: Cliente de MinIO.
        bucket (str): Nombre del bucket.
        modules (dict): Diccionario con los módulos y sus archivos.
        
    Returns:
        tuple: (all_data, all_dates, logs) con los datos, fechas de actualización y logs.
    """
    all_data = {}
    all_dates = {}
    logs = {"warnings": [], "info": []}
    
    try:
        archivos = [obj.object_name for obj in minio_client.list_objects(bucket, recursive=True)]
    except Exception as e:
        logs["warnings"].append(f"Error al listar archivos en MinIO: {str(e)}")
        return all_data, all_dates, logs
        
    extensiones = ['.parquet', '.csv', '.geojson', '.txt', '.xlsx']
    archivos_filtrados = [a for a in archivos if any(a.endswith(ext) for ext in extensiones)]
    logs["info"].append(f"Archivos filtrados: {archivos_filtrados}")

    total = len(archivos_filtrados)
    for i, archivo in enumerate(archivos_filtrados):
        try:
            response = minio_client.get_object(bucket, archivo)
            contenido = response.read()
            response.close()
            response.release_conn()
            nombre = archivo.split('/')[-1]
            df, fecha = procesar_archivo(nombre, contenido, es_buffer=True, logs=logs)
            if df is not None:
                all_data[nombre] = df
                all_dates[nombre] = fecha
        except Exception as e:
            logs["warnings"].append(f"Error al obtener {archivo} de MinIO: {str(e)}")
            continue
    
    logs["info"].append(f"Archivos cargados: {list(all_data.keys())}")
    return all_data, all_dates, logs