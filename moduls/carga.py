import pandas as pd
import geopandas as gpd
import streamlit as st
import pandas as pd
import requests
import json
from io import StringIO
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from utils.console_logger import log_loading_info, log_loading_warning, log_loading_error, log_debug 

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
                df = pd.read_csv(io.BytesIO(contenido), sep=',')
                fecha = datetime.datetime.now()
            else:
                df = pd.read_csv(contenido, sep=',')
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
        logs["warnings"].append(f"Error al obtener {file_name} de MinIO: {str(e)}")
        return None

def obtener_lista_archivos_minio(minio_client, bucket, logs=None):
    if logs is None:
        logs = {"warnings": [], "info": []}
    try:
        return [obj.object_name for obj in minio_client.list_objects(bucket)], logs
    except Exception as e:
        logs["warnings"].append(f"Error al listar archivos en MinIO: {str(e)}")
        return [], logs

def obtener_lista_archivos_gitlab(repo_id, branch, token, logs=None):
    """
    Obtiene una lista de archivos desde un repositorio GitLab.
    
    Args:
        repo_id (str): ID del repositorio en formato "namespace/project".
        branch (str): Rama del repositorio.
        token (str): Token de acceso a GitLab.
        logs (dict, optional): Diccionario para registrar logs. Defaults to None.
    
    Returns:
        tuple: (lista de archivos, logs)
    """
    if logs is None:
        logs = {"warnings": [], "info": []}
    
    if not token:
        logs["warnings"].append("Token de GitLab no proporcionado")
        return [], logs
    
    repo_id_encoded = requests.utils.quote(repo_id, safe='')
    url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/tree'
    headers = {'PRIVATE-TOKEN': token}
    params = {'ref': branch, 'recursive': True}
    
    logs["info"].append(f"Accediendo a GitLab API: {repo_id} (encoded)")
    log_loading_info(f"Accediendo a GitLab API: {repo_id}")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        logs["info"].append(f"Respuesta HTTP: {response.status_code}")
        log_loading_info(f"GitLab API respuesta: {response.status_code}")
        
        if response.status_code == 200:
            items = response.json()
            files = [item['path'] for item in items if item['type'] == 'blob']
            logs["info"].append(f"Se encontraron {len(files)} archivos en GitLab.")
            log_loading_info(f"Archivos encontrados en GitLab: {len(files)}")
            if files:
                logs["info"].append(f"Primeros archivos encontrados: {files[:5]}")
                log_debug("Primeros archivos encontrados", files[:5])
            return files, logs
        elif response.status_code == 404:
            logs["warnings"].append(f"Repositorio no encontrado: {repo_id} o branch '{branch}' no existe")
            log_loading_warning(f"Repositorio no encontrado: {repo_id}")
        elif response.status_code == 401:
            logs["warnings"].append(f"Token no válido o sin permisos para acceder al repositorio {repo_id}")
            log_loading_warning(f"Token no válido para repositorio {repo_id}")
        elif response.status_code == 403:
            logs["warnings"].append(f"Acceso denegado al repositorio {repo_id}")
            log_loading_warning(f"Acceso denegado al repositorio {repo_id}")
        else:
            logs["warnings"].append(f"Error HTTP {response.status_code}: {response.text[:200]}")
            log_loading_error(f"Error HTTP {response.status_code}", response.text[:200])
    except Exception as e:
        logs["warnings"].append(f"Error al obtener lista de archivos: {str(e)}")
        log_loading_error(f"Error al obtener lista de archivos: {str(e)}")
        
        try:
            url_fallback = f'https://gitlab.com/api/v4/projects/{repo_id}/repository/tree'
            logs["info"].append(f"Intentando fallback sin encoding...")
            response = requests.get(url_fallback, headers=headers, params=params)
            if response.status_code == 200:
                items = response.json()
                files = [item['path'] for item in items if item['type'] == 'blob']
                logs["info"].append(f"Fallback exitoso: {len(files)} archivos encontrados.")
                return files, logs
        except:
            pass
    
    try:
        logs["info"].append("Verificando proyectos accesibles con el token...")
        url = 'https://gitlab.com/api/v4/projects?membership=true&per_page=5'
        headers = {'PRIVATE-TOKEN': token}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            projects = response.json()
            if projects:
                projects_info = ", ".join([f"{p['id']}: {p['path_with_namespace']}" for p in projects[:3]])
                logs["info"].append(f"Proyectos disponibles con el token: {projects_info}")
            else:
                logs["warnings"].append("No se tiene acceso a ningún proyecto con este token")
        else:
            logs["warnings"].append(f"Error al verificar proyectos: {response.status_code}")
    except Exception as e:
        logs["warnings"].append(f"Error al listar proyectos: {str(e)}")
    
    return [], logs

def obtener_archivo_gitlab(repo_id, branch, file_name, token, logs=None):
    """
    Obtiene un archivo desde un repositorio GitLab.
    
    Args:
        repo_id (str): ID del repositorio en formato "namespace/project".
        branch (str): Rama del repositorio.
        file_name (str): Nombre del archivo.
        token (str): Token de acceso a GitLab.
        logs (dict, optional): Diccionario para registrar logs. Defaults to None.
    
    Returns:
        tuple: (contenido del archivo, logs)
    """
    if logs is None:
        logs = {"warnings": [], "info": []}
        
    if not token:
        logs["warnings"].append("Token de GitLab no proporcionado")
        return None, logs
        
    # Asegurar que el repo_id esté correctamente formateado
    repo_id_encoded = requests.utils.quote(str(repo_id), safe='')
    
    # Asegurar que el file_path esté correctamente formateado
    file_path_encoded = requests.utils.quote(file_name, safe='')
    
    url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/files/{file_path_encoded}/raw'
    headers = {'PRIVATE-TOKEN': token}
    params = {'ref': branch}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            logs["info"].append(f"Se obtuvo el archivo {file_name} de GitLab.")
            return response.content, logs
        else:
            logs["warnings"].append(f"Error al obtener archivo {file_name}: {response.status_code} - {response.text}")
            return None, logs
    except Exception as e:
        logs["warnings"].append(f"Error de conexión al obtener {file_name}: {str(e)}")
        return None, logs

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

def load_data_from_gitlab(repo_id, branch, token, modules):
    """
    Carga datos desde GitLab.
    
    Args:
        repo_id (str): ID del repositorio en formato "namespace/project".
        branch (str): Rama del repositorio.
        token (str): Token de acceso a GitLab.
        modules (dict): Diccionario con los módulos y sus archivos.
        
    Returns:
        tuple: (all_data, all_dates, logs) con los datos, fechas de actualización y logs.
    """
    all_data = {}
    all_dates = {}
    logs = {"warnings": [], "info": []}
    
    try:
        # Obtener lista de archivos disponibles en GitLab
        archivos_disponibles, logs = obtener_lista_archivos_gitlab(repo_id, branch, token, logs)
        
        if not archivos_disponibles:
            logs["warnings"].append(f"No se encontraron archivos disponibles en GitLab para el repositorio {repo_id}.")
            return all_data, all_dates, logs
            
        # Filtrar por extensiones soportadas
        extensiones = ['.parquet', '.csv', '.geojson', '.txt', '.xlsx']
        archivos_filtrados = [a for a in archivos_disponibles if any(a.endswith(ext) for ext in extensiones)]
        
        # Crear un conjunto de archivos solicitados por los módulos
        archivos_solicitados = set()
        for modulo, archivos in modules.items():
            for archivo in archivos:
                # Normalizar path para comparaciones
                archivo_normalizado = archivo.replace('\\', '/')
                archivos_solicitados.add(archivo_normalizado)
        
        # Procesar cada archivo de todos los módulos
        for modulo, archivos in modules.items():
            for archivo in archivos:
                # En GitLab, los paths pueden venir con estructura de directorios
                archivo_gitlab = archivo.replace('\\', '/')
                
                if archivo_gitlab in archivos_disponibles:
                    try:
                        # Obtener y procesar archivo
                        contenido, logs = obtener_archivo_gitlab(repo_id, branch, archivo_gitlab.replace('/', '%2F'), token, logs)
                        if contenido:
                            df, fecha = procesar_archivo(archivo, contenido, True, logs)
                            if df is not None:
                                all_data[archivo] = df
                                all_dates[archivo] = fecha
                                logs["info"].append(f"Cargado {archivo} correctamente desde GitLab.")
                            else:
                                logs["warnings"].append(f"Error al procesar {archivo} desde GitLab.")
                        else:
                            logs["warnings"].append(f"No se pudo obtener el contenido de {archivo} desde GitLab.")
                    except Exception as e:
                        logs["warnings"].append(f"Error al cargar {archivo} desde GitLab: {str(e)}")
                else:
                    # Buscar archivos con nombre similar (puede estar en otra ruta)
                    nombre_archivo = archivo.split('/')[-1]
                    archivos_similares = [a for a in archivos_disponibles if a.endswith('/' + nombre_archivo)]
                    
                    if archivos_similares:
                        archivo_candidato = archivos_similares[0]
                        try:
                            contenido, logs = obtener_archivo_gitlab(repo_id, branch, archivo_candidato.replace('/', '%2F'), token, logs)
                            if contenido:
                                df, fecha = procesar_archivo(archivo, contenido, True, logs)
                                if df is not None:
                                    all_data[archivo] = df
                                    all_dates[archivo] = fecha
                                    logs["info"].append(f"Cargado {archivo} (desde {archivo_candidato}) correctamente.")
                                else:
                                    logs["warnings"].append(f"Error al procesar {archivo_candidato} desde GitLab.")
                        except Exception as e:
                            logs["warnings"].append(f"Error al cargar {archivo_candidato}: {str(e)}")
                    else:
                        logs["warnings"].append(f"Archivo {archivo} no disponible en GitLab.")
                        
        # Resumen final
        logs["info"].append(f"Total archivos cargados desde GitLab: {len(all_data)}/{len(archivos_solicitados)}")
    except Exception as e:
        logs["warnings"].append(f"Error general en carga desde GitLab: {str(e)}")
    
    return all_data, all_dates, logs