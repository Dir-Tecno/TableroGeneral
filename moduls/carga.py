import pandas as pd
import geopandas as gpd
import streamlit as st
import io
import datetime
import numpy as np
import requests
import os
from minio import Minio
import os 
# Importar utilidades de Sentry
from utils.sentry_utils import (
    capture_exception, 
    add_breadcrumb, 
    sentry_wrap, 
    sentry_context_manager
)

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
        add_breadcrumb(
            category="data_processing",
            message=f"Procesando archivo: {nombre}",
            data={"es_buffer": es_buffer}
        )
        
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
                # Para archivos .txt, usar encoding adecuado y separador de coma
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
        capture_exception(e, extra_data={
            "archivo": nombre,
            "es_buffer": es_buffer,
            "tipo": nombre.split('.')[-1] if '.' in nombre else "desconocido"
        })
        return None, None

@sentry_wrap(module_name="carga", operation="load_data_from_local")
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
    
    add_breadcrumb(
        category="data_loading",
        message=f"Cargando {total} archivos desde ruta local",
        data={"local_path": local_path}
    )
    
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
            capture_exception(e, extra_data={
                "archivo": nombre,
                "file_path": file_path,
                "index": i,
                "total": total
            })
    
    logs["info"].append(f"Archivos cargados desde local: {list(all_data.keys())}")
    return all_data, all_dates, logs

@sentry_wrap(module_name="carga", operation="load_data_from_minio")
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
        capture_exception(e, extra_data={"bucket": bucket})
        return all_data, all_dates, logs
        
    extensiones = ['.parquet', '.csv', '.geojson', '.txt', '.xlsx']
    archivos_filtrados = [a for a in archivos if any(a.endswith(ext) for ext in extensiones)]
    logs["info"].append(f"Archivos filtrados: {archivos_filtrados}")

    total = len(archivos_filtrados)
    add_breadcrumb(
        category="data_loading",
        message=f"Cargando {total} archivos desde MinIO",
        data={"bucket": bucket}
    )
    
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
            capture_exception(e, extra_data={
                "archivo": archivo,
                "bucket": bucket,
                "index": i,
                "total": total
            })
            continue
    
    logs["info"].append(f"Archivos cargados: {list(all_data.keys())}")
    return all_data, all_dates, logs


@sentry_wrap(module_name="carga", operation="load_data_from_gitlab")
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
        add_breadcrumb(
            category="data_loading",
            message=f"Obteniendo lista de archivos de GitLab",
            data={"repo_id": repo_id, "branch": branch}
        )
        
        archivos_disponibles, logs = obtener_lista_archivos_gitlab(repo_id, branch, token, logs)
        
        if not archivos_disponibles:
            logs["warnings"].append(f"No se encontraron archivos disponibles en GitLab para el repositorio {repo_id}.")
            capture_exception(extra_data={
                "error": "No se encontraron archivos disponibles",
                "repo_id": repo_id,
                "branch": branch
            })
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
        
        add_breadcrumb(
            category="data_loading",
            message=f"Procesando {len(archivos_solicitados)} archivos solicitados por módulos",
            data={"archivos_disponibles": len(archivos_disponibles)}
        )
        
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
                            # Obtener fecha real del commit
                            fecha_commit = obtener_fecha_commit_gitlab(repo_id, branch, archivo_gitlab, token, logs)
                            df, _ = procesar_archivo(archivo, contenido, True, logs)
                            if df is not None:
                                all_data[archivo] = df
                                all_dates[archivo] = fecha_commit or datetime.datetime.now()
                                logs["info"].append(f"Cargado {archivo} correctamente desde GitLab.")
                            else:
                                logs["warnings"].append(f"Error al procesar {archivo} desde GitLab.")
                        else:
                            logs["warnings"].append(f"No se pudo obtener el contenido de {archivo} desde GitLab.")
                    except Exception as e:
                        logs["warnings"].append(f"Error al cargar {archivo} desde GitLab: {str(e)}")
                        capture_exception(e, extra_data={
                            "archivo": archivo,
                            "archivo_gitlab": archivo_gitlab,
                            "modulo": modulo,
                            "repo_id": repo_id,
                            "branch": branch
                        })
                else:
                    # Buscar archivos con nombre similar (puede estar en otra ruta)
                    nombre_archivo = archivo.split('/')[-1]
                    archivos_similares = [a for a in archivos_disponibles if a.endswith('/' + nombre_archivo)]
                    
                    if archivos_similares:
                        archivo_candidato = archivos_similares[0]
                        try:
                            contenido, logs = obtener_archivo_gitlab(repo_id, branch, archivo_candidato.replace('/', '%2F'), token, logs)
                            if contenido:
                                # Obtener fecha real del commit
                                fecha_commit = obtener_fecha_commit_gitlab(repo_id, branch, archivo_candidato, token, logs)
                                df, _ = procesar_archivo(archivo, contenido, True, logs)
                                if df is not None:
                                    all_data[archivo] = df
                                    all_dates[archivo] = fecha_commit or datetime.datetime.now()
                                    logs["info"].append(f"Cargado {archivo} (desde {archivo_candidato}) correctamente.")
                                else:
                                    logs["warnings"].append(f"Error al procesar {archivo_candidato} desde GitLab.")
                        except Exception as e:
                            logs["warnings"].append(f"Error al cargar {archivo_candidato}: {str(e)}")
                            capture_exception(e, extra_data={
                                "archivo": archivo,
                                "archivo_candidato": archivo_candidato,
                                "modulo": modulo
                            })
                    else:
                        logs["warnings"].append(f"Archivo {archivo} no disponible en GitLab.")
                        
        # Resumen final
        logs["info"].append(f"Total archivos cargados desde GitLab: {len(all_data)}/{len(archivos_solicitados)}")
    except Exception as e:
        logs["warnings"].append(f"Error general en carga desde GitLab: {str(e)}")
        capture_exception(e, extra_data={
            "error": "Error general en carga desde GitLab",
            "repo_id": repo_id,
            "branch": branch
        })
    
    return all_data, all_dates, logs

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
    
    try:
        response = requests.get(url, headers=headers, params=params)
        logs["info"].append(f"Respuesta HTTP: {response.status_code}")
        
        if response.status_code == 200:
            items = response.json()
            files = [item['path'] for item in items if item['type'] == 'blob']
            logs["info"].append(f"Se encontraron {len(files)} archivos en GitLab.")
            if files:
                logs["info"].append(f"Primeros archivos encontrados: {files[:5]}")
            return files, logs
        elif response.status_code == 404:
            logs["warnings"].append(f"Repositorio no encontrado: {repo_id} o branch '{branch}' no existe")
        elif response.status_code == 401:
            logs["warnings"].append(f"Token no válido o sin permisos para acceder al repositorio {repo_id}")
        elif response.status_code == 403:
            logs["warnings"].append(f"Acceso denegado al repositorio {repo_id}")
        else:
            logs["warnings"].append(f"Error HTTP {response.status_code}: {response.text[:200]}")
    except Exception as e:
        logs["warnings"].append(f"Error al obtener lista de archivos: {str(e)}")
        
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

def obtener_fecha_commit_gitlab(repo_id, branch, file_name, token, logs=None):
    """
    Obtiene la fecha del último commit de un archivo específico en GitLab.
    
    Args:
        repo_id (str): ID del repositorio en formato "namespace/project".
        branch (str): Rama del repositorio.
        file_name (str): Nombre del archivo.
        token (str): Token de acceso a GitLab.
        logs (dict, optional): Diccionario para registrar logs.
    
    Returns:
        datetime: Fecha del último commit o None si no se puede obtener.
    """
    if logs is None:
        logs = {"warnings": [], "info": []}
        
    if not token:
        return None
        
    try:
        # Asegurar que el repo_id esté correctamente formateado
        repo_id_encoded = requests.utils.quote(str(repo_id), safe='')
        
        # Obtener commits para el archivo específico
        url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/commits'
        headers = {'PRIVATE-TOKEN': token}
        params = {'ref': branch, 'path': file_name, 'per_page': 1}
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            commits = response.json()
            if commits:
                commit_date = commits[0]['committed_date']
                # Convertir la fecha ISO a datetime
                fecha_commit = datetime.datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
                logs["info"].append(f"Fecha de commit obtenida para {file_name}: {fecha_commit}")
                return fecha_commit
        else:
            logs["warnings"].append(f"No se pudo obtener fecha de commit para {file_name}: {response.status_code}")
            return None
    except Exception as e:
        logs["warnings"].append(f"Error al obtener fecha de commit para {file_name}: {str(e)}")
        return None

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
    
    # Configurar timeout y reintentos para archivos grandes
    max_retries = 3
    timeout = 60  # 60 segundos de timeout
    
    for intento in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout, stream=True)
            if response.status_code == 200:
                # Leer el contenido en chunks para archivos grandes
                content = b''
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        content += chunk
                logs["info"].append(f"Se obtuvo el archivo {file_name} de GitLab (intento {intento + 1}).")
                return content, logs
            else:
                logs["warnings"].append(f"Error al obtener archivo {file_name}: {response.status_code} - {response.text[:200]}")
                return None, logs
        except requests.exceptions.Timeout:
            logs["warnings"].append(f"Timeout al obtener {file_name} (intento {intento + 1}/{max_retries})")
            if intento == max_retries - 1:
                return None, logs
        except requests.exceptions.ConnectionError as e:
            if "Response ended prematurely" in str(e) or "Connection broken" in str(e):
                logs["warnings"].append(f"Conexión interrumpida al obtener {file_name} (intento {intento + 1}/{max_retries})")
                if intento == max_retries - 1:
                    return None, logs
            else:
                logs["warnings"].append(f"Error de conexión al obtener {file_name}: {str(e)}")
                return None, logs
        except Exception as e:
            logs["warnings"].append(f"Error inesperado al obtener {file_name}: {str(e)}")
            return None, logs
    
    return None, logs

# Función para carga granular de archivos con su propio cacheo independiente
@st.cache_data(ttl=3600)
def load_single_file_from_source(source_type, source_params, archivo):
    """
    Carga un único archivo con su propio cacheo independiente
    
    Args:
        source_type (str): 'gitlab', 'minio' o 'local'
        source_params (dict): Parámetros específicos de la fuente (ej: repo_id, branch, token)
        archivo (str): Ruta al archivo
        
    Returns:
        tuple: (dataframe, fecha, logs)
    """
    logs = {"warnings": [], "info": []}
    
    try:
        if source_type == 'gitlab':
            repo_id = source_params.get('repo_id')
            branch = source_params.get('branch') 
            token = source_params.get('token')
            
            contenido, logs = obtener_archivo_gitlab(repo_id, branch, archivo.replace('/', '%2F'), token, logs)
            if contenido:
                fecha_commit = obtener_fecha_commit_gitlab(repo_id, branch, archivo, token, logs)
                df, _ = procesar_archivo(archivo, contenido, True, logs)
                if df is not None:
                    return df, fecha_commit or datetime.datetime.now(), logs
                    
        elif source_type == 'local':
            local_path = source_params.get('local_path')
            file_path = os.path.join(local_path, archivo)
            
            if os.path.exists(file_path):
                df, fecha = procesar_archivo(archivo, file_path, es_buffer=False, logs=logs)
                if df is not None:
                    return df, fecha, logs
                    
        elif source_type == 'minio':
            minio_client = source_params.get('minio_client')
            bucket = source_params.get('bucket')
            
            try:
                response = minio_client.get_object(bucket, archivo)
                contenido = response.read()
                response.close()
                response.release_conn()
                df, fecha = procesar_archivo(archivo, contenido, es_buffer=True, logs=logs)
                if df is not None:
                    return df, fecha, logs
            except Exception as e:
                logs["warnings"].append(f"Error al obtener {archivo} de MinIO: {str(e)}")
                
    except Exception as e:
        logs["warnings"].append(f"Error al cargar {archivo}: {str(e)}")
    
    return None, None, logs