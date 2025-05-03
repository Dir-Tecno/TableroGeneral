import pandas as pd
import geopandas as gpd
import requests
import streamlit as st
import io
import time
import datetime
import numpy as np
import subprocess
import sys
import os
import glob

def obtener_archivo_gitlab(repo_id, file_path, branch='main', token=None):
    """Obtiene un archivo de GitLab"""
    if not token:
        st.error("Token de GitLab no proporcionado")
        return None
        
    # Asegurar que el repo_id esté correctamente formateado
    repo_id_encoded = requests.utils.quote(str(repo_id), safe='')
    
    # Asegurar que el file_path esté correctamente formateado
    file_path_encoded = requests.utils.quote(file_path, safe='')
    
    url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/files/{file_path_encoded}/raw'
    headers = {'PRIVATE-TOKEN': token}
    params = {'ref': branch}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.content
        else:
            st.error(f"Error al obtener archivo {file_path}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        st.error(f"Error de conexión: {str(e)}")
        return None

def obtener_lista_archivos(repo_id, branch='main', token=None):
    """Obtiene la lista de archivos del repositorio"""
    if not token:
        st.error("Token de GitLab no proporcionado")
        return []
    
    # Probar diferentes formatos de ID
    formatos_id = [
        repo_id,
        requests.utils.quote(repo_id, safe=''),
        repo_id.replace('/', '%2F')
    ]
    
    for id_formato in formatos_id:
        url = f'https://gitlab.com/api/v4/projects/{id_formato}/repository/tree'
        headers = {'PRIVATE-TOKEN': token}
        params = {'ref': branch, 'recursive': True}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                items = response.json()
                archivos = [item['path'] for item in items if item['type'] == 'blob']
                return archivos
        except Exception as e:
            st.error(f"Error al obtener lista de archivos: {str(e)}")
            continue
    
    # Intentar listar proyectos disponibles para ayudar al diagnóstico
    try:
        st.info("Verificando proyectos accesibles con tu token...")
        url = 'https://gitlab.com/api/v4/projects?membership=true&per_page=5'
        headers = {'PRIVATE-TOKEN': token}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            projects = response.json()
            if projects:
                st.info("Proyectos disponibles con tu token:")
                for p in projects:
                    st.info(f"ID: {p['id']}, Path: {p['path_with_namespace']}")
            else:
                st.error("No tienes acceso a ningún proyecto con este token")
        else:
            st.error(f"Error al verificar proyectos: {response.status_code}")
    except Exception as e:
        st.error(f"Error al listar proyectos: {str(e)}")
    
    return []

def convert_numpy_types(df):
    """
    Convierte tipos de datos numpy a tipos Python nativos en un DataFrame.
    Esto ayuda a evitar errores como 'Python value of type numpy.int64 not supported'.

    Args:
        df: DataFrame a convertir

    Returns:
        DataFrame con tipos de datos Python nativos
    """
    if df is None or df.empty:
        return df

    # Función para convertir valores numpy a Python nativos
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

    # Aplicar la conversión a todo el DataFrame
    for col in df.columns:
        if df[col].dtype.kind in 'iufc':  # integers, unsigned integers, floats, complex
            df[col] = df[col].apply(convert_value)

    return df

class DataLoaderGitLab:
    def __init__(self, token):
        self.token = token
        
    def _make_request(self, url, params):
        # Lógica de reintentos y manejo de errores
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.content
            else:
                st.error(f"Error al obtener archivo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            st.error(f"Error de conexión: {str(e)}")
            return None

    def obtener_archivo_gitlab(self, repo_id, file_path, branch='main'):
        # Asegurar que el repo_id esté correctamente formateado
        repo_id_encoded = requests.utils.quote(str(repo_id), safe='')
        
        # Asegurar que el file_path esté correctamente formateado
        file_path_encoded = requests.utils.quote(file_path, safe='')
        
        url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/files/{file_path_encoded}/raw'
        headers = {'PRIVATE-TOKEN': self.token}
        params = {'ref': branch}
        
        return self._make_request(url, params)

    def obtener_lista_archivos(self, repo_id, branch='main'):
        # Probar diferentes formatos de ID
        formatos_id = [
            repo_id,
            requests.utils.quote(repo_id, safe=''),
            repo_id.replace('/', '%2F')
        ]
        
        for id_formato in formatos_id:
            url = f'https://gitlab.com/api/v4/projects/{id_formato}/repository/tree'
            headers = {'PRIVATE-TOKEN': self.token}
            params = {'ref': branch, 'recursive': True}
            
            response = self._make_request(url, params)
            if response is not None:
                items = response.json()
                archivos = [item['path'] for item in items if item['type'] == 'blob']
                return archivos
        
        return []

class ParquetLoader:
    @staticmethod
    def load(buffer):
        try:
            df, error = safe_read_parquet(io.BytesIO(buffer), is_buffer=True)
            if df is not None:
                # Convertir tipos numpy
                df = convert_numpy_types(df)
                return df
            else:
                st.warning(f"Error al cargar archivo: {error}")
                return None
        except Exception as e:
            st.warning(f"Error al cargar archivo: {str(e)}")
            return None

def safe_read_parquet(file_path_or_buffer, is_buffer=False):
    """
    Lee un archivo parquet de manera segura, manejando diferentes errores comunes.
    
    Args:
        file_path_or_buffer: Ruta al archivo o buffer de bytes
        is_buffer: Si es True, file_path_or_buffer es un buffer de bytes
        
    Returns:
        Tupla (DataFrame de pandas, mensaje de error). Si no hay error, el mensaje es None.
    """
    try:
        # Intentar primero con pyarrow si está disponible
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa
            
            if is_buffer:
                table = pq.read_table(file_path_or_buffer)
            else:
                table = pq.read_table(file_path_or_buffer)
            
            # Convertir a pandas con manejo de errores para timestamps
            try:
                df = table.to_pandas()
            except pa.ArrowInvalid as e:
                if "out of bounds timestamp" in str(e):
                    # Si hay error de timestamp fuera de rango, usar opción para ignorar conversión
                    df = table.to_pandas(timestamp_as_object=True)
                else:
                    raise
        except (ImportError, Exception) as e:
            # Si pyarrow no está disponible o falla, intentar con pandas directamente
            try:
                # Intentar con timestamp_as_object
                if is_buffer:
                    df = pd.read_parquet(file_path_or_buffer, timestamp_as_object=True)
                else:
                    df = pd.read_parquet(file_path_or_buffer, timestamp_as_object=True)
            except TypeError:
                # Si falla por el parámetro timestamp_as_object, intentar sin él
                if is_buffer:
                    df = pd.read_parquet(file_path_or_buffer)
                else:
                    df = pd.read_parquet(file_path_or_buffer)
            except Exception as e:
                if "out of bounds timestamp" in str(e):
                    # Para errores de timestamp fuera de rango en pandas, usar opción engine='python'
                    if is_buffer:
                        df = pd.read_parquet(file_path_or_buffer, engine='python')
                    else:
                        df = pd.read_parquet(file_path_or_buffer, engine='python')
                else:
                    raise
        
        # Procesar columnas de fecha para manejar casos extremos
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Convertir fechas extremas a objetos datetime de Python
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    # Si falla, convertir a strings
                    df[col] = df[col].astype(str)
        
        return df, None  # Devolver el DataFrame y None como mensaje de error
    except Exception as e:
        return None, str(e)  # Devolver None como DataFrame y el mensaje de error

def procesar_archivo(nombre, contenido, es_buffer):
    """
    Procesa un archivo (local o buffer) y devuelve el DataFrame y la fecha de modificación.
    nombre: nombre de archivo (ej: data.csv)
    contenido: ruta local (si es_buffer=False) o buffer de bytes (si es_buffer=True)
    es_buffer: True si es buffer (GitLab), False si es ruta local
    """
    import datetime
    import pandas as pd
    import geopandas as gpd
    import io
    import os

    try:
        # Parquet
        if nombre.endswith('.parquet'):
            if es_buffer:
                df = ParquetLoader.load(contenido)
                fecha = datetime.datetime.now()
            else:
                df, error = safe_read_parquet(contenido)
                fecha = datetime.datetime.fromtimestamp(os.path.getmtime(contenido))
            return df, fecha
        # CSV o TXT
        elif nombre.endswith('.csv') or nombre.endswith('.txt'):
            if es_buffer:
                df = pd.read_csv(io.BytesIO(contenido))
                fecha = datetime.datetime.now()
            else:
                df = pd.read_csv(contenido)
                fecha = datetime.datetime.fromtimestamp(os.path.getmtime(contenido))

            return df, fecha
        # GeoJSON
        elif nombre.endswith('.geojson'):
            if es_buffer:
                gdf = gpd.read_file(io.BytesIO(contenido))
                fecha = datetime.datetime.now()
            else:
                gdf = gpd.read_file(contenido)
                fecha = datetime.datetime.fromtimestamp(os.path.getmtime(contenido))
            return gdf, fecha
        else:
            return None, None
    except Exception as e:
        st.warning(f"Error al procesar {nombre}: {str(e)}")
        return None, None

def load_data_from_gitlab(repo_id, branch='main', token=None, use_local=False, local_path=None):
    """
    Carga todos los archivos del repositorio o desde una carpeta local
    
    Args:
        repo_id: ID del repositorio en GitLab
        branch: Rama del repositorio
        token: Token de GitLab
        use_local: Si es True, carga desde la carpeta local en vez de GitLab
        local_path: Ruta a la carpeta local con los archivos (solo si use_local=True)
    """
    try:
        # Diccionarios para datos y fechas
        all_data = {}
        all_dates = {}
        
        # Determinar si usar la carpeta local o GitLab
        if use_local and local_path and os.path.exists(local_path):
            st.info(f"Cargando datos desde carpeta local: {local_path}")
            
            # Buscar archivos con extensiones soportadas en la carpeta local
            extensiones = ['.parquet', '.csv', '.geojson', '.txt']
            archivos_filtrados = []
            
            for ext in extensiones:
                # Buscar archivos con la extensión actual en la carpeta y subcarpetas
                archivos_ext = glob.glob(os.path.join(local_path, f"**/*{ext}"), recursive=True)
                archivos_filtrados.extend(archivos_ext)
            
            # Barra de progreso
            progress = st.progress(0)
            total = len(archivos_filtrados)
            
            # Cargar cada archivo
            for i, archivo_path in enumerate(archivos_filtrados):
                try:
                    # Actualizar progreso
                    progress.progress((i + 1) / total)
                    
                    # Extraer nombre de archivo relativo a la carpeta base
                    nombre = os.path.basename(archivo_path)
                    df, fecha = procesar_archivo(nombre, archivo_path, es_buffer=False)
                    if df is not None:
                        all_data[nombre] = df
                        all_dates[nombre] = fecha

                    
                    elif archivo_path.endswith('.geojson'):
                        try:
                            gdf = gpd.read_file(archivo_path)
                            all_data[nombre] = gdf
                            all_dates[nombre] = datetime.datetime.fromtimestamp(os.path.getmtime(archivo_path))
                        except Exception as e:
                            st.warning(f"Error al cargar {nombre}: {str(e)}")
                
                except Exception as e:
                    st.error(f"Error al procesar {archivo_path}: {str(e)}")
            
            # Ocultar la barra de progreso
            progress.empty()
            
            return all_data, all_dates
        
        else:
            # Cargar desde GitLab usando funciones independientes
            archivos = obtener_lista_archivos(repo_id, branch, token)
            
            if not archivos:
                return {}, {}
            
            # Filtrar por extensiones soportadas
            extensiones = ['.parquet', '.csv', '.geojson', '.txt']
            archivos_filtrados = [a for a in archivos if any(a.endswith(ext) for ext in extensiones)]
            
            # Barra de progreso
            progress = st.progress(0)
            total = len(archivos_filtrados)
            
            # Cargar cada archivo
            for i, archivo in enumerate(archivos_filtrados):
                try:
                    # Actualizar progreso
                    progress.progress((i + 1) / total)
                    
                    # Obtener contenido
                    contenido = obtener_archivo_gitlab(repo_id, archivo, branch, token)
                    if contenido is None:
                        continue
                    
                    # Extraer nombre de archivo
                    nombre = archivo.split('/')[-1]
                    df, fecha = procesar_archivo(nombre, contenido, es_buffer=True)
                    if df is not None:
                        all_data[nombre] = df
                        all_dates[nombre] = fecha

                
                except Exception as e:
                    st.error(f"Error al procesar {archivo}: {str(e)}")
            
            # Ocultar la barra de progreso
            progress.empty()
            
            return all_data, all_dates
    
    except Exception as e:
        st.error(f"Error al cargar datos: {str(e)}")
        return {}, {}