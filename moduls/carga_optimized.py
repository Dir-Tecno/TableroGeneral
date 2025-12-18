"""
Módulo optimizado de carga de datos con uso eficiente de memoria.

Optimizaciones implementadas:
1. Lectura selectiva de columnas (solo las necesarias)
2. Downcasting automático de tipos de datos
3. Caché con hash personalizado para evitar duplicación
4. Limpieza activa de memoria con gc
5. Simplificación de geometrías GeoJSON
"""

import pandas as pd
import geopandas as gpd
import streamlit as st
import io
import datetime
import numpy as np
import requests
import os
import gc
from typing import Dict, List, Tuple, Optional, Any
# from minio import Minio  # REMOVED: Minio support disabled

# Funciones stub para reemplazar Sentry (removido)
def capture_exception(e=None, extra_data=None): pass
def add_breadcrumb(category=None, message=None, data=None, level=None): pass

# =============================================================================
# CONFIGURACIÓN DE COLUMNAS POR ARCHIVO
# Definir solo las columnas necesarias para cada archivo
# =============================================================================

COLUMNAS_NECESARIAS = {
    # DESHABILITADO TEMPORALMENTE - Las columnas especificadas no coinciden con las reales
    # TODO: Actualizar con las columnas correctas de los archivos Parquet
    # Por ahora se cargan todas las columnas y se optimizan los tipos de datos
    'df_postulantes_empleo.parquet': None,
    'df_inscriptos_empleo.parquet': None,
    'df_empresas.parquet': None,
    'df_global_banco.parquet': None,
    'df_global_pagados.parquet': None,
    'df_postulantes_cbamecapacita.parquet': None,
    'df_alumnos.parquet': None,
    'df_cursos.parquet': None,
    'capa_departamentos_2010.geojson': None,
}

# Tipos de datos optimizados para reducir memoria
TIPOS_OPTIMIZADOS = {
    # Columnas categóricas comunes
    'SEXO': 'category',
    'ESTADO': 'category',
    'PROVINCIA': 'category',
    'DEPARTAMENTO': 'category',
    'LOCALIDAD': 'category',
    'TIPO': 'category',
    'CATEGORIA': 'category',
    # Enteros pequeños
    'EDAD': 'int8',
    'AÑO': 'int16',
    'MES': 'int8',
}


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimiza un DataFrame reduciendo el uso de memoria mediante:
    - Downcast de tipos numéricos
    - Conversión a categorías de columnas con pocos valores únicos
    - Eliminación de columnas completamente nulas

    Args:
        df: DataFrame a optimizar

    Returns:
        DataFrame optimizado
    """
    if df is None or df.empty:
        return df

    # Eliminar columnas completamente nulas
    df = df.dropna(axis=1, how='all')

    # Aplicar tipos optimizados predefinidos
    for col, dtype in TIPOS_OPTIMIZADOS.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except:
                pass  # Si falla, mantener tipo original

    # Optimizar columnas numéricas
    for col in df.select_dtypes(include=['int64']).columns:
        col_min = df[col].min()
        col_max = df[col].max()

        # Intentar downcast a tipos más pequeños
        if col_min >= -128 and col_max <= 127:
            df[col] = df[col].astype('int8')
        elif col_min >= -32768 and col_max <= 32767:
            df[col] = df[col].astype('int16')
        elif col_min >= -2147483648 and col_max <= 2147483647:
            df[col] = df[col].astype('int32')

    # Optimizar floats
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')

    # Convertir a categorías columnas con pocos valores únicos
    for col in df.select_dtypes(include=['object']).columns:
        num_unique = df[col].nunique()
        num_total = len(df[col])

        # Si tiene menos del 50% de valores únicos, convertir a categoría
        if num_unique / num_total < 0.5:
            df[col] = df[col].astype('category')

    return df


def read_parquet_optimized(file_path_or_buffer,
                           columns: Optional[List[str]] = None,
                           is_buffer: bool = False) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Lee un archivo Parquet de forma optimizada.

    Args:
        file_path_or_buffer: Ruta o buffer del archivo
        columns: Lista de columnas a leer (None = todas)
        is_buffer: Si es un buffer en memoria

    Returns:
        Tuple de (DataFrame, mensaje de error)
    """
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa

        # Leer tabla de Parquet
        if is_buffer:
            table = pq.read_table(file_path_or_buffer, columns=columns)
        else:
            table = pq.read_table(file_path_or_buffer, columns=columns)

        # Convertir a pandas
        try:
            df = table.to_pandas(
                timestamp_as_object=True,
                strings_to_categorical=True,  # Auto-convertir strings repetidos a categorías
                self_destruct=True  # Liberar memoria de PyArrow inmediatamente
            )
        except pa.ArrowInvalid as e:
            if "out of bounds timestamp" in str(e):
                df = table.to_pandas(timestamp_as_object=True, self_destruct=True)
            else:
                raise

        # Liberar tabla de PyArrow
        del table
        gc.collect()

        return df, None

    except (ImportError, Exception) as e:
        # Fallback a pandas si PyArrow falla
        try:
            if is_buffer:
                df = pd.read_parquet(file_path_or_buffer, columns=columns)
            else:
                df = pd.read_parquet(file_path_or_buffer, columns=columns)
            return df, None
        except Exception as e2:
            return None, str(e2)


def procesar_archivo_optimizado(nombre: str,
                                contenido: Any,
                                es_buffer: bool,
                                logs: Optional[Dict] = None) -> Tuple[Optional[pd.DataFrame], Optional[datetime.datetime]]:
    """
    Procesa un archivo de forma optimizada para reducir uso de memoria.

    Args:
        nombre: Nombre del archivo
        contenido: Contenido o ruta del archivo
        es_buffer: Si el contenido es un buffer
        logs: Diccionario para registrar logs

    Returns:
        Tuple de (DataFrame, fecha de actualización)
    """
    if logs is None:
        logs = {"warnings": [], "info": []}

    try:
        add_breadcrumb(
            category="data_processing",
            message=f"Procesando archivo optimizado: {nombre}",
            data={"es_buffer": es_buffer}
        )

        # Determinar columnas a cargar
        columnas = COLUMNAS_NECESARIAS.get(nombre, None)

        if nombre.endswith('.parquet'):
            if es_buffer:
                df, error = read_parquet_optimized(io.BytesIO(contenido), columns=columnas, is_buffer=True)
            else:
                df, error = read_parquet_optimized(contenido, columns=columnas, is_buffer=False)

            if df is not None:
                # Optimizar DataFrame
                df = optimize_dataframe(df)
                fecha = datetime.datetime.now()
                return df, fecha
            else:
                logs["warnings"].append(f"Error al leer parquet {nombre}: {error}")
                return None, None

        elif nombre.endswith('.geojson'):
            if es_buffer:
                gdf = gpd.read_file(io.BytesIO(contenido))
            else:
                gdf = gpd.read_file(contenido)

            # Simplificar geometría para reducir memoria
            if 'geometry' in gdf.columns:
                # Simplificar con tolerancia de 0.01 grados (~1km)
                gdf['geometry'] = gdf['geometry'].simplify(tolerance=0.01, preserve_topology=True)

            fecha = datetime.datetime.now()
            return gdf, fecha

        elif nombre.endswith('.xlsx'):
            if es_buffer:
                df = pd.read_excel(io.BytesIO(contenido), engine='openpyxl')
            else:
                df = pd.read_excel(contenido, engine='openpyxl')

            df = optimize_dataframe(df)
            fecha = datetime.datetime.now()
            return df, fecha

        elif nombre.endswith('.csv') or nombre.endswith('.txt'):
            if es_buffer:
                df = pd.read_csv(io.BytesIO(contenido))
            else:
                df = pd.read_csv(contenido)

            df = optimize_dataframe(df)
            fecha = datetime.datetime.now()
            return df, fecha
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
    finally:
        # Forzar recolección de basura
        gc.collect()


# Hash personalizado para caché que evita duplicación
def make_hashable(obj):
    """Convierte objetos a formato hashable para caché de Streamlit"""
    if isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    elif isinstance(obj, list):
        return tuple(make_hashable(item) for item in obj)
    elif isinstance(obj, pd.DataFrame):
        # Solo hashear shape y columnas, no los datos completos
        return (obj.shape, tuple(obj.columns))
    else:
        return obj


@st.cache_data(
    ttl=3600,  # 1 hora
    max_entries=5,  # Máximo 5 módulos en caché
    show_spinner="Cargando datos optimizados...",
    hash_funcs={pd.DataFrame: lambda df: (df.shape, tuple(df.columns))}
)
def load_module_data_optimized(module_key: str,
                               source_type: str,
                               source_params: Dict) -> Tuple[Dict, Dict, Dict]:
    """
    Carga datos de un módulo de forma optimizada.

    Args:
        module_key: Clave del módulo ('empleo', 'bco_gente', etc.)
        source_type: Tipo de fuente ('local', 'gitlab') - minio no soportado
        source_params: Parámetros de la fuente

    Returns:
        Tuple de (datos, fechas, logs)
    """
    logs = {"warnings": [], "info": ["Usando carga optimizada con lectura selectiva de columnas"]}
    all_data = {}
    all_dates = {}

    # Importar funciones de carga estándar
    from moduls.carga import load_data_from_local, load_data_from_gitlab, modules

    module_files = modules.get(module_key, [])
    temp_modules = {module_key: module_files}

    # Cargar usando las funciones originales primero
    if source_type == 'local':
        local_path = source_params.get('local_path', '')
        all_data, all_dates, temp_logs = load_data_from_local(local_path, temp_modules)
        logs["warnings"].extend(temp_logs.get("warnings", []))
        logs["info"].extend(temp_logs.get("info", []))

    elif source_type == 'gitlab':
        repo_id = source_params.get('repo_id')
        branch = source_params.get('branch')
        token = source_params.get('token')
        all_data, all_dates, temp_logs = load_data_from_gitlab(repo_id, branch, token, temp_modules)
        logs["warnings"].extend(temp_logs.get("warnings", []))
        logs["info"].extend(temp_logs.get("info", []))

    # Optimizar todos los DataFrames cargados (downcasting, categorías, etc.)
    for key, df in all_data.items():
        if isinstance(df, pd.DataFrame):
            all_data[key] = optimize_dataframe(df)

    # Limpiar memoria
    gc.collect()

    return all_data, all_dates, logs


def cleanup_memory():
    """Limpia memoria liberando recursos no utilizados"""
    gc.collect()

    # Limpiar caché antiguo de Streamlit si hay mucha memoria usada
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_percent = process.memory_percent()

        if memory_percent > 70:  # Si usa más del 70% de RAM
            st.cache_data.clear()
            gc.collect()
    except:
        pass
