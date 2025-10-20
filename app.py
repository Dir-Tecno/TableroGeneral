import streamlit as st
import os
import psutil
import logging

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_resource_usage():
    """Registra el uso de recursos del sistema (CPU, memoria, disco)."""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        logging.info(f"CPU: {cpu_percent}%, Memoria: {memory.percent}%, Disco: {disk.percent}%")
        logging.info(f"Memoria usada: {memory.used / (1024**3):.2f} GB de {memory.total / (1024**3):.2f} GB")
    except Exception as e:
        logging.error(f"Error al registrar uso de recursos: {e}")

# --- Configuración de la Página ---
st.set_page_config(
    page_title="Dashboard Resumen del Ministerio de Desarrollo Social y Promoción del Empleo",
    layout="wide"
)

from utils.sentry_utils import init_sentry, sentry_wrap, sentry_error, capture_exception
# Inicializar Sentry al principio de la aplicación
init_sentry()

from moduls.carga import load_data_from_local, load_data_from_gitlab
from moduls import bco_gente, cbamecapacita, empleo, escrituracion
from utils.styles import setup_page
from utils.ui_components import render_footer, show_notification_bell, insert_google_analytics
from utils.session_helper import safe_session_get, safe_session_set, safe_session_check, is_session_initialized

# --- Integración de Google Analytics ---
insert_google_analytics()

setup_page()
st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)
# --- Configuración General ---
try:
    # Intenta leer desde st.secrets["configuraciones"]
    FUENTE_DATOS = st.secrets["configuraciones"]["FUENTE_DATOS"]
    REPO_ID = st.secrets["configuraciones"]["REPO_ID"]
    BRANCH = st.secrets["configuraciones"]["BRANCH"]
    LOCAL_PATH = st.secrets["configuraciones"]["LOCAL_PATH"]
except KeyError:
    # Si falla, lee directamente de st.secrets (variables de entorno)
    FUENTE_DATOS = st.secrets.get("FUENTE_DATOS", "gitlab")
    REPO_ID = st.secrets.get("REPO_ID", "Dir-Tecno/df_ministerio")
    BRANCH = st.secrets.get("BRANCH", "main")
    LOCAL_PATH = st.secrets.get("LOCAL_PATH", "")

# --- Determinación del Modo de Ejecución ---
from os import path
is_local = path.exists(LOCAL_PATH) and FUENTE_DATOS == "local"

# --- Botón para Limpiar Caché en Modo Desarrollo ---
if is_local:
    st.sidebar.title("🛠️ Opciones de Desarrollo")
    if st.sidebar.button("Limpiar Caché y Recargar"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Caché limpiado. La página se recargará con datos frescos.")
        st.rerun()

# --- Mapeo de Archivos por Módulo ---
modules = {
    'bco_gente': ['df_global_banco.parquet', 'df_global_pagados.parquet'],
    'cba_capacita': ['df_postulantes_cbamecapacita.parquet','df_alumnos.parquet', 'df_cursos.parquet'],
    'empleo': ['df_postulantes_empleo.parquet','df_inscriptos_empleo.parquet', 'df_empresas.parquet','capa_departamentos_2010.geojson'],
}



# --- Funciones Cacheadas para Rendimiento ---



def load_all_data():
    """Carga todos los datos necesarios para la aplicación desde la fuente configurada."""
    if is_local:
        st.success("Modo de desarrollo: Cargando datos desde carpeta local.")
        return load_data_from_local(LOCAL_PATH, modules)


    if FUENTE_DATOS == "gitlab":
        
        # Intenta leer el token desde diferentes ubicaciones
        gitlab_token = None
        
        # Opción 1: Estructura anidada [gitlab] token = "..."
        if "gitlab" in st.secrets and "token" in st.secrets["gitlab"]:
            gitlab_token = st.secrets["gitlab"]["token"]
        
        
        return load_data_from_gitlab(REPO_ID, BRANCH, gitlab_token, modules)

    st.error(f"Fuente de datos no reconocida: {FUENTE_DATOS}")
    return {}, {}, {"warnings": [f"Fuente de datos no reconocida: {FUENTE_DATOS}"], "info": []}

# --- Carga de Datos ---
all_data, all_dates, logs = load_all_data()

# --- Inicializar variables de sesión de forma segura ---
if is_session_initialized():
    # Inicializar variables de sesión necesarias
    if not safe_session_check("campanita_mostrada"):
        safe_session_set("campanita_mostrada", False)
    if not safe_session_check("mostrar_form_comentario"):
        safe_session_set("mostrar_form_comentario", False)

# --- Mostrar Campanita de Novedades DESPUÉS de la carga ---
if is_session_initialized():
    show_notification_bell()

# --- La opción para limpiar caché ahora está en el footer ---

# --- Definición de Pestañas ---
tab_names = ["Programas de Empleo", "CBA Me Capacita", "Banco de la Gente",  "Escrituración"]
tabs = st.tabs(tab_names)
tab_keys = ['empleo', 'cba_capacita', 'bco_gente', 'escrituracion']
tab_functions = [
    empleo.show_empleo_dashboard,
    cbamecapacita.show_cba_capacita_dashboard,
    bco_gente.show_bco_gente_dashboard,
    escrituracion.show_escrituracion_dashboard,
]

# --- Renderizado de Pestañas ---
for idx, tab in enumerate(tabs):
    with tab:
        module_key = tab_keys[idx]
        show_func = tab_functions[idx]
        
        st.markdown(f'<div class="tab-subheader">{tab_names[idx]}</div>', unsafe_allow_html=True)
        
        # Cargar datos solo para este módulo cuando se accede a la pestaña
        data_for_module, dates_for_module, logs_module = load_data_for_module(module_key)

        # Si no hay datos, mostrar el warning SÓLO para módulos que realmente requieren archivos.
        # Para 'escrituracion' queremos mostrar siempre la vista (redirige a un servicio externo).
        if not data_for_module and module_key != "escrituracion":
            st.warning(f"No se encontraron datos para el módulo '{tab_names[idx]}'.")
            with st.expander("🔍 Debug: Ver archivos esperados vs cargados"):
                module_files = modules.get(module_key, [])
                st.write(f"**Archivos esperados para {module_key}:**")
                st.write(module_files)
                st.write(f"**Archivos cargados:**")
                st.write(list(data_for_module.keys()))
                st.write(f"**Archivos coincidentes:**")
                coincidentes = [f for f in module_files if f in data_for_module]
                st.write(coincidentes if coincidentes else "Ninguno")
                
                # Mostrar logs de carga del módulo
                if logs_module:
                    st.write("**Logs de carga:**")
                    if logs_module.get("warnings"):
                        st.error("Warnings:")
                        for warning in logs_module["warnings"]:
                            st.write(f"⚠️ {warning}")
                    if logs_module.get("info"):
                        st.info("Info:")
                        for info in logs_module["info"]:
                            st.write(f"ℹ️ {info}")
            continue

        try:
            # Pasar los datos cargados a la función del dashboard del módulo
            show_func(data_for_module, dates_for_module, is_local)
        except Exception as e:
            st.error(f"Error al renderizar el dashboard '{tab_names[idx]}': {e}")
            st.exception(e)

# --- Footer ---
render_footer()

@st.cache_data
def load_data_for_module(module_key):
    """Carga datos solo para un módulo específico usando lazy loading."""
    log_resource_usage()  # Registrar uso de recursos antes de cargar
    
    module_files = modules.get(module_key, [])
    if not module_files:
        logging.warning(f"No se encontraron archivos definidos para el módulo '{module_key}'")
        return {}, {}, {"warnings": [f"No se encontraron archivos definidos para el módulo '{module_key}'"], "info": []}
    
    logging.info(f"Cargando datos para módulo '{module_key}': {module_files}")
    
    if is_local:
        all_data, all_dates, logs = load_data_from_local(LOCAL_PATH, {module_key: module_files})
    elif FUENTE_DATOS == "gitlab":
        # Intenta leer el token desde diferentes ubicaciones
        gitlab_token = None
        
        # Opción 1: Estructura anidada [gitlab] token = "..."
        if "gitlab" in st.secrets and "token" in st.secrets["gitlab"]:
            gitlab_token = st.secrets["gitlab"]["token"]
        
        all_data, all_dates, logs = load_data_from_gitlab(REPO_ID, BRANCH, gitlab_token, {module_key: module_files})
    else:
        error_msg = f"Fuente de datos no reconocida: {FUENTE_DATOS}"
        logging.error(error_msg)
        return {}, {}, {"warnings": [error_msg], "info": []}
    
    log_resource_usage()  # Registrar uso de recursos después de cargar
    
    # Filtrar solo los archivos del módulo
    data_for_module = {file: all_data.get(file) for file in module_files if file in all_data}
    dates_for_module = {file: all_dates.get(file) for file in module_files if file in all_dates}
    
    loaded_files = list(data_for_module.keys())
    logging.info(f"Datos cargados para '{module_key}': {loaded_files}")
    
    if not data_for_module:
        warning_msg = f"No se pudieron cargar datos para el módulo '{module_key}'"
        logging.warning(warning_msg)
        logs["warnings"].append(warning_msg)
    
    return data_for_module, dates_for_module, logs