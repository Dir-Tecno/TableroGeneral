import streamlit as st
import pandas as pd
import os
# --- Configuración de la Página ---
st.set_page_config(
    page_title="Dashboard Resumen del Ministerio de Desarrollo Social y Promoción del Empleo",
    layout="wide"
)

from utils.sentry_utils import init_sentry, sentry_wrap, sentry_error, capture_exception
# Inicializar Sentry al principio de la aplicación
init_sentry()

from moduls.carga import load_data_from_local, load_data_from_gitlab, load_data_from_gitlab_with_cache
from moduls.carga_optimized import cleanup_memory, optimize_dataframe
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
    LOCAL_PATH = st.secrets.get("LOCAL_PATH", "df_ministerio")

# --- Determinación del Modo de Ejecución ---
from os import path
is_local = path.exists(LOCAL_PATH) and FUENTE_DATOS == "local"


if is_local:
    st.sidebar.title("🛠️ Opciones de Desarrollo")

    # Mostrar uso de RAM
    try:
        import psutil
        process = psutil.Process(os.getpid())
        ram_gb = process.memory_info().rss / 1024**3
        ram_percent = process.memory_percent()
        st.sidebar.metric(
            "Uso de RAM",
            f"{ram_gb:.2f} GB",
            f"{ram_percent:.1f}%"
        )
    except ImportError:
        pass  # psutil no instalado

    if st.sidebar.button("Recargar Datos"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Caché limpiado. La página se recargará con datos frescos.")
        st.rerun()

# Señal visual prominente para modo desarrollo/local
if is_local:
    try:
        local_exists = path.exists(LOCAL_PATH)
    except Exception:
        local_exists = False

    st.markdown(
        f"""
        <div style="background:#fff3cd;padding:12px;border-left:6px solid #ffc107;border-radius:4px;margin-bottom:10px">
            <strong>⚠️ Modo Desarrollo (LOCAL) activo</strong><br/>
            Fuente de datos: <code>{FUENTE_DATOS}</code> — Ruta local: <code>{LOCAL_PATH}</code><br/>
            Ruta accesible: <strong>{'Sí' if local_exists else 'No'}</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("🔍 Debug rápido (información local)", expanded=True):
        st.write({
            "is_local": is_local,
            "FUENTE_DATOS": FUENTE_DATOS,
            "LOCAL_PATH": LOCAL_PATH,
            "LOCAL_PATH_exists": local_exists,
        })
        # Mostrar contenido de la carpeta local (si existe)
        if local_exists:
            try:
                files = os.listdir(LOCAL_PATH)
                st.write(f"Archivos en {LOCAL_PATH}:", files)
            except Exception as e:
                st.write(f"No se pudo listar {LOCAL_PATH}: {e}")
        else:
            st.info("La ruta local configurada no existe o no es accesible desde este entorno.")

# --- Mapeo de Archivos por Módulo ---
modules = {
    'bco_gente': ['df_global_banco.parquet', 'df_global_pagados.parquet'],
    'cba_capacita': ['df_postulantes_cbamecapacita.parquet','df_alumnos.parquet', 'df_cursos.parquet'],
    'empleo': ['df_postulantes_empleo.parquet','df_inscriptos_empleo.parquet', 'df_empresas.parquet','capa_departamentos_2010.geojson'],
}



# --- Funciones Cacheadas para Rendimiento ---
# OPTIMIZACIÓN APLICADA: Carga lazy por módulo para reducir uso de RAM
# - TTL reducido de 3600s a 1800s (30 min)
# - Máximo 10 entradas en caché por módulo
# - Solo carga datos cuando se accede a una pestaña específica

@st.cache_data(ttl=1800, max_entries=10, show_spinner="Cargando datos del módulo...")
def load_module_data(module_key):
    """Carga datos específicos para un módulo individual (carga lazy)."""
    module_files = modules.get(module_key, [])
    if not module_files:
        return {}, {}, {"warnings": [f"No hay archivos definidos para el módulo {module_key}"], "info": []}

    # Crear un diccionario temporal solo con los archivos de este módulo
    temp_modules = {module_key: module_files}

    if is_local:
        data, dates, logs = load_data_from_local(LOCAL_PATH, temp_modules)
        # Optimizar DataFrames después de cargarlos
        for key, df in data.items():
            if isinstance(df, pd.DataFrame):
                data[key] = optimize_dataframe(df)
        return data, dates, logs

    # MINIO SUPPORT REMOVED - only local and gitlab sources supported

    if FUENTE_DATOS == "gitlab":
        gitlab_token = None
        if "gitlab" in st.secrets and "token" in st.secrets["gitlab"]:
            gitlab_token = st.secrets["gitlab"]["token"]

        if not gitlab_token:
            return {}, {}, {"warnings": ["Token de GitLab no configurado."], "info": []}
        elif gitlab_token == "TU_TOKEN_DE_GITLAB_AQUI":
            return {}, {}, {"warnings": ["Token de GitLab no configurado (valor de ejemplo)."], "info": []}

        # USAR CACHÉ EN DISCO - Descarga solo cuando es necesario
        data, dates, logs = load_data_from_gitlab_with_cache(REPO_ID, BRANCH, gitlab_token, temp_modules)
        # Optimizar DataFrames después de cargarlos
        for key, df in data.items():
            if isinstance(df, pd.DataFrame):
                data[key] = optimize_dataframe(df)
        return data, dates, logs

    return {}, {}, {"warnings": [f"Fuente de datos no reconocida: {FUENTE_DATOS}"], "info": []}

@st.cache_data(ttl=1800, max_entries=5, show_spinner="Cargando datos del dashboard...")  # Cachear datos por 30 min, máximo 5 entradas
def load_all_data():
    """Carga todos los datos necesarios para la aplicación desde la fuente configurada."""
    if is_local:
        st.success("Modo de desarrollo: Cargando datos desde carpeta local.")
        return load_data_from_local(LOCAL_PATH, modules)

    # MINIO SUPPORT REMOVED - only local and gitlab sources supported

    if FUENTE_DATOS == "gitlab":
        
        # Intenta leer el token desde diferentes ubicaciones
        gitlab_token = None
        
        # Opción 1: Estructura anidada [gitlab] token = "..."
        if "gitlab" in st.secrets and "token" in st.secrets["gitlab"]:
            gitlab_token = st.secrets["gitlab"]["token"]
        
        # Validar el token
        if not gitlab_token:
            st.error("❌ El token de GitLab no está configurado en los secretos.")
            st.info("📝 Configura el token en tu archivo `.streamlit/secrets.toml` usando una de estas opciones:")
            st.code("""# Opción 1 (recomendada):
                        [gitlab]
                        token = "tu_token_aqui" """)
            return {}, {}, {"warnings": ["Token de GitLab no configurado."], "info": []}
        elif gitlab_token == "TU_TOKEN_DE_GITLAB_AQUI":
            st.error("❌ El token de GitLab tiene el valor de ejemplo. Por favor, configura tu token real.")
            return {}, {}, {"warnings": ["Token de GitLab no configurado (valor de ejemplo)."], "info": []}
        
        return load_data_from_gitlab(REPO_ID, BRANCH, gitlab_token, modules)

    st.error(f"Fuente de datos no reconocida: {FUENTE_DATOS}")
    return {}, {}, {"warnings": [f"Fuente de datos no reconocida: {FUENTE_DATOS}"], "info": []}

# --- Carga de Datos (Solo para inicialización) ---
# Nota: Ahora usamos carga lazy por módulo, pero mantenemos esta función para compatibilidad
all_data, all_dates, logs = {}, {}, {"warnings": [], "info": ["Usando carga lazy por módulo"]}

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
        
        # Carga lazy: solo cargar datos cuando se accede al módulo
        try:
            module_data, module_dates, module_logs = load_module_data(module_key)
            data_for_module = module_data
            dates_for_module = module_dates
        except Exception as e:
            st.error(f"Error al cargar datos para {tab_names[idx]}: {str(e)}")
            data_for_module = {}
            dates_for_module = {}
            module_logs = {"warnings": [f"Error de carga: {str(e)}"], "info": []}

        # Si no hay datos, mostrar el warning SÓLO para módulos que realmente requieren archivos.
        # Para 'escrituracion' queremos mostrar siempre la vista (redirige a un servicio externo).
        if not data_for_module and module_key != "escrituracion":
            st.warning(f"No se encontraron datos para el módulo '{tab_names[idx]}'.")
            with st.expander("🔍 Debug: Ver archivos esperados vs cargados"):
                module_files = modules.get(module_key, [])
                st.write(f"**Archivos esperados para {module_key}:**")
                st.write(module_files)
                st.write(f"**Archivos cargados para este módulo:**")
                st.write(list(data_for_module.keys()))
                
                # Mostrar logs de carga del módulo
                if module_logs:
                    st.write("**Logs de carga del módulo:**")
                    if module_logs.get("warnings"):
                        st.error("Warnings:")
                        for warning in module_logs["warnings"]:
                            st.write(f"⚠️ {warning}")
                    if module_logs.get("info"):
                        st.info("Info:")
                        for info in module_logs["info"]:
                            st.write(f"ℹ️ {info}")
            continue

        try:
            # Pasar los datos filtrados a la función del dashboard del módulo
            show_func(data_for_module, dates_for_module, is_local)
        except Exception as e:
            st.error(f"Error al renderizar el dashboard '{tab_names[idx]}': {e}")
            st.exception(e)

# --- Limpieza de Memoria ---
# Liberar memoria después de renderizar todas las pestañas
cleanup_memory()

# --- Footer ---
render_footer()