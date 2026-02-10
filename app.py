import streamlit as st
import pandas as pd
import os
from os import path

# --- Configuración de la Página ---
st.set_page_config(
    page_title="Dashboard Resumen del Ministerio de Desarrollo Social y Promoción del Empleo",
    layout="wide"
)

# --- Imports ---
# Sentry removido
from moduls.carga import load_data_from_local, load_data_from_gitlab, load_data_from_gitlab_with_cache
from moduls.carga_optimized import cleanup_memory, optimize_dataframe
from moduls import bco_gente, cbamecapacita, empleo, escrituracion
from utils.styles import setup_page
from utils.ui_components import render_footer, show_notification_bell, insert_google_analytics
from utils.session_helper import safe_session_get, safe_session_set, safe_session_check, is_session_initialized

# --- Inicialización ---
insert_google_analytics()
setup_page()

# --- Configuración de Datos ---
# ⚠️ CAMBIA ESTA VARIABLE PARA CAMBIAR ENTRE MODO DESARROLLO Y PRODUCCIÓN
FUENTE_DATOS = "local"  

def get_data_config():
    """Obtiene la configuración de fuente de datos de manera centralizada."""
    # Configuración por defecto (se puede sobreescribir con secrets si existen)
    default_config = {
        "FUENTE_DATOS": FUENTE_DATOS,
        "REPO_ID": "Dir-Tecno/df_tablero",
        "BRANCH": "main",
        "LOCAL_PATH": "df_tablero-main"
    }

    return default_config

def get_gitlab_token():
    """Obtiene y valida el token de GitLab."""
    gitlab_token = None

    # Opción 1: Estructura anidada [gitlab] token = "..."
    if "gitlab" in st.secrets and "token" in st.secrets["gitlab"]:
        gitlab_token = st.secrets["gitlab"]["token"]
        
    return gitlab_token

def setup_development_mode(config):
    """Configura el modo de desarrollo si está activo.

    Returns:
        bool: True si está en modo local (desarrollo), False si está en modo GitLab (producción)
    """
    # Solo dos modos simples:
    is_local_mode = config["FUENTE_DATOS"] == "local"

    if is_local_mode:
        # Sidebar de desarrollo
        st.sidebar.title("🛠️ Modo Desarrollo")

        # Mostrar uso de RAM
        try:
            import psutil
            process = psutil.Process(os.getpid())
            ram_gb = process.memory_info().rss / 1024**3
            ram_percent = process.memory_percent()
            st.sidebar.metric("Uso de RAM", f"{ram_gb:.2f} GB", f"{ram_percent:.1f}%")
        except ImportError:
            pass

        # Botón para recargar datos
        if st.sidebar.button("Recargar Datos"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("Caché limpiado. La página se recargará con datos frescos.")
            st.rerun()

        # Señal visual prominente
        st.markdown(
            f"""
            <div style="background:#fff3cd;padding:12px;border-left:6px solid #ffc107;border-radius:4px;margin-bottom:10px">
                <strong>⚠️ Modo Desarrollo Activo</strong><br/>
                Fuente de datos: <code>{config["FUENTE_DATOS"]}</code> — Ruta local: <code>{config["LOCAL_PATH"]}</code>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Debug expander
        with st.expander("🔍 Información del Sistema", expanded=False):
            st.write({
                "Modo": "Desarrollo (Local)",
                "FUENTE_DATOS": config["FUENTE_DATOS"],
                "LOCAL_PATH": config["LOCAL_PATH"],
                "Ruta existe": path.exists(config["LOCAL_PATH"])
            })

    return is_local_mode

# --- Obtener configuración ---
config = get_data_config()
is_local_mode = setup_development_mode(config)

# --- Mapeo de Archivos por Módulo ---
modules = {
    'bco_gente': ['df_global_banco.parquet', 'df_global_pagados.parquet'],
    'cba_capacita': ['df_postulantes_cbamecapacita.parquet','df_alumnos.parquet', 'df_cursos.parquet'],
    'empleo': ['df_postulantes_empleo.parquet','df_inscriptos_empleo.parquet', 'df_empresas.parquet','capa_departamentos_2010.geojson'],
}

# --- Función de Carga de Datos (Lazy Loading) ---
def load_module_data(module_key, config, is_local_mode):
    """Carga datos específicos para un módulo individual (carga lazy)."""
    module_files = modules.get(module_key, [])
    if not module_files:
        return {}, {}, {"warnings": [f"No hay archivos definidos para el módulo {module_key}"], "info": []}

    temp_modules = {module_key: module_files}

    # Carga desde fuente local
    if is_local_mode:
        data, dates, logs = load_data_from_local(config["LOCAL_PATH"], temp_modules)
        # Optimizar DataFrames
        for key, df in data.items():
            if isinstance(df, pd.DataFrame):
                data[key] = optimize_dataframe(df)
        return data, dates, logs

    # Carga desde GitLab
    if config["FUENTE_DATOS"] == "gitlab":
        gitlab_token = get_gitlab_token()
        if not gitlab_token:
            return {}, {}, {"warnings": ["Token de GitLab no configurado."], "info": []}

        # Usar caché en disco
        data, dates, logs = load_data_from_gitlab_with_cache(
            config["REPO_ID"], config["BRANCH"], gitlab_token, temp_modules
        )
        # Optimizar DataFrames
        for key, df in data.items():
            if isinstance(df, pd.DataFrame):
                data[key] = optimize_dataframe(df)
        return data, dates, logs

    return {}, {}, {"warnings": [f"Fuente de datos no reconocida: {config['FUENTE_DATOS']}"], "info": []}

# --- Header Principal ---
st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)

# --- Inicializar variables de sesión ---
if is_session_initialized():
    if not safe_session_check("campanita_mostrada"):
        safe_session_set("campanita_mostrada", False)
    if not safe_session_check("mostrar_form_comentario"):
        safe_session_set("mostrar_form_comentario", False)

# --- Mostrar Campanita de Novedades ---
if is_session_initialized():
    show_notification_bell()

# --- Definición de Pestañas ---
tab_names = ["Programas de Empleo", "CBA Me Capacita", "Banco de la Gente", "Escrituración"]
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
            module_data, module_dates, module_logs = load_module_data(module_key, config, is_local_mode)
            data_for_module = module_data
            dates_for_module = module_dates
        except Exception as e:
            st.error(f"Error al cargar datos para {tab_names[idx]}: {str(e)}")
            data_for_module = {}
            dates_for_module = {}
            module_logs = {"warnings": [f"Error de carga: {str(e)}"], "info": []}

        # Si no hay datos, mostrar warning (excepto para escrituracion)
        if not data_for_module and module_key != "escrituracion":
            st.warning(f"No se encontraron datos para el módulo '{tab_names[idx]}'.")
            with st.expander("🔍 Debug: Ver archivos esperados vs cargados"):
                module_files = modules.get(module_key, [])
                st.write(f"**Archivos esperados para {module_key}:**")
                st.write(module_files)
                st.write(f"**Archivos cargados para este módulo:**")
                st.write(list(data_for_module.keys()))

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
            show_func(data_for_module, dates_for_module, is_local_mode)
        except Exception as e:
            st.error(f"Error al renderizar el dashboard '{tab_names[idx]}': {e}")
            st.exception(e)

# --- Limpieza de Memoria ---
cleanup_memory()

# --- Footer ---
render_footer()