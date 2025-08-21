import streamlit as st
# --- Configuración de la Página ---
st.set_page_config(
    page_title="Dashboard Resumen del Ministerio de Desarrollo Social y Promoción del Empleo",
    layout="wide"
)
from moduls.carga import load_data_from_minio, load_data_from_local, load_data_from_gitlab
from moduls import bco_gente, cbamecapacita, empleo, escrituracion
from utils.styles import setup_page
from utils.ui_components import render_footer, show_notification_bell
from minio import Minio
from os import path


setup_page()
st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)
# --- Configuración General ---
FUENTE_DATOS = "gitlab"  # Opciones: 'minio', 'gitlab', 'local'
REPO_ID = "Dir-Tecno/Repositorio-Reportes"
BRANCH = "main"
LOCAL_PATH = r"D:\DESARROLLO\REPORTES\TableroGeneral\Repositorio-Reportes-main"
MINIO_BUCKET = "repositorio-dashboard"

# --- Determinación del Modo de Ejecución ---
is_local = path.exists(LOCAL_PATH) and FUENTE_DATOS == "local"
is_production = not is_local

# --- Mapeo de Archivos por Módulo ---
modules = {
    'bco_gente': ['VT_CUMPLIMIENTO_FORMULARIOS.parquet', 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet', 
                   'capa_departamentos_2010.geojson', 'LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'],
    'cba_capacita': ['VT_ALUMNOS_EN_CURSOS.parquet','VT_INSCRIPCIONES_PRG129.parquet', 'VT_CURSOS_SEDES_GEO.parquet', 'capa_departamentos_2010.geojson'],
    'empleo': ['LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt','VT_REPORTES_PPP_MAS26.parquet', 'vt_empresas_adheridas.parquet','vt_empresas_ARCA.parquet', 'VT_PUESTOS_X_FICHAS.parquet','capa_departamentos_2010.geojson'],
    'escrituracion': ['https://docs.google.com/spreadsheets/d/1V9vXwMQJjd4kLdJZQncOSoWggQk8S7tBKxbOSEIUoQ8/edit#gid=1593263408']
    
}

# --- Funciones Cacheadas para Rendimiento ---

@st.cache_resource
def get_minio_client():
    """Crea y cachea el cliente de MinIO para evitar reconexiones."""
    try:
        client = Minio(
            st.secrets["minio_endpoint"],
            access_key=st.secrets["minio_access_key"],
            secret_key=st.secrets["minio_secret_key"],
            secure=False
        )
        # Probar la conexión listando buckets
        client.list_buckets()
        return client
    except Exception as e:
        st.error(f"Error al conectar con MinIO: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner="Cargando datos del dashboard...")  # Cachear datos por 1 hora
def load_all_data():
    """Carga todos los datos necesarios para la aplicación desde la fuente configurada."""
    if is_local:
        st.success("Modo de desarrollo: Cargando datos desde carpeta local.")
        return load_data_from_local(LOCAL_PATH, modules)

    if FUENTE_DATOS == "minio":
        minio_client = get_minio_client()
        if minio_client:
            st.success("Modo de producción: Cargando datos desde MinIO.")
            return load_data_from_minio(minio_client, MINIO_BUCKET, modules)
        else:
            st.error("No se pudo establecer la conexión con MinIO. No se pueden cargar los datos.")
            return {}, {}, {"warnings": ["Fallo en conexión a MinIO"], "info": []}

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

# --- Carga de Datos ---
all_data, all_dates, logs = load_all_data()

# --- Mostrar Campanita de Novedades DESPUÉS de la carga ---
show_notification_bell()



# --- Definición de Pestañas ---
tab_names = ["CBA Me Capacita", "Banco de la Gente", "Programas de Empleo", "Escrituración"]
tabs = st.tabs(tab_names)
tab_keys = ['cba_capacita', 'bco_gente', 'empleo', 'escrituracion']
tab_functions = [
    cbamecapacita.show_cba_capacita_dashboard,
    bco_gente.show_bco_gente_dashboard,
    empleo.show_empleo_dashboard,
    escrituracion.show_escrituracion_dashboard,
]

# --- Renderizado de Pestañas ---
for idx, tab in enumerate(tabs):
    with tab:
        module_key = tab_keys[idx]
        show_func = tab_functions[idx]
        
        st.markdown(f'<div class="tab-subheader">{tab_names[idx]}</div>', unsafe_allow_html=True)
        
        # Filtrar datos y fechas para el módulo actual
        module_files = modules.get(module_key, [])
        data_for_module = {file: all_data.get(file) for file in module_files if file in all_data}
        dates_for_module = {file: all_dates.get(file) for file in module_files if file in all_dates}

        if not data_for_module:
            st.warning(f"No se encontraron datos para el módulo '{tab_names[idx]}'.")
            with st.expander("🔍 Debug: Ver archivos esperados vs cargados"):
                st.write(f"**Archivos esperados para {module_key}:**")
                st.write(module_files)
                st.write(f"**Archivos cargados desde GitLab:**")
                st.write(list(all_data.keys()))
                st.write(f"**Archivos coincidentes:**")
                coincidentes = [f for f in module_files if f in all_data]
                st.write(coincidentes if coincidentes else "Ninguno")
                
                # Mostrar logs de carga
                if logs:
                    st.write("**Logs de carga:**")
                    if logs.get("warnings"):
                        st.error("Warnings:")
                        for warning in logs["warnings"]:
                            st.write(f"⚠️ {warning}")
                    if logs.get("info"):
                        st.info("Info:")
                        for info in logs["info"]:
                            st.write(f"ℹ️ {info}")
            continue

        try:
            # Pasar los datos filtrados a la función del dashboard del módulo
            show_func(data_for_module, dates_for_module, is_local)
        except Exception as e:
            st.error(f"Error al renderizar el dashboard '{tab_names[idx]}': {e}")
            st.exception(e)

# --- Footer ---
render_footer()