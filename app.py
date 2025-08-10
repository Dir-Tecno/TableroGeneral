from moduls.carga import load_data_from_minio, load_data_from_local, load_data_from_gitlab
import streamlit as st
from moduls import bco_gente, cbamecapacita, empleo, emprendimientos
from utils.styles import setup_page
from utils.ui_components import render_footer, show_notification_bell
from minio import Minio
from os import path

# --- Configuración de la Página ---
st.set_page_config(
    page_title="Dashboard Resumen del Ministerio de Desarrollo Social y Promoción del Empleo",
    layout="wide"
)
setup_page()
st.markdown('<div class="main-header">Tablero General de Reportes para TEST</div>', unsafe_allow_html=True)
show_notification_bell()

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
    'emprendimientos': ['desarrollo_emprendedor.csv']
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

@st.cache_data(ttl=3600)  # Cachear datos por 1 hora
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
        st.success("Modo de producción: Cargando datos desde GitLab.")
        # Intenta leer el token desde la sección [gitlab] o como clave principal
        gitlab_token = st.secrets.get("gitlab", {}).get("token") or st.secrets.get("gitlab_token")
        if not gitlab_token or gitlab_token == "TU_TOKEN_DE_GITLAB_AQUI":
            st.error("El token de GitLab no está configurado en los secretos. Por favor, añádelo a tu archivo .streamlit/secrets.toml")
            return {}, {}, {"warnings": ["Token de GitLab no configurado."], "info": []}
        return load_data_from_gitlab(REPO_ID, BRANCH, gitlab_token, modules)

    st.error(f"Fuente de datos no reconocida: {FUENTE_DATOS}")
    return {}, {}, {"warnings": [f"Fuente de datos no reconocida: {FUENTE_DATOS}"], "info": []}

# --- Carga de Datos ---
all_data, all_dates, logs = load_all_data()

# --- Sección de Depuración (Opcional) ---
with st.expander("🔍 Estado de la Carga de Datos (Depuración)"):
    st.write("**Archivos Cargados Exitosamente:**", list(all_data.keys()))
    st.write("**Fechas de Modificación:**", {k: v.strftime('%Y-%m-%d %H:%M:%S') if v else None for k, v in all_dates.items()})
    if not all_data:
        st.error("El diccionario 'all_data' está vacío. La carga de datos falló.")
    
    st.write("---")
    st.write("### Logs de Carga:")
    if logs and logs.get("warnings"):
        st.write("#### ⚠️ Advertencias:")
        for warning in logs["warnings"]:
            st.warning(warning)
    if logs and logs.get("info"):
        st.write("#### ℹ️ Información:")
        for info in logs["info"]:
            st.info(info)

# --- Definición de Pestañas ---
tab_names = ["CBA Me Capacita", "Banco de la Gente", "Programas de Empleo", "Emprendimientos"]
tabs = st.tabs(tab_names)
tab_keys = ['cba_capacita', 'bco_gente', 'empleo', 'emprendimientos']
tab_functions = [
    cbamecapacita.show_cba_capacita_dashboard,
    bco_gente.show_bco_gente_dashboard,
    empleo.show_empleo_dashboard,
    emprendimientos.show_emprendimientos_dashboard
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
            continue

        try:
            # Pasar los datos filtrados a la función del dashboard del módulo
            show_func(data_for_module, dates_for_module, is_local)
        except Exception as e:
            st.error(f"Error al renderizar el dashboard '{tab_names[idx]}': {e}")
            st.exception(e)

# --- Footer ---
render_footer()