from moduls.carga import load_data_from_minio, load_data_from_local, load_data_from_gitlab
import streamlit as st
from moduls import bco_gente, cbamecapacita, empleo 
from utils.styles import setup_page
from utils.ui_components import render_footer, show_notification_bell
import concurrent.futures
from minio import Minio
from os import path

# Función centralizada para cargar datos según la fuente configurada
def load_data_by_source(source_type, local_path, minio_client, bucket, repo_id, branch, token, modules, module_key):
    """Función centralizada para cargar datos según la fuente configurada"""
    if source_type == "local" and path.exists(local_path):
        all_data, all_dates, logs = load_data_from_local(local_path, modules)
    elif source_type == "minio":
        all_data, all_dates, logs = load_data_from_minio(minio_client, bucket, modules)
    elif source_type == "gitlab":
        all_data, all_dates, logs = load_data_from_gitlab(repo_id, branch, token, modules)
    else:
        # Default o error
        all_data, all_dates = {}, {}
        logs = {"warnings": ["Fuente de datos no válida"], "info": []}
        
    # Filtrar datos para el módulo específico
    data = {k: all_data.get(k) for k in modules[module_key] if k in all_data}
    dates = {k: all_dates.get(k) for k in modules[module_key] if k in all_dates}
    return data, dates, logs

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Resumen del Ministerio de Desarrollo Social y Promoción del Empleo", 
    layout="wide"
)

# Aplicar estilos y banner desde el módulo de estilos
setup_page()

# Mostrar título principal
st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)

# Mostrar campanita de novedades como elemento flotante
show_notification_bell()

# Configuración general

# Opciones de fuente de datos: 'minio', 'gitlab', 'local'
FUENTE_DATOS = "gitlab"  # Configurable por código: minio, gitlab o local

# Configuración de GitLab
repo_id = "Dir-Tecno/Repositorio-Reportes"
branch = "main"

# Ruta local para desarrollo
local_path = r"D:\DESARROLLO\REPORTES\TableroGeneral\Repositorio-Reportes-main"

# Determinar el modo de desarrollo basado en la fuente de datos
is_local = path.exists(local_path) and FUENTE_DATOS == "local"
is_minio = FUENTE_DATOS == "minio" and not is_local
is_gitlab = FUENTE_DATOS == "gitlab" and not is_local

# Compatibilidad con código existente
is_development = is_local
is_production = not is_development

# Mostrar información sobre el modo de carga
if is_development:
    if is_local:
        st.success("Modo de desarrollo: Cargando datos desde carpeta local")
    elif is_minio:
        st.success("Modo de producción: Cargando datos desde MinIO")
    elif is_gitlab:
        st.success("Modo de producción: Cargando datos desde GitLab")

# Obtener token desde secrets (solo necesario en modo producción)
token = None
if is_production:
    try:
        token = st.secrets["gitlab"]["token"]
    except Exception as e:
        st.error(f"Error al obtener token: {str(e)}")
        st.stop()

# Mapeo de archivos por módulo
modules = {
    'bco_gente': ['VT_CUMPLIMIENTO_FORMULARIOS.parquet', 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet', 
                   'capa_departamentos_2010.geojson', 'LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'],
    'cba_capacita': ['VT_ALUMNOS_EN_CURSOS.parquet','VT_INSCRIPCIONES_PRG129.parquet', 'VT_CURSOS_SEDES_GEO.parquet', 'capa_departamentos_2010.geojson'],
    'empleo': ['LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt','VT_REPORTES_PPP_MAS26.parquet', 'vt_empresas_adheridas.parquet','vt_empresas_ARCA.parquet', 'VT_PUESTOS_X_FICHAS.parquet','capa_departamentos_2010.geojson']
}

# Configuración MinIO
MINIO_ENDPOINT = "5.161.118.67:7003"
MINIO_ACCESS_KEY = "dirtecno"
MINIO_SECRET_KEY = "dirtecnon0r3cu3rd0"
MINIO_BUCKET = "repositorio-reportes"  

# Cliente MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Listar objetos en el bucket de MinIO (en modo producción)
if is_production:
    try:
        # Solo imprimir en consola, no en la UI de Streamlit
        for obj in minio_client.list_objects(MINIO_BUCKET, recursive=True):
            print(obj.object_name)
    except Exception as e:
        print(f"Error al listar objetos en MinIO: {str(e)}")

# Crear pestañas
tab_names = ["CBA Me Capacita", "Banco de la Gente",  "Programas de Empleo"]
tabs = st.tabs(tab_names)
tab_keys = ['cba_capacita', 'bco_gente', 'empleo']
tab_functions = [
    cbamecapacita.show_cba_capacita_dashboard,
    bco_gente.show_bco_gente_dashboard,
    empleo.show_empleo_dashboard,
]

for idx, tab in enumerate(tabs):
    with tab:
        module_key = tab_keys[idx]
        show_func = tab_functions[idx]
        st.markdown(f'<div class="tab-subheader">{tab_names[idx]}</div>', unsafe_allow_html=True)
        data_key = f"{module_key}_data"
        dates_key = f"{module_key}_dates"
        if data_key not in st.session_state or dates_key not in st.session_state:
            # Mensaje según la fuente de datos configurada
            if is_local:
                spinner_message = "Cargando datos desde carpeta local..."
            elif is_minio:
                spinner_message = "Cargando datos desde MinIO..."
            elif is_gitlab:
                spinner_message = "Cargando datos desde GitLab..."
            else:
                spinner_message = "Cargando datos..."
                
            with st.spinner(spinner_message):
                # Determinar la fuente de datos a usar
                source_type = "local" if is_local else "minio" if is_minio else "gitlab" if is_gitlab else "unknown"
                
                # Ejecutar la carga de datos en un hilo separado usando la función centralizada
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        load_data_by_source,
                        source_type,
                        local_path,
                        minio_client,
                        MINIO_BUCKET,
                        repo_id,
                        branch,
                        token,
                        modules,
                        module_key
                    )
                    data, dates, logs = future.result()
                
                # Actualizar session_state SOLO desde el hilo principal
                st.session_state[data_key] = data
                st.session_state[dates_key] = dates
                st.session_state[f"{module_key}_logs"] = logs
                
                # Mostrar logs después de que el hilo haya terminado
                logs_key = f"{module_key}_logs"
                if logs_key in st.session_state:
                    # Mostrar advertencias
                    for warning in st.session_state[logs_key].get("warnings", []):
                        st.warning(warning)
                    
                    # Mostrar información (opcional, puede ser comentado para reducir la salida)
                    # for info in st.session_state[logs_key].get("info", []):
                    #    st.info(info)
        st.markdown("***") # Separador visual

        # Verificar que las claves existen en session_state antes de llamar a show_func
        if data_key in st.session_state and dates_key in st.session_state:
            try:
                show_func(st.session_state[data_key], st.session_state[dates_key], is_development)
            except Exception as e:
                st.error(f"Error al mostrar el dashboard: {str(e)}")
                st.exception(e)
        else:
            st.error(f"Error: Faltan datos necesarios. data_key: {data_key in st.session_state}, dates_key: {dates_key in st.session_state}")

# Renderizar el footer al final de la página, fuera de las pestañas
render_footer()