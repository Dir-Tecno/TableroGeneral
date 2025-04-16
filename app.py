from moduls.carga import load_data_from_gitlab
import streamlit as st
import moduls.carga as carga
from moduls import bco_gente, cbamecapacita, empleo
from utils.styles import setup_page
import os

# Configuración de la página
st.set_page_config(page_title="Dashboard Integrado", layout="wide")

# Aplicar estilos y banner desde el módulo de estilos
setup_page()

st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)

# Configuración fija de GitLab
repo_id = "Dir-Tecno/Repositorio-Reportes"
branch = "main"

# Ruta local para desarrollo
local_path = "D:\\DESARROLLO\\DIRTECNO\\EMPLEO\\REPORTES\\TableroGeneral\\Repositorio-Reportes-main"

# Determinar el modo de carga (local o GitLab)
# En un entorno de producción, esta variable podría configurarse mediante una variable de entorno
is_development = os.path.exists(local_path)

# Mostrar información sobre el modo de carga
if is_development:
    st.sidebar.success("Modo de desarrollo: Cargando datos desde carpeta local")
else:
    st.sidebar.info("Modo de producción: Cargando datos desde GitLab")

# Obtener token desde secrets (solo necesario en modo producción)
token = None
if not is_development:
    try:
        token = st.secrets["gitlab"]["token"]
    except Exception as e:
        st.error(f"Error al obtener token: {str(e)}")
        st.stop()

# Nuevo enfoque de carga
# Cargar todos los datos una sola vez
with st.spinner("Cargando datos.."):
    all_data, all_dates = load_data_from_gitlab(repo_id, branch, token, use_local=is_development, local_path=local_path if is_development else None)
    
    if not all_data:
        st.error("No se pudieron cargar los datos. Verifica el token y el ID del repositorio.")
        # Mostrar información de diagnóstico
        st.info("Información de diagnóstico:")
        st.code(f"Repositorio ID: {repo_id}")
        st.code(f"Token configurado: {'Sí' if token else 'No'}")
        st.stop()
    
    # Mapeo de archivos por módulo
    modules = {
        'bco_gente': ['vt_nomina_rep_dpto_localidad.parquet', 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet', 
                       'Detalle_recupero.csv', 'capa_departamentos_2010.geojson', 'LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'],
        'cba_capacita': ['ALUMNOS_X_LOCALIDAD.parquet', 'capa_departamentos_2010.geojson'],
        'empleo': ['VT_REPORTES_PPP_MAS26.parquet', 'vt_empresas_adheridas.parquet','vt_empresas_ARCA.parquet', 'VT_PUESTOS_X_FICHAS.parquet','capa_departamentos_2010.geojson', 'VT_REPORTE_LIQUIDACION_LOCALIDAD.parquet']
    }

    # Filtrar los datos para cada módulo
    bco_gente_data = {k: all_data.get(k) for k in modules['bco_gente'] if k in all_data}
    bco_gente_dates = {k: all_dates.get(k) for k in modules['bco_gente'] if k in all_dates}
    
    cba_capacita_data = {k: all_data.get(k) for k in modules['cba_capacita'] if k in all_data}
    cba_capacita_dates = {k: all_dates.get(k) for k in modules['cba_capacita'] if k in all_dates}
    
    empleo_data = {k: all_data.get(k) for k in modules['empleo'] if k in all_data}
    empleo_dates = {k: all_dates.get(k) for k in modules['empleo'] if k in all_dates}

# Crear pestañas
tabs = st.tabs(["Banco de la Gente", "CBA Me Capacita", "Programas de Empleo"])

# Pestaña 1: Banco de la Gente
with tabs[0]:
    st.markdown('<div class="tab-subheader">Banco de la Gente</div>', unsafe_allow_html=True)
    # Pasar solo los datos específicos del módulo
    bco_gente.show_bco_gente_dashboard(bco_gente_data, bco_gente_dates)

# Pestaña 2: CBA ME CAPACITA
with tabs[1]:
    st.markdown('<div class="tab-subheader">CBA ME CAPACITA</div>', unsafe_allow_html=True)
    # Pasar solo los datos específicos del módulo
    cbamecapacita.show_cba_capacita_dashboard(cba_capacita_data, cba_capacita_dates)

# Pestaña 3: Programas de Empleo
with tabs[2]:
    st.markdown('<div class="tab-subheader">Programas de Empleo</div>', unsafe_allow_html=True)
    # Pasar solo los datos específicos del módulo
    empleo.show_empleo_dashboard(empleo_data, empleo_dates)
