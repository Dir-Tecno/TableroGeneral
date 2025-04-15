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
with st.spinner("Cargando datos desde el repositorio..."):
    data, dates = load_data_from_gitlab(repo_id, branch, token, use_local=is_development, local_path=local_path if is_development else None)

# Crear pestañas
tabs = st.tabs(["Banco de la Gente", "CBA Me Capacita", "Programas de Empleo"])

# Pestaña 1: Banco de la Gente
with tabs[0]:
    st.markdown('<div class="tab-subheader">Banco de la Gente</div>', unsafe_allow_html=True)
    with st.spinner("Cargando dashboard de Banco de la Gente..."):
        bco_gente.show_bco_gente_dashboard(data, dates)

# Pestaña 2: CBA ME CAPACITA
with tabs[1]:
    st.markdown('<div class="tab-subheader">CBA ME CAPACITA</div>', unsafe_allow_html=True)
    with st.spinner("Cargando dashboard de CBA ME CAPACITA..."):
        cbamecapacita.show_cba_capacita_dashboard(data, dates)

# Pestaña 3: Programas de Empleo
with tabs[2]:
    st.markdown('<div class="tab-subheader">Programas de Empleo</div>', unsafe_allow_html=True)
    with st.spinner("Cargando dashboard de Programas de Empleo..."):
        empleo.show_empleo_dashboard(data, dates)
