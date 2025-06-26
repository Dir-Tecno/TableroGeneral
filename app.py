from moduls.carga import load_data_from_gitlab
import streamlit as st
import moduls.carga as carga
from moduls import bco_gente, cbamecapacita, empleo, emprendimientos 
from utils.styles import setup_page
from utils.ui_components import render_footer
import os
import concurrent.futures
import sys

# Determinar si estamos en producción (no en desarrollo local)
is_production = not os.path.exists(os.path.join(os.path.dirname(__file__), "Repositorio-Reportes-main"))

# Deshabilitar solo la recarga automática en producción, pero mantener la actualización de caché
if is_production:
    # Esta es una forma de configurar Streamlit programáticamente
    # Equivalente a server.runOnSave = false en .streamlit/config.toml
    os.environ['STREAMLIT_SERVER_RUN_ON_SAVE'] = 'false'
    # No desactivamos clear_on_change para permitir que las recargas manuales (F5) obtengan datos frescos

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Integrado", 
    layout="wide"
)

# Aplicar estilos y banner desde el módulo de estilos
setup_page()

st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)

# Configuración fija de GitLab
repo_id = "Dir-Tecno/Repositorio-Reportes"
branch = "main"

# Ruta local para desarrollo
local_path = r"D:\DESARROLLO\DIRTECNO\EMPLEO\REPORTES\TableroGeneral\Repositorio-Reportes-main"
#local_path = "/mnt/d/DESARROLLO/DIRTECNO/EMPLEO/REPORTES/TableroGeneral/Repositorio-Reportes-main"

# Ya determinamos is_production arriba, ahora definimos is_development para mantener compatibilidad
is_development = not is_production

# Mostrar información sobre el modo de carga solo en desarrollo
if is_development:
    st.success("Modo de desarrollo: Cargando datos desde carpeta local")

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
    'bco_gente': ['VT_CUMPLIMIENTO_FORMULARIOS.parquet','vt_nomina_rep_dpto_localidad.parquet', 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet', 
                   'Detalle_recupero.csv', 'capa_departamentos_2010.geojson', 'LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'],
    'cba_capacita': ['VT_ALUMNOS_EN_CURSOS.parquet','VT_INSCRIPCIONES_PRG129.parquet', 'VT_CURSOS_SEDES_GEO.parquet', 'capa_departamentos_2010.geojson'],
    'empleo': ['ppp_jesi.xlsx','mas26_jesi.xlsx','LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt','LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - DATOS_CENSALES.txt','VT_REPORTES_PPP_MAS26.parquet', 'vt_empresas_adheridas.parquet','vt_empresas_ARCA.parquet', 'VT_PUESTOS_X_FICHAS.parquet','capa_departamentos_2010.geojson', 'VT_REPORTE_LIQUIDACION_LOCALIDAD.parquet'],
    'empredimientos': ['desarrollo_emprendedor.xlsx']
}



# Crear pestañas
tab_names = ["CBA Me Capacita", "Banco de la Gente",  "Programas de Empleo","Empredimientos"]
tabs = st.tabs(tab_names)
tab_keys = ['cba_capacita', 'bco_gente', 'empleo','empredimientos']
tab_functions = [
    cbamecapacita.show_cba_capacita_dashboard,
    bco_gente.show_bco_gente_dashboard,
    empleo.show_empleo_dashboard,
    emprendimientos.show_emprendimientos_dashboard
]

for idx, tab in enumerate(tabs):
    with tab:
        module_key = tab_keys[idx]
        show_func = tab_functions[idx]
        st.markdown(f'<div class="tab-subheader">{tab_names[idx]}</div>', unsafe_allow_html=True)
        data_key = f"{module_key}_data"
        dates_key = f"{module_key}_dates"
        if data_key not in st.session_state or dates_key not in st.session_state:
            with st.spinner("Cargando datos..."):
                def load_only_data():
                    all_data, all_dates = load_data_from_gitlab(
                        repo_id, branch, token, 
                        use_local=is_development, 
                        local_path=local_path if is_development else None
                    )
                    data = {k: all_data.get(k) for k in modules[module_key] if k in all_data}
                    dates = {k: all_dates.get(k) for k in modules[module_key] if k in all_dates}
                    return data, dates
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(load_only_data)
                    data, dates = future.result()
                st.session_state[data_key] = data
                st.session_state[dates_key] = dates
        st.markdown("***") # Separador visual

        # Verificar que las claves existen en session_state antes de llamar a show_func
        if data_key in st.session_state and dates_key in st.session_state:
            # Llamar a la función que muestra el dashboard de la pestaña actual
            try:
                show_func(st.session_state[data_key], st.session_state[dates_key], is_development)
            except Exception as e:
                st.error(f"Error al mostrar el dashboard: {str(e)}")
                st.exception(e)  # Muestra el traceback completo
        else:
            st.error(f"Error: Faltan datos necesarios. data_key: {data_key in st.session_state}, dates_key: {dates_key in st.session_state}")

# Renderizar el footer al final de la página, fuera de las pestañas
render_footer()