from moduls.carga import load_data_from_gitlab
import streamlit as st
import moduls.carga as carga
from moduls import bco_gente, cbamecapacita, empleo
from utils.styles import setup_page
import os
import concurrent.futures

# Configuración de la página
st.set_page_config(page_title="Dashboard Integrado", layout="wide")

# Aplicar estilos y banner desde el módulo de estilos
setup_page()

st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)

# Configuración fija de GitLab
repo_id = "Dir-Tecno/Repositorio-Reportes"
branch = "main"

# Ruta local para desarrollo
local_path = "D:\DESARROLLO\DIRTECNO\EMPLEO\REPORTES\TableroGeneral\Repositorio-Reportes-main"
#local_path = "F:\desarrollo\ReporteSistemas\TableroGeneral\Repositorio-Reportes-main"

# Determinar el modo de carga (local o GitLab)
# En un entorno de producción, esta variable podría configurarse mediante una variable de entorno
is_development = os.path.exists(local_path)

# Mostrar información sobre el modo de carga
if is_development:
    st.success("Modo de desarrollo: Cargando datos desde carpeta local")
else:
    pass

# Obtener token desde secrets (solo necesario en modo producción)
token = None
if not is_development:
    try:
        token = st.secrets["gitlab"]["token"]
    except Exception as e:
        st.error(f"Error al obtener token: {str(e)}")
        st.stop()

# Mapeo de archivos por módulo
modules = {
    'bco_gente': ['vt_nomina_rep_dpto_localidad.parquet', 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet', 
                   'Detalle_recupero.csv', 'capa_departamentos_2010.geojson', 'LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'],
    'cba_capacita': ['VT_INSCRIPCIONES_PRG129.parquet', 'VT_CURSOS_SEDES_GEO.parquet', 'capa_departamentos_2010.geojson'],
    'empleo': ['VT_REPORTES_PPP_MAS26.parquet', 'vt_empresas_adheridas.parquet','vt_empresas_ARCA.parquet', 'VT_PUESTOS_X_FICHAS.parquet','capa_departamentos_2010.geojson', 'VT_REPORTE_LIQUIDACION_LOCALIDAD.parquet']
}



# Crear pestañas
tab_names = ["Banco de la Gente", "CBA Me Capacita", "Programas de Empleo"]
tabs = st.tabs(tab_names)
tab_keys = ['bco_gente', 'cba_capacita', 'empleo']
tab_functions = [
    bco_gente.show_bco_gente_dashboard,
    cbamecapacita.show_cba_capacita_dashboard,
    empleo.show_empleo_dashboard
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

        # Llamar a la función que muestra el dashboard de la pestaña actual, pasando is_development
        show_func(st.session_state[data_key], st.session_state[dates_key], is_development)