from moduls.carga import load_data_from_minio
import streamlit as st
from moduls import bco_gente, cbamecapacita, empleo, emprendimientos 
from utils.styles import setup_page
from utils.ui_components import render_footer
import concurrent.futures
from minio import Minio

# Configuración de la página
st.set_page_config(page_title="Dashboard Integrado", layout="wide")

# Aplicar estilos y banner desde el módulo de estilos
setup_page()

st.markdown('<div class="main-header">Tablero General de Reportes</div>', unsafe_allow_html=True)

# Mapeo de archivos por módulo
modules = {
    'bco_gente': ['vt_nomina_rep_dpto_localidad.parquet', 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet', 
                   'Detalle_recupero.csv', 'capa_departamentos_2010.geojson', 'LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'],
    'cba_capacita': ['VT_INSCRIPCIONES_PRG129.parquet', 'VT_CURSOS_SEDES_GEO.parquet', 'capa_departamentos_2010.geojson'],
    'empleo': ['ppp_jesi.xlsx','mas26_jesi.xlsx','LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt','LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - DATOS_CENSALES.txt','VT_REPORTES_PPP_MAS26.parquet', 'vt_empresas_adheridas.parquet','vt_empresas_ARCA.parquet', 'VT_PUESTOS_X_FICHAS.parquet','capa_departamentos_2010.geojson', 'VT_REPORTE_LIQUIDACION_LOCALIDAD.parquet'],
    'empredimientos': ['desarrollo_emprendedor.xlsx']
}

# Configuración MinIO
MINIO_ENDPOINT = "5.161.118.67:7003"
MINIO_ACCESS_KEY = "dirtecno"
MINIO_SECRET_KEY = "dirtecnon0r3cu3rd0"
MINIO_BUCKET = "repositorio-dashboard"  

# Cliente MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Listar objetos en el bucket de MinIO (nuevo código integrado)
for obj in minio_client.list_objects(MINIO_BUCKET, recursive=True):
    print(obj.object_name)

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
            with st.spinner("Cargando datos desde MinIO..."):
                def load_only_data():
                    all_data, all_dates = load_data_from_minio(
                        minio_client, MINIO_BUCKET, modules
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
            try:
                show_func(st.session_state[data_key], st.session_state[dates_key], False)
            except Exception as e:
                st.error(f"Error al mostrar el dashboard: {str(e)}")
                st.exception(e)
        else:
            st.error(f"Error: Faltan datos necesarios. data_key: {data_key in st.session_state}, dates_key: {dates_key in st.session_state}")

# Renderizar el footer al final de la página, fuera de las pestañas
render_footer()