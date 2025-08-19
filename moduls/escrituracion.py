# --- IMPORTS ---
import pandas as pd
from datetime import datetime
import streamlit as st
from utils.ui_components import display_kpi_row
from utils.styles import apply_styles

# --- CONFIGURACIÓN ---
COLUMNS = [
    'Departamento', 'Localidad', 'Seccional', 'Barrio', 'Mza. Plano', 'Lote Plano', 'Mza. Oficial', 'Lote oficial',
    'Beneficiario', 'DNI', 'Telefono', 'COTITULAR Nombre y Apellido', 'COTITULAR DNI', 'COTITULAR Telefono',
    'Nomenclatura Catastral', 'Cuenta Rentas', 'Matricula', 'Fecha Ingreso Colegio de Escribanos',
    'Fecha de Sorteo', 'Fecha de Aceptacion', 'Escribano Designado', 'Contacto Escribano', 'Fecha de Firma',
    'Factura', 'Observaciones del Colegio', 'Fecha de Ingreso al Registro', 'NUMERO DE REGISTRO', 'Estado (diario)',
    'Fecha de envío PT digital', 'Fecha de Entrega', 'Estado', 'Estado de Firma', 'MontoEjecutado',
    'Testimonios Digitales', 'Observación', 'Fecha Envio Planilla', 'Fecha Facturación', 'Fecha de Pago'
]

# --- FUNCIONES DE DATOS OPTIMIZADAS ---
def calcular_diferencia_dias_vectorizado(df, fecha1, fecha2):
    """
    Calcula la diferencia en días entre dos columnas de fechas de manera vectorizada.
    """
    fechas1 = pd.to_datetime(df[fecha1], format='%d/%m/%Y', errors='coerce')
    fechas2 = pd.to_datetime(df[fecha2], format='%d/%m/%Y', errors='coerce')
    return (fechas2 - fechas1).dt.days

def generar_reporte(df):
    """
    Aplica semaforización entre pares de fechas relevantes y agrega columnas de diferencia en días.
    """
    pares_fechas = [
        ('Fecha Ingreso Colegio de Escribanos', 'Fecha de Sorteo', 'diferencia_ingreso_sorteo'),
        ('Fecha de Sorteo', 'Fecha de Aceptacion', 'diferencia_sorteo_aceptacion'),
        ('Fecha de Aceptacion', 'Fecha de Firma', 'diferencia_aceptacion_firma'),
        ('Fecha de Firma', 'Fecha de Ingreso al Registro', 'diferencia_firma_ingreso'),
        ('Fecha de Ingreso al Registro', 'Fecha de envío PT digital', 'diferencia_ingreso_testimonio')
    ]

    for fecha1, fecha2, diff_col in pares_fechas:
        df[diff_col] = calcular_diferencia_dias_vectorizado(df, fecha1, fecha2).fillna(0).astype(int)
    return df

# --- DASHBOARD PRINCIPAL OPTIMIZADO ---
@st.cache_data
def cargar_datos(sheet_url, creds_json):
    """
    Conecta a Google Sheets y retorna un DataFrame con los datos, usando caché para mejorar el rendimiento.
    """
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url).sheet1
    data = sheet.get_all_records()
    return pd.DataFrame(data)

def mostrar_dashboard(df):
    """
    Renderiza el dashboard principal con filtros horizontales, tarjetas KPI y tablas estilizadas con filas pintadas.
    """
    apply_styles()

    if df.empty:
        st.info("Carga los datos para ver el reporte.")
        return

    df = generar_reporte(df)

    # Filtros horizontales
    col1, col2, col3, col4, col5 = st.columns(5)
    depto_sel = col1.selectbox('Departamento', ['Todos'] + sorted(df['Departamento'].dropna().unique()), key='filtro_departamento')
    loc_sel = col2.selectbox('Localidad', ['Todos'] + sorted(df['Localidad'].dropna().unique()), key='filtro_localidad')
    barrio_sel = col3.selectbox('Barrio', ['Todos'] + sorted(df['Barrio'].dropna().unique()), key='filtro_barrio')
    estado_sel = col4.selectbox('Estado', ['Todos'] + sorted(df['Estado'].dropna().unique()), key='filtro_estado')
    dni_busqueda = col5.text_input('DNI', key='filtro_dni')

    filtros = {
        'Departamento': depto_sel,
        'Localidad': loc_sel,
        'Barrio': barrio_sel,
        'Estado': estado_sel
    }

    for columna, valor in filtros.items():
        if valor != 'Todos':
            df = df[df[columna] == valor]

    if dni_busqueda:
        df = df[df['DNI'].astype(str).str.contains(dni_busqueda, na=False)]

    # Tarjetas KPI de estados
    estados_disponibles = df['Estado'].value_counts().to_dict()
    kpi_data = [
        {
            "title": estado,
            "value_form": cantidad,
            "color_class": f'kpi-accent-{i % 5 + 1}',
            "tooltip": f"Cantidad de registros en estado '{estado}'"
        }
        for i, (estado, cantidad) in enumerate(estados_disponibles.items())
    ]
    if kpi_data:
        display_kpi_row(kpi_data, num_columns=len(kpi_data))
    else:
        st.info("No hay registros para los filtros seleccionados.")

    # Tablas por cada comparación de fechas con estilos personalizados
    st.markdown('---')

    comparaciones = [
        ('Fecha Ingreso Colegio de Escribanos', 'Fecha de Sorteo', 'diferencia_ingreso_sorteo', 'Ingreso Colegio vs Sorteo'),
        ('Fecha de Sorteo', 'Fecha de Aceptacion', 'diferencia_sorteo_aceptacion', 'Sorteo vs Aceptación'),
        ('Fecha de Aceptacion', 'Fecha de Firma', 'diferencia_aceptacion_firma', 'Aceptación vs Firma'),
        ('Fecha de Firma', 'Fecha de Ingreso al Registro', 'diferencia_firma_ingreso', 'Firma vs Ingreso Diario'),
        ('Fecha de Ingreso al Registro', 'Fecha de envío PT digital', 'diferencia_ingreso_testimonio', 'Ingreso Diario vs Testimonio')
    ]

    claves = ['Departamento', 'Localidad', 'Barrio', 'Beneficiario', 'DNI']

    for f1, f2, diff_col, titulo in comparaciones:
        st.markdown(f'### {titulo}')
        columnas_tabla = claves + [f1, f2, diff_col]
        tabla = df[columnas_tabla].copy()

        # Filtros de color horizontales
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            color_filtro = st.radio(
                '',  
                options=['Todos', 'Verde', 'Amarillo', 'Rojo'],
                key=f'color_filtro_{titulo}',
                horizontal=True
            )

        if color_filtro == 'Verde':
            tabla = tabla[tabla[diff_col] <= 3]
        elif color_filtro == 'Amarillo':
            tabla = tabla[(tabla[diff_col] > 3) & (tabla[diff_col] <= 7)]
        elif color_filtro == 'Rojo':
            tabla = tabla[tabla[diff_col] > 7]

        def pintar_fila(row):
            dias = row[diff_col]
            if dias is None or dias < 0:
                return ['background-color: #f0f0f0'] * len(row)
            elif dias <= 3:
                return ['background-color: #b6fcd5'] * len(row)  # Verde
            elif dias <= 7:
                return ['background-color: #fff7b2'] * len(row)  # Amarillo
            else:
                return ['background-color: #ffb2b2'] * len(row)

        styled_table = tabla.style.apply(pintar_fila, axis=1)

        st.dataframe(styled_table, use_container_width=True)
        
# --- DASHBOARD WRAPPER ---
def show_escrituracion_dashboard(data=None, dates=None, is_development=False):
    """
    Función principal para mostrar el dashboard de escrituración.
    """
    if dates:
        st.info(f"Última actualización: {dates}")
    sheet_url = 'https://docs.google.com/spreadsheets/d/1V9vXwMQJjd4kLdJZQncOSoWggQk8S7tBKxbOSEIUoQ8/edit#gid=1593263408'
    creds_json = st.secrets.get('ESCRITURACION_CREDS_JSON', 'credenciales.json')
    try:
        df = cargar_datos(sheet_url, creds_json)
        if df is None or df.empty:
            st.warning('No hay datos para mostrar.')
            return
        mostrar_dashboard(df)
    except Exception as e:
        st.error(f"Error al cargar datos desde Google Sheets: {e}")
