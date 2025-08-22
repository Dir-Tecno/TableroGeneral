import streamlit as st
import pandas as pd
import os
from utils.ui_components import display_kpi_row, show_dev_dataframe_info, show_last_update
from utils.session_helper import safe_session_set

def show_emprendimientos_dashboard(data=None, dates=None, is_development=False):
    """
    Función principal que muestra el dashboard de emprendimientos.
    
    Args:
        data: Diccionario con los dataframes cargados
        dates: Diccionario con las fechas de actualización
        is_development: Booleano que indica si estamos en modo desarrollo
    """
    if data is None:
        st.error("No se pudieron cargar los datos de Emprendimientos.")
        return

    # Mostrar info de desarrollo de los DataFrames
    if is_development:
        # Activar el modo debug para mostrar información detallada
        safe_session_set('debug_mode', True)
        
        # Filtrar el diccionario de datos para evitar objetos de geometría
        filtered_data = {}
        for key, value in data.items():
            # Excluir archivos GeoJSON que causan problemas de representación
            if not key.endswith('.geojson'):
                filtered_data[key] = value
            else:
                # Informar que se ha excluido un archivo GeoJSON
                st.info(f"Archivo GeoJSON excluido de la vista de desarrollo: {key}")
        
        # Mostrar información de los datos filtrados
        show_dev_dataframe_info(filtered_data, modulo_nombre="Emprendimientos")

    # Cargar y preprocesar los datos
    df, has_data = load_and_preprocess_data(data, dates, is_development)
    
    if has_data:
        # Renderizar el dashboard principal
        render_dashboard(df)

def load_and_preprocess_data(data, dates=None, is_development=False):
    """
    Carga y preprocesa los datos necesarios para el dashboard.
    
    Args:
        data: Diccionario de dataframes cargados
        dates: Diccionario de fechas de actualización de los archivos
        is_development: Booleano que indica si estamos en modo desarrollo
        
    Returns:
        Tupla con el dataframe procesado y flag de disponibilidad
    """
    with st.spinner("Cargando y procesando datos de emprendimientos..."):
        nombre_archivo = 'desarrollo_emprendedor.csv'
        
        # Extraer el dataframe necesario
        df = data.get(nombre_archivo)
        has_data = df is not None and not df.empty

        if not has_data:
            st.error(f"No se encontró el archivo '{nombre_archivo}' en los datos cargados.")
            st.write('Archivos disponibles:', list(data.keys()) if data else 'Sin datos')
            return None, False

        # Limpieza y preparación de datos
        df.columns = [col.strip() for col in df.columns]

        # Verificar columnas requeridas
        columnas_clave = ['CUIL', 'DNI', 'Nombre del Emprendimiento']
        for col in columnas_clave:
            if col not in df.columns:
                st.error(f"Falta la columna '{col}' en el archivo de datos.")
                return None, False

        # Limpieza básica de datos
        df = df.drop_duplicates(subset=columnas_clave, keep='first')
        df['Edad'] = pd.to_numeric(df['Edad'], errors='coerce')
        df['año'] = pd.to_numeric(df['año'], errors='coerce')

        return df, True

def render_dashboard(df):
    """
    Renderiza el dashboard principal con los datos procesados.
    
    Args:
        df: DataFrame con los datos de emprendimientos
    """
    st.header('Dashboard de Emprendimientos')

    # Filtros
    df_filtrado = apply_filters(df)

    # KPIs principales
    display_kpis(df_filtrado)

    # Gráfico de rubros
    display_rubros_chart(df_filtrado)

    # Tabla resumen
    st.markdown('### Vista previa de los datos')
    st.dataframe(df_filtrado.head(30))

def apply_filters(df):
    """
    Aplica los filtros seleccionados por el usuario al DataFrame.
    
    Args:
        df: DataFrame original
    Returns:
        DataFrame filtrado
    """
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        anio_sel = st.selectbox('Año', options=['Todos'] + sorted(df['año'].dropna().unique().astype(int).tolist()))
    with col2:
        depto_sel = st.selectbox('Departamento', options=['Todos'] + sorted(df['Departamento'].dropna().unique()))
    with col3:
        Localidad_sel = st.selectbox('Localidad', options=['Todos'] + sorted(df['Localidad'].dropna().unique()))
    with col4:
        etapa_sel = st.selectbox('Etapa del emprendimiento', options=['Todos'] + sorted(df['Etapa del emprendimiento'].dropna().unique()))
    with col5:
        genero_sel = st.selectbox('Género', options=['Todos'] + sorted(df['Genero'].dropna().unique()))

    df_filtrado = df.copy()
    if anio_sel != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['año'] == anio_sel]
    if depto_sel != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['Departamento'] == depto_sel]
    if Localidad_sel != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['Localidad'] == Localidad_sel]
    if etapa_sel != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['Etapa del emprendimiento'] == etapa_sel]
    if genero_sel != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['Genero'] == genero_sel]

    return df_filtrado

def display_kpis(df):
    """
    Muestra los KPIs principales del dashboard.
    
    Args:
        df: DataFrame filtrado
    """
    total_emprendimientos = df['Nombre del Emprendimiento'].nunique()
    total_participantes = df['CUIL'].nunique()
    promedio_edad = df['Edad'].mean()
    total_mujeres = (df['Genero'].str.lower() == 'femenino').sum()
    total_hombres = (df['Genero'].str.lower() == 'masculino').sum()

    kpi_data = [
        {'title': 'Emprendimientos únicos', 'value_form': total_emprendimientos, 'color_class': 'kpi-primary'},
        {'title': 'Participantes únicos', 'value_form': total_participantes, 'color_class': 'kpi-secondary'},
        {'title': 'Edad promedio', 'value_form': f'{promedio_edad:.1f}' if not pd.isna(promedio_edad) else 'N/A', 'color_class': 'kpi-accent-1'},
        {'title': 'Mujeres', 'value_form': total_mujeres, 'color_class': 'kpi-accent-2'},
        {'title': 'Hombres', 'value_form': total_hombres, 'color_class': 'kpi-accent-3'},
    ]
    display_kpi_row(kpi_data, num_columns=5)

def display_rubros_chart(df):
    """
    Muestra el gráfico de rubros.
    
    Args:
        df: DataFrame filtrado
    """
    st.markdown('#### Emprendimientos por Rubro')
    rubros = df['Rubro Ejecutado']
    sin_info = ['sin informacion', 'sin información', 'sin información ']
    rubros = rubros[~rubros.str.strip().str.lower().isin(sin_info)]
    rubros = rubros.value_counts().head(10)
    st.bar_chart(rubros)