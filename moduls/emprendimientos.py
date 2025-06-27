import streamlit as st
import pandas as pd
import os
from utils.ui_components import display_kpi_row, show_dev_dataframe_info

def show_emprendimientos_dashboard(data=None, dates=None, is_development=False):
    """
    Muestra el dashboard de Emprendimientos. Estructura compatible con app.py y los otros módulos.
    """
    nombre_archivo = 'desarrollo_emprendedor.csv'
    local_path = os.path.join(
        os.path.dirname(__file__),
        '..', 'Repositorio-Reportes-main', nombre_archivo
    )
    local_path = os.path.abspath(local_path)

    df = None
    # 1. Modo desarrollo: cargar localmente si existe
    if os.path.exists(local_path):
        df = pd.read_csv(local_path, sep=';')
        is_development = True
    # 2. Producción: tomar el DataFrame desde data (ya cargado por load_data_from_gitlab)
    elif data and nombre_archivo in data:
        df = data[nombre_archivo]
    else:
        st.error(f"No se encontró el archivo '{nombre_archivo}' ni localmente ni en GitLab.")
        st.write('Archivos disponibles:', list(data.keys()) if data else 'Sin datos')
        return

    df.columns = [col.strip() for col in df.columns]

    columnas_clave = ['CUIL', 'DNI', 'Nombre del Emprendimiento']
    for col in columnas_clave:
        if col not in df.columns:
            st.error(f"Falta la columna '{col}' en el archivo de datos.")
            st.stop()

    # Limpieza básica de datos
    df = df.drop_duplicates(subset=columnas_clave, keep='first')
    df['Edad'] = pd.to_numeric(df['Edad'], errors='coerce')
    df['año'] = pd.to_numeric(df['año'], errors='coerce')

    st.header('Dashboard de Emprendimientos')

    # Filtros
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

    # Aplicar filtros
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

    # KPIs principales
    total_emprendimientos = df_filtrado['Nombre del Emprendimiento'].nunique()
    total_participantes = df_filtrado['CUIL'].nunique()
    promedio_edad = df_filtrado['Edad'].mean()
    total_mujeres = (df_filtrado['Genero'].str.lower() == 'femenino').sum()
    total_hombres = (df_filtrado['Genero'].str.lower() == 'masculino').sum()

    kpi_data = [
        {'title': 'Emprendimientos únicos', 'value_form': total_emprendimientos, 'color_class': 'kpi-primary'},
        {'title': 'Participantes únicos', 'value_form': total_participantes, 'color_class': 'kpi-secondary'},
        {'title': 'Edad promedio', 'value_form': f'{promedio_edad:.1f}' if not pd.isna(promedio_edad) else 'N/A', 'color_class': 'kpi-accent-1'},
        {'title': 'Mujeres', 'value_form': total_mujeres, 'color_class': 'kpi-accent-2'},
        {'title': 'Hombres', 'value_form': total_hombres, 'color_class': 'kpi-accent-3'},
    ]
    display_kpi_row(kpi_data, num_columns=5)

    # Gráfico de rubros
    st.markdown('#### Emprendimientos por Rubro')
    rubros = df_filtrado['Rubro Ejecutado']
    # Filtrar variantes de "Sin informacion"
    sin_info = ['sin informacion', 'sin información', 'sin información ']
    rubros = rubros[~rubros.str.strip().str.lower().isin(sin_info)]
    rubros = rubros.value_counts().head(10)
    st.bar_chart(rubros)

    # Tabla resumen
    st.markdown('### Vista previa de los datos')
    st.dataframe(df_filtrado.head(30))

    # Modo desarrollo
    if is_development:
        show_dev_dataframe_info(data, modulo_nombre='Emprendimientos')