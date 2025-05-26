import streamlit as st
import pandas as pd
import plotly.express as px
from utils.ui_components import display_kpi_row
from utils.data_cleaning import clean_thousand_separator, convert_decimal_separator
import geopandas as gpd
import json

def create_cbamecapacita_kpi(resultados):
    """
    Crea los KPIs específicos para el módulo CBA Me Capacita.
    
    Args:
        resultados (dict): Diccionario con los resultados de conteo por categoría
    Returns:
        list: Lista de diccionarios con datos de KPI para CBA Me Capacita
    """
    kpis = [
        {
            "title": "POSTULANTES",
            "value_form": f"{resultados.get('Postulantes', 0):,}".replace(',', '.'),
            "color_class": "kpi-primary",
            "delta": "",
            "delta_color": "#d4f7d4"
        },
        {
            "title": "CURSOS ACTIVOS",
            "value_form": f"{resultados.get('Cursos Activos', 0):,}".replace(',', '.'),
            "color_class": "kpi-secondary",
            "delta": "",
            "delta_color": "#d4f7d4"
        },
        {
            "title": "CAPACITACIONES ELEGIDAS",
            "value_form": f"{resultados.get('Capacitaciones Elegidas', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-2",
            "delta": "",
            "delta_color": "#d4f7d4"
        }
    ]
    return kpis

def load_and_preprocess_data(data):
    """
    Carga y preprocesa los datos principales del dashboard CBA ME CAPACITA.
    - Limpia separadores de miles en columnas numéricas.
    - Devuelve los DataFrames listos para usar.
    """
    # Cargar DataFrames principales
    df_postulantes = None
    df_cursos = None
    if isinstance(data, dict):
        df_postulantes = data.get("VT_INSCRIPCIONES_PRG129.parquet")
        df_cursos = data.get("VT_CURSOS_SEDES_GEO.parquet")
    elif isinstance(data, list):
        for df in data:
            if "CUIL" in df.columns:
                df_postulantes = df
            if "ID_PLANIFICACION" in df.columns and "N_CURSO" in df.columns:
                df_cursos = df
    # Limpieza de separador de miles en ambos DataFrames
    df_postulantes = clean_thousand_separator(df_postulantes)
    df_cursos = clean_thousand_separator(df_cursos)

    # Cruce solicitado: agregar CUIL de postulantes a cursos directamente en df_cursos
    if df_cursos is not None and df_postulantes is not None:
        # Asegurar que ID_CERTIFICACION e ID_CAPACITACION sean enteros si existen
        if 'ID_CAPACITACION' in df_postulantes.columns:
            df_postulantes['ID_CAPACITACION'] = pd.to_numeric(df_postulantes['ID_CAPACITACION'], errors='coerce').fillna(0).astype(int)
        if 'ID_CURSO' in df_cursos.columns and 'ID_CAPACITACION' in df_postulantes.columns and 'CUIL' in df_postulantes.columns:
            # Agrupar por ID_CAPACITACION y contar CUILs no nulos
            cuil_count = (
                df_postulantes.groupby('ID_CAPACITACION')['CUIL']
                .apply(lambda x: x.notnull().sum())
                .reset_index()
                .rename(columns={'CUIL': 'POSTULACIONES'})
            )
            df_cursos = df_cursos.merge(
                cuil_count,
                how='left',
                left_on='ID_CURSO',
                right_on='ID_CAPACITACION'
            )
        if 'POSTULACIONES' in df_cursos.columns:
            df_cursos['POSTULACIONES'] = pd.to_numeric(df_cursos['POSTULACIONES'], errors='coerce').fillna(0).astype(int)
    
    return df_postulantes, df_cursos

def show_cba_capacita_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de CBA ME CAPACITA.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
    if data is None:
        st.error("No se pudieron cargar los datos de CBA ME CAPACITA.")
        return

    # Mostrar columnas en modo desarrollo
    from utils.ui_components import show_dev_dataframe_info
    if is_development:
        show_dev_dataframe_info(data, modulo_nombre="CBA Me Capacita")

    

    # --- Usar función de carga y preprocesamiento ---
    df_postulantes, df_cursos = load_and_preprocess_data(data)



    # KPIs reales usando VT_INSCRIPCIONES_PRG129.parquet (postulantes) y VT_CURSOS_SEDES_GEO.parquet (cursos)
    total_postulantes = df_postulantes["CUIL"].nunique() if df_postulantes is not None else 0
    cursos_activos = df_cursos["ID_PLANIFICACION"].nunique() if df_cursos is not None else 0
    total_capacitaciones = df_postulantes["ID_CAPACITACION"].nunique() if df_postulantes is not None and "ID_CAPACITACION" in df_postulantes.columns else 0
    # Mostrar información de actualización de datos
    from utils.ui_components import show_last_update
    show_last_update(dates, 'VT_INSCRIPCIONES_PRG129.parquet')
    
    # Crear un diccionario con los resultados para pasarlo a la función de KPIs
    resultados = {
        "Postulantes": total_postulantes,
        "Cursos Activos": cursos_activos,
        "Capacitaciones Elegidas": total_capacitaciones
    }
    
    # Usar la función para crear los KPIs
    kpi_data = create_cbamecapacita_kpi(resultados)
    display_kpi_row(kpi_data)

    # Crear pestañas para diferentes vistas
    tab1, tab2 = st.tabs(["Alumnos", "Cursos"])
    
    with tab1:
        st.subheader("Análisis de Postulantes")
        if df_postulantes is not None and not df_postulantes.empty:
            # Filtros interactivos
            col1, col2 = st.columns(2)
            with col1:
                departamentos = sorted(df_postulantes['N_DEPARTAMENTO'].dropna().unique())
                selected_dpto = st.selectbox("Departamento:", ["Todos"] + departamentos)
            with col2:
                localidades = sorted(df_postulantes['N_LOCALIDAD'].dropna().unique())
                selected_loc = st.selectbox("Localidad:", ["Todos"] + localidades)
            df_filtered = df_postulantes.copy()
            if selected_dpto != "Todos":
                df_filtered = df_filtered[df_filtered['N_DEPARTAMENTO'] == selected_dpto]
            if selected_loc != "Todos":
                df_filtered = df_filtered[df_filtered['N_LOCALIDAD'] == selected_loc]
            # 1. Cantidad de Postulaciones por N_DEPARTAMENTO y N_LOCALIDAD
            st.subheader("Cantidad de Postulaciones por Departamento y Localidad")
            df_group = df_filtered.groupby(['N_DEPARTAMENTO','N_LOCALIDAD']).size().reset_index(name='Cantidad')
            st.dataframe(df_group, use_container_width=True, hide_index=True)
            # 2. Distribución por rangos de edad
            st.subheader("Distribución por Rangos de Edad")
            today = pd.Timestamp.today()
            if 'FEC_NACIMIENTO' in df_filtered.columns:
                df_filtered = df_filtered.copy()
                df_filtered['FEC_NACIMIENTO'] = pd.to_datetime(df_filtered['FEC_NACIMIENTO'], errors='coerce')
                df_filtered['EDAD'] = ((today - df_filtered['FEC_NACIMIENTO']).dt.days // 365).astype('Int64')
                bins = [0, 17, 29, 39, 49, 59, 69, 200]
                labels = ['<18', '18-29', '30-39', '40-49', '50-59', '60-69','70+']
                df_filtered['RANGO_EDAD'] = pd.cut(df_filtered['EDAD'], bins=bins, labels=labels, right=True)
                edad_group = df_filtered['RANGO_EDAD'].value_counts().sort_index().reset_index()
                edad_group.columns = ['Rango de Edad','Cantidad']
                fig_edad = px.bar(edad_group, x='Rango de Edad', y='Cantidad', title='Distribución por Rango de Edad', text_auto=True, color='Rango de Edad', color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_edad, use_container_width=True)
            else:
                st.info("No se encontró la columna FEC_NACIMIENTO para calcular edades.")
            # 3. TOP 10 de CAPACITACION elegida
            st.subheader("Top 10 de Capacitaciones Elegidas")
            if 'CAPACITACION' in df_filtered.columns:
                top_cap = df_filtered['CAPACITACION'].value_counts().head(10).reset_index()
                top_cap.columns = ['Capacitación','Cantidad']
                fig_topcap = px.bar(top_cap, x='Capacitación', y='Cantidad', title='Top 10 de Capacitaciones', text_auto=True)
                st.plotly_chart(fig_topcap, use_container_width=True)
            else:
                st.info("No se encontró la columna CAPACITACION para el top de capacitaciones.")
            # 4. Tres tortas: EDUCACION, TIPO_TRABAJO y SEXO
            st.subheader("Distribución por Nivel Educativo, Tipo de Trabajo y Género")
            cols = st.columns(3)
            
            # Generar colores para los gráficos
            color_sequence_edu = px.colors.qualitative.Pastel
            color_sequence_trabajo = px.colors.qualitative.Set2
            color_sequence_sexo = px.colors.qualitative.Vivid
            
            # 4.1 Gráfico de Nivel Educativo
            if 'EDUCACION' in df_filtered.columns:
                edu_group = df_filtered['EDUCACION'].value_counts().reset_index()
                edu_group.columns = ['Educación','Cantidad']
                
                # Crear un diccionario de colores para cada nivel educativo
                edu_colors = {}
                for i, nivel in enumerate(edu_group['Educación']):
                    color_idx = i % len(color_sequence_edu)
                    edu_colors[nivel] = color_sequence_edu[color_idx]
                
                fig_edu = px.pie(edu_group, names='Educación', values='Cantidad', title='Nivel Educativo',
                                 color='Educación', color_discrete_map=edu_colors)
                cols[0].plotly_chart(fig_edu, use_container_width=True)
                
                # Tabla TOP 10 cursos por cada Nivel Educativo
                if 'CAPACITACION' in df_filtered.columns:
                    cols[0].markdown('**Top 10 cursos más seleccionados por Nivel Educativo:**')
                    for nivel in df_filtered['EDUCACION'].dropna().unique():
                        top_cursos = (
                            df_filtered[df_filtered['EDUCACION'] == nivel]
                            .groupby('CAPACITACION')
                            .size()
                            .reset_index(name='Cantidad')
                            .sort_values('Cantidad', ascending=False)
                            .head(10)
                        )
                        # Usar el color correspondiente al nivel educativo
                        color = edu_colors.get(nivel, '#f0f2f6')
                        # Barra visual sin texto antes del expander, usando el CSS ajustado por el usuario
                        cols[0].markdown(
                            f'<hr style="border-top: none; border-right: none; border-bottom: none; border-left: 12px solid {color}; height: 14px; width: 32px; margin: 0px 0px -71px; display: inline-block; vertical-align: middle;">',
                            unsafe_allow_html=True
                        )
                        # Crear el expander con texto normal
                        with cols[0].expander(f"Nivel Educativo: {nivel}", expanded=False):
                            # Crear gráfico de barras horizontal con Plotly
                            if not top_cursos.empty:
                                # Limitar el texto de los cursos para mejor visualización
                                top_cursos['CAPACITACION_CORTO'] = top_cursos['CAPACITACION'].apply(lambda x: x[:40] + '...' if len(x) > 40 else x)
                                
                                # Crear el gráfico de barras horizontales
                                fig = px.bar(
                                    top_cursos,
                                    x='Cantidad',
                                    y='CAPACITACION_CORTO',
                                    orientation='h',
                                    color_discrete_sequence=[color],
                                    text='Cantidad',  # Mostrar la cantidad dentro de la barra
                                    height=400
                                )
                                
                                # Personalizar el gráfico
                                fig.update_traces(
                                    textposition='inside',
                                    textfont=dict(color='white'),
                                    hovertemplate='<b>%{y}</b><br>Cantidad: %{x}'
                                )
                                
                                fig.update_layout(
                                    margin=dict(l=10, r=10, t=10, b=10),
                                    xaxis_title=None,
                                    yaxis_title=None,
                                    yaxis=dict(autorange="reversed")  # Invertir el eje Y para que el mayor valor esté arriba
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("No hay datos disponibles para este nivel educativo.")
                else:
                    cols[0].info("No se encontró la columna CAPACITACION para mostrar los cursos.")
            else:
                cols[0].info("No se encontró la columna EDUCACION.")
            
            # 4.2 Gráfico de Tipo de Trabajo
            if 'TIPO_TRABAJO' in df_filtered.columns:
                tipo_group = df_filtered['TIPO_TRABAJO'].value_counts().reset_index()
                tipo_group.columns = ['Tipo de Trabajo','Cantidad']
                
                # Crear un diccionario de colores para cada tipo de trabajo
                trabajo_colors = {}
                for i, tipo in enumerate(tipo_group['Tipo de Trabajo']):
                    color_idx = i % len(color_sequence_trabajo)
                    trabajo_colors[tipo] = color_sequence_trabajo[color_idx]
                
                fig_tipo = px.pie(tipo_group, names='Tipo de Trabajo', values='Cantidad', title='Tipo de Trabajo',
                                  color='Tipo de Trabajo', color_discrete_map=trabajo_colors)
                cols[1].plotly_chart(fig_tipo, use_container_width=True)
                
                # Tabla TOP 10 cursos por cada Tipo de Trabajo
                if 'CAPACITACION' in df_filtered.columns:
                    cols[1].markdown('**Top 10 cursos más seleccionados por Tipo de Trabajo:**')
                    for tipo in df_filtered['TIPO_TRABAJO'].dropna().unique():
                        top_cursos = (
                            df_filtered[df_filtered['TIPO_TRABAJO'] == tipo]
                            .groupby('CAPACITACION')
                            .size()
                            .reset_index(name='Cantidad')
                            .sort_values('Cantidad', ascending=False)
                            .head(10)
                        )
                        # Usar el color correspondiente al tipo de trabajo
                        color = trabajo_colors.get(tipo, '#f0f2f6')
                        # Barra visual sin texto antes del expander, usando el CSS ajustado por el usuario
                        cols[1].markdown(
                            f'<hr style="border-top: none; border-right: none; border-bottom: none; border-left: 12px solid {color}; height: 14px; width: 32px; margin: 0px 0px -71px; display: inline-block; vertical-align: middle;">',
                            unsafe_allow_html=True
                        )
                        # Crear el expander con texto normal
                        with cols[1].expander(f"Tipo de Trabajo: {tipo}", expanded=False):
                            # Crear gráfico de barras horizontal con Plotly
                            if not top_cursos.empty:
                                # Limitar el texto de los cursos para mejor visualización
                                top_cursos['CAPACITACION_CORTO'] = top_cursos['CAPACITACION'].apply(lambda x: x[:40] + '...' if len(x) > 40 else x)
                                
                                # Crear el gráfico de barras horizontales
                                fig = px.bar(
                                    top_cursos,
                                    x='Cantidad',
                                    y='CAPACITACION_CORTO',
                                    orientation='h',
                                    color_discrete_sequence=[color],
                                    text='Cantidad',  # Mostrar la cantidad dentro de la barra
                                    height=400
                                )
                                
                                # Personalizar el gráfico
                                fig.update_traces(
                                    textposition='inside',
                                    textfont=dict(color='white'),
                                    hovertemplate='<b>%{y}</b><br>Cantidad: %{x}'
                                )
                                
                                fig.update_layout(
                                    margin=dict(l=10, r=10, t=10, b=10),
                                    xaxis_title=None,
                                    yaxis_title=None,
                                    yaxis=dict(autorange="reversed")  # Invertir el eje Y para que el mayor valor esté arriba
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("No hay datos disponibles para este tipo de trabajo.")
                else:
                    cols[1].info("No se encontró la columna CAPACITACION para mostrar los cursos.")
            else:
                cols[1].info("No se encontró la columna TIPO_TRABAJO.")
                
            # 4.3 NUEVO: Gráfico de Sexo
            if 'ID_SEXO' in df_filtered.columns:
                
                
                
                # Definir mapeo para todos los posibles formatos
                sexo_map = {
                    1: 'Varón', '1': 'Varón',
                    2: 'Mujer', '2': 'Mujer',
                    3: 'Ambos', '3': 'Ambos',
                    4: 'No Binario', '4': 'No Binario'
                }
                
                # Aplicar el mapeo de manera más robusta
                # Primero convertir a string para asegurar compatibilidad
                df_filtered['SEXO'] = df_filtered['ID_SEXO'].astype(str)
                
                # Luego aplicar el reemplazo
                for key, value in sexo_map.items():
                    df_filtered.loc[df_filtered['SEXO'] == str(key), 'SEXO'] = value
                
                # Marcar los valores no mapeados como "No especificado"
                unmapped = ~df_filtered['SEXO'].isin(sexo_map.values())
                df_filtered.loc[unmapped, 'SEXO'] = 'No especificado'
                
                sexo_group = df_filtered['SEXO'].value_counts().reset_index()
                sexo_group.columns = ['Sexo','Cantidad']
                
                # Colores para el gráfico de sexo
                sexo_colors = {
                    'Varón': '#2196F3',  # Azul
                    'Mujer': '#E91E63',   # Rosa
                    'No especificado': '#9E9E9E'  # Gris
                }
                
                fig_sexo = px.pie(sexo_group, names='Sexo', values='Cantidad', title='Distribución por Género',
                                 color='Sexo', color_discrete_map=sexo_colors)
                cols[2].plotly_chart(fig_sexo, use_container_width=True)
                
                # Tabla TOP 10 cursos por cada Sexo
                if 'CAPACITACION' in df_filtered.columns:
                    cols[2].markdown('**Top 10 cursos más seleccionados por Género:**')
                    for sexo in df_filtered['SEXO'].dropna().unique():
                        top_cursos = (
                            df_filtered[df_filtered['SEXO'] == sexo]
                            .groupby('CAPACITACION')
                            .size()
                            .reset_index(name='Cantidad')
                            .sort_values('Cantidad', ascending=False)
                            .head(10)
                        )
                        # Usar el color correspondiente al sexo
                        color = sexo_colors.get(sexo, '#9E9E9E')
                        # Barra visual sin texto antes del expander, usando el CSS ajustado por el usuario
                        cols[2].markdown(
                            f'<hr style="border-top: none; border-right: none; border-bottom: none; border-left: 12px solid {color}; height: 14px; width: 32px; margin: 0px 0px -71px; display: inline-block; vertical-align: middle;">',
                            unsafe_allow_html=True
                        )
                        # Crear el expander con texto normal
                        with cols[2].expander(f"Género: {sexo}", expanded=False):
                            # Crear gráfico de barras horizontal con Plotly
                            if not top_cursos.empty:
                                # Limitar el texto de los cursos para mejor visualización
                                top_cursos['CAPACITACION_CORTO'] = top_cursos['CAPACITACION'].apply(lambda x: x[:40] + '...' if len(x) > 40 else x)
                                
                                # Crear el gráfico de barras horizontales
                                fig = px.bar(
                                    top_cursos,
                                    x='Cantidad',
                                    y='CAPACITACION_CORTO',
                                    orientation='h',
                                    color_discrete_sequence=[color],
                                    text='Cantidad',  # Mostrar la cantidad dentro de la barra
                                    height=400
                                )
                                
                                # Personalizar el gráfico
                                fig.update_traces(
                                    textposition='inside',
                                    textfont=dict(color='white'),
                                    hovertemplate='<b>%{y}</b><br>Cantidad: %{x}'
                                )
                                
                                fig.update_layout(
                                    margin=dict(l=10, r=10, t=10, b=10),
                                    xaxis_title=None,
                                    yaxis_title=None,
                                    yaxis=dict(autorange="reversed")  # Invertir el eje Y para que el mayor valor esté arriba
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("No hay datos disponibles para este sexo.")
                else:
                    cols[2].info("No se encontró la columna CAPACITACION para mostrar los cursos.")
            else:
                cols[2].info("No se encontró la columna ID_SEXO.")
        else:
            st.warning("No hay datos de postulantes disponibles para mostrar reportes de alumnos.")
    
    with tab2:
        st.markdown("## Sector Productivos por Departamento")
        # Botón para descargar el Excel con columnas seleccionadas
        # Mostrar botón solo si existe el DataFrame
        if df_cursos is not None:
            import io
            columnas_exportar = [
                "N_INSTITUCION",
                "N_CURSO",
                "HORA_INICIO",
                "HORA_FIN",
                "N_SECTOR_PRODUCTIVO",
                "N_SEDE",
                "CONVENIO_MUNICIPIO_COMUNA",
                "N_DEPARTAMENTO",
                "N_LOCALIDAD",
                "POSTULACIONES"
            ]
            # Filtrar solo columnas existentes
            columnas_existentes = [col for col in columnas_exportar if col in df_cursos.columns]
            df_export = df_cursos[columnas_existentes].copy()
            buffer = io.BytesIO()
            df_export.to_excel(buffer, index=False)
            st.download_button(
                label="Descargar Excel de Cursos (columnas seleccionadas)",
                data=buffer.getvalue(),
                file_name="cursos_sector_productivo.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Obtener GeoJSON de departamentos
            geojson_departamentos = None
            if isinstance(data, dict):
                geojson_departamentos = data.get("capa_departamentos_2010.geojson")
            elif isinstance(data, list):
                for df in data:
                    if isinstance(df, dict) and "features" in df:
                        geojson_departamentos = df
                        break

           # Limpiar y convertir LATITUD y LONGITUD
            df_cursos = convert_decimal_separator(df_cursos, columns=["LATITUD", "LONGITUD"])

            # Asegúrate de que los valores sean strings antes de usar .str.extract()
            for col in ["LATITUD", "LONGITUD"]:
                df_cursos[col] = df_cursos[col].astype(str)  # Convertir a string
                df_cursos[col] = df_cursos[col].str.extract(r"(-?\d+\.\d+)").astype(float)
                df_cursos = df_cursos.dropna(subset=["LATITUD", "LONGITUD"])

            # Agrupar y contar para tabla (incluye ID_DEPARTAMENTO para relación con geojson)
            df_agrupado_tabla = df_cursos.groupby([
                "ID_DEPARTAMENTO", "N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO"
            ]).agg(
                Cantidad=("N_SECTOR_PRODUCTIVO", "size"),
                POSTULACIONES =("POSTULACIONES","sum")
            ).reset_index()

            # --- NUEVO MAPA: Choropleth por Departamento ---
            if geojson_departamentos is not None and 'ID_DEPARTAMENTO' in df_agrupado_tabla.columns:
                # Sumar cantidad por departamento
                choropleth_data = df_agrupado_tabla.groupby(['ID_DEPARTAMENTO', 'N_DEPARTAMENTO'], as_index=False)["Cantidad"].sum()
                # Agregar columna de sectores productivos agregados por departamento
                sectores_por_depto = df_agrupado_tabla.groupby(['ID_DEPARTAMENTO', 'N_DEPARTAMENTO'])['N_SECTOR_PRODUCTIVO'].apply(lambda x: ', '.join(sorted(set(x)))).reset_index(name='SectoresProductivos')
                choropleth_data = choropleth_data.merge(sectores_por_depto, on=['ID_DEPARTAMENTO', 'N_DEPARTAMENTO'], how='left')
                col_map_depto, col_tabla_depto = st.columns([2, 3])
                with col_map_depto:
                    st.markdown("### Mapa por Departamento (Sectores Productivos)")
                    fig_choro = px.choropleth_mapbox(
                        choropleth_data,
                        geojson=geojson_departamentos,
                        locations='ID_DEPARTAMENTO',
                        featureidkey="properties.CODDEPTO",
                        color='Cantidad',
                        hover_name='N_DEPARTAMENTO',
                        hover_data={
                            'Cantidad': True,
                            'SectoresProductivos': True,
                        },
                        mapbox_style="carto-positron",
                        color_continuous_scale="YlGnBu",
                        opacity=0.7,
                        zoom=6,
                        center={"lat":-31.4, "lon":-64.2}, # Córdoba centro aprox
                    )
                    fig_choro.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
                    st.plotly_chart(fig_choro, use_container_width=True)
                with col_tabla_depto:
                    st.markdown("### Tabla Sector Productivo por Departamento")
                    st.dataframe(
                        df_agrupado_tabla[["N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO", "Cantidad", "POSTULACIONES"]],
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.dataframe(df_agrupado_tabla[["N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO", "Cantidad"]], use_container_width=True, hide_index=True)

            # Agrupar y contar para mapa de sedes
            df_agrupado_mapa = df_cursos.groupby([
                "N_SEDE", "N_LOCALIDAD", "N_DEPARTAMENTO", "LATITUD", "LONGITUD"
            ]).size().reset_index(name="Cantidad")

            # Mapa de sedes
            col_mapa, col_tabla = st.columns([1, 3])

            with col_mapa:
                st.markdown("### Mapa de Sedes")
                fig = px.scatter_mapbox(
                    df_agrupado_mapa,
                    lat="LATITUD",
                    lon="LONGITUD",
                    color="Cantidad",
                    size="Cantidad",
                    hover_name="N_SEDE",
                    hover_data={
                        "N_LOCALIDAD": True,
                        "N_DEPARTAMENTO": True,
                        "Cantidad": True,
                        "LATITUD": False,
                        "LONGITUD": False
                    },
                    zoom=6,
                    mapbox_style="carto-positron",
                    color_continuous_scale="Viridis",
                    labels={
                        "N_LOCALIDAD": "Localidad",
                        "N_DEPARTAMENTO": "Departamento",
                        "Cantidad": "Total Cursos"
                    }
                )
                # Añadir contorno de departamentos si está disponible
                if geojson_departamentos is not None:
                    if isinstance(geojson_departamentos, gpd.GeoDataFrame):
                        geojson_departamentos = json.loads(geojson_departamentos.to_json())
                    elif isinstance(geojson_departamentos, str):
                        geojson_departamentos = json.loads(geojson_departamentos)

                    existing_layers = list(fig.layout.mapbox.layers) if hasattr(fig.layout.mapbox, 'layers') else []
                    fig.update_layout(
                        mapbox_layers=[
                            {
                                "source": geojson_departamentos,
                                "type": "line",
                                "color": "#d0e3f1",
                                "line": {"width": 1}
                            }
                        ] + existing_layers
                    )
                st.plotly_chart(fig, use_container_width=True)

            with col_tabla:
                st.markdown("### Cantidad de Cursos por Departamento y Localidad")
                df_cursos_depto_loc = df_cursos.groupby([
                    "N_DEPARTAMENTO", "N_LOCALIDAD"
                ]).size().reset_index(name="Cantidad")
                styled_table = (
                    df_cursos_depto_loc[["N_DEPARTAMENTO", "N_LOCALIDAD", "Cantidad"]].style
                    .background_gradient(cmap="Blues")
                    .format({"Cantidad": "{:,.0f}"})
                )
                st.dataframe(
                    styled_table,
                    use_container_width=True,
                    hide_index=True  # Si tu Streamlit es 1.22 o superior
                )
        else:
            st.warning("No se encontró el DataFrame de cursos con la estructura esperada.")