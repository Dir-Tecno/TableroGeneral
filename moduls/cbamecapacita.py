import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
from utils.ui_components import display_kpi_row, show_dev_dataframe_info, show_last_update
from utils.kpi_tooltips import TOOLTIPS_DESCRIPTIVOS
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
            "delta_color": "#d4f7d4",
            "tooltip": TOOLTIPS_DESCRIPTIVOS["POSTULANTES"]
        },
        {
            "title": "CURSOS ACTIVOS",
            "value_form": f"{resultados.get('Cursos Activos', 0):,}".replace(',', '.'),
            "color_class": "kpi-secondary",
            "delta": "",
            "delta_color": "#d4f7d4",
            "tooltip": TOOLTIPS_DESCRIPTIVOS["CURSOS ACTIVOS"]
        },
        {
            "title": "CURSOS COMENZADOS",
            "value_form": f"{resultados.get('Cursos Comenzados', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-3",
            "delta": "",
            "delta_color": "#d4f7d4",
            "tooltip": TOOLTIPS_DESCRIPTIVOS["CURSOS COMENZADOS"]
        },
        {
            "title": "PARTICIPANTES INSCRIPTOS",
            "value_form": f"{resultados.get('Participantes inscriptos', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-1",
            "delta": "",
            "delta_color": "#d4f7d4",
            "tooltip": TOOLTIPS_DESCRIPTIVOS["PARTICIPANTES INSCRIPTOS"]
        },
        {
            "title": "CAPACITACIONES ELEGIDAS",
            "value_form": f"{resultados.get('Capacitaciones Elegidas', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-2",
            "delta": "",
            "delta_color": "#d4f7d4",
            "tooltip": TOOLTIPS_DESCRIPTIVOS["CAPACITACIONES ELEGIDAS"]
        }
    ]
    return kpis

def load_and_preprocess_data(data, is_development=False):
    """
    Carga y preprocesa los datos principales del dashboard CBA ME CAPACITA.
    - Limpia separadores de miles en columnas numéricas.
    - Devuelve los DataFrames listos para usar.
    """
    # Cargar DataFrames principales
    df_postulantes = None
    df_cursos = None
    df_alumnos = None
    if isinstance(data, dict):
        df_postulantes = data.get("VT_INSCRIPCIONES_PRG129.parquet")
        df_cursos = data.get("VT_CURSOS_SEDES_GEO.parquet")
        df_alumnos = data.get("VT_ALUMNOS_EN_CURSOS.parquet")
    elif isinstance(data, list):
        for df in data:
            if "CUIL" in df.columns:
                df_postulantes = df
            if "ID_PLANIFICACION" in df.columns and "N_CURSO" in df.columns:
                df_cursos = df
    # Limpieza de separador de miles en ambos DataFrames
    #df_postulantes = clean_thousand_separator(df_postulantes)
    #df_cursos = clean_thousand_separator(df_cursos)

    # --- Tratamiento de N_DEPARTAMENTO y ZONA ---
    if df_postulantes is not None and 'N_DEPARTAMENTO' in df_postulantes.columns:
        departamentos_validos = [
            "CAPITAL", "CALAMUCHITA", "COLON", "CRUZ DEL EJE", "GENERAL ROCA", "GENERAL SAN MARTIN",
            "ISCHILIN", "JUAREZ CELMAN", "MARCOS JUAREZ", "MINAS", "POCHO", "PRESIDENTE ROQUE SAENZ PEÑA",
            "PUNILLA", "RIO CUARTO", "RIO PRIMERO", "RIO SECO", "RIO SEGUNDO", "SAN ALBERTO", "SAN JAVIER",
            "SAN JUSTO", "SANTA MARIA", "SOBREMONTE", "TERCERO ARRIBA", "TOTORAL", "TULUMBA", "UNION"
        ]
        # Normalizar N_DEPARTAMENTO: los que no estén en la lista pasan a 'OTROS'
        df_postulantes['N_DEPARTAMENTO'] = df_postulantes['N_DEPARTAMENTO'].where(
            df_postulantes['N_DEPARTAMENTO'].isin(departamentos_validos), 'OTROS'
        )

        # Añadir columna de ZONA FAVORECIDA
        zonas_favorecidas = [
            'PRESIDENTE ROQUE SAENZ PEÑA', 'GENERAL ROCA', 'RIO SECO', 'TULUMBA', 
            'POCHO', 'SAN JAVIER', 'SAN ALBERTO', 'MINAS', 'CRUZ DEL EJE', 
            'TOTORAL', 'SOBREMONTE', 'ISCHILIN'
        ]
        df_postulantes['ZONA'] = df_postulantes['N_DEPARTAMENTO'].apply(
            lambda x: 'ZONA NOC Y SUR' if x in zonas_favorecidas else 'ZONA REGULAR'
        )

        # Corregir N_LOCALIDAD a 'CORDOBA' si el departamento es CAPITAL
        if 'N_LOCALIDAD' in df_postulantes.columns:
            capital_mask = df_postulantes['N_DEPARTAMENTO'] == 'CAPITAL'
            df_postulantes.loc[capital_mask, 'N_LOCALIDAD'] = 'CORDOBA'

    # Cruce solicitado: agregar CUIL de postulantes a cursos directamente en df_cursos
    if df_cursos is not None and df_postulantes is not None:
        # Asegurar que ID_CERTIFICACION e ID_CERTIFICACION sean enteros si existen
        if 'ID_CERTIFICACION' in df_postulantes.columns:
            df_postulantes['ID_CERTIFICACION'] = pd.to_numeric(df_postulantes['ID_CERTIFICACION'], errors='coerce').fillna(0).astype(int)
        if 'ID_PLANIFICACION' in df_cursos.columns and 'ID_CERTIFICACION' in df_postulantes.columns and 'CUIL' in df_postulantes.columns:
            # Agrupar por ID_CERTIFICACION y contar CUILs no nulos
            cuil_count = (
                df_postulantes.groupby('ID_CERTIFICACION')['CUIL']
                .apply(lambda x: x.notnull().sum())
                .reset_index()
                .rename(columns={'CUIL': 'POSTULACIONES'})
            )
            df_cursos = df_cursos.merge(
                cuil_count,
                how='left',
                left_on='ID_PLANIFICACION',
                right_on='ID_CERTIFICACION'
            )
        if 'POSTULACIONES' in df_cursos.columns:
            df_cursos['POSTULACIONES'] = pd.to_numeric(df_cursos['POSTULACIONES'], errors='coerce').fillna(0).astype(int)
    if df_alumnos is not None and df_cursos is not None:
        if 'ID_ALUMNO' in df_alumnos.columns and 'ID_PLANIFICACION' in df_alumnos.columns:
            # Debug: Ver valores únicos en ID_PLANIFICACION
            print(f"Valores únicos en ID_PLANIFICACION de df_alumnos: {df_alumnos['ID_PLANIFICACION'].nunique()}")

            alumnos_count = (
                df_alumnos.groupby('ID_PLANIFICACION')['ID_ALUMNO']
                .apply(lambda x: x.notnull().sum())
                .reset_index()
                .rename(columns={'ID_ALUMNO': 'ALUMNOS'})
            )
            # Debug: Ver el resultado del conteo
            print(f"Total de grupos en alumnos_count: {len(alumnos_count)}")

            df_cursos = df_cursos.merge(
                alumnos_count,
                how='left',
                left_on='ID_PLANIFICACION',
                right_on='ID_PLANIFICACION'
            )

        if 'ALUMNOS' in df_cursos.columns:
            df_cursos['ALUMNOS'] = pd.to_numeric(df_cursos['ALUMNOS'], errors='coerce').fillna(0).astype(int)
            # Debug: Confirmar que ALUMNOS está en df_cursos
            print(f"Columna ALUMNOS en df_cursos: {df_cursos['ALUMNOS'].sum()} alumnos en total")
        else:
            print("ADVERTENCIA: La columna ALUMNOS no se agregó a df_cursos")

        # Agregar conteo de alumnos egresados a df_cursos
        if df_alumnos is not None and 'N_ESTADO' in df_alumnos.columns:
            # Debug: Ver valores únicos en N_ESTADO
            print(f"Estados únicos en df_alumnos: {df_alumnos['N_ESTADO'].unique()}")
            
            # Contar alumnos egresados por ID_PLANIFICACION
            egresados_count = (
                df_alumnos[df_alumnos['N_ESTADO'] == 'EGRESADO']
                .groupby('ID_PLANIFICACION')['ID_ALUMNO']
                .apply(lambda x: x.notnull().sum())
                .reset_index()
                .rename(columns={'ID_ALUMNO': 'EGRESADOS'})
            )
            # Debug: Ver el resultado del conteo de egresados
            print(f"Total de grupos con egresados: {len(egresados_count)}")
            print(f"Total de egresados: {egresados_count['EGRESADOS'].sum()}")

            df_cursos = df_cursos.merge(
                egresados_count,
                how='left',
                left_on='ID_PLANIFICACION',
                right_on='ID_PLANIFICACION'
            )

        if 'EGRESADOS' in df_cursos.columns:
            df_cursos['EGRESADOS'] = pd.to_numeric(df_cursos['EGRESADOS'], errors='coerce').fillna(0).astype(int)
            # Debug: Confirmar que EGRESADOS está en df_cursos
            print(f"Columna EGRESADOS en df_cursos: {df_cursos['EGRESADOS'].sum()} egresados en total")
        else:
            print("ADVERTENCIA: La columna EGRESADOS no se agregó a df_cursos")

    # Agregar columna COMENZADO a df_cursos
    if df_cursos is not None and 'FEC_INICIO' in df_cursos.columns:
        # Convertir FEC_INICIO a datetime
        df_cursos['FEC_INICIO'] = pd.to_datetime(df_cursos['FEC_INICIO'], errors='coerce')
        # Fecha actual
        fecha_actual = pd.Timestamp.now().normalize()
        # Marcar cursos comenzados (fecha actual >= FEC_INICIO)
        df_cursos['COMENZADO'] = df_cursos['FEC_INICIO'].notna() & (df_cursos['FEC_INICIO'] <= fecha_actual)

    # Debug: Verificar columnas finales
    if df_cursos is not None:
        print(f"Columnas en df_cursos: {df_cursos.columns.tolist()}")
        if 'ALUMNOS' in df_cursos.columns:
            print(f"Estadísticas de ALUMNOS: Min={df_cursos['ALUMNOS'].min()}, Max={df_cursos['ALUMNOS'].max()}, Media={df_cursos['ALUMNOS'].mean():.2f}")
        if 'COMENZADO' in df_cursos.columns:
            print(f"Cursos comenzados: {df_cursos['COMENZADO'].sum()} de {len(df_cursos)} ({df_cursos['COMENZADO'].mean()*100:.2f}%)")

        # Añadir columna "No asignados" como la diferencia entre POSTULACIONES y ALUMNOS
        if 'POSTULACIONES' in df_cursos.columns and 'ALUMNOS' in df_cursos.columns:
            df_cursos['No asignados'] = df_cursos['POSTULACIONES'] - df_cursos['ALUMNOS']
            # Asegurar que no haya valores negativos (en caso de inconsistencias en los datos)
            df_cursos['No asignados'] = df_cursos['No asignados'].apply(lambda x: max(0, x))
            print(f"Columna 'No asignados' creada: Min={df_cursos['No asignados'].min()}, Max={df_cursos['No asignados'].max()}, Total={df_cursos['No asignados'].sum()}")
        else:
            print("ADVERTENCIA: No se pudo crear la columna 'No asignados' porque faltan las columnas POSTULACIONES o ALUMNOS")

    # Cruzar df_postulantes con df_alumnos para obtener el campo ALUMNO
    if df_postulantes is not None and df_alumnos is not None and 'CUIL' in df_alumnos.columns:
        if 'CUIL' in df_postulantes.columns and 'CUIL' in df_alumnos.columns:
            print("Agregando columna ALUMNO a df_postulantes desde df_alumnos...")

            # Crear un set con los CUILs únicos de df_alumnos para hacer la búsqueda más eficiente
            cuils_alumnos = set(df_alumnos['CUIL'].dropna().unique())

            # Crear columna ALUMNO: contiene el CUIL si está en df_alumnos, sino None/NaN
            df_postulantes['ALUMNO'] = df_postulantes['CUIL'].apply(
                lambda x: x if x in cuils_alumnos else None
            )

            # Verificar el resultado del cruce
            alumnos_count = df_postulantes['ALUMNO'].notnull().sum()
            total_postulantes = len(df_postulantes)
            print(f"Columna ALUMNO agregada a df_postulantes. {alumnos_count} de {total_postulantes} postulantes son alumnos ({alumnos_count/total_postulantes*100:.2f}%)")
        else:
            print("ADVERTENCIA: No se puede realizar el cruce porque faltan columnas necesarias en los DataFrames")
    if is_development:
        st.write("Información de DataFrames ya cruzados")
        show_dev_dataframe_info(df_postulantes, "df_postulantes")
        show_dev_dataframe_info(df_alumnos, "df_alumnos")
        show_dev_dataframe_info(df_cursos, "df_cursos")
    return df_postulantes, df_alumnos, df_cursos

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

    # Mostrar columnas en  desarrollo
    if is_development:
        show_dev_dataframe_info(data, modulo_nombre="CBA Me Capacita")

    

    # --- Usar función de carga y preprocesamiento ---
    df_postulantes, df_alumnos, df_cursos = load_and_preprocess_data(data, is_development)



    # KPIs reales usando VT_INSCRIPCIONES_PRG129.parquet (postulantes) y VT_CURSOS_SEDES_GEO.parquet (cursos)
    # Asegurarse de que los DataFrames existen y tienen las columnas necesarias
    total_postulantes = 0
    if df_postulantes is not None and "CUIL" in df_postulantes.columns:
        total_postulantes = df_postulantes["CUIL"].dropna().nunique()
        
    total_alumnos = 0
    if df_alumnos is not None and "ID_ALUMNO" in df_alumnos.columns:
        total_alumnos = df_alumnos["ID_ALUMNO"].dropna().nunique()
        
    cursos_activos = 0
    if df_cursos is not None and "ID_PLANIFICACION" in df_cursos.columns:
        cursos_activos = df_cursos["ID_PLANIFICACION"].dropna().nunique()
        
    total_capacitaciones = 0
    if df_postulantes is not None and "ID_CERTIFICACION" in df_postulantes.columns:
        total_capacitaciones = df_postulantes["ID_CERTIFICACION"].dropna().nunique()
    
    # Calcular cursos ya comenzados usando la columna COMENZADO
    cursos_comenzados = 0
    if df_cursos is not None and 'COMENZADO' in df_cursos.columns:
        # Usar la columna COMENZADO ya calculada en load_and_preprocess_data
        cursos_comenzados = df_cursos[df_cursos['COMENZADO']]['ID_PLANIFICACION'].nunique()
    # Mostrar información de actualización de datos
    show_last_update(dates, 'VT_INSCRIPCIONES_PRG129.parquet')
    
    # Crear un diccionario con los resultados para pasarlo a la función de KPIs
    resultados = {
        "Postulantes": total_postulantes,
        "Cursos Activos": cursos_activos,
        "Cursos Comenzados": cursos_comenzados,
        "Capacitaciones Elegidas": total_capacitaciones,
        "Participantes inscriptos": total_alumnos
    }
    
    # Usar la función para crear los KPIs
    kpi_data = create_cbamecapacita_kpi(resultados)
    display_kpi_row(kpi_data)

    # Crear pestañas para diferentes vistas
    tab1, tab2 = st.tabs(["Alumnos", "Cursos"])
    
    with tab1:
        st.subheader("Análisis de Postulantes y alumnos")
      
        # 5. Distribución de alumnos por estado (INSCRIPTO, ACTIVO, etc.)
        st.subheader("Distribución de Alumnos por Estado")
        
        if df_alumnos is not None and 'N_ESTADO' in df_alumnos.columns:
            # Contar alumnos por estado
            alumnos_por_estado = df_alumnos['N_ESTADO'].value_counts().reset_index()
            alumnos_por_estado.columns = ['Estado', 'Cantidad']
            
            # Ordenar por cantidad descendente
            alumnos_por_estado = alumnos_por_estado.sort_values('Cantidad', ascending=False)
            
            # Crear paleta de colores para los estados
            color_sequence_estados = px.colors.qualitative.Bold
            
            # Crear gráfico de barras para estados de alumnos
            fig_estados = px.bar(
                alumnos_por_estado, 
                x='Estado', 
                y='Cantidad',
                text='Cantidad',
                title='Distribución de Alumnos por Estado',
                color='Estado',
                color_discrete_sequence=color_sequence_estados
            )
            
            # Mejorar diseño del gráfico
            fig_estados.update_traces(texttemplate='%{text:,}', textposition='outside')
            fig_estados.update_layout(
                xaxis_title='Estado',
                yaxis_title='Cantidad de Alumnos',
                xaxis={'categoryorder':'total descending'}
            )
            
            # Mostrar el gráfico
            st.plotly_chart(fig_estados, use_container_width=True)
            
            # Añadir una tabla con los porcentajes
            total_alumnos = alumnos_por_estado['Cantidad'].sum()
            alumnos_por_estado['Porcentaje'] = (alumnos_por_estado['Cantidad'] / total_alumnos * 100).round(2)
            alumnos_por_estado['Porcentaje'] = alumnos_por_estado['Porcentaje'].apply(lambda x: f"{x}%")
            
            with st.expander("Ver detalles de estados de alumnos"):
                st.dataframe(alumnos_por_estado, hide_index=True)
        else:
            st.info("No se encontraron datos sobre estados de alumnos.")
        
        st.markdown("***") # Separador
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
            # 1. Cantidad de Postulaciones y Participantes por N_DEPARTAMENTO y N_LOCALIDAD
            st.subheader("Cantidad de Postulaciones y Participantes por Departamento y Localidad")
            # Agrupar por N_DEPARTAMENTO y N_LOCALIDAD, contando CUILs únicos y ALUMNO not null
            df_group = df_filtered.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD']).agg(
                POSTULACIONES=('CUIL', 'nunique'),
                ALUMNOS=('ALUMNO', lambda x: x.notnull().sum())
            ).reset_index()
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
                # Crear dos series de datos: una para todos los postulantes y otra solo para alumnos
                # Agrupar todos los postulantes por rango de edad
                postulantes_por_edad = df_filtered['RANGO_EDAD'].value_counts().sort_index()
                
                # Agrupar solo los alumnos por rango de edad
                alumnos_por_edad = df_filtered[df_filtered['ALUMNO'].notna()]['RANGO_EDAD'].value_counts().sort_index()
                
                # Crear un DataFrame combinado para visualización
                edad_group = pd.DataFrame({
                    'Rango de Edad': postulantes_por_edad.index,
                    'POSTULANTES': postulantes_por_edad.values,
                    'ALUMNOS': alumnos_por_edad.reindex(postulantes_por_edad.index).fillna(0).values
                })
                
                # Reformatear los datos para graficar barras agrupadas
                edad_group_melted = pd.melt(
                    edad_group, 
                    id_vars=['Rango de Edad'],
                    value_vars=['POSTULANTES', 'ALUMNOS'],
                    var_name='Categoría', 
                    value_name='Cantidad'
                )
                
                # Crear gráfico de barras agrupadas
                fig_edad = px.bar(
                    edad_group_melted, 
                    x='Rango de Edad', 
                    y='Cantidad', 
                    color='Categoría',
                    barmode='group',  # Barras agrupadas
                    title='Distribución por Rango de Edad: Postulantes vs Alumnos', 
                    text_auto=True,  # Mostrar valores en las barras
                    color_discrete_map={
                        'POSTULANTES': '#FFA726',  # Color naranja para postulantes
                        'ALUMNOS': '#66BB6A'  # Color verde para alumnos (similar al usado para COMENZADO)
                    }
                )
                
                # Personalizar el gráfico
                fig_edad.update_layout(
                    xaxis_title='Rango de Edad',
                    yaxis_title='Cantidad',
                    legend_title='Categoría',
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                )
                
                st.plotly_chart(fig_edad, use_container_width=True)
            else:
                st.info("No se encontró la columna FEC_NACIMIENTO para calcular edades.")
            # 3. TOP 10 de CAPACITACIONES: Comparación entre postulaciones y capacitaciones con alumnos
            st.subheader("Top 10 de Capacitaciones: Postulaciones vs Capacitaciones con Alumnos")
            if 'N_CERTIFICACION' in df_filtered.columns:
                # Crear dos columnas para mostrar gráficos lado a lado
                col1, col2 = st.columns(2)
                
                with col1:
                    # Top 10 de todas las postulaciones
                    top_cap_postulaciones = df_filtered['N_CERTIFICACION'].value_counts().head(10).reset_index()
                    top_cap_postulaciones.columns = ['Capacitación', 'Cantidad']
                    fig_topcap_post = px.bar(
                        top_cap_postulaciones, 
                        x='Capacitación', 
                        y='Cantidad', 
                        title='Top 10 de Capacitaciones Postuladas',
                        text_auto=True,
                        color_discrete_sequence=['#FFA726']  # Color naranja para postulantes
                    )
                    fig_topcap_post.update_layout(
                        xaxis_title='Capacitación',
                        yaxis_title='Cantidad de Postulaciones'
                    )
                    st.plotly_chart(fig_topcap_post, use_container_width=True)
                    
                with col2:
                    # Top 10 de capacitaciones efectivamente activas con alumnos
                    # Filtrar solo registros que tienen ALUMNO (no nulo)
                    alumnos_df = df_filtered[df_filtered['ALUMNO'].notna()]
                    if len(alumnos_df) > 0:
                        top_cap_activas = alumnos_df['N_CERTIFICACION'].value_counts().head(10).reset_index()
                        top_cap_activas.columns = ['Capacitación', 'Cantidad']
                        fig_topcap_activas = px.bar(
                            top_cap_activas, 
                            x='Capacitación', 
                            y='Cantidad', 
                            title='Top 10 de Capacitaciones Efectivamente Activas',
                            text_auto=True,
                            color_discrete_sequence=['#66BB6A']  # Color verde para alumnos
                        )
                        fig_topcap_activas.update_layout(
                            xaxis_title='Capacitación',
                            yaxis_title='Cantidad de Alumnos'
                        )
                        st.plotly_chart(fig_topcap_activas, use_container_width=True)
                    else:
                        st.info("No se encontraron capacitaciones con alumnos asignados.")
                        
                # Añadir un expander con la tabla comparativa detallada
                with st.expander("Ver tabla comparativa de capacitaciones"):
                    # Obtener todas las capacitaciones únicas de ambos top 10
                    # Primero asegurarse de que hay alumnos antes de intentar acceder a sus capacitaciones
                    if len(alumnos_df) > 0:
                        # Obtener las capacitaciones de los alumnos
                        alumnos_top_caps = alumnos_df['N_CERTIFICACION'].value_counts().head(10).reset_index()
                        alumnos_top_caps.columns = ['Capacitación', 'Cantidad']
                        
                        # Concatenar capacitaciones de postulantes y alumnos
                        all_caps = pd.concat([
                            top_cap_postulaciones['Capacitación'], 
                            alumnos_top_caps['Capacitación']
                        ]).unique()
                    else:
                        # Si no hay alumnos, usar solo las capacitaciones de postulantes
                        all_caps = top_cap_postulaciones['Capacitación'].unique()
                    
                    # Crear DataFrame comparativo
                    comp_data = []
                    for cap in all_caps:
                        postulaciones = top_cap_postulaciones[top_cap_postulaciones['Capacitación'] == cap]['Cantidad'].sum() \
                            if cap in top_cap_postulaciones['Capacitación'].values else 0
                        alumnos = len(alumnos_df[alumnos_df['N_CERTIFICACION'] == cap])
                        tasa = round((alumnos / postulaciones * 100), 2) if postulaciones > 0 else 0
                        comp_data.append([cap, postulaciones, alumnos, f"{tasa}%"])
                    
                    df_comp = pd.DataFrame(comp_data, columns=['Capacitación', 'Postulaciones', 'Alumnos', 'Tasa de Conversión'])
                    df_comp = df_comp.sort_values(by='Postulaciones', ascending=False)
                    st.dataframe(df_comp)
            else:
                st.info("No se encontró la columna N_CERTIFICACION para el top de capacitaciones.")
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
                if 'N_CERTIFICACION' in df_filtered.columns:
                    cols[0].markdown('**Top 10 cursos más seleccionados por Nivel Educativo:**')
                    for nivel in df_filtered['EDUCACION'].dropna().unique():
                        top_cursos = (
                            df_filtered[df_filtered['EDUCACION'] == nivel]
                            .groupby('N_CERTIFICACION')
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
                                top_cursos['CAPACITACION_CORTO'] = top_cursos['N_CERTIFICACION'].apply(lambda x: x[:40] + '...' if len(x) > 40 else x)
                                
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
                if 'N_CERTIFICACION' in df_filtered.columns:
                    cols[1].markdown('**Top 10 cursos más seleccionados por Tipo de Trabajo:**')
                    for tipo in df_filtered['TIPO_TRABAJO'].dropna().unique():
                        top_cursos = (
                            df_filtered[df_filtered['TIPO_TRABAJO'] == tipo]
                            .groupby('N_CERTIFICACION')
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
                                top_cursos['CAPACITACION_CORTO'] = top_cursos['N_CERTIFICACION'].apply(lambda x: x[:40] + '...' if len(x) > 40 else x)
                                
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
                    1: 'Varón', '01': 'Varón',
                    2: 'Mujer', '02': 'Mujer',
                    3: 'Ambos', '03': 'Ambos',
                    4: 'No Binario', '04': 'No Binario'
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
                if 'N_CERTIFICACION' in df_filtered.columns:
                    cols[2].markdown('**Top 10 cursos más seleccionados por Género:**')
                    for sexo in df_filtered['SEXO'].dropna().unique():
                        top_cursos = (
                            df_filtered[df_filtered['SEXO'] == sexo]
                            .groupby('N_CERTIFICACION')
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
                                top_cursos['CAPACITACION_CORTO'] = top_cursos['N_CERTIFICACION'].apply(lambda x: x[:40] + '...' if len(x) > 40 else x)
                                
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
        

        # Gráficos de Gauge para visualizar ocupación de cursos
        if df_cursos is not None and 'ALUMNOS' in df_cursos.columns:
            st.markdown("### Porcentaje de Ocupación de Cursos")
            st.markdown("*Considerando 20 alumnos como 100% de ocupación*")
            
            # Calcular el porcentaje de ocupación (20 alumnos = 100%)
            df_cursos['Porcentaje_Ocupacion'] = (df_cursos['ALUMNOS'] / 20 * 100).clip(upper=100)
            
            # Crear categorías de ocupación
            df_cursos['Categoria_Ocupacion'] = pd.cut(
                df_cursos['Porcentaje_Ocupacion'],
                bins=[0, 25, 50, 75, 100],
                labels=['Baja (0-25%)', 'Media-Baja (25-50%)', 'Media-Alta (50-75%)', 'Alta (75-100%)']
            )
            
            # Definir el umbral para alta ocupación (75%)
            umbral_alta_ocupacion = 75
            
            # Asegurarse de que todas las categorías estén correctamente asignadas
            # Verificar que los cursos con Porcentaje_Ocupacion >= 75 estén en la categoría 'Alta (75-100%)'
            df_cursos['Categoria_Ocupacion'] = pd.cut(
                df_cursos['Porcentaje_Ocupacion'],
                bins=[0, 25, 50, 75, 100],
                labels=['Baja (0-25%)', 'Media-Baja (25-50%)', 'Media-Alta (50-75%)', 'Alta (75-100%)'],
                include_lowest=True,
                right=True  # Asegura que 75 esté en la categoría 'Alta'
            )
            
            # Contar cursos por categoría de ocupación
            df_ocupacion = df_cursos['Categoria_Ocupacion'].value_counts().reset_index()
            df_ocupacion.columns = ['Categoría', 'Cantidad']
            
            # Asegurar que las categorías estén en el orden correcto para la visualización
            orden_categorias = ['Baja (0-25%)', 'Media-Baja (25-50%)', 'Media-Alta (50-75%)', 'Alta (75-100%)']
            df_ocupacion['Orden'] = df_ocupacion['Categoría'].map({cat: i for i, cat in enumerate(orden_categorias)})
            df_ocupacion = df_ocupacion.sort_values('Orden').drop('Orden', axis=1)
            
            # Calcular estadísticas adicionales usando exactamente el mismo criterio
            total_cursos = len(df_cursos)
            cursos_alta_ocupacion = len(df_cursos[df_cursos['Categoria_Ocupacion'] == 'Alta (75-100%)'])
            porcentaje_cursos_alta_ocupacion = (cursos_alta_ocupacion / total_cursos * 100) if total_cursos > 0 else 0
            
            # Asegurarse de que los valores sean consistentes entre el gráfico y la métrica
            alta_en_grafico = df_ocupacion[df_ocupacion['Categoría'] == 'Alta (75-100%)']['Cantidad'].values[0] if 'Alta (75-100%)' in df_ocupacion['Categoría'].values else 0
            cursos_alta_ocupacion = alta_en_grafico  # Usar el valor del gráfico para consistencia
            
            # Crear layout con 2 columnas
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # Crear gráfico de barras para mostrar cantidad de cursos por categoría de ocupación
                fig_barras = px.bar(
                    df_ocupacion,
                    x='Categoría',
                    y='Cantidad',
                    color='Categoría',
                    text_auto=True,
                    title='Distribución de Cursos por Nivel de Ocupación',
                    color_discrete_sequence=['#FF5252', '#FFA726', '#FFEB3B', '#66BB6A']
                )
                fig_barras.update_layout(xaxis_title=None)
                st.plotly_chart(fig_barras, use_container_width=True)
                
                # Mostrar estadísticas de alta ocupación debajo del gráfico
                st.metric(
                    label=f"Cursos con alta ocupación (>={umbral_alta_ocupacion}%)", 
                    value=f"{cursos_alta_ocupacion} de {total_cursos}",
                    delta=f"{porcentaje_cursos_alta_ocupacion:.1f}%"
                )
            
            with col2:
                # Calcular promedio de ocupación para el gauge
                ocupacion_promedio = df_cursos['Porcentaje_Ocupacion'].mean()
                
                # Crear gauge chart para mostrar ocupación promedio
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=ocupacion_promedio,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "Ocupación Promedio de Cursos"},
                    gauge={
                        'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                        'bar': {'color': "darkblue"},
                        'bgcolor': "white",
                        'borderwidth': 2,
                        'bordercolor': "gray",
                        'steps': [
                            {'range': [0, 25], 'color': "#FF5252"},  # Rojo
                            {'range': [25, 50], 'color': "#FFA726"},  # Naranja
                            {'range': [50, 75], 'color': "#FFEB3B"},  # Amarillo
                            {'range': [75, 100], 'color': "#66BB6A"}  # Verde
                        ],
                    }
                ))
                
                fig_gauge.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=50, b=20)
                )
                
                st.plotly_chart(fig_gauge, use_container_width=True)
                
                # Ya no necesitamos este indicador métrico aquí, lo hemos movido a la columna 1
        
        # Gráfico de mosaico para distribución de postulantes por curso
        if df_cursos is not None and 'POSTULACIONES' in df_cursos.columns:
            st.markdown("### Distribución de Cursos por Cantidad de Postulantes")
            
            # Crear rangos de postulantes (de 20 en 20)
            df_cursos['Rango_Postulantes'] = pd.cut(
                df_cursos['POSTULACIONES'], 
                bins=range(0, max(df_cursos['POSTULACIONES']) + 21, 20),
                labels=[f'{i}-{i+19}' for i in range(0, max(df_cursos['POSTULACIONES']), 20)],
                right=False
            )
            
            # Contar cursos por rango de postulantes
            df_rangos = df_cursos.groupby('Rango_Postulantes', observed=False).size().reset_index(name='Cantidad_Cursos')
            
            # Filtrar rangos con 0 cursos
            df_rangos = df_rangos[df_rangos['Cantidad_Cursos'] > 0]
            
            # Crear gráfico de mosaico con Altair
            chart = alt.Chart(df_rangos).mark_bar().encode(
                x=alt.X('Rango_Postulantes:N', title='Rango de Postulantes por Curso', sort=None),
                y=alt.Y('Cantidad_Cursos:Q', title='Cantidad de Cursos'),
                color=alt.Color('Rango_Postulantes:N', 
                               legend=None,
                               scale=alt.Scale(scheme='viridis')),
                tooltip=['Rango_Postulantes', 'Cantidad_Cursos']
            ).properties(
                title='Distribución de Cursos por Cantidad de Postulantes',
                width=600,
                height=400
            )
            
            # Añadir etiquetas de texto
            text = chart.mark_text(
                align='center',
                baseline='middle',
                dy=-10,
                color='white',
                fontWeight='bold'
            ).encode(
                text='Cantidad_Cursos:Q'
            )
            
            # Combinar gráfico y etiquetas
            final_chart = (chart + text)
            
            # Mostrar el gráfico
            st.altair_chart(final_chart, use_container_width=True)
        
        st.markdown("## Sector Productivos por Departamento")
        # Mostrar DataFrame solo si existe
        if df_cursos is not None:
            import io
            columnas_exportar = [
                "ID_PLANIFICACION",
                "N_INSTITUCION",
                "N_CURSO",
                "FEC_INICIO",
                "FEC_FIN",
                "N_SECTOR_PRODUCTIVO",
                "N_SEDE",
                "N_DEPARTAMENTO",
                "N_LOCALIDAD",
                "N_CALLE",
                "ALTURA",
                "POSTULACIONES",
                "ALUMNOS",
                "EGRESADOS",
                "No asignados",
                "COMENZADO"
            ]
            # Filtrar solo columnas existentes
            columnas_existentes = [col for col in columnas_exportar if col in df_cursos.columns]
            df_export = df_cursos[columnas_existentes].copy()
            
            # Mostrar tabla con estilos
            st.markdown("### Tabla de Cursos")
            
            # Seleccionar solo las columnas solicitadas para mostrar
            columnas_mostrar = [
                "N_INSTITUCION",
                "N_CURSO",
                "FEC_INICIO",
                "FEC_FIN",
                "N_SECTOR_PRODUCTIVO",
                "N_SEDE",
                "N_DEPARTAMENTO",
                "N_LOCALIDAD",
                "POSTULACIONES",
                "ALUMNOS",
                "EGRESADOS",
                "No asignados",
                "COMENZADO"
            ]
            
            # Filtrar solo columnas existentes para mostrar
            columnas_mostrar_existentes = [col for col in columnas_mostrar if col in df_export.columns]
            df_display = df_export[columnas_mostrar_existentes].copy()
            
            # Aplicar estilos al DataFrame
            styled_display = df_display.style\
                .background_gradient(subset=["POSTULACIONES"], cmap="Blues")\
                .background_gradient(subset=["ALUMNOS"], cmap="Greens")\
                .background_gradient(subset=["No asignados"], cmap="Oranges")\
                .format({"POSTULACIONES": "{:,.0f}", "ALUMNOS": "{:,.0f}", "No asignados": "{:,.0f}"})
            
            # Resaltar visualmente los cursos comenzados con color verde
            if "COMENZADO" in df_display.columns:
                styled_display = styled_display.apply(lambda x: ['background-color: #a8f0a8' if x['COMENZADO'] else '' for i in range(len(x))], axis=1)
            
            # Mostrar la tabla con estilos
            st.dataframe(
                styled_display,
                use_container_width=True,
                hide_index=True
            )
            
            # Verificar que la columna "No asignados" esté presente en df_export
            if 'No asignados' not in df_export.columns and 'POSTULACIONES' in df_export.columns and 'ALUMNOS' in df_export.columns:
                # Si no existe, crearla nuevamente
                df_export['No asignados'] = df_export['POSTULACIONES'] - df_export['ALUMNOS']
                df_export['No asignados'] = df_export['No asignados'].apply(lambda x: max(0, x))
            
            # Botón para descargar Excel debajo de la tabla
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
            from utils.data_cleaning import convert_decimal_separator
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
                POSTULACIONES =("POSTULACIONES","sum"),
                ALUMNOS =("ALUMNOS","sum")
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
                        df_agrupado_tabla[["N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO", "Cantidad", "POSTULACIONES", "ALUMNOS"]],
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