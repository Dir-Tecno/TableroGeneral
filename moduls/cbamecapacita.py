import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
from utils.ui_components import display_kpi_row, show_dev_dataframe_info, show_last_update
from utils.plot_styles import apply_base_style, set_shared_yaxis
from utils.kpi_tooltips import TOOLTIPS_DESCRIPTIVOS
import geopandas as gpd
import json

pd.set_option('future.no_silent_downcasting', True)


def _normalize_datetime_columns(df):
    """Convierte todas las columnas datetime del DataFrame a timezone-naive.

    - Convierte strings a datetime cuando sea posible.
    - Si la columna tiene tz, la elimina (.dt.tz_localize(None)).
    - Devuelve el DataFrame modificado (copia superficial).
    """
    if df is None:
        return df
    df = df.copy()
    for col in df.columns:
        try:
            # Si ya es dtype datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Remover timezone si existe
                try:
                    if hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                except Exception:
                    # En algunos casos usar dt.tz_convert
                    try:
                        df[col] = df[col].dt.tz_convert(None)
                    except Exception:
                        pass
            elif df[col].dtype == 'object':
                # Intentar convertir strings a datetime con formatos específicos
                try:
                    # Intentar primero con formato DD/MM/YYYY (común en Argentina)
                    temp = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                    if temp.isna().all():
                        # Si falla, intentar con YYYY-MM-DD
                        temp = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
                        if temp.isna().all():
                            # Si ambos fallan, intentar con YYYY-MM-DD HH:MM:SS
                            temp = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                    if temp.notna().any():
                        df[col] = temp
                        try:
                            if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                                df[col] = df[col].dt.tz_localize(None)
                        except Exception:
                            pass
                except Exception:
                    # Si hay algún error con los formatos, intentar sin formato (manteniendo el warning controlado)
                    temp = pd.to_datetime(df[col], errors='coerce')
                    if temp.notna().any():
                        df[col] = temp
                        try:
                            if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                                df[col] = df[col].dt.tz_localize(None)
                        except Exception:
                            pass
        except Exception:
            # Si falla para una columna, seguimos con la siguiente
            continue
    return df

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

def show_cba_capacita_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de CBA ME CAPACITA.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
    # Mostrar última actualización al inicio del dashboard
    if dates:
        show_last_update(dates, 'df_postulantes_cbamecapacita.parquet')
    
    if data is None:
        st.error("No se pudieron cargar los datos de CBA ME CAPACITA.")
        return

    # Mostrar columnas en  desarrollo
    if is_development:
        show_dev_dataframe_info(data, modulo_nombre="CBA Me Capacita", is_development=is_development)

    # --- Usar función de carga y preprocesamiento ---
    # df_postulantes, df_alumnos, df_cursos = load_and_preprocess_data(data, is_development)

    df_postulantes = data.get('df_postulantes_cbamecapacita.parquet')
    df_alumnos = data.get('df_alumnos.parquet')
    df_cursos = data.get('df_cursos.parquet')

    # Normalizar datetime en todos los dataframes cargados para evitar conflictos de timezone
    if df_postulantes is not None:
        df_postulantes = _normalize_datetime_columns(df_postulantes)
    if df_alumnos is not None:
        df_alumnos = _normalize_datetime_columns(df_alumnos)
    if df_cursos is not None:
        df_cursos = _normalize_datetime_columns(df_cursos)


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
    tab1, tab2, tab3 = st.tabs(["Alumnos", "Cursos", "Gestión Cursos"])
    
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
            
            # Crear gráfico de barras horizontales para mejor estética
            fig_estados = px.bar(
                alumnos_por_estado, 
                y='Estado',  # Barras horizontales
                x='Cantidad',
                text='Cantidad',
                title='Distribución de Alumnos por Estado',
                color='Cantidad',  # Color continuo basado en cantidad
                color_continuous_scale='Viridis',  # Paleta atractiva
                orientation='h'  # Horizontal
            )
            
            # Mejorar diseño del gráfico
            fig_estados.update_traces(
                texttemplate='%{text:,}', 
                textposition='inside',  # Texto dentro de las barras
                textfont=dict(color='white', size=12),  # Texto blanco para contraste
                hovertemplate='<b>%{y}</b><br>Cantidad: %{x:,}<extra></extra>'  # Mejor hover
            )
            fig_estados.update_layout(
                xaxis_title='Cantidad de Alumnos',
                yaxis_title='Estado',
                yaxis={'categoryorder':'total ascending'},  # Orden descendente en horizontal
                coloraxis_showscale=False,  # Ocultar barra de color para simplicidad
                margin=dict(l=20, r=20, t=60, b=20)  # Márgenes ajustados
            )
            
            # Mostrar el gráfico
            apply_base_style(fig_estados, rotate_x=False, showlegend=False, height=400, text_inside=True)
            st.plotly_chart(fig_estados)
            
            # Añadir una tabla con los porcentajes
            total_alumnos = alumnos_por_estado['Cantidad'].sum()
            alumnos_por_estado['Porcentaje'] = (alumnos_por_estado['Cantidad'] / total_alumnos * 100).round(2)
            alumnos_por_estado['Porcentaje'] = alumnos_por_estado['Porcentaje'].apply(lambda x: f"{x}%")
            
            
        else:
            st.info("No se encontraron datos sobre estados de alumnos.")
        
        st.markdown("***") # Separador
        if df_postulantes is not None and not df_postulantes.empty:
            # Filtros interactivos
            col1, col2 = st.columns(2)
            with col1:
                departamentos = sorted(df_postulantes['N_DEPARTAMENTO'].dropna().unique())
                selected_dpto = st.selectbox("Departamento:", ["Todos"] + departamentos, key="cbame_postulantes_dpto")
            with col2:
                localidades = sorted(df_postulantes['N_LOCALIDAD'].dropna().unique())
                selected_loc = st.selectbox("Localidad:", ["Todos"] + localidades, key="cbame_postulantes_loc")
            df_filtered = df_postulantes.copy()
            if selected_dpto != "Todos":
                df_filtered = df_filtered[df_filtered['N_DEPARTAMENTO'] == selected_dpto]
            if selected_loc != "Todos":
                df_filtered = df_filtered[df_filtered['N_LOCALIDAD'] == selected_loc]
            # 1. Cantidad de Postulaciones y Participantes por N_DEPARTAMENTO y N_LOCALIDAD
            st.subheader("Cantidad de Postulaciones y Participantes por Departamento y Localidad")
            # Agrupar por N_DEPARTAMENTO y N_LOCALIDAD, contando CUILs únicos y ALUMNO not null
            df_group = df_filtered.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD'], observed=True).agg(
                POSTULACIONES=('CUIL', 'nunique'),
                ALUMNOS=('ALUMNO', lambda x: x.notnull().sum())
            ).reset_index()
            st.dataframe(df_group, hide_index=True)
            # 2. Distribución por rangos de edad
            st.subheader("Distribución por Rangos de Edad")
            today = pd.Timestamp.today().tz_localize(None)  # Hacer tz-naive
            if 'FEC_NACIMIENTO' in df_filtered.columns:
                df_filtered = df_filtered.copy()
                df_filtered['FEC_NACIMIENTO'] = pd.to_datetime(df_filtered['FEC_NACIMIENTO'], format='%d/%m/%Y', errors='coerce')
                # Si el formato DD/MM/YYYY falla, intentar con YYYY-MM-DD
                if df_filtered['FEC_NACIMIENTO'].isna().all():
                    df_filtered['FEC_NACIMIENTO'] = pd.to_datetime(df_filtered['FEC_NACIMIENTO'], format='%Y-%m-%d', errors='coerce')
                # Asegurar que FEC_NACIMIENTO sea tz-naive
                if df_filtered['FEC_NACIMIENTO'].dt.tz is not None:
                    df_filtered['FEC_NACIMIENTO'] = df_filtered['FEC_NACIMIENTO'].dt.tz_localize(None)
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
                
                apply_base_style(fig_edad, rotate_x=False, showlegend=True, height=380)
                st.plotly_chart(fig_edad)
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
                    apply_base_style(fig_topcap_post, rotate_x=True, showlegend=False, height=360)
                    st.plotly_chart(fig_topcap_post)
                    
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
                        apply_base_style(fig_topcap_activas, rotate_x=True, showlegend=False, height=360)
                        st.plotly_chart(fig_topcap_activas)
                    else:
                        st.info("No se encontraron capacitaciones con alumnos asignados.")
                        
                # Alinear eje Y entre ambos top-10 si ambos existen
                try:
                    if 'fig_topcap_post' in locals() and 'fig_topcap_activas' in locals():
                        set_shared_yaxis([fig_topcap_post, fig_topcap_activas], pad=0.12)
                except Exception:
                    pass

                # Bloque de tabla comparativa eliminado (código comentado limpiado)
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
                cols[0].plotly_chart(fig_edu)
                
                # Bloque de Top10 por Nivel Educativo eliminado (código comentado limpiado)
            
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
                cols[1].plotly_chart(fig_tipo)
                
                # Bloque de Top10 por Tipo de Trabajo eliminado (código comentado limpiado)
                
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
                cols[2].plotly_chart(fig_sexo)
                
                # Bloque de Top10 por Sexo eliminado (código comentado limpiado)
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
                # Crear gráfico de barras horizontales para mostrar cantidad de cursos por categoría de ocupación
                fig_barras = px.bar(
                    df_ocupacion,
                    y='Categoría',  # Barras horizontales
                    x='Cantidad',
                    text='Cantidad',
                    title='Distribución de Cursos por Nivel de Ocupación',
                    color='Cantidad',  # Color continuo basado en cantidad
                    color_continuous_scale='Viridis',  # Paleta atractiva
                    orientation='h'  # Horizontal
                )
                fig_barras.update_traces(
                    texttemplate='%{text:,}', 
                    textposition='inside',  # Texto dentro de las barras
                    textfont=dict(color='white', size=12),  # Texto blanco para contraste
                    hovertemplate='<b>%{y}</b><br>Cantidad: %{x:,}<extra></extra>'  # Mejor hover
                )
                fig_barras.update_layout(
                    xaxis_title='Cantidad de Cursos',
                    yaxis_title='Nivel de Ocupación',
                    yaxis={'categoryorder':'array', 'categoryarray': orden_categorias},  # Mantener orden lógico
                    coloraxis_showscale=False,  # Ocultar barra de color para simplicidad
                    margin=dict(l=20, r=20, t=60, b=20)  # Márgenes ajustados
                )
                apply_base_style(fig_barras, rotate_x=False, showlegend=False, height=360, text_inside=True)
                st.plotly_chart(fig_barras)
                
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
                
                apply_base_style(fig_gauge, rotate_x=False, showlegend=False, height=300)
                st.plotly_chart(fig_gauge)
                
        
        
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
            
            # Convertir columnas datetime con timezone a timezone-naive para Excel
            for col in df_export.columns:
                if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                    try:
                        # Si la columna tiene timezone, removerla
                        if hasattr(df_export[col].dtype, 'tz') and df_export[col].dtype.tz is not None:
                            df_export[col] = df_export[col].dt.tz_localize(None)
                        # También intentar con dt.tz_convert(None) si existe zona horaria
                        elif hasattr(df_export[col].dt, 'tz') and df_export[col].dt.tz is not None:
                            df_export[col] = df_export[col].dt.tz_localize(None)
                    except Exception:
                        # Si hay algún error, intentar convertir de forma general
                        try:
                            df_export[col] = pd.to_datetime(df_export[col]).dt.tz_localize(None)
                        except Exception:
                            pass  # Si no se puede convertir, dejar como está
            
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
            
            # Convertir COMENZADO a Sí/No para simplicidad
            if "COMENZADO" in df_display.columns:
                df_display['COMENZADO'] = df_display['COMENZADO'].map({True: 'Sí', False: 'No'})
            
            # Aplicar estilos al DataFrame
            styled_display = df_display.style\
                .background_gradient(subset=["POSTULACIONES"], cmap="Blues")\
                .background_gradient(subset=["ALUMNOS"], cmap="Greens")\
                .background_gradient(subset=["No asignados"], cmap="Oranges")\
                .format({"POSTULACIONES": "{:,.0f}", "ALUMNOS": "{:,.0f}", "No asignados": "{:,.0f}"})
            
            # Mostrar la tabla con estilos
            st.dataframe(
                styled_display,
                width='stretch',
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
        
        # Sección 'Sector Productivos por Departamento' eliminada.
        # Mantener la limpieza mínima de coordenadas para garantizar que el Mapa de Sedes funcione.
        if df_cursos is not None:
            from utils.data_cleaning import convert_decimal_separator
            df_cursos = convert_decimal_separator(df_cursos, columns=["LATITUD", "LONGITUD"]) if 'LATITUD' in df_cursos.columns or 'LONGITUD' in df_cursos.columns else df_cursos

            for col in ["LATITUD", "LONGITUD"]:
                if col in df_cursos.columns:
                    df_cursos[col] = df_cursos[col].astype(str)
                    df_cursos[col] = df_cursos[col].str.extract(r"(-?\d+[.,]\d+)")
                    df_cursos[col] = df_cursos[col].str.replace(',', '.', regex=False)
                    df_cursos[col] = pd.to_numeric(df_cursos[col], errors='coerce')

            # Eliminar filas sin coordenadas válidas
            if 'LATITUD' in df_cursos.columns and 'LONGITUD' in df_cursos.columns:
                df_cursos = df_cursos.dropna(subset=["LATITUD", "LONGITUD"]).copy()
                try:
                    df_cursos['LATITUD'] = df_cursos['LATITUD'].round(4)
                    df_cursos['LONGITUD'] = df_cursos['LONGITUD'].round(4)
                except Exception:
                    pass

            # Agrupar y contar para mapa de sedes
            df_agrupado_mapa = df_cursos.groupby([
                "N_SEDE", "N_LOCALIDAD", "N_DEPARTAMENTO", "LATITUD", "LONGITUD"
            ], observed=True).size().reset_index(name="Cantidad")

            # Mapa de sedes
            col_mapa, col_tabla = st.columns([1, 1])

            with col_mapa:
                st.markdown("### Mapa de Sedes")
                try:
                    if df_agrupado_mapa.empty:
                        st.info("No hay datos de sedes con coordenadas válidas para mostrar el mapa.")
                    else:
                        # Asegurar tipos de datos
                        df_agrupado_mapa['LATITUD'] = df_agrupado_mapa['LATITUD'].astype(float)
                        df_agrupado_mapa['LONGITUD'] = df_agrupado_mapa['LONGITUD'].astype(float)
                        
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
                        # Inicializar geojson_departamentos de forma segura antes de su uso
                        geojson_departamentos = None
                        if isinstance(data, dict):
                            geojson_departamentos = data.get("capa_departamentos_2010.geojson")
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and "features" in item:
                                    geojson_departamentos = item
                                    break

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
                            try:
                                apply_base_style(fig, rotate_x=False, showlegend=False, height=420, colorbar_thickness=10, colorbar_len=0.32, colorbar_x=1.02)
                            except Exception:
                                pass
                            st.plotly_chart(fig)
                        else:
                            st.plotly_chart(fig)
                except Exception as e:
                    st.error(f"Error al cargar el mapa de sedes: {str(e)}")
                    st.info("Verifica que los datos de coordenadas sean válidos.")

            with col_tabla:
                st.markdown("### Cantidad de Cursos por Departamento y Localidad")
                df_cursos_depto_loc = df_cursos.groupby([
                    "N_DEPARTAMENTO", "N_LOCALIDAD"
                ], observed=True).size().reset_index(name="Cantidad")
                styled_table = (
                    df_cursos_depto_loc[["N_DEPARTAMENTO", "N_LOCALIDAD", "Cantidad"]].style
                    .background_gradient(cmap="Blues")
                    .format({"Cantidad": "{:,.0f}"})
                )
                st.dataframe(
                    styled_table,
                    width='stretch',
                    hide_index=True  # Si tu Streamlit es 1.22 o superior
                )
        else:
            st.warning("No se encontró el DataFrame de cursos con la estructura esperada.")
    
    with tab3:
        st.markdown("Accede al sistema de gestión de capacitaciones:")
        
        # Crear un botón estilizado igual al de escrituracion
        st.markdown(
            f'<div style="text-align:center; margin-top:20px;">'
            f'<a href="https://gestion-capacitaciones.duckdns.org/" target="_blank" rel="noopener noreferrer">'
            f'<button style="background:#2980b9;color:#fff;border:none;padding:12px 20px;border-radius:8px;font-size:16px;cursor:pointer;margin-bottom:20px;">'
            f'Abrir Gestión de Cursos'
            f'</button>'
            f'</a>'
            f'</div>',
            unsafe_allow_html=True,
        )