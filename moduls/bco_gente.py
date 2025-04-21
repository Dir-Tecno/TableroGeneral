import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row, create_bco_gente_kpis
from utils.styles import COLORES_IDENTIDAD
from utils.kpi_tooltips import ESTADO_CATEGORIAS, TOOLTIPS_DESCRIPTIVOS

# Crear diccionario para tooltips de categorías (técnico, lista de estados)
tooltips_categorias = {k: ", ".join(v) for k, v in ESTADO_CATEGORIAS.items()}

def load_and_preprocess_data(data):
    """
    Carga y preprocesa los datos necesarios para el dashboard.
    
    Args:
        data: Diccionario de dataframes cargados desde GitLab
        
    Returns:
        Tupla con los dataframes procesados y flags de disponibilidad
    """
    with st.spinner("Cargando y procesando datos..."):
        # Extraer los dataframes necesarios
        df_global = data.get('vt_nomina_rep_dpto_localidad.parquet')
        df_recupero = data.get('VT_NOMINA_REP_RECUPERO_X_ANIO.parquet')
        geojson_data = data.get('capa_departamentos_2010.geojson')
        df_localidad_municipio = data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt')
        
        # Verificar silenciosamente si los archivos existen
        has_global_data = df_global is not None and not df_global.empty
        has_recupero_data = df_recupero is not None and not df_recupero.empty
        # Check if geojson_data was loaded (it might be bytes, dict, or None initially)
        has_geojson_data = geojson_data is not None 
        # Check if df_localidad_municipio (likely a string) is not None and not an empty string
        has_localidad_municipio_data = df_localidad_municipio is not None and df_localidad_municipio != "" 
        
        # Renombrar valores en N_LINEA_PRESTAMO
        if has_global_data and 'N_LINEA_PRESTAMO' in df_global.columns:
            # Reemplazar "L4." por "INICIAR EMPRENDIMIENTO"
            df_global['N_LINEA_PRESTAMO'] = df_global['N_LINEA_PRESTAMO'].replace("L4.", "INICIAR EMPRENDIMIENTO")
        
        # Corregir localidades del departamento CAPITAL
        if has_global_data and 'N_DEPARTAMENTO' in df_global.columns and 'N_LOCALIDAD' in df_global.columns:
            # Crear una máscara para identificar registros del departamento CAPITAL
            capital_mask = df_global['N_DEPARTAMENTO'] == 'CAPITAL'
            
            # Aplicar la corrección de localidad
            df_global.loc[capital_mask, 'N_LOCALIDAD'] = 'CORDOBA'
            
            # Si existe la columna ID_LOCALIDAD, corregirla también
            if 'ID_LOCALIDAD' in df_global.columns:
                df_global.loc[capital_mask, 'ID_LOCALIDAD'] = 1
        
        # Asegurarse de que N_ESTADO_PRESTAMO sea string
        if has_global_data and 'N_ESTADO_PRESTAMO' in df_global.columns:
            try:
                df_global['N_ESTADO_PRESTAMO'] = df_global['N_ESTADO_PRESTAMO'].astype(str)
            except Exception as e:
                st.error(f"Error al convertir N_ESTADO_PRESTAMO a string: {e}")
        
        # Realizar el cruce entre df_global y df_recupero si ambos están disponibles
        if has_global_data and has_recupero_data and 'NRO_SOLICITUD' in df_recupero.columns:
            try:
                # Verificar si existen las columnas necesarias en df_recupero
                required_columns = ['NRO_SOLICITUD', 'DEUDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']
                missing_columns = [col for col in required_columns if col not in df_recupero.columns]
                
                if not missing_columns:
                    # Seleccionar solo las columnas necesarias de df_recupero para el merge
                    df_recupero_subset = df_recupero[required_columns].copy()
                    
                    # Renombrar DEUDA como DEUDA_VENCIDA
                    df_recupero_subset = df_recupero_subset.rename(columns={'DEUDA': 'DEUDA_VENCIDA'})
                    
                    # Convertir columnas numéricas a tipo float
                    for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                        df_recupero_subset[col] = pd.to_numeric(df_recupero_subset[col], errors='coerce')
                    
                    # Realizar el merge (left join)
                    df_global = pd.merge(
                        df_global,
                        df_recupero_subset,
                        on='NRO_SOLICITUD',
                        how='left'
                    )
                    
                    # Rellenar valores NaN con 0
                    for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                        df_global[col] = pd.to_numeric(df_global[col], errors='coerce').fillna(0)
                    
                    # Añadir campos calculados
                    df_global['DEUDA_A_RECUPERAR'] = df_global['DEUDA_VENCIDA'] + df_global['DEUDA_NO_VENCIDA']
                    df_global['RECUPERADO'] = df_global['MONTO_OTORGADO'] - df_global['DEUDA_A_RECUPERAR']
                    
                else:
                    st.warning(f"No se pudo realizar el cruce con datos de recupero. Faltan columnas: {', '.join(missing_columns)}")
            except Exception as e:
                st.warning(f"Error al realizar el cruce con datos de recupero: {str(e)}")
        
        # Añadir la categoría a cada estado
        if has_global_data and 'N_ESTADO_PRESTAMO' in df_global.columns:
            # Crear la columna CATEGORIA basada en el estado del préstamo
            df_global['CATEGORIA'] = 'Otros'
            for categoria, estados in ESTADO_CATEGORIAS.items():
                # Crear una máscara para los estados que pertenecen a esta categoría
                mask = df_global['N_ESTADO_PRESTAMO'].isin(estados)
                # Asignar la categoría a los registros que cumplen con la máscara
                df_global.loc[mask, 'CATEGORIA'] = categoria
        
        # Filtrar registros con N_DEPARTAMENTO nulo o igual a "BURRUYACU"
        if has_global_data and 'N_DEPARTAMENTO' in df_global.columns:
            # Crear máscara para identificar registros a excluir
            exclude_mask = (df_global['N_DEPARTAMENTO'].isna()) | (df_global['N_DEPARTAMENTO'] == 'BURRUYACU')
            
            # Filtrar el DataFrame para excluir estos registros
            df_global = df_global[~exclude_mask]
            
            # Verificar si todavía hay datos después del filtrado
            has_global_data = not df_global.empty
        
        # Filtrar líneas de préstamo que no deben ser consideradas
        if has_global_data and 'N_LINEA_PRESTAMO' in df_global.columns:
            # Lista de líneas de préstamo a excluir
            lineas_a_excluir = ["L1", "L3", "L4", "L6"]
            
            # Filtrar el DataFrame para excluir estas líneas
            df_global = df_global[~df_global['N_LINEA_PRESTAMO'].isin(lineas_a_excluir)]
            
            # Verificar si todavía hay datos después del filtrado
            has_global_data = not df_global.empty

        return df_global, df_recupero, geojson_data, df_localidad_municipio, has_global_data, has_recupero_data, has_geojson_data, has_localidad_municipio_data

def render_filters(df_filtrado_global):
    """
    Renderiza los filtros de la interfaz de usuario.
    
    Args:
        df_filtrado_global: DataFrame filtrado con datos globales
        
    Returns:
        Tupla con los valores seleccionados en los filtros
    """
    with st.spinner("Cargando filtros..."):
        # Contenedor para filtros
        st.markdown('<h3 style="font-size: 18px; margin-top: 0;">Filtros</h3>', unsafe_allow_html=True)

        # Crear tres columnas para los filtros
        col1, col2, col3 = st.columns(3)
        
        # Filtro de departamento en la primera columna
        with col1:
            departamentos = sorted(df_filtrado_global['N_DEPARTAMENTO'].dropna().unique())
            all_dpto_option = "Todos los departamentos"
            selected_dpto = st.selectbox("Departamento:", [all_dpto_option] + list(departamentos), key="bco_dpto_filter")
        
        # Filtrar por departamento seleccionado
        if selected_dpto != all_dpto_option:
            df_filtrado = df_filtrado_global[df_filtrado_global['N_DEPARTAMENTO'] == selected_dpto]
            # Filtro de localidad (dependiente del departamento)
            localidades = sorted(df_filtrado['N_LOCALIDAD'].dropna().unique())
            all_loc_option = "Todas las localidades"
            
            # Mostrar filtro de localidad en la segunda columna
            with col2:
                selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key="bco_loc_filter")
            
            if selected_loc != all_loc_option:
                df_filtrado = df_filtrado[df_filtrado['N_LOCALIDAD'] == selected_loc]
        else:
            # Si no se seleccionó departamento, mostrar todas las localidades
            localidades = sorted(df_filtrado_global['N_LOCALIDAD'].dropna().unique())
            all_loc_option = "Todas las localidades"
            df_filtrado = df_filtrado_global
            
            # Mostrar filtro de localidad en la segunda columna
            with col2:
                selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key="bco_loc_filter")
            
            if selected_loc != all_loc_option:
                df_filtrado = df_filtrado[df_filtrado['N_LOCALIDAD'] == selected_loc]
        
        # Filtro de línea de préstamo en la tercera columna
        with col3:
            lineas_prestamo = sorted(df_filtrado['N_LINEA_PRESTAMO'].dropna().unique())
            all_lineas_option = "Todas las líneas"
            selected_linea = st.selectbox("Línea de préstamo:", [all_lineas_option] + list(lineas_prestamo), key="bco_linea_filter")
        
        if selected_linea != all_lineas_option:
            df_filtrado = df_filtrado[df_filtrado['N_LINEA_PRESTAMO'] == selected_linea]
        
        
        return df_filtrado, selected_dpto, selected_loc, selected_linea

def show_bco_gente_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de Banco de la Gente.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
   
    # Mostrar columnas en modo desarrollo
    if is_development:
        st.markdown("***")
        st.caption("Información de Desarrollo (Columnas de DataFrames - Bco. Gente)")
        if isinstance(data, dict):
            for name, df in data.items():
                if df is not None:
                    with st.expander(f"Columnas en: `{name}`"):
                        st.write(df.columns.tolist())
                else:
                    st.warning(f"DataFrame '{name}' no cargado o vacío.")
        else:
            st.warning("Formato de datos inesperado para Banco de la Gente.")
        st.markdown("***")
    
    df_global = None
    df_recupero = None
    
    # Cargar y preprocesar datos
    df_global, df_recupero, geojson_data, df_localidad_municipio, has_global_data, has_recupero_data, has_geojson_data, has_localidad_municipio_data = load_and_preprocess_data(data)
    
    # Verificar que los datos globales existan antes de continuar
    if not has_global_data:
        st.error("No se pudieron cargar los datos globales de Banco de la Gente. Verifique que el archivo 'vt_nomina_rep_dpto_localidad.parquet' exista en el repositorio.")
        return
    
    # Crear una copia del DataFrame para trabajar con él
    df_filtrado_global = df_global.copy()
    
    # Renderizar filtros y obtener datos filtrados
    df_filtrado, selected_dpto, selected_loc, selected_linea = render_filters(df_filtrado_global)
    
    # Mostrar información de actualización de datos
    if dates and any(dates.values()):
        latest_date = max([d for d in dates.values() if d is not None], default=None)
        if latest_date:
            st.caption(f"Última actualización de datos: {latest_date}")
    
    # Crear pestañas para las diferentes vistas
    tab_global, tab_recupero = st.tabs(["GLOBAL", "RECUPERO"])
    
    with tab_global:
        # Mostrar los datos filtrados en la pestaña GLOBAL
        if has_global_data:
            with st.spinner("Cargando visualizaciones globales..."):
                mostrar_global(df_filtrado, tooltips_categorias, df_recupero)
        else:
            st.warning("No hay datos globales disponibles para mostrar.")
    
    with tab_recupero:
        # Mostrar los datos de recupero en la pestaña RECUPERO
        if has_global_data and df_global is not None and not df_global.empty:
            with st.spinner("Cargando visualizaciones de recupero..."):
                # Pasar el DataFrame ya filtrado por render_filters
                mostrar_recupero(df_filtrado, df_localidad_municipio, geojson_data)
        else:
            st.info("No hay datos de recupero disponibles para mostrar.")

def mostrar_global(df_filtrado_global, tooltips_categorias, df_recupero=None):
    """
    Muestra los datos globales del Banco de la Gente.
    
    Args:
        df_filtrado_global: DataFrame filtrado con datos globales
        tooltips_categorias: Diccionario con tooltips para cada categoría
        df_recupero: DataFrame con datos de recupero para la serie histórica
    """
    # Crear el conteo de estados
    try:
        conteo_estados = (
            df_filtrado_global.groupby("N_ESTADO_PRESTAMO")
            .size()
            .rename("conteo")
            .reset_index()
        )
        
        # Crear el diccionario de resultados con los totales para cada categoría
        resultados = {
            categoria: conteo_estados[conteo_estados["N_ESTADO_PRESTAMO"].isin(estados)]['conteo'].sum()
            for categoria, estados in ESTADO_CATEGORIAS.items()
        }
    except Exception as e:
        st.error(f"Error al calcular conteo de estados: {e}")
        resultados = {categoria: 0 for categoria in ESTADO_CATEGORIAS.keys()}
    
    # Usar la función de ui_components para crear y mostrar KPIs
    from utils.ui_components import create_bco_gente_kpis
    kpi_data = create_bco_gente_kpis(resultados, tooltips_categorias)
    display_kpi_row(kpi_data)

    # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)
     # Nueva tabla: Conteo de Préstamos por Línea y Estado
    st.subheader("Conteo de Préstamos por Línea y Estado")
    col_tabla, col_torta = st.columns([3,1])
    with col_tabla:
        try:
            # Verificar que las columnas necesarias existan en el DataFrame
            required_columns = ['N_LINEA_PRESTAMO', 'N_ESTADO_PRESTAMO', 'NRO_SOLICITUD']
            missing_columns = [col for col in required_columns if col not in df_filtrado_global.columns]
        
            if missing_columns:
                st.warning(f"No se pueden mostrar el conteo de préstamos por línea. Faltan columnas: {', '.join(missing_columns)}")
            else:
                # Definir las categorías a mostrar
                categorias_mostrar = ["A Pagar - Convocatoria", "Pagados", "En proceso de pago"]

                # Usar @st.cache_data para evitar recalcular si los datos no cambian
                @st.cache_data
                def prepare_linea_data(df, categorias_mostrar):
                    # Crear copia del DataFrame para manipulación
                    df_conteo = df.copy()

                    # Agregar columna de categoría basada en N_ESTADO_PRESTAMO
                    df_conteo['CATEGORIA'] = 'Otros'
                    for categoria in categorias_mostrar:
                        estados = ESTADO_CATEGORIAS.get(categoria, [])
                        mask = df_conteo['N_ESTADO_PRESTAMO'].isin(estados)
                        df_conteo.loc[mask, 'CATEGORIA'] = categoria

                    # Filtrar para incluir solo las categorías seleccionadas
                    df_conteo = df_conteo[df_conteo['CATEGORIA'].isin(categorias_mostrar)]

                    # Crear pivot table: Línea de préstamo vs Categoría
                    pivot_linea = pd.pivot_table(
                        df_conteo,
                        index=['N_LINEA_PRESTAMO'],
                        columns='CATEGORIA',
                        values='NRO_SOLICITUD',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()

                    # Asegurar que todas las categorías estén en la tabla
                    for categoria in categorias_mostrar:
                        if categoria not in pivot_linea.columns:
                            pivot_linea[categoria] = 0

                    # Calcular totales por línea
                    pivot_linea['Total'] = pivot_linea[categorias_mostrar].sum(axis=1)

                    # Agregar fila de totales
                    totales = pivot_linea[categorias_mostrar + ['Total']].sum()
                    totales_row = pd.DataFrame([['Total'] + totales.values.tolist()], 
                                              columns=['N_LINEA_PRESTAMO'] + categorias_mostrar + ['Total'])
                    return pd.concat([pivot_linea, totales_row], ignore_index=True)

                # Obtener el DataFrame procesado usando caché
                pivot_df = prepare_linea_data(df_filtrado_global, categorias_mostrar)

                # Crear HTML personalizado para la tabla de conteo por línea
                html_table_linea = """
                    <style>
                        .linea-table {
                            width: 100%;
                            border-collapse: collapse;
                            margin-bottom: 20px;
                            font-size: 14px;
                        }
                        .linea-table th, .linea-table td {
                            padding: 8px;
                            border: 1px solid #ddd;
                            text-align: right;
                        }
                        .linea-table th {
                            background-color: #0072bb;
                            color: white;
                            text-align: center;
                        }
                        .linea-table td:first-child {
                            text-align: left;
                        }
                        .linea-table .total-row {
                            background-color: #f2f2f2;
                            font-weight: bold;
                        }
                        .linea-table .total-col {
                            font-weight: bold;
                        }
                        .linea-table .group-header {
                            background-color: #005587;
                        }
                        .linea-table .value-header {
                            background-color: #0072bb;
                        }
                        .linea-table .total-header {
                            background-color: #004b76;
                        }
                    </style>
                """

                # Crear tabla HTML
                html_table_linea += '<table class="linea-table"><thead><tr>'
                html_table_linea += '<th class="group-header">Línea de Préstamo</th>'

                # Agregar encabezados para cada categoría
                for categoria in categorias_mostrar:
                    # Usar tooltips_categorias si está disponible, de lo contrario crear uno básico
                    tooltip_text = ""
                    if 'tooltips_categorias' in locals() or 'tooltips_categorias' in globals():
                        tooltip_text = tooltips_categorias.get(categoria, "")
                    else:
                        # Crear tooltip básico con los estados de la categoría
                        tooltip_text = ", ".join(ESTADO_CATEGORIAS.get(categoria, []))

                    html_table_linea += f'<th class="value-header" title="{tooltip_text}">{categoria}</th>'

                # Encabezado para la columna de total
                html_table_linea += '<th class="total-header">Total</th>'
                html_table_linea += '</tr></thead><tbody>'

                # Agregar filas para cada línea de préstamo
                for idx, row in pivot_df.iterrows():
                    # Formato especial para la fila de totales
                    if row['N_LINEA_PRESTAMO'] == 'Total':
                        html_table_linea += '<tr class="total-row">'
                    else:
                        html_table_linea += '<tr>'

                    # Columna de línea de préstamo
                    html_table_linea += f'<td>{row["N_LINEA_PRESTAMO"]}</td>'

                    # Columnas para cada categoría
                    for categoria in categorias_mostrar:
                        valor = int(row[categoria]) if categoria in row else 0
                        html_table_linea += f'<td>{valor}</td>'

                    # Columna de total
                    html_table_linea += f'<td class="total-col">{int(row["Total"])}</td>'
                    html_table_linea += '</tr>'

                html_table_linea += '</tbody></table>'

                # Mostrar la tabla
                st.markdown(html_table_linea, unsafe_allow_html=True)
        except Exception as e:
            st.warning(f"Error al generar la tabla de conteo por línea: {str(e)}")
    # prueba de torta
    with col_torta:
        try:
            # Asumimos que categorias_mostrar = ["A Pagar - Convocatoria", "Pagados", "En proceso de pago"]
            # Filtrar el DataFrame ANTES de agrupar, usando la columna de estado
            # st.dataframe(df_filtrado_global)
            df_filtrado_torta = df_filtrado_global[df_filtrado_global['CATEGORIA'].isin(categorias_mostrar)]
            
            # Agrupar el DataFrame filtrado por línea de préstamo
            grafico_torta = df_filtrado_torta.groupby('N_LINEA_PRESTAMO').size().reset_index(name='Cantidad')
            
            # Si el dataframe resultante está vacío, mostrar mensaje
            if grafico_torta.empty:
                 st.info("No hay datos en las categorías seleccionadas para mostrar en el gráfico.")
            else:
                 colores_identidad = COLORES_IDENTIDAD
                 fig_torta = px.pie(
                     grafico_torta, 
                     names='N_LINEA_PRESTAMO', 
                     values='Cantidad', 
                     color_discrete_sequence=colores_identidad
                 )
                 fig_torta.update_traces(
                     textposition='inside',
                     textinfo='percent+label',
                     marker=dict(line=dict(color='#FFFFFF', width=1))
                     
                 )
                 fig_torta.update_layout(
                     legend_title="Líneas de Préstamo",
                     font=dict(size=12),
                     uniformtext_minsize=10,
                     uniformtext_mode='hide',
                     legend_orientation="h", # Orientación horizontal
                     legend_yanchor="bottom",
                     legend_y=-0.1, # Ajustar posición vertical (debajo del gráfico)
                     legend_xanchor="center",
                     legend_x=0.5 # Centrar horizontalmente
                 )
                 st.plotly_chart(fig_torta, use_container_width=True)
        except Exception as e:
            st.warning(f"Error al generar la torta de conteo por línea: {str(e)}")
    # Tabla de estados de préstamos agrupados
    st.subheader("Estados de Préstamos por Categoría")
    try: #Tabla de estados de préstamos agrupados por categoría
        # Verificar que las columnas necesarias existan en el DataFrame
        required_columns = ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'N_ESTADO_PRESTAMO', 'NRO_SOLICITUD']
        missing_columns = [col for col in required_columns if col not in df_filtrado_global.columns]
        
        if missing_columns:
            st.warning(f"No se pueden mostrar los estados de préstamos. Faltan columnas: {', '.join(missing_columns)}")
        else:
            # Filtro específico para esta tabla - Categorías de estado
            categorias_orden = list(ESTADO_CATEGORIAS.keys())
            # Excluir "Rechazados - Bajas" de las categorías disponibles
            if "Rechazados - Bajas" in categorias_orden:
                categorias_orden.remove("Rechazados - Bajas")
            
            # Usar st.session_state para mantener las categorías seleccionadas
            if 'selected_categorias' not in st.session_state:
                st.session_state.selected_categorias = categorias_orden
                
            col1, col2 = st.columns([3, 1])
            
            with col1: # Multiselect para seleccionar categorías
                selected_categorias = st.multiselect(
                    "Filtrar por categorías de estado:",
                    options=categorias_orden,
                    default=st.session_state.selected_categorias,
                    key="estado_categoria_filter"
                )
                
                # Actualizar session_state solo si cambia la selección
                if selected_categorias != st.session_state.selected_categorias:
                    st.session_state.selected_categorias = selected_categorias
            
            # Si no se selecciona ninguna categoría, mostrar todas
            if not selected_categorias:
                selected_categorias = categorias_orden
                
            # Crear copia del DataFrame para manipulación
            # Usar @st.cache_data para evitar recalcular si los datos no cambian
            @st.cache_data
            def prepare_categoria_data(df, categorias):
                df_copy = df.copy()

                # Agregar columna de categoría basada en N_ESTADO_PRESTAMO
                df_copy['CATEGORIA'] = 'Otros'
                for categoria, estados in ESTADO_CATEGORIAS.items():
                    mask = df_copy['N_ESTADO_PRESTAMO'].isin(estados)
                    df_copy.loc[mask, 'CATEGORIA'] = categoria

                # Crear pivot table con conteo agrupado por categorías
                pivot_df = df_copy.pivot_table(
                    index=['N_DEPARTAMENTO', 'N_LOCALIDAD'],
                    columns='CATEGORIA',
                    values='NRO_SOLICITUD',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                # Asegurar que todas las categorías estén en la tabla
                for categoria in ESTADO_CATEGORIAS.keys():
                    if categoria not in pivot_df.columns:
                        pivot_df[categoria] = 0
                
                # Reordenar columnas para mostrar en orden consistente
                return pivot_df.reindex(columns=['N_DEPARTAMENTO', 'N_LOCALIDAD'] + list(ESTADO_CATEGORIAS.keys()))
            
            # Obtener el DataFrame procesado usando caché
            pivot_df = prepare_categoria_data(df_filtrado_global, categorias_orden)
            
            # Filtrar solo las columnas seleccionadas
            columnas_mostrar = ['N_DEPARTAMENTO', 'N_LOCALIDAD'] + selected_categorias
            pivot_df_filtered = pivot_df[columnas_mostrar].copy()
            
            # Agregar columna de total para las categorías seleccionadas
            pivot_df_filtered['Total'] = pivot_df_filtered[selected_categorias].sum(axis=1)
            
            # Agregar fila de totales
            totales = pivot_df_filtered[selected_categorias + ['Total']].sum()
            totales_row = pd.DataFrame([['Total', 'Total'] + totales.values.tolist()], 
                                      columns=['N_DEPARTAMENTO', 'N_LOCALIDAD'] + selected_categorias + ['Total'])
            pivot_df_filtered = pd.concat([pivot_df_filtered, totales_row], ignore_index=True)
            
            # Aplicar estilo a la tabla usando pandas Styler
            def highlight_totals(val):
                if val == 'Total':
                    return 'background-color: #f2f2f2; font-weight: bold'
                return ''
            
            def highlight_total_rows(s):
                is_total_row = s.iloc[0] == 'Total' or s.iloc[1] == 'Total'
                return ['background-color: #f2f2f2; font-weight: bold' if is_total_row else '' for _ in s]
            
            # Crear objeto Styler
            styled_df = pivot_df_filtered.style \
                .applymap(highlight_totals, subset=['N_DEPARTAMENTO', 'N_LOCALIDAD'])
            
            # Aplicar formato a las columnas numéricas
            numeric_columns = selected_categorias + ['Total']
            
            styled_df = styled_df \
                .apply(highlight_total_rows, axis=1, subset=numeric_columns) \
                .format({col: '{:,.0f}' for col in selected_categorias + ['Total']}) \
                .background_gradient(subset=selected_categorias, cmap='Blues', low=0.1, high=0.9) \
                .set_properties(**{'text-align': 'right'}, subset=numeric_columns) \
                .set_properties(**{'text-align': 'left'}, subset=['N_DEPARTAMENTO', 'N_LOCALIDAD'])
            
            # Configurar las columnas para st.dataframe
            column_config = {
                "N_DEPARTAMENTO": st.column_config.TextColumn("Departamento"),
                "N_LOCALIDAD": st.column_config.TextColumn("Localidad"),
                "Total": st.column_config.NumberColumn(
                    "Total",
                    help="Suma total de todas las categorías seleccionadas"
                )
            }
            
            # Mostrar la tabla con st.dataframe
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
                height=400
            )
            
            # Línea divisoria en gris claro
            st.markdown("<hr style='border: 1px solid #f0f0f0;'>", unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"Error al generar la tabla de estados: {str(e)}")
    # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)

    # Serie Histórica y gráfico de torta
    try: #Serie Histórica y gráfico de torta
        st.subheader("Serie Histórica de Préstamos")
        # Verificar si existe df_recupero y las columnas necesarias
        if df_recupero is None or df_recupero.empty:
            st.info("No hay datos disponibles para la serie histórica.")
        elif 'NRO_SOLICITUD' not in df_recupero.columns or 'FEC_FORM' not in df_recupero.columns:
            st.info("No hay datos disponibles para la serie histórica.")
        else:
            df_fechas = df_recupero.copy()

            if 'FEC_FORM' not in df_fechas.columns:
                st.error("DEBUG: 'FEC_FORM' no encontrada en df_fechas!")
                return # Salir si falta la columna clave

            # Convertir la columna a datetime si no lo es
            if not pd.api.types.is_datetime64_any_dtype(df_fechas['FEC_FORM']):
                df_fechas['FEC_FORM'] = pd.to_datetime(df_fechas['FEC_FORM'], errors='coerce')

            # Eliminar filas con fechas inválidas
            df_fechas = df_fechas.dropna(subset=['FEC_FORM'])

            # Filtrar fechas futuras (posteriores a la fecha actual)
            fecha_actual = datetime.now()
            df_fechas = df_fechas[df_fechas['FEC_FORM'] <= fecha_actual]

            if df_fechas.empty:
                st.info("No hay datos disponibles para la serie histórica.")
            else:
                # Mostrar rango de fechas disponibles       
                fecha_min = df_fechas['FEC_FORM'].min().strftime('%d/%m/%Y')
                fecha_max = df_fechas['FEC_FORM'].max().strftime('%d/%m/%Y')
                st.caption(f"Rango de fechas disponibles: {fecha_min} - {fecha_max}")

                # --- Filtro de Fechas para Serie Histórica ---
                min_date_recupero = df_fechas['FEC_FORM'].min()
                max_date_recupero = df_fechas['FEC_FORM'].max()
                
                # Convertir a datetime.date si son Timestamps para el widget
                min_value_dt = min_date_recupero.date() if isinstance(min_date_recupero, pd.Timestamp) else min_date_recupero
                max_value_dt = max_date_recupero.date() if isinstance(max_date_recupero, pd.Timestamp) else max_date_recupero

                # Valor por defecto: últimos 2 años si es posible, sino todo el rango
                # Convertir a Timestamp para calcular la fecha de hace 2 años
                min_value_ts = pd.Timestamp(min_value_dt)
                max_value_ts = pd.Timestamp(max_value_dt)
                two_years_ago_ts = max_value_ts - pd.DateOffset(years=2)
                # Comparar Timestamps y obtener el Timestamp de inicio por defecto
                default_start_ts = max(min_value_ts, two_years_ago_ts) 
                # Convertir de nuevo a date para el valor del widget st.date_input
                default_start_dt = default_start_ts.date()

                # Generar opciones para el slider (inicio de cada mes)
                date_range = pd.date_range(start=min_value_dt, end=max_value_dt, freq='MS') # MS: Month Start
                options = [date.date() for date in date_range] # Convertir a lista de datetime.date

                # Si no hay opciones (rango muy corto), no mostrar slider
                if not options:
                    st.warning("No hay suficiente rango de fechas para mostrar el slider.")
                    start_date, end_date = min_value_dt, max_value_dt # Usar el rango completo
                else:
                    # Calcular valor por defecto para el slider
                    default_end_slider = options[-1] # Último mes disponible
                    # Encontrar el inicio de mes de hace 2 años
                    two_years_ago_month_start = (pd.Timestamp(max_value_dt).replace(day=1) - pd.DateOffset(years=2)).date()
                    # Encontrar la opción más cercana a hace 2 años (o la primera si todo es más reciente)
                    default_start_slider = min(options, key=lambda date: abs(date - two_years_ago_month_start))
                    # Asegurarse que el inicio por defecto no sea posterior al fin por defecto
                    if default_start_slider > default_end_slider:
                        default_start_slider = default_end_slider
                    
                    # Si solo hay una opción, seleccionarla como inicio y fin
                    if len(options) == 1:
                         default_start_slider = options[0]
                         default_end_slider = options[0]
                    
                    start_date, end_date = st.select_slider(
                        'Seleccionar período de la serie histórica:',
                        options=options,
                        value=(default_start_slider, default_end_slider),
                        format_func=lambda date: date.strftime("%b %Y"), # Formato Mes Año
                        key='slider_fecha_serie_historica'
                    )

                # ----------------------------------------------

                # Filtrar df_fechas según el rango seleccionado
                # Convertir start_date y end_date a Timestamp para comparación
                start_ts = pd.Timestamp(start_date)
                # El end_date del slider es el *inicio* del último mes seleccionado.
                # Para incluir todo ese mes, vamos al inicio del *siguiente* mes.
                end_ts = pd.Timestamp(end_date) + pd.offsets.MonthBegin(1)
                 
                df_fechas_filtrado = df_fechas[(df_fechas['FEC_FORM'] >= start_ts) & (df_fechas['FEC_FORM'] < end_ts)]

                # --- Generar Serie Histórica solo si hay datos después de filtrar ---
                if df_fechas_filtrado.empty:
                    st.info("No hay datos de formularios para el período seleccionado.")
                    # Asegurarse de que col_serie exista aunque esté vacía para que col_torta funcione
                    col_serie, col_torta = st.columns([3, 1])
                    with col_serie:
                        st.write("") # Platzhalter
                else:
                    # Agrupar por mes y año, y contar NRO_SOLICITUD
                    df_fechas_filtrado['AÑO_MES'] = df_fechas_filtrado['FEC_FORM'].dt.strftime('%Y-%m')
                    serie_historica = df_fechas_filtrado.groupby('AÑO_MES').size().reset_index(name='Cantidad')
                    serie_historica['FECHA'] = pd.to_datetime(serie_historica['AÑO_MES'] + '-01')
                    serie_historica = serie_historica.sort_values('FECHA')

                    fig_historia = px.line(
                        serie_historica, 
                        x='FECHA', 
                        y='Cantidad', 
                        title='Evolución de Formularios por Mes (Período Seleccionado)',
                        labels={'Cantidad': 'Cantidad de Formularios', 'FECHA': 'Mes'},
                        markers=True
                    )
                    fig_historia.update_layout(
                        xaxis=dict(
                            title='Fecha',
                            title_font_size=14,
                            tickfont_size=12,
                            gridcolor='lightgray',
                            tickformat='%b %Y'  # Formato de mes y año
                        ),
                        yaxis=dict(
                            title='Cantidad de Formularios',
                            title_font_size=14,
                            tickfont_size=12,
                            gridcolor='lightgray'
                        ),
                        plot_bgcolor='white'
                    )

                    # Mostrar el gráfico y la serie histórica en columnas (serie: 3/4, torta: 1/4)
                    try:
                        st.plotly_chart(fig_historia, use_container_width=True)
                        with st.expander("Ver datos de la serie histórica"):
                            tabla_data = serie_historica.copy()
                            tabla_data['Mes-Año'] = tabla_data['FECHA'].dt.strftime('%b %Y')
                            tabla_data_sorted = tabla_data.sort_values('FECHA', ascending=False)
                            st.dataframe(
                                tabla_data_sorted[['Mes-Año', 'Cantidad']],
                                hide_index=True
                            )
                    except Exception as e:
                        st.warning(f"Error en la serie histórica: {e}")
                        # Mostrar tabla_data solo si existe
                        try:
                            if 'tabla_data' in locals():
                                st.dataframe(tabla_data)
                        except Exception:
                            st.error("No se pudo mostrar la tabla de datos")

    except Exception as e:
        st.warning(f"Error al procesar sección Serie Histórica/Torta: {str(e)}")
    # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)



def mostrar_recupero(df_filtrado, df_localidad_municipio, geojson_data):
    """
    Muestra la sección de recupero de deudas, utilizando datos ya filtrados.
    
    Args:
        df_filtrado: DataFrame con datos globales ya filtrados por render_filters.
        df_localidad_municipio: DataFrame con mapeo localidad-municipio.
        geojson_data: Datos GeoJSON para mapas.
    """
    st.header("Análisis de Recupero")
    if df_filtrado is None or df_filtrado.empty:
        # Ajustar mensaje, ya que el df está filtrado
        st.warning("No hay datos disponibles para el análisis de recupero con los filtros seleccionados.")
        return

    # --- Nueva Sección: Tabla Agrupada de Pagados (usando datos ya filtrados) ---
    st.subheader("Detalle de Préstamos Pagados por Localidad (Según Filtros Aplicados)")
    
    # Asegurarse de que las columnas necesarias existen en el df_filtrado
    # 'N_LINEA_PRESTAMO' ya está implícitamente filtrada por render_filters, pero la necesitamos para la verificación
    required_cols = ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'N_LINEA_PRESTAMO', 
                     'CATEGORIA', 'NRO_SOLICITUD', 'DEUDA_VENCIDA', 
                     'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO', 
                     'DEUDA_A_RECUPERAR', 'RECUPERADO']
    if not all(col in df_filtrado.columns for col in required_cols):
        st.error("Faltan columnas requeridas en los datos filtrados para la tabla de pagados. Columnas presentes: " + str(df_filtrado.columns.tolist()))
        return

    # --- Filtros multiselect eliminados: La tabla ahora usa df_filtrado que viene de render_filters ---
        
    # Filtrar solo por la categoría "Pagados" sobre el DataFrame ya filtrado
    df_filtrado_pagados = df_filtrado[
        (df_filtrado['CATEGORIA'] == "Pagados")
    ].copy()
    # Las condiciones de N_DEPARTAMENTO, N_LOCALIDAD, N_LINEA_PRESTAMO ya no son necesarias aquí
    # porque df_filtrado ya las tiene aplicadas desde render_filters.
    
    if df_filtrado_pagados.empty:
        st.info("No se encontraron préstamos 'Pagados' con los filtros seleccionados.")
    else:
        # Agrupar y agregar (usando el df_filtrado_pagados)
        # Agrupamos por Departamento y Localidad para el desglose.
        df_agrupado = df_filtrado_pagados.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD']).agg(
            Cantidad_Solicitudes=('NRO_SOLICITUD', 'count'),
            Total_Deuda_Vencida=('DEUDA_VENCIDA', 'sum'),
            Total_Deuda_No_Vencida=('DEUDA_NO_VENCIDA', 'sum'),
            Total_Monto_Otorgado=('MONTO_OTORGADO', 'sum'),
            Total_Deuda_A_Recuperar=('DEUDA_A_RECUPERAR', 'sum'),
            Total_Recuperado=('RECUPERADO', 'sum')
        ).reset_index()
        
        # Formatear columnas de moneda
        currency_cols = ['Total_Deuda_Vencida', 'Total_Deuda_No_Vencida', 
                         'Total_Monto_Otorgado', 'Total_Deuda_A_Recuperar', 'Total_Recuperado']
        for col in currency_cols:
            df_agrupado[col] = df_agrupado[col].apply(
                lambda x: f"${x:,.0f}".replace(',', '.') if pd.notna(x) and isinstance(x, (int, float)) else "$0"
            )

        # Renombrar columnas para la tabla
        df_agrupado.rename(columns={
            'N_DEPARTAMENTO': 'Departamento',
            'N_LOCALIDAD': 'Localidad',
            'Cantidad_Solicitudes': 'Cant. Solicitudes',
            'Total_Deuda_Vencida': 'Deuda Vencida ($)',
            'Total_Deuda_No_Vencida': 'Deuda No Vencida ($)',
            'Total_Monto_Otorgado': 'Monto Otorgado ($)',
            'Total_Deuda_A_Recuperar': 'Deuda a Recuperar ($)',
            'Total_Recuperado': 'Recuperado ($)'
        }, inplace=True)
        
        # Mostrar tabla
        st.dataframe(df_agrupado, use_container_width=True)
        
    st.markdown("--- ") # Separador para la siguiente sección