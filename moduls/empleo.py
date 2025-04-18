import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row
from utils.map_utils import is_mapping_available, create_choropleth_map, display_map

# Importación condicional para Altair
try:
    import altair as alt
    ALTAIR_AVAILABLE = True
except ImportError:
    ALTAIR_AVAILABLE = False
    st.warning("Algunas visualizaciones de gráficos no estarán disponibles. Para habilitar todas las funciones, instale el paquete: altair")

# Importaciones condicionales para manejar posibles errores
try:
    import folium
    from streamlit_folium import folium_static
    import geopandas as gpd
    MAPPING_AVAILABLE = True
except ImportError:
    MAPPING_AVAILABLE = False
    st.warning("Algunas funcionalidades de mapas no estarán disponibles. Para habilitar todas las funciones, instale los paquetes: folium, streamlit-folium y geopandas")

def enviar_a_slack(mensaje, valoracion):
    """
    Envía un mensaje a Slack con la valoración del usuario.
    
    Args:
        mensaje: El mensaje del usuario
        valoracion: La valoración del 1 al 5
    
    Returns:
        bool: True si el mensaje se envió correctamente, False en caso contrario
    """
    try:
        # URL del webhook de Slack (reemplazar con la URL real)
        webhook_url = "https://hooks.slack.com/services/your/webhook/url"
        
        # Crear el mensaje con formato
        estrellas = "⭐" * valoracion
        payload = {
            "text": f"*Nueva valoración del reporte:* {estrellas}\n*Comentario:* {mensaje}"
        }
        
        # Enviar la solicitud POST a Slack
        response = requests.post(webhook_url, json=payload)
        
        # Verificar si la solicitud fue exitosa
        return response.status_code == 200
    except Exception as e:
        st.error(f"Error al enviar a Slack: {str(e)}")
        return False

def calculate_cupo(cantidad_empleados, empleador, adherido):
    # Condición para el programa PPP
    if adherido == "PPP - PROGRAMA PRIMER PASO [2024]":
        if cantidad_empleados < 1:
            return 0
        elif cantidad_empleados <= 5:
            return 1
        elif cantidad_empleados <= 10:
            return 2
        elif cantidad_empleados <= 25:
            return 3
        elif cantidad_empleados <= 50:
            return math.ceil(0.2 * cantidad_empleados)
        else:
            return math.ceil(0.1 * cantidad_empleados)

    # Condición para el programa EMPLEO +26
    elif adherido == "EMPLEO +26":
        if empleador == 'N':
            return 1
        if cantidad_empleados < 1:
            return 1
        elif cantidad_empleados <= 7:
            return 2
        elif cantidad_empleados <= 30:
            return math.ceil(0.2 * cantidad_empleados)
        elif cantidad_empleados <= 165:
            return math.ceil(0.15 * cantidad_empleados)
        else:
            return math.ceil(0.1 * cantidad_empleados)
    
    return 0

def load_and_preprocess_data(data, dates=None):
    """
    Carga y preprocesa los datos necesarios para el dashboard.
    
    Args:
        data: Diccionario de dataframes cargados desde GitLab
        dates: Diccionario de fechas de actualización de los archivos
        
    Returns:
        Tupla con los dataframes procesados y flags de disponibilidad
    """
    with st.spinner("Cargando y procesando datos de empleo..."):
        # Extraer los dataframes necesarios
        df_inscriptos_raw = data.get('VT_REPORTES_PPP_MAS26.parquet')
        geojson_data = data.get('capa_departamentos_2010.geojson')
        
        # Cargar el dataset de empresas
        df_empresas = data.get('vt_empresas_adheridas.parquet')
        has_empresas = df_empresas is not None and not df_empresas.empty
        
        # Cargar el nuevo dataset de liquidación por localidad
        df_liquidacion = data.get('VT_REPORTE_LIQUIDACION_LOCALIDAD.parquet')
        has_liquidacion = df_liquidacion is not None and not df_liquidacion.empty
        
        # Cargar dataset de población
        df_poblacion = data.get('POBLACION.parquet')
        has_poblacion = df_poblacion is not None and not df_poblacion.empty
        
        # Verificar si hay datos geojson
        has_geojson = geojson_data is not None
        
        # Solo mostrar mensaje si hay error al cargar el dataset
        if not has_liquidacion:
            st.warning("No se pudo cargar el dataset de liquidación por localidad.")
        
        # Verificar que los datos estén disponibles
        if df_inscriptos_raw is None or df_inscriptos_raw.empty:
            st.error("No se pudieron cargar los datos de inscripciones.")
            return None, None, None, None, False, False, False, False
        
        # Filtrar para excluir el estado "ADHERIDO"
        df_inscriptos = df_inscriptos_raw[df_inscriptos_raw['N_ESTADO_FICHA'] != "ADHERIDO"].copy()

        # Convertir campos numéricos a enteros para eliminar decimales (.0)
        integer_columns = [
            "ID_DEPARTAMENTO_GOB", 
            "ID_LOCALIDAD_GOB",
            "ID_FICHA",
            "IDETAPA",
            "CUPO",
            "ID_MOD_CONT_AFIP",
            "EDAD"
        ]
        
        # Convertir solo las columnas que existen en el DataFrame
        for col in integer_columns:
            if col in df_inscriptos.columns:
                # Primero convertir a float para manejar posibles NaN, luego a int
                df_inscriptos[col] = df_inscriptos[col].fillna(-1)  # Reemplazar NaN con -1 temporalmente
                df_inscriptos[col] = df_inscriptos[col].astype(int)
                # Opcional: volver a convertir -1 a NaN si es necesario
                df_inscriptos.loc[df_inscriptos[col] == -1, col] = pd.NA
        
        # Corregir localidades del departamento CAPITAL a "CORDOBA"
        if 'N_DEPARTAMENTO' in df_inscriptos.columns and 'N_LOCALIDAD' in df_inscriptos.columns:
            # Crear una máscara para identificar registros del departamento CAPITAL
            capital_mask = df_inscriptos['N_DEPARTAMENTO'] == 'CAPITAL'
            
            # Aplicar la corrección solo a los registros del departamento CAPITAL
            df_inscriptos.loc[capital_mask, 'N_LOCALIDAD'] = 'CORDOBA'
        
        # Añadir columna de ZONA FAVORECIDA
        zonas_favorecidas = [
            'PRESIDENTE ROQUE SAENZ PEÑA', 'GENERAL ROCA', 'RIO SECO', 'TULUMBA', 
            'POCHO', 'SAN JAVIER', 'SAN ALBERTO', 'MINAS', 'CRUZ DEL EJE', 
            'TOTORAL', 'SOBREMONTE', 'ISCHILIN'
        ]
        
        # Crear la columna ZONA
        df_inscriptos['ZONA'] = df_inscriptos['N_DEPARTAMENTO'].apply(
            lambda x: 'ZONA FAVORECIDA' if x in zonas_favorecidas else 'ZONA REGULAR'
        )
        
        # Obtener la fecha de última actualización
        if dates:
            file_dates = [dates.get(k) for k in dates.keys() if 'VT_REPORTES_PPP_MAS26.parquet' in k]
            latest_date = file_dates[0] if file_dates else None
            
            if latest_date:
                latest_date = pd.to_datetime(latest_date)
                st.markdown(f"""
                    <div style="background-color:#e9ecef; padding:10px; border-radius:5px; margin-bottom:20px; font-size:0.9em;">
                        <i class="fas fa-sync-alt"></i> <strong>Última actualización:</strong> {latest_date.strftime('%d/%m/%Y')}
                    </div>
                """, unsafe_allow_html=True)
        
        # Preparar datos para los filtros
        # Limpiar y preparar los datos
        df = df_inscriptos.copy()
        
        # Mapeo de programas según IDETAPA
        programas = {
            53: "Programa Primer Paso",
            51: "Más 26",
            54: "CBA Mejora",
            55: "Nueva Oportunidad"
        }
        
        # Crear columna con nombres de programas
        if 'IDETAPA' in df.columns:
            df['PROGRAMA'] = df['IDETAPA'].map(lambda x: programas.get(x, f"Programa {x}"))
        else:
            df['PROGRAMA'] = "No especificado"
            
        has_fichas = True  # Si llegamos hasta aquí, tenemos datos de fichas
        
        return df, df_empresas, df_poblacion, geojson_data, has_fichas, has_empresas, has_poblacion, has_geojson

def render_filters(df_inscriptos):
    """
    Renderiza los filtros de la interfaz de usuario.
    
    Args:
        df_inscriptos: DataFrame con los datos de inscripciones
        
    Returns:
        Tupla con el DataFrame filtrado y los filtros seleccionados
    """
    # Contenedor para filtros
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    st.markdown('<h3 style="font-size: 18px; margin-top: 0;">Filtros</h3>', unsafe_allow_html=True)
    
    # Crear dos columnas para los filtros
    col1, col2 = st.columns(2)
    
    # Filtro de departamento en la primera columna
    with col1:
        departamentos = sorted(df_inscriptos['N_DEPARTAMENTO'].dropna().unique())
        all_dpto_option = "Todos los departamentos"
        selected_dpto = st.selectbox("Departamento:", [all_dpto_option] + list(departamentos))
    
    # Filtrar por departamento seleccionado
    if selected_dpto != all_dpto_option:
        df_filtered = df_inscriptos[df_inscriptos['N_DEPARTAMENTO'] == selected_dpto]
        # Filtro de localidad (dependiente del departamento)
        localidades = sorted(df_filtered['N_LOCALIDAD'].dropna().unique())
        all_loc_option = "Todas las localidades"
        
        # Mostrar filtro de localidad en la segunda columna
        with col2:
            selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades))
        
        if selected_loc != all_loc_option:
                if isinstance(selected_loc, str) and selected_loc.isdigit():
                    # Si la localidad seleccionada es un número en formato string
                    selected_loc_int = int(selected_loc)
                    df_filtered = df_filtered[df_filtered['N_LOCALIDAD'].fillna(-1).astype(int) == selected_loc_int]
                else:
                    # Si la localidad seleccionada es un string no numérico
                    df_filtered = df_filtered[df_filtered['N_LOCALIDAD'].fillna('').astype(str) == str(selected_loc)]
    else:
        df_filtered = df_inscriptos
        # Si no se seleccionó departamento, mostrar todas las localidades
        localidades = sorted(df_inscriptos['N_LOCALIDAD'].dropna().unique())
        all_loc_option = "Todas las localidades"
        
        # Mostrar filtro de localidad en la segunda columna
        with col2:
            selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades))
        
        if selected_loc != all_loc_option:
            df_filtered = df_filtered[df_filtered['N_LOCALIDAD'] == selected_loc]
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Mostrar resumen de filtros aplicados
    filtros_aplicados = []
    if selected_dpto != all_dpto_option:
        filtros_aplicados.append(f"Departamento: {selected_dpto}")
    if selected_loc != all_loc_option:
        filtros_aplicados.append(f"Localidad: {selected_loc}")
        
    if filtros_aplicados:
        st.markdown(f"""
            <div style="background-color:#e9ecef; padding:10px; border-radius:5px; margin-bottom:20px; font-size:0.9em;">
                <strong>Filtros aplicados:</strong> {' | '.join(filtros_aplicados)}
            </div>
        """, unsafe_allow_html=True)
    
    return df_filtered, selected_dpto, selected_loc, all_dpto_option, all_loc_option

def render_dashboard(df_inscriptos, df_empresas, df_poblacion, geojson_data, has_empresas, has_geojson):
    """
    Renderiza el dashboard principal con los datos procesados.
    """
    with st.spinner("Generando visualizaciones..."):
        # Mostrar columnas de df_inscriptos para depuración
        st.markdown("### Columnas de df_inscriptos")
        st.write(df_inscriptos.columns.tolist())

        # Calcular KPIs importantes antes de aplicar filtros
        total_beneficiarios = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'] == "BENEFICIARIO"].shape[0]
        total_beneficiarios_cti = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'] == "BENEFICIARIO- CTI"].shape[0]
        total_general = total_beneficiarios + total_beneficiarios_cti
        
        # Calcular beneficiarios por zona
        beneficiarios_zona_favorecida = df_inscriptos[(df_inscriptos['N_ESTADO_FICHA'].isin(["BENEFICIARIO", "BENEFICIARIO- CTI"])) & 
                                        (df_inscriptos['ZONA'] == 'ZONA FAVORECIDA')].shape[0]
        
        # Mostrar KPIs en la parte superior
        st.markdown('<div class="kpi-container">', unsafe_allow_html=True)
        
        # Usar la función auxiliar para mostrar KPIs
        kpi_data = [
            {
                "title": "BENEFICIARIOS",
                "value": total_beneficiarios,
                "color_class": "kpi-primary"
            },
            {
                "title": "BENEFICIARIOS CTI",
                "value": total_beneficiarios_cti,
                "color_class": "kpi-secondary"
            },
            {
                "title": "TOTAL BENEFICIARIOS",
                "value": total_general,
                "color_class": "kpi-accent-3"
            },
            {
                "title": "ZONA FAVORECIDA",
                "value": beneficiarios_zona_favorecida,
                "color_class": "kpi-accent-4"
            }
        ]
        
        display_kpi_row(kpi_data, num_columns=4)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Aplicar filtros a los datos
        df_filtered, selected_dpto, selected_loc, all_dpto_option, all_loc_option = render_filters(df_inscriptos)
        
        # Crear pestañas para organizar el contenido
        tab_beneficiarios, tab_empresas = st.tabs(["Beneficiarios", "Empresas"])
        
        # Contenido de la pestaña Beneficiarios
        with tab_beneficiarios:
            # Conteo de ID_FICHA por PROGRAMA y ESTADO_FICHA
            pivot_table = df_filtered.pivot_table(
                index='PROGRAMA',
                columns='N_ESTADO_FICHA',
                values='ID_FICHA',
                aggfunc='count',
                fill_value=0
            )
            
            # Definir el orden de las columnas por grupos
            grupo1 = ["POSTULANTE APTO", "INSCRIPTO", "BENEFICIARIO"]
            grupo2 = ["INSCRIPTO - CTI", "RETENIDO - CTI", "VALIDADO - CTI", "BENEFICIARIO- CTI", "EX BENEFICARIO", "BAJA - CTI"]
            grupo3 = ["POSTULANTE SIN EMPRESA", "FUERA CUPO DE EMPRESA", "RECHAZO FORMAL", "INSCRIPTO NO ACEPTADO", "DUPLICADO", "EMPRESA NO APTA"]
            
            # Crear una lista con todas las columnas en el orden deseado
            columnas_ordenadas = grupo1 + grupo2 + grupo3
            
            # Añadir totales primero para cálculos internos, pero no los mostraremos
            pivot_table['Total'] = pivot_table.sum(axis=1)
            pivot_table.loc['Total'] = pivot_table.sum()
            
            # Reordenar con las columnas existentes más cualquier otra columna y el total al final (para cálculos)
            pivot_table = pivot_table.reindex(columns=columnas_ordenadas + [col for col in pivot_table.columns if col not in columnas_ordenadas and col != 'Total'] + ['Total'])
            
            # Mostrar tabla con estilo mejorado
            st.markdown('<div class="section-title">Conteo de Fichas por Programa y Estado</div>', unsafe_allow_html=True)
            
            # Convertir pivot table a DataFrame para mejor visualización
            pivot_df = pivot_table.reset_index()
            
            # Separar las columnas por grupos
            grupo1_cols = [col for col in grupo1 if col in pivot_table.columns]
            grupo2_cols = [col for col in grupo2 if col in pivot_table.columns]
            grupo3_cols = [col for col in grupo3 if col in pivot_table.columns]
            otros_cols = [col for col in pivot_table.columns if col not in grupo1 and col not in grupo2 and col not in grupo3 and col != 'Total' and col != 'PROGRAMA']
            
            # Crear HTML personalizado para mostrar la tabla principal (grupos 1 y 2)
            html_table_main = """
            <div style="overflow-x: auto; margin-bottom: 20px;">
                <table class="styled-table">
                    <thead>
                        <tr>
                            <th rowspan="2">PROGRAMA</th>
                            <th colspan="{}" style="background-color: var(--color-primary); border-right: 2px solid white;">Grupo 1</th>
                            <th colspan="{}" style="background-color: var(--color-secondary); border-right: 2px solid white;">Grupo 2</th>
                        </tr>
                        <tr>
            """.format(
                len(grupo1_cols),
                len(grupo2_cols)
            )
            
            # Agregar las cabeceras de columnas para la tabla principal
            for col in grupo1_cols + grupo2_cols:
                # Determinar el estilo según el grupo
                if col == "BENEFICIARIO":
                    style = 'style="background-color: #0066a0; color: white;"'  # Versión más oscura del color primario
                elif col == "BENEFICIARIO- CTI":
                    style = 'style="background-color: #0080b3; color: white;"'  # Versión más oscura del color secundario
                elif col in grupo1:
                    style = 'style="background-color: var(--color-primary);"'
                elif col in grupo2:
                    style = 'style="background-color: var(--color-secondary);"'
                else:
                    style = 'style="background-color: var(--color-accent-2);"'
                
                html_table_main += f'<th {style}>{col}</th>'
            
            html_table_main += """
                        </tr>
                    </thead>
                    <tbody>
            """
            
            # Agregar filas de datos para la tabla principal
            for index, row in pivot_df.iterrows():
                html_table_main += '<tr>'
                
                # Columna PROGRAMA
                if row['PROGRAMA'] == 'Total':
                    html_table_main += f'<td style="font-weight: bold; background-color: #f2f2f2;">{row["PROGRAMA"]}</td>'
                else:
                    html_table_main += f'<td>{row["PROGRAMA"]}</td>'
                
                # Columnas de datos para grupos 1 y 2
                for col in grupo1_cols + grupo2_cols:
                    if row['PROGRAMA'] == 'Total':
                        # Destacar también las celdas de totales para BENEFICIARIO y BENEFICIARIO- CTI
                        if col == "BENEFICIARIO":
                            cell_style = 'style="font-weight: bold; background-color: #e6f0f7; text-align: right;"'
                        elif col == "BENEFICIARIO- CTI":
                            cell_style = 'style="font-weight: bold; background-color: #e6f0f7; text-align: right;"'
                        else:
                            cell_style = 'style="font-weight: bold; background-color: #f2f2f2; text-align: right;"'
                    else:
                        # Destacar las celdas de datos para BENEFICIARIO y BENEFICIARIO- CTI
                        if col == "BENEFICIARIO":
                            cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                        elif col == "BENEFICIARIO- CTI":
                            cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                        else:
                            cell_style = 'style="text-align: right;"'
                    
                    html_table_main += f'<td {cell_style}>{int(row[col])}</td>'
                
                html_table_main += '</tr>'
            
            html_table_main += """
                    </tbody>
                </table>
            </div>
            """
            
            # Mostrar la tabla principal
            st.markdown(html_table_main, unsafe_allow_html=True)
            
            # Crear un botón desplegable para mostrar la tabla del grupo 3 y otros
            if grupo3_cols or otros_cols:  # Solo mostrar si hay columnas del grupo 3 u otros
                with st.expander("Ver casos especiales (Grupo 3) y otros estados"):
                    # Crear HTML para la tabla del grupo 3 y otros
                    html_table_grupo3 = """
                    <div style="overflow-x: auto; margin-bottom: 20px;">
                        <table class="styled-table">
                            <thead>
                                <tr>
                                    <th>PROGRAMA</th>
                    """
                    
                    # Agregar cabeceras para el grupo 3
                    for col in grupo3_cols:
                        if col == "BENEFICIARIO":
                            style = 'style="background-color: #0066a0; color: white;"'  # Versión más oscura del color primario
                        elif col == "BENEFICIARIO- CTI":
                            style = 'style="background-color: #0080b3; color: white;"'  # Versión más oscura del color secundario
                        else:
                            style = 'style="background-color: var(--color-accent-3);"'
                        html_table_grupo3 += f'<th {style}>{col}</th>'
                    
                    # Agregar cabeceras para otros
                    if otros_cols:
                        # Crear un título para la sección "Otros" que incluya los nombres de los estados
                        otros_nombres = ", ".join(otros_cols)
                        html_table_grupo3 += f'<th colspan="{len(otros_cols)}" style="background-color: var(--color-accent-2);">Otros (Estados: {otros_nombres})</th>'
                    
                    # Si hay columnas en "otros", agregar una segunda fila para los nombres específicos
                    if otros_cols:
                        html_table_grupo3 += """
                                </tr>
                                <tr>
                                    <th></th>
                        """
                        # Agregar los nombres de cada estado en "otros"
                        for _ in grupo3_cols:
                            html_table_grupo3 += "<th></th>"  # Celdas vacías para alinear con grupo3
                        
                        for col in otros_cols:
                            if col == "BENEFICIARIO":
                                style = 'style="background-color: #0066a0; color: white;"'  # Versión más oscura del color primario
                            elif col == "BENEFICIARIO- CTI":
                                style = 'style="background-color: #0080b3; color: white;"'  # Versión más oscura del color secundario
                            else:
                                style = 'style="background-color: var(--color-accent-2);"'
                            html_table_grupo3 += f'<th {style}>{col}</th>'
                    
                    html_table_grupo3 += """
                                </tr>
                            </thead>
                            <tbody>
                    """
                    
                    # Agregar filas de datos para la tabla del grupo 3 y otros
                    for index, row in pivot_df.iterrows():
                        if row['PROGRAMA'] != 'Total':
                            html_table_grupo3 += '<tr>'
                            
                            # Columna PROGRAMA
                            html_table_grupo3 += f'<td>{row["PROGRAMA"]}</td>'
                            
                            # Columnas de datos para el grupo 3
                            for col in grupo3_cols:
                                # Destacar las celdas de datos para BENEFICIARIO y BENEFICIARIO- CTI
                                if col == "BENEFICIARIO":
                                    cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                                elif col == "BENEFICIARIO- CTI":
                                    cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                                else:
                                    cell_style = 'style="text-align: right;"'
                                html_table_grupo3 += f'<td {cell_style}>{int(row[col])}</td>'
                            
                            # Columnas de datos para otros
                            for col in otros_cols:
                                # Destacar las celdas de datos para BENEFICIARIO y BENEFICIARIO- CTI
                                if col == "BENEFICIARIO":
                                    cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                                elif col == "BENEFICIARIO- CTI":
                                    cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                                else:
                                    cell_style = 'style="text-align: right;"'
                                html_table_grupo3 += f'<td {cell_style}>{int(row[col])}</td>'
                            
                            html_table_grupo3 += '</tr>'
                    
                    html_table_grupo3 += """
                            </tbody>
                        </table>
                    </div>
                    """
                    
                    # Mostrar la tabla del grupo 3 y otros
                    st.markdown(html_table_grupo3, unsafe_allow_html=True)
            
            # Mostrar tabla de beneficiarios por localidad
            st.subheader("Beneficiarios por Localidad")
            
            # Filtrar solo beneficiarios del DataFrame ya filtrado por departamento/localidad
            beneficiarios_estados = ["BENEFICIARIO", "BENEFICIARIO- CTI"]
            df_beneficiarios = df_filtered[df_filtered['N_ESTADO_FICHA'].isin(beneficiarios_estados)]
            
            if df_beneficiarios.empty:
                st.warning("No hay beneficiarios con los filtros seleccionados.")
            else:
                # Crear pivot table para mostrar cada estado en una columna separada
                df_pivot = df_beneficiarios.pivot_table(
                    index=['N_DEPARTAMENTO', 'N_LOCALIDAD'],
                    columns='N_ESTADO_FICHA',
                    values='ID_FICHA',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                # Renombrar columnas para mejor visualización
                if 'BENEFICIARIO' not in df_pivot.columns:
                    df_pivot['BENEFICIARIO'] = 0
                if 'BENEFICIARIO- CTI' not in df_pivot.columns:
                    df_pivot['BENEFICIARIO- CTI'] = 0
                
                # Añadir columna de total
                df_pivot['TOTAL'] = df_pivot['BENEFICIARIO'] + df_pivot['BENEFICIARIO- CTI']
                
                # Ordenar por departamento y total (descendente)
                df_pivot_sorted = df_pivot.sort_values(['N_DEPARTAMENTO', 'TOTAL'], ascending=[True, False])
                
                # Aplicar formato y estilo a la tabla
                styled_df = df_pivot_sorted.style \
                    .background_gradient(subset=['BENEFICIARIO', 'BENEFICIARIO- CTI', 'TOTAL'], cmap='Blues') \
                    .format(thousands=".", precision=0)
                
                # Mostrar tabla con estilo mejorado y sin índice
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "N_DEPARTAMENTO": st.column_config.TextColumn(
                            "Departamento"),
                        "N_LOCALIDAD": st.column_config.TextColumn(
                            "Localidad"),
                        "BENEFICIARIO": st.column_config.NumberColumn(
                            "Beneficiarios",
                            help="Cantidad de beneficiarios regulares"),
                        "BENEFICIARIO- CTI": st.column_config.NumberColumn(
                            "Beneficiarios CTI",
                            help="Beneficiarios en situación crítica"),
                        "TOTAL": st.column_config.NumberColumn(
                            "Total General",
                            help="Suma total de beneficiarios")
                    },
                    height=400
                )
            
            # Mostrar distribución geográfica si hay datos geojson y no hay filtros específicos
            if has_geojson and selected_dpto == all_dpto_option:
                st.markdown('<h3 style="font-size: 20px; margin: 20px 0 15px 0;">Distribución Geográfica</h3>', unsafe_allow_html=True)
                
                # Filtrar solo beneficiarios
                beneficiarios_estados = ["BENEFICIARIO", "BENEFICIARIO- CTI"]
                df_beneficiarios = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'].isin(beneficiarios_estados)]
                
                if df_beneficiarios.empty:
                    st.warning("No hay beneficiarios para mostrar en el mapa.")
                    return
                
                # Contar beneficiarios por departamento
                df_beneficiarios['ID_DEPARTAMENTO_GOB'] = df_beneficiarios['ID_DEPARTAMENTO_GOB'].fillna(-1).astype(int)
                
                # Agrupar y contar
                df_mapa = df_beneficiarios.groupby('ID_DEPARTAMENTO_GOB').size().reset_index(name='Cantidad')
                
                # Convertir a string para el mapa (sin decimales porque ya es entero)
                df_mapa['ID_DEPARTAMENTO_GOB'] = df_mapa['ID_DEPARTAMENTO_GOB'].astype(str)
                
                # Reemplazar "-1" con un valor adecuado para NaN si es necesario
                df_mapa.loc[df_mapa['ID_DEPARTAMENTO_GOB'] == "-1", 'ID_DEPARTAMENTO_GOB'] = "Sin asignar"
                
                # Mostrar tabla de datos para el mapa antes de renderizar el mapa
                st.markdown(f"### Vista previa de df_mapa (agrupado por ID_DEPARTAMENTO_GOB)")
                st.dataframe(df_mapa, use_container_width=True)

                # Verificar si los módulos de mapeo están disponibles
                if not is_mapping_available():
                    st.warning("La visualización de mapas no está disponible. Para habilitar esta función, instale los paquetes: folium, streamlit-folium y geopandas")
                    return

                # Crear y mostrar el mapa usando Plotly
                with st.spinner("Generando mapa..."):
                    fig = px.choropleth_mapbox(
                        df_mapa,
                        geojson=geojson_data,
                        locations='ID_DEPARTAMENTO_GOB',
                        color='Cantidad',
                        featureidkey="properties.CODDEPTO",
                        center={"lat": -31.4, "lon": -64.2},  # Coordenadas aproximadas de Córdoba
                        zoom=6,  # Nivel de zoom
                        opacity=0.7,  # Opacidad de los polígonos
                        mapbox_style="carto-positron",  # Estilo de mapa más limpio
                        color_continuous_scale="Blues",
                        labels={'Cantidad': 'Beneficiarios'},
                        title="Distribución de Beneficiarios por Departamento"
                    )
                    
                    # Ajustar el diseño
                    fig.update_layout(
                        margin={"r":0,"t":50,"l":0,"b":0},
                        coloraxis_colorbar={
                            "title": "Cantidad",
                            "tickformat": ",d"
                        },
                        title={
                            'text': "Distribución de Beneficiarios por Departamento",
                            'y':0.97,
                            'x':0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'
                        }
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab_empresas:
            if has_empresas:
                show_companies(df_empresas, geojson_data)
            else:
                st.markdown("""
                    <div class="info-box status-warning">
                        <strong>Información:</strong> No hay datos de empresas disponibles.
                    </div>
                """, unsafe_allow_html=True)

def show_companies(df_empresas, geojson_data):
    # Verificar si los módulos de mapeo están disponibles
    if not is_mapping_available() and geojson_data is not None:
        st.warning("La visualización de mapas no está disponible. Para habilitar esta función, instale los paquetes: folium, streamlit-folium y geopandas")

    # Asegúrate de que las columnas numéricas sean del tipo correcto
    if 'CANTIDAD_EMPLEADOS' in df_empresas.columns:
        df_empresas['CANTIDAD_EMPLEADOS'] = pd.to_numeric(df_empresas['CANTIDAD_EMPLEADOS'], errors='coerce')
        df_empresas['CANTIDAD_EMPLEADOS'] = df_empresas['CANTIDAD_EMPLEADOS'].fillna(0)
    else:
        df_empresas['CANTIDAD_EMPLEADOS'] = 0
        
    if 'VACANTES' in df_empresas.columns:
        df_empresas['VACANTES'] = pd.to_numeric(df_empresas['VACANTES'], errors='coerce')
        df_empresas['VACANTES'] = df_empresas['VACANTES'].fillna(0)
    else:
        df_empresas['VACANTES'] = 0

    # Calcular la columna 'CUPO'
    if all(col in df_empresas.columns for col in ['CANTIDAD_EMPLEADOS', 'EMPLEADOR', 'ADHERIDO']):
        df_empresas['CUPO'] = df_empresas.apply(lambda row: calculate_cupo(row['CANTIDAD_EMPLEADOS'], row['EMPLEADOR'], row['ADHERIDO']), axis=1)
    else:
        df_empresas['CUPO'] = 0

    # Filtrar por CUIT único y eliminar duplicados
    columns_to_select = [col for col in ['N_LOCALIDAD', 'N_DEPARTAMENTO', 'CUIT', 'N_EMPRESA', 
                                       'NOMBRE_TIPO_EMPRESA', 'ADHERIDO', 'CANTIDAD_EMPLEADOS', 
                                       'VACANTES', 'CUPO', 'IMP_GANANCIAS', 'IMP_IVA', 'MONOTRIBUTO',
                                       'INTEGRANTE_SOC', 'EMPLEADOR', 'ACTIVIDAD_MONOTRIBUTO'] 
                       if col in df_empresas.columns]
    
    df_display = df_empresas[columns_to_select].drop_duplicates(subset='CUIT')
    df_display = df_display.sort_values(by='CUPO', ascending=False).reset_index(drop=True)

    # Filtrar empresas adheridas al PPP 2024
    if 'ADHERIDO' in df_empresas.columns:
        df_empresas_puestos = df_empresas[df_empresas['ADHERIDO'] == 'PPP - PROGRAMA PRIMER PASO [2024]'].copy()
    else:
        df_empresas_puestos = pd.DataFrame()
    
    # Improved section title
    st.markdown('<div class="section-title">Programa Primer Paso - PERFIL de la demanda por categorías</div>', unsafe_allow_html=True)
    
    # Resto del código de visualización con mejoras visuales
    if not df_empresas_puestos.empty and 'N_DEPARTAMENTO' in df_empresas_puestos.columns:
        with st.expander("Selecciona los departamentos (haz clic para expandir)"):
            departamentos_unicos = df_empresas_puestos['N_DEPARTAMENTO'].unique()
            departamentos_seleccionados = st.multiselect(
                label="Selecciona departamentos",
                options=departamentos_unicos,
                default=departamentos_unicos.tolist(),
                help='Mantén presionada la tecla Ctrl (o Cmd en Mac) para seleccionar múltiples opciones.',
                label_visibility="collapsed",
                key="departamentos_multiselect"  # Added unique key
            )

        df_empresas_puestos = df_empresas_puestos[df_empresas_puestos['N_DEPARTAMENTO'].isin(departamentos_seleccionados)]
        
        if all(col in df_empresas_puestos.columns for col in ['N_CATEGORIA_EMPLEO', 'NOMBRE_TIPO_EMPRESA', 'CUIT']):
            df_puesto_agg = df_empresas_puestos.groupby(['N_CATEGORIA_EMPLEO', 'NOMBRE_TIPO_EMPRESA']).agg({'CUIT': 'nunique'}).reset_index()
            top_10_categorias = df_puesto_agg.groupby('N_CATEGORIA_EMPLEO')['CUIT'].nunique().nlargest(10).index
            df_puesto_agg_top10 = df_puesto_agg[df_puesto_agg['N_CATEGORIA_EMPLEO'].isin(top_10_categorias)]

            st.markdown("""<div class="info-box">Este gráfico representa las empresas adheridas al programa PPP, que cargaron el PERFIL de su demanda, expresado en categorias.</div>""", unsafe_allow_html=True)
            
            # Improved chart with better colors and styling
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            
            if ALTAIR_AVAILABLE:
                chart_cat = alt.Chart(df_puesto_agg_top10).mark_bar(
                    cornerRadiusTopRight=5,
                    cornerRadiusBottomRight=5
                ).encode( 
                    x=alt.X('CUIT:Q', title=''),  
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''), 
                    tooltip=['N_CATEGORIA_EMPLEO', 'NOMBRE_TIPO_EMPRESA', 'CUIT'],
                    text=alt.Text('CUIT', format=',d'),
                    color=alt.value('#4e73df')  # Consistent color scheme
                ).properties(
                    width=600,
                    height=400
                )
                
                # Agregar las labels al gráfico
                text = alt.Chart(df_puesto_agg_top10).mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,
                    color='white'  # Better contrast for text
                ).encode(
                    x=alt.X('CUIT:Q', title=''),  
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''), 
                    text='CUIT'
                )
    
                # Primero combinar los gráficos con layer
                combined_chart = alt.layer(chart_cat, text)
                
                # Luego aplicar la configuración al gráfico combinado
                combined_chart = combined_chart.configure_axisY(labels=False, domain=False, ticks=False)
                
                # Mostrar el gráfico combinado
                st.altair_chart(combined_chart, use_container_width=True)
            else:
                # Alternativa usando Plotly si Altair no está disponible
                fig = px.bar(
                    df_puesto_agg_top10, 
                    x='CUIT', 
                    y='N_CATEGORIA_EMPLEO',
                    text='CUIT',
                    labels={'CUIT': '', 'N_CATEGORIA_EMPLEO': ''},
                    height=400,
                    color_discrete_sequence=['#4e73df']
                )
                fig.update_layout(
                    yaxis={'categoryorder': 'total ascending'},
                    showlegend=False
                )
                fig.update_traces(
                    textposition='inside',
                    textfont_color='white'
                )
                st.plotly_chart(fig, use_container_width=True)
                
            st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr style='border: 1px solid #e0e0e0; margin: 20px 0;'>", unsafe_allow_html=True)

    # Métricas y tabla final con mejor diseño
    empresas_adh = df_display['CUIT'].nunique()
    
    st.markdown("""
        <div class="metric-card">
            <div class="metric-label">Empresas Adheridas</div>
            <div class="metric-value">{:,}</div>
        </div>
    """.format(empresas_adh), unsafe_allow_html=True)

    st.markdown("""<div class="info-box">Las empresas en esta tabla se encuentran adheridas a uno o más programas de empleo, han cumplido con los requisitos establecidos y han proporcionado sus datos a través de los registros de programasempleo.cba.gov.ar</div>""", unsafe_allow_html=True)

    # Mostrar el DataFrame con mejor estilo
    st.dataframe(df_display, hide_index=True, use_container_width=True)

    st.markdown("<hr style='border: 1px solid #e0e0e0; margin: 20px 0;'>", unsafe_allow_html=True)

    # --- Nuevo apartado: Perfil de Demanda con mejor estilo ---
    st.markdown('<div class="section-title">Perfil de Demanda</div>', unsafe_allow_html=True)

    # Filtrar solo los datos que tengan información de puesto y categoría
    required_columns = ['N_EMPRESA', 'CUIT', 'N_PUESTO_EMPLEO', 'N_CATEGORIA_EMPLEO']
    if all(col in df_empresas.columns for col in required_columns):
        df_perfil_demanda = df_empresas.dropna(subset=required_columns)
    else:
        df_perfil_demanda = pd.DataFrame()

    if df_perfil_demanda.empty:
        st.markdown("""
            <div class="info-box status-info">
                <strong>Información:</strong> No hay datos disponibles de perfil de demanda.
            </div>
        """, unsafe_allow_html=True)
    else:
        # Crear las dos columnas
        col1, col2 = st.columns(2)

        # --- Visualización 1: Tabla Agrupada (en col1) con mejor estilo ---
        with col1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<h3 style="font-size: 18px; margin-bottom: 15px;">Puestos y Categorías Demandadas por Empresa</h3>', unsafe_allow_html=True)
            # Agrupar por empresa, puesto y categoría, sin columna de cantidad
            df_grouped = df_perfil_demanda.groupby(['N_EMPRESA','CUIT','N_PUESTO_EMPLEO', 'N_CATEGORIA_EMPLEO']).size().reset_index()
            # Eliminar la columna "0" que se crea
            df_grouped = df_grouped.drop(columns=[0])
            st.dataframe(df_grouped, hide_index=True, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # --- Visualización 2: Gráfico de Barras por Categoría (Top 10) (en col2) con mejor estilo ---
        with col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<h3 style="font-size: 18px; margin-bottom: 15px;">Top 10 - Distribución de Categorías de Empleo</h3>', unsafe_allow_html=True)

            # Agrupar por categoría y contar las ocurrencias
            df_cat_count = df_perfil_demanda.groupby('N_CATEGORIA_EMPLEO')['CUIT'].nunique().reset_index(name='Empresas que Buscan')
            df_cat_count = df_cat_count.sort_values(by='Empresas que Buscan', ascending=False)

            if len(df_cat_count) > 9:
                #tomar el top 9
                df_cat_count_top_9 = df_cat_count.head(9).copy()

                # Agrupar el resto en "Otros"
                otros_count = df_cat_count.iloc[9:]['Empresas que Buscan'].sum()
                df_otros = pd.DataFrame([{'N_CATEGORIA_EMPLEO': 'Otros', 'Empresas que Buscan': otros_count}]) 

                # Concatenar el top 9 con "Otros"
                df_cat_count_final = pd.concat([df_cat_count_top_9, df_otros], ignore_index=True)
            else:
                df_cat_count_final = df_cat_count.copy()

            if ALTAIR_AVAILABLE:
                chart_cat = alt.Chart(df_cat_count_final).mark_bar(
                    cornerRadiusTopRight=5,
                    cornerRadiusBottomRight=5
                ).encode( 
                    x=alt.X('Empresas que Buscan', title=''),  
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''), 
                    tooltip=['N_CATEGORIA_EMPLEO', 'Empresas que Buscan'],
                    text=alt.Text('Empresas que Buscan', format=',d'),
                    color=alt.value('#4e73df')  # Consistent color scheme
                ).properties(
                    width=600,
                    height=400
                )
                
                # Agregar las labels al gráfico
                text = alt.Chart(df_cat_count_final).mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,
                    color='white'  # Better contrast for text
                ).encode(
                    x=alt.X('Empresas que Buscan', title=''),  
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''), 
                    text='Empresas que Buscan'
                )
    
                # Primero combinar los gráficos con layer
                combined_chart = alt.layer(chart_cat, text)
                
                # Luego aplicar la configuración al gráfico combinado
                combined_chart = combined_chart.configure_axisY(labels=False, domain=False, ticks=False)
                
                # Mostrar el gráfico combinado
                st.altair_chart(combined_chart, use_container_width=True)
            else:
                # Alternativa usando Plotly si Altair no está disponible
                fig = px.bar(
                    df_cat_count_final, 
                    x='Empresas que Buscan', 
                    y='N_CATEGORIA_EMPLEO',
                    text='Empresas que Buscan',
                    labels={'Empresas que Buscan': '', 'N_CATEGORIA_EMPLEO': ''},
                    height=400,
                    color_discrete_sequence=['#4e73df']
                )
                fig.update_layout(
                    yaxis={'categoryorder': 'total ascending'},
                    showlegend=False
                )
                fig.update_traces(
                    textposition='inside',
                    textfont_color='white'
                )
                st.plotly_chart(fig, use_container_width=True)
                
            st.markdown('</div>', unsafe_allow_html=True)

def show_inscriptions(df_inscriptos, df_poblacion, geojson_data, file_date):
    """
    Muestra la vista de inscripciones con mejor estilo visual
    
    Args:
        df_inscriptos: DataFrame de VT_REPORTES_PPP_MAS26.parquet
        df_poblacion: DataFrame de poblacion_departamentos.csv (puede ser None)
        geojson_data: Datos GeoJSON para mapas
        file_date: Fecha de actualización de los archivos
    """
    
    # Verificar si los módulos de mapeo están disponibles cuando se intenta usar geojson_data
    if not is_mapping_available() and geojson_data is not None:
        st.warning("La visualización de mapas no está disponible. Para habilitar esta función, instale los paquetes: folium, streamlit-folium y geopandas")

    # Verificar que los DataFrames no estén vacíos
    if df_inscriptos is None:
        st.markdown("""
            <div class="info-box status-warning">
                <strong>Información:</strong> Uno o más DataFrames necesarios no están disponibles.
            </div>
        """, unsafe_allow_html=True)
        return
    
    try:
        # Limpiar CUIL
        if 'CUIL' in df_inscriptos.columns:
            df_inscriptos['CUIL'] = df_inscriptos['CUIL'].astype(str).str.replace("-", "", regex=False)
        
        # Definir mapeo de programas según IDETAPA
        programas = {
            53: "Programa Primer Paso",
            51: "Más 26",
            54: "CBA Mejora",
            55: "Nueva Oportunidad"
        }
        
        # Filtrar para obtener solo los registros con IDETAPA válidas
        if 'IDETAPA' in df_inscriptos.columns:
            # Obtener las etapas disponibles en los datos
            etapas_disponibles = df_inscriptos['IDETAPA'].dropna().unique()
            etapas_validas = [etapa for etapa in etapas_disponibles if etapa in programas.keys()]
            
            if len(etapas_validas) == 0:
                st.warning("No se encontraron programas válidos en los datos.")
                return
                
            # Crear selector de programa con estilo mejorado
            st.markdown('<div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; margin-bottom: 20px;">', unsafe_allow_html=True)
            st.markdown('<h3 style="font-size: 18px; margin: 0 0 10px 0;">Seleccionar Programa</h3>', unsafe_allow_html=True)
            
            # Determinar el programa por defecto (usar el primero disponible)
            programa_default = etapas_validas[0] if etapas_validas else 53
            
            # Crear opciones para el selector
            opciones_programa = {programas.get(etapa, f"Programa {etapa}"): etapa for etapa in etapas_validas}
            
            # Selector de programa
            programa_seleccionado_nombre = st.selectbox(
                "Programa:",
                options=list(opciones_programa.keys()),
                index=0,
                label_visibility="collapsed"
            )
            
            # Obtener el ID de etapa seleccionado
            programa_seleccionado = opciones_programa[programa_seleccionado_nombre]
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Filtrar los datos según el programa seleccionado
            df_programa = df_inscriptos[df_inscriptos['IDETAPA'] == programa_seleccionado].copy()
        else:
            st.warning("No se encontró la columna IDETAPA en los datos.")
            return
            
        # Título dinámico según el programa seleccionado
        st.markdown(f'<h2 style="font-size: 24px; margin-bottom: 20px;">Dashboard de {programa_seleccionado_nombre}</h2>', unsafe_allow_html=True)
            
        # Filtrar los DataFrames según el programa seleccionado
        if not df_programa.empty and 'ID_EST_FIC' in df_programa.columns:
            df_match = df_programa[(df_programa['ID_EST_FIC'] == 8)]
            df_cti_inscripto = df_programa[(df_programa['ID_EST_FIC'] == 12) & (df_programa['ID_EMP'].notnull())]
            df_cti_validos = df_programa[df_programa['ID_EST_FIC'] == 13]
            df_cti_benficiario = df_programa[df_programa['ID_EST_FIC'] == 14]
        else:
            df_match = pd.DataFrame()
            df_cti_inscripto = pd.DataFrame()
            df_cti_validos = pd.DataFrame()
            df_cti_benficiario = pd.DataFrame()
        
        # REPORTE PPP con mejor estilo
        file_date_inscripciones = pd.to_datetime(file_date) if file_date else datetime.now()
        file_date_inscripciones = file_date_inscripciones - timedelta(hours=3)
        
        st.markdown(f"""
            <div style="background-color:#e9ecef; padding:10px; border-radius:5px; margin-bottom:20px; font-size:0.9em;">
                <i class="fas fa-sync-alt"></i> <strong>Última actualización:</strong> {file_date_inscripciones.strftime('%d/%m/%Y %H:%M')}
            </div>
        """, unsafe_allow_html=True)
        
        # Calcular métricas para el programa seleccionado
        if not df_match.empty:
            total_match = len(df_match)
        else:
            total_match = 0

        if not df_programa.empty and 'ID_EST_FIC' in df_programa.columns:
            conteo_estados = df_programa['ID_EST_FIC'].value_counts()
            total_empresa_no_apta = conteo_estados.get(2, 0)  
            total_benef = conteo_estados.get(14, 0)
            total_validos = conteo_estados.get(13, 0)
            total_inscriptos = conteo_estados.get(12, 0)
            total_pendientes = conteo_estados.get(3, 0)
            total_rechazados = conteo_estados.get(17, 0) + conteo_estados.get(18, 0) + conteo_estados.get(19, 0)
        else:
            total_empresa_no_apta = 0
            total_benef = 0
            total_validos = 0
            total_inscriptos = 0
            total_pendientes = 0
            total_rechazados = 0
        
        # Crear tarjetas de métricas con mejor estilo
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
                <div class="metric-card status-info">
                     <div class="metric-label">Total Match {programa_seleccionado_nombre}</div>
                     <div class="metric-value">{total_match:,}</div>
                </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
                <div class="metric-card status-info">
                    <div class="metric-label">Total Beneficiarios {programa_seleccionado_nombre}</div>
                    <div class="metric-value">{total_benef:,}</div>
                </div>
            """, unsafe_allow_html=True)
        
        # Resto del código de visualización con mejoras visuales
        # Aquí puedes añadir más visualizaciones según sea necesario
    
    except Exception as e:
        st.markdown(f"""
            <div class="info-box status-warning">
                <strong>Información:</strong> Se mostrarán los datos disponibles: {str(e)}
            </div>
        """, unsafe_allow_html=True)

def show_empleo_dashboard(data, dates=None):
    """
    Muestra el dashboard de programas de empleo.
    
    Args:
        data: Diccionario de dataframes cargados desde GitLab
        dates: Diccionario de fechas de actualización de los archivos
    """
    try:
        # Cargar y preprocesar datos
        df_inscriptos, df_empresas, df_poblacion, geojson_data, has_fichas, has_empresas, has_poblacion, has_geojson = load_and_preprocess_data(data, dates)
        
        # Verificar si hay datos de fichas
        if not has_fichas:
            return
        
        # Renderizar el dashboard principal
        render_dashboard(df_inscriptos, df_empresas, df_poblacion, geojson_data, has_empresas, has_geojson)
    
    except Exception as e:
        st.error(f"Error al procesar los datos: {str(e)}")