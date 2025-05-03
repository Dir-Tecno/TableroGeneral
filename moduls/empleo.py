import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row
from utils.map_utils import create_choropleth_map, display_map
from utils.styles import COLORES_IDENTIDAD
from utils.data_cleaning import clean_thousand_separator, convert_decimal_separator
from utils.kpi_tooltips import TOOLTIPS_DESCRIPTIVOS, ESTADO_TOOLTIPS
import folium
from streamlit_folium import folium_static
import geopandas as gpd
import math
import requests
import altair as alt

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
        
        # Cargar datos de circuitos electorales
        df_circuitos = data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt')
        has_circuitos = df_circuitos is not None and not df_circuitos.empty
        
        # Crear df_emp_ben: cantidad de beneficiarios por empresa (CUIT)
        df_emp_ben = (
            df_inscriptos_raw[
                (df_inscriptos_raw["IDETAPA"].isin([51, 53, 54, 55])) &
                (df_inscriptos_raw["N_ESTADO_FICHA"] == "BENEFICIARIO")
            ]
            .assign(CUIT=lambda df: df["EMP_CUIT"].astype(str).str.replace("-", ""))
            .groupby("CUIT", as_index=False)
            .agg(BENEF=("ID_FICHA", "count"))
        )
        
        # Cargar el dataset de empresas
        df_empresas = data.get('vt_empresas_adheridas.parquet')
        has_empresas = df_empresas is not None and not df_empresas.empty

        # --- NUEVO: Cruce con ARCA ---
        df_arca = data.get('vt_empresas_ARCA.parquet')
        if has_empresas and df_arca is not None and not df_arca.empty:
            # Limpiar CUIT en ambos DataFrames (quitar guiones y asegurar string)
            df_empresas['CUIT'] = df_empresas['CUIT'].astype(str).str.replace('-', '', regex=False)
            df_arca['CUIT'] = df_arca['CUIT'].astype(str).str.replace('-', '', regex=False)
            # Seleccionar solo las columnas de interés de ARCA
            cols_arca = ['CUIT', 'IMP_GANANCIAS', 'IMP_IVA', 'MONOTRIBUTO', 'INTEGRANTE_SOC', 'EMPLEADOR', 'ACTIVIDAD_MONOTRIBUTO','NOMBRE_TIPO_EMPRESA']
            df_arca_sel = df_arca[cols_arca].copy()
            # Merge left
            df_empresas = df_empresas.merge(df_arca_sel, on='CUIT', how='left')

        # Cruce de df_display con df_emp_ben por CUIT
        if "CUIT" in df_empresas.columns:
            df_empresas = df_empresas.merge(df_emp_ben, on="CUIT", how="left")
        
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
        
        # Limpiar separador de miles en los DataFrames principales
        df_empresas = clean_thousand_separator(df_empresas)
        df_inscriptos_raw = clean_thousand_separator(df_inscriptos_raw)
        df_poblacion = clean_thousand_separator(df_poblacion)

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
        
        # Añadir la columna ZONA también al dataframe de empresas
        if has_empresas and 'N_DEPARTAMENTO' in df_empresas.columns:
            df_empresas['ZONA'] = df_empresas['N_DEPARTAMENTO'].apply(
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
        df_inscriptos_sin_adherido = df_inscriptos.copy()
        
        # Mapeo de programas según IDETAPA
        programas = {
            53: "Programa Primer Paso",
            51: "Más 26",
            54: "CBA Mejora",
            55: "Nueva Oportunidad"
        }
        
        # Crear columna con nombres de programas
        if 'IDETAPA' in df_inscriptos_sin_adherido.columns:
            df_inscriptos_sin_adherido['PROGRAMA'] = df_inscriptos_sin_adherido['IDETAPA'].map(lambda x: programas.get(x, f"Programa {x}"))
        else:
            df_inscriptos_sin_adherido['PROGRAMA'] = "No especificado"
            
        has_fichas = True  # Si llegamos hasta aquí, tenemos datos de fichas
        
        # Preprocesar el dataframe de circuitos electorales si está disponible
        if has_circuitos:
            try:
                # Asegurarse de que las columnas estén correctamente tipadas
                if 'ID_LOCALIDAD' in df_circuitos.columns:
                    df_circuitos['ID_LOCALIDAD'] = pd.to_numeric(df_circuitos['ID_LOCALIDAD'], errors='coerce')
                
                # Limpiar datos si es necesario
                df_circuitos = clean_thousand_separator(df_circuitos)
                df_circuitos = convert_decimal_separator(df_circuitos)
                
                # Si hay datos de inscriptos y circuitos, intentar cruzarlos
                if df_inscriptos is not None and not df_inscriptos.empty:
                    # Verificar columnas comunes para el cruce
                    common_cols = set(df_inscriptos.columns) & set(df_circuitos.columns)
                    join_cols = [col for col in ['ID_LOCALIDAD', 'COD_LOCALIDAD'] if col in common_cols]
                    
                    if join_cols:
                        # Realizar el cruce si hay columnas comunes
                        join_col = join_cols[0]  # Usar la primera columna común encontrada
                        df_inscriptos = pd.merge(
                            df_inscriptos,
                            df_circuitos,
                            on=join_col,
                            how='left',
                            suffixes=('', '_circuito')
                        )
                        st.success(f"Datos de circuitos electorales integrados correctamente usando la columna {join_col}")
                    else:
                        st.warning("No se encontraron columnas comunes para cruzar los datos de inscriptos con circuitos electorales")
            except Exception as e:
                st.error(f"Error al procesar datos de circuitos electorales: {str(e)}")
                has_circuitos = False
        
        # Retornar los dataframes procesados y los flags de disponibilidad
        return df_inscriptos_sin_adherido, df_empresas, df_poblacion, geojson_data, has_fichas, has_empresas, has_poblacion, has_geojson

def render_filters(df_inscriptos, key_prefix=""):
    """
    Renderiza los filtros de la interfaz de usuario.
    
    Args:
        df_inscriptos: DataFrame con los datos de inscripciones
        
    Returns:
        Tupla con el DataFrame filtrado y los filtros seleccionados
    """
    # Mantener una copia del DataFrame original para no modificarlo
    df_filtered = df_inscriptos.copy()
    
    with st.container():
        # Contenedor de filtros con 3 columnas
        col1, col2, col3 = st.columns(3)
        
        # Filtro de departamento en la primera columna
        with col1:
            # Solo mostrar el filtro de departamento si la columna existe en el dataframe
            if 'N_DEPARTAMENTO' in df_inscriptos.columns:
                departamentos = sorted(df_inscriptos['N_DEPARTAMENTO'].dropna().unique())
                all_dpto_option = "Todos los departamentos"
                selected_dpto = st.selectbox("Departamento:", [all_dpto_option] + list(departamentos), key=f"{key_prefix}_dpto_filter")
                
                # Inicializar variables con valores por defecto
                selected_loc = None
                all_loc_option = None
                
                # Filtrar por departamento si se seleccionó uno
                if selected_dpto != all_dpto_option:
                    df_filtered = df_filtered[df_filtered['N_DEPARTAMENTO'] == selected_dpto]
                    
                    # Solo mostrar el filtro de localidad si la columna existe en el dataframe
                    if 'N_LOCALIDAD' in df_inscriptos.columns:
                        localidades = sorted(df_filtered['N_LOCALIDAD'].dropna().unique())
                        all_loc_option = "Todas las localidades"
                        selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key=f"{key_prefix}_loc_filter")
                        
                        # Filtrar por localidad si se seleccionó una
                        if selected_loc != all_loc_option:
                            df_filtered = df_filtered[df_filtered['N_LOCALIDAD'] == selected_loc]
                    else:
                        all_loc_option = None
                        selected_loc = None
            else:
                # Si no existe la columna N_DEPARTAMENTO, establecer valores por defecto
                selected_dpto = None
                all_dpto_option = None
                selected_loc = None
                all_loc_option = None
        
        # Filtro de zona favorecida en la segunda columna
        with col2:
            # Solo mostrar el filtro de ZONA si la columna existe en el dataframe
            if 'ZONA' in df_inscriptos.columns:
                zonas = sorted(df_inscriptos['ZONA'].dropna().unique())
                all_zona_option = "Todas las zonas"
                selected_zona = st.selectbox("Zona:", [all_zona_option] + list(zonas), key=f"{key_prefix}_zona_filter")
            else:
                all_zona_option = "Todas las zonas"
                selected_zona = all_zona_option
                
        # Añadir más filtros en la tercera columna si es necesario
        with col3:
            # Puedes añadir más filtros aquí si lo deseas
            pass
            
    # Filtrar por zona si se seleccionó una y la columna existe
    if 'ZONA' in df_inscriptos.columns and selected_zona != all_zona_option:
        df_filtered = df_filtered[df_filtered['ZONA'] == selected_zona]
    
    # Mostrar resumen de filtros aplicados
    filtros_aplicados = []
    
    if selected_dpto != all_dpto_option:
        filtros_aplicados.append(f"Departamento: {selected_dpto}")
        if selected_loc is not None and selected_loc != all_loc_option:
            filtros_aplicados.append(f"Localidad: {selected_loc}")
            
    if 'ZONA' in df_inscriptos.columns and selected_zona != all_zona_option:
        filtros_aplicados.append(f"Zona: {selected_zona}")
    
    if filtros_aplicados:
        filtros_texto = ", ".join(filtros_aplicados)
        st.markdown(f"**Filtros aplicados:** {filtros_texto}")
    else:
        st.markdown("**Mostrando todos los datos**")
    
    return df_filtered, selected_dpto, selected_loc, all_dpto_option, all_loc_option

def render_dashboard(df_inscriptos, df_empresas, df_poblacion, geojson_data, has_empresas, has_geojson):
    """
    Renderiza el dashboard principal con los datos procesados.
    """
    with st.spinner("Generando visualizaciones..."):
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
                "title": "BENEFICIARIOS TOTALES",
                "value": f"{total_beneficiarios:,}".replace(',', '.'),
                "color_class": "kpi-primary",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS TOTALES", "")
            },
            {
                "title": "BENEFICIARIOS EL",
                "value": f"{total_beneficiarios:,}".replace(',', '.'),
                "color_class": "kpi-secondary",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS EL", "")
            },
            {
                "title": "ZONA FAVORECIDA",
                "value": f"{beneficiarios_zona_favorecida:,}".replace(',', '.'),
                "color_class": "kpi-accent-3",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("ZONA FAVORECIDA", "")
            },
            {
                "title": "BENEFICIARIOS CTI",
                "value": f"{total_beneficiarios_cti:,}".replace(',', '.'),
                "color_class": "kpi-accent-4",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS CTI", "")
            }
        ]
        
        display_kpi_row(kpi_data, num_columns=4)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Crear pestañas para organizar el contenido
        tab_beneficiarios, tab_empresas = st.tabs(["Beneficiarios", "Empresas"])
        
        # Contenido de la pestaña Beneficiarios
        with tab_beneficiarios:
            # Filtros específicos para la pestaña Beneficiarios
            df_filtered, selected_dpto, selected_loc, all_dpto_option, all_loc_option = render_filters(df_inscriptos, key_prefix="benef")
            
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
            grupo2 = ["INSCRIPTO - CTI", "RETENIDO - CTI", "VALIDADO - CTI", "BENEFICIARIO- CTI", "BAJA - CTI"]
            grupo3 = ["POSTULANTE SIN EMPRESA", "FUERA CUPO DE EMPRESA", "RECHAZO FORMAL", "INSCRIPTO NO ACEPTADO", "DUPLICADO", "EMPRESA NO APTA"]
            
            # Crear una lista con todas las columnas en el orden deseado
            columnas_ordenadas = grupo1 + grupo2 + grupo3
            
            # Añadir totales primero para cálculos internos, pero no los mostraremos
            pivot_table['Total'] = pivot_table.sum(axis=1)
            pivot_table.loc['Total'] = pivot_table.sum()
            
            # Reordenar con las columnas existentes más cualquier otra columna y el total al final (para cálculos)
            pivot_table = pivot_table.reindex(columns=columnas_ordenadas + [col for col in pivot_table.columns if col not in columnas_ordenadas and col != 'Total'] + ['Total'])
            
            # Mostrar tabla con estilo mejorado
            st.markdown('<div class="section-title">Conteo de personas por Programa y Estado</div>', unsafe_allow_html=True)
            
            # Convertir pivot table a DataFrame para mejor visualización
            pivot_df = pivot_table.reset_index()
            
            # Separar las columnas por grupos
            grupo1_cols = [col for col in grupo1 if col in pivot_table.columns]
            grupo2_cols = [col for col in grupo2 if col in pivot_table.columns]
            grupo3_cols = [col for col in grupo3 if col in pivot_table.columns]
            otros_cols = [col for col in pivot_table.columns if col not in grupo1 and col not in grupo2 and col not in grupo3 and col != 'Total' and col != 'PROGRAMA']
            
            # Quitar columna EX BENEFICIARIO y añadir columna Sub total
            cols_to_sum = [col for col in pivot_table.columns if col in ("BENEFICIARIO", "BENEFICIARIO- CTI")]
            columns_no_ex = [col for col in pivot_table.columns if col != "EX BENEFICARIO"]
            columns_final = columns_no_ex + ["Sub total"]

            html_table_main = """
            <div style="overflow-x: auto; margin-bottom: 20px;">
                <table class="styled-table">
                    <thead>
                        <tr>
                            <th rowspan="2">PROGRAMA</th>
                            <th colspan="{}" style="background-color: var(--color-primary); border-right: 2px solid white;">Beneficiarios EL (Entrenamiento Laboral)</th>
                            <th colspan="{}" style="background-color: var(--color-secondary); border-right: 2px solid white;">Beneficiarios CTI (Contratados)</th>
                            <th rowspan="2" style="background-color: #e6f0f7; color: #333;">Totales Beneficiario</th>
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
                
                # Agregar el tooltip si existe para este estado
                tooltip = ESTADO_TOOLTIPS.get(col, "")
                if tooltip:
                    html_table_main += f'<th {style} title="{tooltip}">{col}</th>'
                else:
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
                    
                    html_table_main += f'<td {cell_style}>{int(row[col]):,}'.replace(',', '.')+'</td>'
                
                # Celda Sub total
                val1 = int(row['BENEFICIARIO']) if 'BENEFICIARIO' in row and not pd.isnull(row['BENEFICIARIO']) else 0
                val2 = int(row['BENEFICIARIO- CTI']) if 'BENEFICIARIO- CTI' in row and not pd.isnull(row['BENEFICIARIO- CTI']) else 0
                cell_value = val1 + val2
                cell_style = 'style="background-color: #e6f0f7; text-align: right; font-weight: bold;"'
                html_table_main += f'<td {cell_style}>{cell_value:,}'.replace(',', '.')+'</td>'
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
                with st.expander("Ver casos especiales (Bajas y Rechazos) y otros estados"):
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
                                html_table_grupo3 += f'<td {cell_style}>{int(row[col]):,}'.replace(',', '.')+'</td>'
                            
                            # Columnas de datos para otros
                            for col in otros_cols:
                                # Destacar las celdas de datos para BENEFICIARIO y BENEFICIARIO- CTI
                                if col == "BENEFICIARIO":
                                    cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                                elif col == "BENEFICIARIO- CTI":
                                    cell_style = 'style="background-color: #e6f0f7; text-align: right;"'
                                else:
                                    cell_style = 'style="text-align: right;"'
                                html_table_grupo3 += f'<td {cell_style}>{int(row[col]):,}'.replace(',', '.')+'</td>'
                            
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
            
            # Filtrar solo beneficiarios
            beneficiarios_estados = ["BENEFICIARIO", "BENEFICIARIO- CTI"]
            df_beneficiarios = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'].isin(beneficiarios_estados)]
            
            if df_beneficiarios.empty:
                st.warning("No hay beneficiarios con los filtros seleccionados.")
            else:
                # Enfoque más directo: separar por tipo de beneficiario y luego unir
                # 1. Crear dataframe para beneficiarios EL
                df_beneficiarios_el = df_beneficiarios[df_beneficiarios['N_ESTADO_FICHA'] == "BENEFICIARIO"]
                df_el_count = df_beneficiarios_el.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD']).size().reset_index(name='BENEFICIARIO')
                
                # 2. Crear dataframe para beneficiarios CTI
                df_beneficiarios_cti = df_beneficiarios[df_beneficiarios['N_ESTADO_FICHA'] == "BENEFICIARIO- CTI"]
                df_cti_count = df_beneficiarios_cti.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD']).size().reset_index(name='BENEFICIARIO- CTI')
                
                # 3. Unir los dos dataframes
                df_mapa = pd.merge(df_el_count, df_cti_count, on=['N_DEPARTAMENTO', 'N_LOCALIDAD'], how='outer')
                
                # 4. Rellenar los NAs con ceros
                df_mapa['BENEFICIARIO'] = df_mapa['BENEFICIARIO'].fillna(0).astype(int)
                df_mapa['BENEFICIARIO- CTI'] = df_mapa['BENEFICIARIO- CTI'].fillna(0).astype(int)
                
                # 5. Añadir columna de total
                df_mapa['TOTAL'] = df_mapa['BENEFICIARIO'] + df_mapa['BENEFICIARIO- CTI']
                # Ordenar por 'TOTAL' descendente (y N_DEPARTAMENTO como criterio secundario)
                df_mapa_sorted = df_mapa.sort_values(['TOTAL', 'N_DEPARTAMENTO'], ascending=[False, True])
                
                # Aplicar formato y estilo a la tabla
                styled_df = df_mapa_sorted.style \
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
                
                # Enfoque más directo: separar por tipo de beneficiario y luego unir
                # 1. Crear dataframe para beneficiarios EL
                df_beneficiarios_el = df_beneficiarios[df_beneficiarios['N_ESTADO_FICHA'] == "BENEFICIARIO"]
                df_el_count = df_beneficiarios_el.groupby(['ID_DEPARTAMENTO_GOB', 'N_DEPARTAMENTO']).size().reset_index(name='BENEFICIARIO')
                
                # 2. Crear dataframe para beneficiarios CTI
                df_beneficiarios_cti = df_beneficiarios[df_beneficiarios['N_ESTADO_FICHA'] == "BENEFICIARIO- CTI"]
                df_cti_count = df_beneficiarios_cti.groupby(['ID_DEPARTAMENTO_GOB', 'N_DEPARTAMENTO']).size().reset_index(name='BENEFICIARIO- CTI')
                
                # 3. Unir los dos dataframes
                df_mapa = pd.merge(df_el_count, df_cti_count, on=['ID_DEPARTAMENTO_GOB', 'N_DEPARTAMENTO'], how='outer')
                
                # 4. Rellenar los NAs con ceros
                df_mapa['BENEFICIARIO'] = df_mapa['BENEFICIARIO'].fillna(0).astype(int)
                df_mapa['BENEFICIARIO- CTI'] = df_mapa['BENEFICIARIO- CTI'].fillna(0).astype(int)
                
                # 5. Añadir columna de total
                df_mapa['Total'] = df_mapa['BENEFICIARIO'] + df_mapa['BENEFICIARIO- CTI']
                
                # Convertir a string para el mapa (sin decimales porque ya es entero)
                df_mapa['ID_DEPARTAMENTO_GOB'] = df_mapa['ID_DEPARTAMENTO_GOB'].apply(lambda x: str(int(x)) if pd.notnull(x) else "")
                
                # Reemplazar "-1" con un valor adecuado para NaN si es necesario
                df_mapa.loc[df_mapa['ID_DEPARTAMENTO_GOB'] == "-1", 'ID_DEPARTAMENTO_GOB'] = "Sin asignar"
                
                # Detectar si geojson_data es un DataFrame y convertir a GeoJSON estándar
                import geopandas as gpd
                geojson_dict = None
                if isinstance(geojson_data, (pd.DataFrame, gpd.GeoDataFrame)):
                    try:
                        gdf = gpd.GeoDataFrame(geojson_data)
                        geojson_dict = gdf.__geo_interface__
                    except Exception as e:
                        st.error(f"Error convirtiendo DataFrame a GeoJSON: {e}")
                elif isinstance(geojson_data, dict) and 'features' in geojson_data:
                    geojson_dict = geojson_data
                else:
                    st.warning("geojson_data no es un DataFrame ni un GeoJSON estándar. Revisa la fuente de datos.")

                # Normalizar tipos y depurar IDs antes de graficar
                if isinstance(geojson_dict, dict) and 'features' in geojson_dict:
                    for f in geojson_dict['features']:
                        f['properties']['CODDEPTO'] = str(f['properties']['CODDEPTO']).strip()

                else:
                    st.warning("geojson_dict no tiene la clave 'features' o no es un dict. Revisa la carga del GeoJSON.")
                
                # Crear un layout con 4 columnas (3 para la tabla y 1 para el mapa)
                table_col, map_col = st.columns([3, 1])
                
                # Mostrar tabla de datos para el mapa en las primeras 3 columnas
                with table_col:
                    st.markdown(f"### Beneficiarios por Departamento")
                    # Crear una copia del dataframe sin la columna ID_DEPARTAMENTO_GOB para mostrar
                    df_mapa_display = df_mapa.drop(columns=['ID_DEPARTAMENTO_GOB']).copy()
                    # Renombrar columnas para mejor visualización
                    df_mapa_display = df_mapa_display.rename(columns={
                        'N_DEPARTAMENTO': 'Departamento',
                        'BENEFICIARIO': 'Beneficiarios EL',
                        'BENEFICIARIO- CTI': 'Beneficiarios CTI',
                        'Total': 'Total Beneficiarios'
                    })
                    st.dataframe(df_mapa_display, use_container_width=True)

                # Crear y mostrar el mapa usando Plotly en la última columna
                with map_col:
                    with st.spinner("Generando mapa..."):
                        fig = px.choropleth_mapbox(
                            df_mapa,
                            geojson=geojson_dict,
                            locations='ID_DEPARTAMENTO_GOB',
                            color='Total',
                            featureidkey="properties.CODDEPTO",
                            hover_data=['N_DEPARTAMENTO', 'BENEFICIARIO', 'BENEFICIARIO- CTI', 'Total'],
                            center={"lat": -31.4, "lon": -64.2},  # Coordenadas aproximadas de Córdoba
                            zoom=6,  # Nivel de zoom
                            opacity=0.7,  # Opacidad de los polígonos
                            mapbox_style="carto-positron",  # Estilo de mapa más limpio
                            color_continuous_scale="Blues",
                            labels={'Total': 'Beneficiarios'},
                            title="Distribución de Beneficiarios"
                        )
                        
                        # Ajustar el diseño
                        fig.update_layout(
                            margin={"r":0,"t":50,"l":0,"b":0},
                            coloraxis_colorbar={
                                "title": "Cantidad",
                                "tickformat": ",d"
                            },
                            title={
                                'text': "Beneficiarios por Departamento",
                                'y':0.97,
                                'x':0.5,
                                'xanchor': 'center',
                                'yanchor': 'top'
                            },
                            # Reducir el tamaño para adaptarse a la columna más pequeña
                            height=400
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
        
        with tab_empresas:
            if has_empresas:
                # Filtros específicos para la pestaña Empresas
                df_filtered_empresas, selected_dpto_emp, selected_loc_emp, all_dpto_option_emp, all_loc_option_emp = render_filters(df_empresas, key_prefix="emp")
                
                # Mostrar los datos de empresas con los filtros aplicados
                show_companies(df_filtered_empresas, geojson_data)
            else:
                st.markdown("""
                    <div class="info-box status-warning">
                        <strong>Información:</strong> No hay datos de empresas disponibles.
                    </div>
                """, unsafe_allow_html=True)

def show_companies(df_empresas, geojson_data):
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
                                       'INTEGRANTE_SOC', 'EMPLEADOR', 'ACTIVIDAD_MONOTRIBUTO', 'BENEF'] 
                       if col in df_empresas.columns]

    if 'CUIT' in df_empresas.columns and 'ADHERIDO' in df_empresas.columns:
        df_empresas['ADHERIDO'] = df_empresas.groupby('CUIT')['ADHERIDO'].transform(lambda x: ', '.join(sorted(set(x))))
    
    # Usar columns_to_select para crear df_display correctamente
    df_display = df_empresas[columns_to_select].drop_duplicates(subset='CUIT')
    df_display = df_display.sort_values(by='CUPO', ascending=False).reset_index(drop=True)


            

            

    st.markdown("<hr style='border: 1px solid #e0e0e0; margin: 20px 0;'>", unsafe_allow_html=True)

    # Métricas y tabla final con mejor diseño
    empresas_adh = df_display['CUIT'].nunique()
    
    # Calcular empresas con y sin beneficiarios
    empresas_con_benef = df_display[df_display['BENEF'] > 0]['CUIT'].nunique()
    empresas_sin_benef = df_display[pd.isna(df_display['BENEF'])]['CUIT'].nunique()
    
    # Layout para los KPIs - 3 columnas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Empresas Adheridas</div>
                <div class="metric-value">{:}</div>
                <div class="metric-tooltip" title="{}"></div>
            </div>
        """.format(empresas_adh, TOOLTIPS_DESCRIPTIVOS.get("EMPRESAS ADHERIDAS", "")), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Empresas con Beneficiarios</div>
                <div class="metric-value">{:}</div>
                <div class="metric-tooltip" title="{}"></div>
            </div>
        """.format(empresas_con_benef, TOOLTIPS_DESCRIPTIVOS.get("EMPRESAS CON BENEFICIARIOS", "")), unsafe_allow_html=True)
        
    with col3:
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Empresas sin Beneficiarios</div>
                <div class="metric-value">{:}</div>
                <div class="metric-tooltip" title="{}"></div>
            </div>
        """.format(empresas_sin_benef, TOOLTIPS_DESCRIPTIVOS.get("EMPRESAS SIN BENEFICIARIOS", "")), unsafe_allow_html=True)

    st.markdown("""<div class="info-box">Las empresas (Empresas y Monotributistas) en esta tabla se encuentran adheridas a uno o más programas de empleo, han cumplido con los requisitos establecidos por los programas en su momento y salvo omisiones, han proporcionado sus datos a través de los registros de programasempleo.cba.gov.ar</div>""", unsafe_allow_html=True)

    # Mostrar el DataFrame con mejor estilo, dentro de un expander
    with st.expander("Ver tabla de empresas adheridas", expanded=False):
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
            # Gráfico de torta por tipo de empresa
            if 'NOMBRE_TIPO_EMPRESA' in df_perfil_demanda.columns:
                tipo_empresa_count = (
                df_perfil_demanda.groupby('NOMBRE_TIPO_EMPRESA')['CUIT'].nunique()
                .reset_index()
                .rename(columns={'NOMBRE_TIPO_EMPRESA': 'Tipo de Empresa', 'CUIT': 'Cantidad'})
)
                fig_pie = px.pie(tipo_empresa_count, names='Tipo de Empresa', values='Cantidad',
                                 title='Distribución por Tipo de Empresa',
                                 color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No hay datos de tipo de empresa para graficar.")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # --- Visualización 2: Gráfico de Barras por Categoría (Top 10) (en col2) con mejor estilo ---
        with col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<h3 style="font-size: 18px; margin-bottom: 15px;">Top 10 - Distribución de Categorías de Empleo</h3>', unsafe_allow_html=True)

            # Agrupar por categoría y contar las ocurrencias
            df_cat_count = df_perfil_demanda.groupby('N_CATEGORIA_EMPLEO')['CUIT'].nunique().reset_index(name='Empresas que Buscan')
            df_cat_count = df_cat_count.sort_values(by='Empresas que Buscan', ascending=False)

            if len(df_cat_count) > 9:
                # Tomar el top 9 directamente, sin agregar 'Otros'
                df_cat_count_final = df_cat_count.head(9).copy()
            else:
                df_cat_count_final = df_cat_count.copy()

            if True:
                # Crear gráfico de barras con texto de categoría y conteo visible
                chart_cat = alt.Chart(df_cat_count_final).mark_bar(
                    cornerRadiusTopRight=5,
                    cornerRadiusBottomRight=5
                ).encode( 
                    x=alt.X('Empresas que Buscan', title=''),  
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''), 
                    tooltip=['N_CATEGORIA_EMPLEO', 'Empresas que Buscan'],
                    color=alt.value('#4e73df')  # Consistent color scheme
                ).properties(
                    width=600,
                    height=400
                )
                # Texto de conteo
                text_count = alt.Chart(df_cat_count_final).mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,
                    color='white',
                    fontWeight='bold',
                    size=16
                ).encode(
                    x=alt.X('Empresas que Buscan', title=''),  
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''), 
                    text=alt.Text('Empresas que Buscan', format=',d')
                )
                # Texto de categoría (ubicado a la izquierda de la barra)
                text_cat = alt.Chart(df_cat_count_final).mark_text(
                    align='right',
                    baseline='middle',
                    dx=-8,
                    color='black',
                    fontWeight='bold',
                    size=14
                ).encode(
                    x=alt.value(0),
                    y=alt.Y('N_CATEGORIA_EMPLEO:N', title=''),
                    text='N_CATEGORIA_EMPLEO'
                )
                # Combinar gráfico de barras, texto de conteo y texto de categoría
                combined_chart = alt.layer(chart_cat, text_count, text_cat)
                # Configuración visual
                combined_chart = combined_chart.configure_axisY(labels=False, domain=False, ticks=False)
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
                     <div class="metric-value">{total_match}</div>
                </div>
            """)
        
        with col2:
            st.markdown(f"""
                <div class="metric-card status-info">
                    <div class="metric-label">Total Beneficiarios {programa_seleccionado_nombre}</div>
                    <div class="metric-value">{total_benef}</div>
                </div>
            """)
        
        # Resto del código de visualización con mejoras visuales
        # Aquí puedes añadir más visualizaciones según sea necesario
    
    except Exception as e:
        st.markdown(f"""
            <div class="info-box status-warning">
                <strong>Información:</strong> Se mostrarán los datos disponibles: {str(e)}
            </div>
        """, unsafe_allow_html=True)

def show_empleo_dashboard(data, dates=None, is_development=False):
    """
    Muestra el dashboard de programas de empleo.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
    # Mostrar columnas en modo desarrollo
    if is_development:
        st.markdown("***")
        st.caption("Información de Desarrollo (Columnas de DataFrames - Empleo)")
        if isinstance(data, dict):
            for name, df in data.items():
                if df is not None:
                    with st.expander(f"Columnas en: `{name}`"):
                        st.write(df.columns.tolist())
                        st.write("Primeras 5 filas:")
                        st.dataframe(df.head())
                        st.write(f"Total de registros: {len(df)}")
                else:
                    st.warning(f"DataFrame '{name}' no cargado o vacío.")
        else:
            st.warning("Formato de datos inesperado para Empleo.")
        st.markdown("***")
        
    try:
        # Cargar y preprocesar datos
        df_inscriptos, df_empresas, df_poblacion, geojson_data, has_fichas, has_empresas, has_poblacion, has_geojson = load_and_preprocess_data(data, dates)
        

        if not has_fichas:
            return
        
        # Renderizar el dashboard principal
        subtab_names = ["General", "Datos Censales"]
        sub_tabs = st.tabs(subtab_names)

        # Subpestaña General (coloca aquí el dashboard principal de empleo)
        with sub_tabs[0]:
            # Aquí va el contenido actual del dashboard de empleo
            st.markdown("### Dashboard General de Programas de Empleo")
            render_dashboard(df_inscriptos, df_empresas, df_poblacion, geojson_data, has_empresas, has_geojson)

        # Subpestaña Datos Censales
        with sub_tabs[1]:
            st.markdown("### Datos Censales de Localidades")
            st.info("""
            **¿Cómo se calcula la tasa de desocupación?**  
            La tasa de desempleo se calcula dividiendo el número de personas desocupadas por la Población Económicamente Activa (PEA) y multiplicando por 100. Fuente: INDEC.
            """)
            df_censales = data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt')
            if df_censales is not None and not getattr(df_censales, 'empty', True):
                departamentos = sorted(df_censales['DEPARTAMENTO'].dropna().unique())
                depto_sel = st.selectbox("Filtrar por Departamento", options=["Todos"] + departamentos)
                if depto_sel != "Todos":
                    df_censales = df_censales[df_censales['DEPARTAMENTO'] == depto_sel]
                localidades = sorted(df_censales['LOCALIDAD'].dropna().unique())
                loc_sel = st.selectbox("Filtrar por Localidad", options=["Todas"] + localidades)
                if loc_sel != "Todas":
                    df_censales = df_censales[df_censales['LOCALIDAD'] == loc_sel]
                df_tabla = df_censales[["DEPARTAMENTO", "LOCALIDAD", "Tasa de Actividad", "Tasa de Empleo", "Tasa de desocupación"]].copy()
                for col in ["Tasa de Actividad", "Tasa de Empleo", "Tasa de desocupación"]:
                    df_tabla[col] = df_tabla[col].round(2)
                # --- Agregar fila de totales ---
                total_row = pd.DataFrame({
                    "DEPARTAMENTO": ["Total"],
                    "LOCALIDAD": ["Total"],
                    "Tasa de Actividad": [df_tabla["Tasa de Actividad"].mean()],
                    "Tasa de Empleo": [df_tabla["Tasa de Empleo"].mean()],
                    "Tasa de desocupación": [df_tabla["Tasa de desocupación"].mean()]
                })
                df_tabla_tot = pd.concat([df_tabla, total_row], ignore_index=True)

                # --- Reemplazar NaN/None por guion largo para mejor visualización ---
                df_tabla_tot = df_tabla_tot.where(pd.notnull(df_tabla_tot), '—')
                # Formatear las columnas de porcentaje como strin  g con símbolo %
                for col in ["Tasa de Actividad", "Tasa de Empleo", "Tasa de desocupación"]:
                    df_tabla_tot[col] = df_tabla_tot[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) and x != '—' else x)
                # --- Configuración de columnas con formato y tooltips ---
                column_config = {
                    "DEPARTAMENTO": st.column_config.TextColumn("Departamento"),
                    "LOCALIDAD": st.column_config.TextColumn("Localidad"),
                    "Tasa de Actividad": st.column_config.NumberColumn("Tasa de Actividad", help="Porcentaje de la PEA sobre la población total", format="%.2f%%"),
                    "Tasa de Empleo": st.column_config.NumberColumn("Tasa de Empleo", help="Porcentaje de ocupados sobre la PEA", format="%.2f%%"),
                    "Tasa de desocupación": st.column_config.NumberColumn("Tasa de Desocupación", help="Porcentaje de desocupados sobre la PEA", format="%.2f%%")
                }
                st.dataframe(
                    df_tabla_tot,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config,
                    height=400
                )
            else:
                st.warning("No se encontraron datos censales para mostrar.")

    except Exception as e:
        st.error(f"Error al procesar los datos: {str(e)}")