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
from io import StringIO
import os


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
    
    # Inicializar la lista de filtros aplicados
    filtros_aplicados = []
    
    with st.container():
        # Contenedor de filtros con 2 columnas
        col1, col2= st.columns(2)
        
        # Filtro de departamento en la primera columna
        with col1:
            # Solo mostrar el filtro de departamento si la columna existe en el dataframe
            if 'N_DEPARTAMENTO' in df_inscriptos.columns:
                departamentos = sorted(df_inscriptos['N_DEPARTAMENTO'].dropna().unique())
                all_dpto_option = "Todos los departamentos"
                selected_dpto = st.selectbox("Departamento (Beneficiarios):", [all_dpto_option] + list(departamentos), key=f"{key_prefix}_dpto_filter")
                
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

def show_empleo_dashboard(data, dates, is_development=False):
    """
    Función principal que muestra el dashboard de empleo.
    
    Args:
        data: Diccionario con los dataframes cargados
        dates: Diccionario con las fechas de actualización
        is_development: Booleano que indica si estamos en modo desarrollo
    """
    if data is None:
        st.error("No se pudieron cargar los datos de Programas de Empleo.")
        return
    # Mostrar info de desarrollo de los DataFrames
    if is_development:
        from utils.ui_components import show_dev_dataframe_info
        show_dev_dataframe_info(data, modulo_nombre="Empleo")
    try:
        # Cargar y preprocesar los datos
        df_inscriptos, df_empresas, df_poblacion, geojson_data, has_fichas, has_empresas, has_poblacion, has_geojson = load_and_preprocess_data(data, dates, is_development)
        
        # Renderizar el dashboard principal
        render_dashboard(df_inscriptos, df_empresas, df_poblacion, geojson_data, has_empresas, has_geojson)
        
        # Agregar sección específica para datos censales
        st.markdown("### Información Demográfica y Estadísticas Laborales por Localidad")
        st.markdown("Esta sección presenta indicadores demográficos y laborales clave por localidad según datos censales")
        
        with st.expander("Ver Datos Censales de Localidades", expanded=False):
            st.info("""
            **¿Cómo se calcula la tasa de desocupación?**  
            La tasa de desempleo se calcula dividiendo el número de personas desocupadas por la Población Económicamente Activa (PEA) y multiplicando por 100. Fuente: INDEC.
            """)
            df_censales = data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - DATOS_CENSALES.txt')
            if df_censales is not None and not getattr(df_censales, 'empty', True):
                col1, col2 = st.columns(2)
                with col1:
                    departamentos_censales = sorted(df_censales['CODIGOS.Departamento'].dropna().unique())
                    depto_sel_censal = st.selectbox("Filtrar por Departamento (Censal):", options=["Todos"] + departamentos_censales, key="censo_depto")
                with col2:
                    if depto_sel_censal != "Todos":
                        df_censales_filtrado_loc = df_censales[df_censales['CODIGOS.Departamento'] == depto_sel_censal]
                    else:
                        df_censales_filtrado_loc = df_censales
                    
                    localidades_censales = sorted(df_censales_filtrado_loc['CODIGOS.Localidad'].dropna().unique())
                    loc_sel_censal = st.selectbox("Filtrar por Localidad (Censal):", options=["Todas"] + localidades_censales, key="censo_loc")
                
                df_censales_display = df_censales.copy()
                if depto_sel_censal != "Todos":
                    df_censales_display = df_censales_display[df_censales_display['CODIGOS.Departamento'] == depto_sel_censal]
                if loc_sel_censal != "Todas":
                    df_censales_display = df_censales_display[df_censales_display['CODIGOS.Localidad'] == loc_sel_censal]
                
                cols_tabla_censal = ["CODIGOS.Departamento", "CODIGOS.Localidad", "Tasa de Actividad", "Tasa de Empleo", "Tasa de desocupación"]
                df_tabla_censal = df_censales_display[cols_tabla_censal].copy()
                
                
                # Solo mostrar los datos originales, sin fila de totales
                df_tabla_censal_tot = df_tabla_censal.copy()

                st.dataframe(
                    df_tabla_censal_tot,
                    use_container_width=True
                )
            else:
                st.warning("Datos censales no disponibles o vacíos.") 

        with st.expander("Ver datos de población (basado en ppp_jesi.xlsx y mas26_jesi.xlsx)"):
            try:
                df_ppp_excel = data.get('ppp_jesi.xlsx')
                df_mas26_excel = data.get('mas26_jesi.xlsx')
                
                if df_ppp_excel is not None and not df_ppp_excel.empty and df_mas26_excel is not None and not df_mas26_excel.empty:

                    ppp_cols_esperadas = ['Población de 15 a 24 años', 'TOTAL PEA', 'OCUPADA', 'DESOCUPADA']
                    mas26_cols_esperadas = ['Población mayor de 25 años', 'TOTAL PEA', 'OCUPADA', 'DESOCUPADA']

                    if not all(col in df_ppp_excel.columns for col in ppp_cols_esperadas):
                        st.warning(f"El archivo 'ppp_jesi.xlsx' no contiene todas las columnas esperadas: {', '.join(ppp_cols_esperadas)}")
                    elif not all(col in df_mas26_excel.columns for col in mas26_cols_esperadas):
                        st.warning(f"El archivo 'mas26_jesi.xlsx' no contiene todas las columnas esperadas: {', '.join(mas26_cols_esperadas)}")
                    else:
                        df_ppp_clean = df_ppp_excel[ppp_cols_esperadas].copy()
                        df_mas26_clean = df_mas26_excel[mas26_cols_esperadas].copy()
                        
                        df_ppp_clean = df_ppp_clean.rename(columns={'Población de 15 a 24 años': 'POBLACION'})
                        df_mas26_clean = df_mas26_clean.rename(columns={'Población mayor de 25 años': 'POBLACION'})
                        
                        df_ppp_clean['GRUPO'] = 'Población de 15 a 24 años'
                        df_mas26_clean['GRUPO'] = 'Población mayor de 25 años'
                        
                        df_poblacion_completo = pd.concat([df_ppp_clean, df_mas26_clean], ignore_index=True)
                        
                        st.subheader("Datos de Población por Grupo Etario")
                        st.dataframe(df_poblacion_completo)
                        
                        colores_grafico = []
                        if COLORES_IDENTIDAD and isinstance(COLORES_IDENTIDAD, dict) and len(COLORES_IDENTIDAD.values()) >= 3:
                            colores_grafico = list(COLORES_IDENTIDAD.values())[:3]
                        else:
                            colores_grafico = px.colors.qualitative.Plotly[:3]

                        fig_poblacion = px.bar(
                            df_poblacion_completo, 
                            x='GRUPO', 
                            y=['TOTAL PEA', 'OCUPADA', 'DESOCUPADA'],
                            barmode='group', 
                            title="Distribución de la Población por Situación Laboral",
                            color_discrete_sequence=colores_grafico
                        )
                        fig_poblacion.update_layout(xaxis_title="Grupo Etario", yaxis_title="Cantidad de Personas", legend_title="Situación Laboral", height=500)
                        st.plotly_chart(fig_poblacion, use_container_width=True)
                        
                        st.subheader("Tasas de Empleo y Desocupación")
                        df_tasas = df_poblacion_completo.copy()
                        df_tasas['Tasa de Actividad'] = np.where(df_tasas['POBLACION'] != 0, (df_tasas['TOTAL PEA'] / df_tasas['POBLACION'] * 100), 0).round(2)
                        df_tasas['Tasa de Empleo'] = np.where(df_tasas['POBLACION'] != 0, (df_tasas['OCUPADA'] / df_tasas['POBLACION'] * 100), 0).round(2)
                        df_tasas['Tasa de Desocupación'] = np.where(df_tasas['TOTAL PEA'] != 0, (df_tasas['DESOCUPADA'] / df_tasas['TOTAL PEA'] * 100), 0).round(2)
                        
                        st.dataframe(df_tasas[['GRUPO', 'Tasa de Actividad', 'Tasa de Empleo', 'Tasa de Desocupación']])
                        
                        colores_tasas = []
                        if COLORES_IDENTIDAD and isinstance(COLORES_IDENTIDAD, dict) and len(COLORES_IDENTIDAD.values()) >= 6:
                            colores_tasas = list(COLORES_IDENTIDAD.values())[3:6]
                        else:
                            colores_tasas = px.colors.qualitative.Plotly[3:6]

                        fig_tasas = px.bar(
                            df_tasas, 
                            x='GRUPO', 
                            y=['Tasa de Actividad', 'Tasa de Empleo', 'Tasa de Desocupación'],
                            barmode='group', 
                            title="Tasas de Empleo por Grupo Etario",
                            color_discrete_sequence=colores_tasas
                        )
                        fig_tasas.update_layout(xaxis_title="Grupo Etario", yaxis_title="Porcentaje (%)", legend_title="Indicador", height=500)
                        st.plotly_chart(fig_tasas, use_container_width=True)
                else:
                    st.warning("No se pudieron cargar los archivos de población (ppp_jesi.xlsx y/o mas26_jesi.xlsx) para esta sección o están vacíos.")
            except Exception as e:
                st.error(f"Error al procesar los datos de población en esta sección: {str(e)}")

    except Exception as e:
        st.error(f"Error al mostrar el dashboard de empleo: {str(e)}")

def load_and_preprocess_data(data, dates=None, is_development=False):
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
        #df_empresas = clean_thousand_separator(df_empresas)
        #df_inscriptos_raw = clean_thousand_separator(df_inscriptos_raw)
        #df_poblacion = clean_thousand_separator(df_poblacion)
        
        # Cargar y limpiar datos censales (si existen)
        df_censales = data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - DATOS_CENSALES.txt')
        if df_censales is not None and not df_censales.empty:
            # Limpiar caracteres especiales en columnas numéricas
            numeric_cols = ['Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación']
            for col in numeric_cols:
                if col in df_censales.columns:
                    # Convertir a string primero para manejar cualquier tipo de dato
                    df_censales[col] = df_censales[col].astype(str)
                    # Reemplazar guión largo u otros caracteres no numéricos con NaN
                    df_censales[col] = df_censales[col].replace(['\u2014', '\u2013', '\u2212', '-', 'nan', 'None', 'null', ''], pd.NA)
                    # Convertir a numérico
                    df_censales[col] = pd.to_numeric(df_censales[col], errors='coerce')
            # Actualizar el dataset en el diccionario de datos
            data['LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - DATOS_CENSALES.txt'] = df_censales

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
                # Mostrar la fecha de última actualización
        from utils.ui_components import show_last_update
        show_last_update(dates, 'VT_REPORTES_PPP_MAS26.parquet')
        

        
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
        df_inscriptos_cruzado = None  # Para debug visual
        if has_circuitos:
            try:
                # Asegurarse de que las columnas estén correctamente tipadas
                if 'ID_LOCALIDAD' in df_circuitos.columns:
                    df_circuitos['ID_LOCALIDAD'] = pd.to_numeric(df_circuitos['ID_LOCALIDAD'], errors='coerce')
                
                # Limpiar datos si es necesario
                #df_circuitos = clean_thousand_separator(df_circuitos)
                #df_circuitos = convert_decimal_separator(df_circuitos)
                
                # Si hay datos de inscriptos y circuitos, intentar cruzarlos
                if df_inscriptos is not None and not df_inscriptos.empty:
                    if 'ID_LOCALIDAD_GOB' in df_inscriptos.columns and 'ID_LOCALIDAD' in df_circuitos.columns:
                        df_inscriptos = pd.merge(
                            df_inscriptos,
                            df_circuitos,
                            left_on='ID_LOCALIDAD_GOB',
                            right_on='ID_LOCALIDAD',
                            how='left',
                            suffixes=('', '_circuito')
                        )
                    # Guardar copia para debug visual si estamos en modo desarrollo
                    df_inscriptos_cruzado = df_inscriptos.copy()

            except Exception as e:
                st.error(f"Error al procesar datos de circuitos electorales: {str(e)}")
                has_circuitos = False
        
        # Mostrar df_inscriptos cruzado solo en modo desarrollo
        if is_development:
            if df_inscriptos_cruzado is not None:
                with st.expander('🔍 Visualización DEBUG: df_inscriptos cruzado (post-merge) NO RETORNA DE LA FUNCION DE CARGA', expanded=False):
                    st.dataframe(df_inscriptos_cruzado.head(50))
                    st.write(f"Filas: {df_inscriptos_cruzado.shape[0]}, Columnas: {df_inscriptos_cruzado.shape[1]}")
        # Retornar los dataframes procesados y los flags de disponibilidad
        return df_inscriptos_sin_adherido, df_empresas, df_poblacion, geojson_data, has_fichas, has_empresas, has_poblacion, has_geojson



def render_dashboard(df_inscriptos, df_empresas, df_poblacion, geojson_data, has_empresas, has_geojson):
    """
    Renderiza el dashboard principal con los datos procesados.
    """
    with st.spinner("Generando visualizaciones..."):
        # Calcular KPIs importantes antes de aplicar filtros
        total_beneficiarios = df_inscriptos[df_inscriptos['BEN_N_ESTADO'].isin(["BENEFICIARIO RETENIDO", "ACTIVO", "BAJA PEDIDO POR EMPRESA"])].shape[0]
        total_beneficiarios_cti = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'] == "BENEFICIARIO- CTI"].shape[0]
        total_general = total_beneficiarios + total_beneficiarios_cti
        
        # Calcular beneficiarios por zona
        beneficiarios_zona_favorecida = df_inscriptos[((df_inscriptos['N_ESTADO_FICHA'].isin(["BENEFICIARIO- CTI"])) | 
                                        (df_inscriptos['BEN_N_ESTADO'].isin(["BENEFICIARIO RETENIDO", "ACTIVO", "BAJA PEDIDO POR EMPRESA"])))&
                                        (df_inscriptos['ZONA'] == 'ZONA FAVORECIDA')].shape[0]
        
        # Mostrar KPIs en la parte superior
        st.markdown('<div class="kpi-container">', unsafe_allow_html=True)
        
        # Usar la función auxiliar para mostrar KPIs
        kpi_data = [
            {
                "title": "BENEFICIARIOS TOTALES",
                "value_form": f"{total_general:,}".replace(',', '.'),
                "color_class": "kpi-primary",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS TOTALES", "")
            },
            {
                "title": "BENEFICIARIOS EL",
                "value_form": f"{total_beneficiarios:,}".replace(',', '.'),
                "color_class": "kpi-secondary",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS EL", "")
            },
            {
                "title": "ZONA FAVORECIDA",
                "value_form": f"{beneficiarios_zona_favorecida:,}".replace(',', '.'),
                "color_class": "kpi-accent-3",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("ZONA FAVORECIDA", "")
            },
            {
                "title": "BENEFICIARIOS CTI",
                "value_form": f"{total_beneficiarios_cti:,}".replace(',', '.'),
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
            # Contenedor para los filtros específicos de la pestaña Beneficiarios
            df_filtered, selected_dpto, selected_loc, all_dpto_option, all_loc_option = render_filters(df_inscriptos, key_prefix="benef_tab")
            
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
                    
                    # Manejar posibles valores NaN antes de convertir a entero
                    if pd.isna(row[col]):
                        html_table_main += f'<td {cell_style}>0</td>'
                    else:
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
                                # Manejar posibles valores NaN antes de convertir a entero
                                if pd.isna(row[col]):
                                    html_table_grupo3 += f'<td {cell_style}>0</td>'
                                else:
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
                                # Manejar posibles valores NaN antes de convertir a entero
                                if pd.isna(row[col]):
                                    html_table_grupo3 += f'<td {cell_style}>0</td>'
                                else:
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
                # Pasar directamente el DataFrame de empresas sin aplicar los filtros de render_filters
                # ya que los filtros se manejarán internamente en show_companies
                show_companies(df_empresas, geojson_data)
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
        # Guardamos la lista original de programas para cada CUIT antes de agrupar
        df_empresas['PROGRAMAS_LISTA'] = df_empresas['ADHERIDO']
        df_empresas['ADHERIDO'] = df_empresas.groupby('CUIT')['ADHERIDO'].transform(lambda x: ', '.join(sorted(set(x))))
    
    # Usar columns_to_select para crear df_display correctamente
    df_display = df_empresas[columns_to_select + (['PROGRAMAS_LISTA'] if 'PROGRAMAS_LISTA' in df_empresas.columns else [])].drop_duplicates(subset='CUIT')
    df_display = df_display.sort_values(by='CUPO', ascending=False).reset_index(drop=True)
    
    # Extraer todos los programas únicos para el filtro multiselect
    programas_unicos = []
    if 'ADHERIDO' in df_display.columns:
        # Extraer todos los programas únicos de la columna ADHERIDO
        todos_programas = df_display['ADHERIDO'].str.split(', ').explode().dropna().unique()
        programas_unicos = sorted(todos_programas)
    st.markdown("<hr style='border: 1px solid #e0e0e0; margin: 20px 0;'>", unsafe_allow_html=True)
    
    # Añadir filtros en la pestaña de empresas
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col_filtro1, col_filtro2 = st.columns(2)
    
    # Primera columna para el filtro de programas (subido como solicitado)
    with col_filtro1:
        if programas_unicos:
            st.markdown('<div class="filter-label">Programa:</div>', unsafe_allow_html=True)
            selected_programas = st.multiselect("Seleccionar programas", options=programas_unicos, default=[], label_visibility="collapsed")
        else:
            selected_programas = []
    
    # Segunda columna para el filtro de departamento
    with col_filtro2:
        st.markdown('<div class="filter-label">Departamento (Empresas):</div>', unsafe_allow_html=True)
        if 'N_DEPARTAMENTO' in df_display.columns:
            departamentos = sorted(df_display['N_DEPARTAMENTO'].dropna().unique())
            selected_dpto = st.selectbox("Seleccionar departamento de empresas", options=["Todos los departamentos"] + departamentos, label_visibility="collapsed")
        else:
            selected_dpto = "Todos los departamentos"
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Aplicar filtros al dataframe
    df_filtered = df_display.copy()
    
    # Filtrar por departamento si se seleccionó uno específico
    if selected_dpto != "Todos los departamentos" and 'N_DEPARTAMENTO' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['N_DEPARTAMENTO'] == selected_dpto]
    
    # Filtrar por programas seleccionados
    if selected_programas:
        # Crear una máscara para filtrar empresas que tengan al menos uno de los programas seleccionados
        # Primero verificamos si la columna PROGRAMAS_LISTA existe (datos originales)
        if 'PROGRAMAS_LISTA' in df_filtered.columns:
            # Creamos un conjunto con los CUITs de empresas que tienen alguno de los programas seleccionados
            cuits_con_programas = set()
            for programa in selected_programas:
                # Obtenemos los CUITs de empresas con este programa
                cuits_programa = df_empresas[df_empresas['PROGRAMAS_LISTA'] == programa]['CUIT'].unique()
                cuits_con_programas.update(cuits_programa)
            # Filtramos el dataframe para incluir solo las empresas con los CUITs seleccionados
            df_filtered = df_filtered[df_filtered['CUIT'].isin(cuits_con_programas)]
        else:
            # Si no tenemos la columna original, usamos el campo ADHERIDO agregado
            mask = df_filtered['ADHERIDO'].apply(lambda x: any(programa in x.split(', ') for programa in selected_programas))
            df_filtered = df_filtered[mask]
    
    # Mostrar mensaje con el número de registros después de aplicar los filtros
    st.markdown(f'<div class="filter-info">Mostrando {len(df_filtered)} de {len(df_display)} empresas</div>', unsafe_allow_html=True)

    # Métricas y tabla final con mejor diseño
    empresas_adh = df_filtered['CUIT'].nunique()
    
    # Calcular empresas con y sin beneficiarios
    empresas_con_benef = df_filtered[df_filtered['BENEF'] > 0]['CUIT'].nunique()
    empresas_sin_benef = df_filtered[df_filtered['BENEF'].isna()]['CUIT'].nunique()
    
    # Calcular empresas por programa para mostrar en los KPIs usando los datos originales
    programas_conteo = {}
    programas_con_benef = {}
    programas_sin_benef = {}
    
    if 'PROGRAMAS_LISTA' in df_empresas.columns:
        # Usamos el dataframe original (antes del agrupamiento) para contar correctamente
        df_empresas_original = df_empresas.copy()
        
        # Aplicamos los mismos filtros que aplicamos a df_filtered
        if selected_dpto != "Todos los departamentos" and 'N_DEPARTAMENTO' in df_empresas_original.columns:
            df_empresas_original = df_empresas_original[df_empresas_original['N_DEPARTAMENTO'] == selected_dpto]
            
        for programa in programas_unicos:
            # Contar empresas que tienen este programa específico
            empresas_programa = df_empresas_original[df_empresas_original['PROGRAMAS_LISTA'] == programa]
            cuits_programa = empresas_programa['CUIT'].unique()
            
            # Total de empresas por programa
            programas_conteo[programa] = len(cuits_programa)
            
            # Empresas con beneficiarios por programa
            if 'BENEF' in df_empresas_original.columns:
                cuits_con_benef = empresas_programa[empresas_programa['BENEF'] > 0]['CUIT'].unique()
                programas_con_benef[programa] = len(cuits_con_benef)
                
                # Empresas sin beneficiarios por programa
                cuits_sin_benef = empresas_programa[empresas_programa['BENEF'].isna()]['CUIT'].unique()
                programas_sin_benef[programa] = len(cuits_sin_benef)
    
    # Obtener los dos programas principales para mostrar en cada KPI
    programas_principales = sorted(programas_conteo.items(), key=lambda x: x[1], reverse=True)[:2] if programas_conteo else []
    programas_con_benef_principales = sorted(programas_con_benef.items(), key=lambda x: x[1], reverse=True)[:2] if programas_con_benef else []
    programas_sin_benef_principales = sorted(programas_sin_benef.items(), key=lambda x: x[1], reverse=True)[:2] if programas_sin_benef else []
    
    # Layout para los KPIs - 3 columnas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Crear el subtexto con el desglose por programa
        subtexto = ""
        if programas_principales:
            subtexto_items = [f"{prog}: {count}" for prog, count in programas_principales]
            subtexto = f"<div class='metric-subtitle'>{' - '.join(subtexto_items)}</div>"
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Empresas Adheridas</div>
                <div class="metric-value">{:}</div>
                {}
                <div class="metric-tooltip" title="{}"></div>
            </div>
        """.format(empresas_adh, subtexto, TOOLTIPS_DESCRIPTIVOS.get("EMPRESAS ADHERIDAS", "")), unsafe_allow_html=True)
    
    with col2:
        # Crear el subtexto con el desglose por programa para empresas con beneficiarios
        subtexto_con_benef = ""
        if programas_con_benef_principales:
            subtexto_items = [f"{prog}: {count}" for prog, count in programas_con_benef_principales]
            subtexto_con_benef = f"<div class='metric-subtitle'>{' - '.join(subtexto_items)}</div>"
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Empresas con Beneficiarios</div>
                <div class="metric-value">{:}</div>
                {}
                <div class="metric-tooltip" title="{}"></div>
            </div>
        """.format(empresas_con_benef, subtexto_con_benef, TOOLTIPS_DESCRIPTIVOS.get("EMPRESAS CON BENEFICIARIOS", "")), unsafe_allow_html=True)
        
    with col3:
        # Crear el subtexto con el desglose por programa para empresas sin beneficiarios
        subtexto_sin_benef = ""
        if programas_sin_benef_principales:
            subtexto_items = [f"{prog}: {count}" for prog, count in programas_sin_benef_principales]
            subtexto_sin_benef = f"<div class='metric-subtitle'>{' - '.join(subtexto_items)}</div>"
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Empresas sin Beneficiarios</div>
                <div class="metric-value">{:}</div>
                {}
                <div class="metric-tooltip" title="{}"></div>
            </div>
        """.format(empresas_sin_benef, subtexto_sin_benef, TOOLTIPS_DESCRIPTIVOS.get("EMPRESAS SIN BENEFICIARIOS", "")), unsafe_allow_html=True)

    st.markdown("""<div class="info-box">Las empresas (Empresas y Monotributistas) en esta tabla se encuentran adheridas a uno o más programas de empleo, han cumplido con los requisitos establecidos por los programas en su momento y salvo omisiones, han proporcionado sus datos a través de los registros de programasempleo.cba.gov.ar</div>""", unsafe_allow_html=True)

    # Mostrar el DataFrame con mejor estilo, dentro de un expander
    with st.expander("Ver tabla de empresas adheridas", expanded=False):
        st.dataframe(df_filtered, hide_index=True, use_container_width=True)

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

