import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row, show_last_update
from utils.map_utils import create_choropleth_map, display_map
from utils.styles import COLORES_IDENTIDAD
from utils.data_cleaning import clean_thousand_separator, convert_decimal_separator
from utils.kpi_tooltips import TOOLTIPS_DESCRIPTIVOS, ESTADO_TOOLTIPS
from streamlit_folium import folium_static
import geopandas as gpd
import math
import altair as alt
from io import StringIO
import datetime
import io


def create_empleo_kpis(resultados, programa_nombre=""):
    """
    Crea los KPIs específicos para el módulo Programas de Empleo.
    
    Args:
        resultados (dict): Diccionario con los resultados de conteo por categoría
        programa_nombre (str): Nombre del programa seleccionado para mostrar en los títulos
    Returns:
        list: Lista de diccionarios con datos de KPI para Programas de Empleo
    """
    kpis = [
        {
            "title": f"TOTAL MATCH {programa_nombre}",
            "value_form": f"{resultados.get('total_match', 0):,}".replace(',', '.'),
            "color_class": "kpi-primary",
            "delta": "",
            "delta_color": "#d4f7d4"
        },
        {
            "title": f"TOTAL BENEFICIARIOS {programa_nombre}",
            "value_form": f"{resultados.get('total_benef', 0):,}".replace(',', '.'),
            "color_class": "kpi-secondary",
            "delta": "",
            "delta_color": "#d4f7d4"
        },
        {
            "title": f"POSTULANTES VALIDADOS",
            "value_form": f"{resultados.get('total_validos', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-1",
            "delta": "",
            "delta_color": "#d4f7d4"
        }
    ]
    return kpis




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

def render_tab_filters(df, key_prefix):
    """
    Renderiza los filtros para una pestaña específica y devuelve el DataFrame filtrado.
    """
    df_filtered = df.copy()
    filtros_aplicados = []

    col1, col2 = st.columns(2)

    with col1:
        if 'N_DEPARTAMENTO' in df.columns:
            departamentos = sorted(df['N_DEPARTAMENTO'].dropna().unique())
            all_dpto_option = "Todos los departamentos"
            selected_dpto = st.selectbox("Departamento:", [all_dpto_option] + departamentos, key=f"{key_prefix}_dpto")

            if selected_dpto != all_dpto_option:
                df_filtered = df_filtered[df_filtered['N_DEPARTAMENTO'] == selected_dpto]
                filtros_aplicados.append(f"Departamento: {selected_dpto}")

    with col2:
        if 'ZONA' in df.columns:
            zonas = sorted(df['ZONA'].dropna().unique())
            all_zona_option = "Todas las zonas"
            selected_zona = st.selectbox("Zona:", [all_zona_option] + zonas, key=f"{key_prefix}_zona")
            if selected_zona != all_zona_option:
                df_filtered = df_filtered[df_filtered['ZONA'] == selected_zona]
                filtros_aplicados.append(f"Zona: {selected_zona}")

    if filtros_aplicados:
        st.markdown(f"**Filtros aplicados:** {', '.join(filtros_aplicados)}")
    else:
        st.markdown("**Mostrando todos los datos para esta sección**")

    return df_filtered

def show_empleo_dashboard(data, dates, is_development=False):
    """
    Función principal que muestra el dashboard de empleo.
    """
    if dates:
        show_last_update(dates, 'VT_REPORTES_PPP_MAS26.parquet')
    
    if data is None:
        st.error("No se pudieron cargar los datos de Programas de Empleo.")
        return

    if is_development:
        from utils.ui_components import show_dev_dataframe_info
        show_dev_dataframe_info(data, modulo_nombre="Empleo", is_development=is_development)

    df_postulantes_empleo, df_inscriptos, df_empresas, geojson_data = load_and_preprocess_data(data, dates, is_development)
    
    render_dashboard(df_postulantes_empleo, df_inscriptos, df_empresas, geojson_data)

def load_and_preprocess_data(data, dates=None, is_development=False):
    """
    Carga y preprocesa los datos necesarios para el dashboard.
    """
    with st.spinner("Cargando y procesando datos de empleo..."):
        df_postulantes_empleo = data.get('VT_INSCRIPCIONES_EMPLEO.parquet')
        df_inscriptos_raw = data.get('VT_REPORTES_PPP_MAS26.parquet')
        geojson_data = data.get('capa_departamentos_2010.geojson')
        df_circuitos = data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt')
        has_circuitos = df_circuitos is not None and not df_circuitos.empty

        df_emp_ben = (
            df_inscriptos_raw[
                (df_inscriptos_raw["IDETAPA"].isin([51, 53, 54, 55,57])) &
                (df_inscriptos_raw["N_ESTADO_FICHA"] == "BENEFICIARIO")
            ]
            .assign(CUIT=lambda df: df["EMP_CUIT"].astype(str).str.replace("-", ""))
            .groupby("CUIT", as_index=False)
            .agg(BENEF=("ID_FICHA", "count"))
        )
        df_empresas = data.get('vt_empresas_ARCA.parquet')

        if "CUIT" in df_empresas.columns:
            df_empresas = df_empresas.merge(df_emp_ben, on="CUIT", how="left")

        if df_inscriptos_raw is None or df_inscriptos_raw.empty:
            st.error("No se pudieron cargar los datos de inscripciones.")
            return None, None, None, None
        
        df_inscriptos = df_inscriptos_raw[df_inscriptos_raw['N_ESTADO_FICHA'] != "ADHERIDO"].copy()

        integer_columns = ["ID_DEPARTAMENTO_GOB", "ID_LOCALIDAD_GOB", "ID_FICHA", "IDETAPA", "CUPO", "ID_MOD_CONT_AFIP", "EDAD"]
        for col in integer_columns:
            if col in df_inscriptos.columns:
                df_inscriptos[col] = df_inscriptos[col].fillna(-1).astype(int)
                df_inscriptos.loc[df_inscriptos[col] == -1, col] = pd.NA
        
        if 'N_DEPARTAMENTO' in df_inscriptos.columns and 'N_LOCALIDAD' in df_inscriptos.columns:
            capital_mask = df_inscriptos['N_DEPARTAMENTO'] == 'CAPITAL'
            df_inscriptos.loc[capital_mask, 'N_LOCALIDAD'] = 'CORDOBA'
        
        if 'BEN_N_ESTADO' in df_inscriptos.columns:
            estado_ben_mask = df_inscriptos['BEN_N_ESTADO'] == 'BAJA POR FINALIZACION DE PROGR'
            df_inscriptos.loc[estado_ben_mask, 'N_ESTADO_FICHA'] = 'BENEFICIARIO FIN PROGRAMA'

        zonas_favorecidas = ['PRESIDENTE ROQUE SAENZ PEÑA', 'GENERAL ROCA', 'RIO SECO', 'TULUMBA', 'POCHO', 'SAN JAVIER', 'SAN ALBERTO', 'MINAS', 'CRUZ DEL EJE', 'TOTORAL', 'SOBREMONTE', 'ISCHILIN']
        df_inscriptos['ZONA'] = df_inscriptos['N_DEPARTAMENTO'].apply(lambda x: 'ZONA NOC Y SUR' if x in zonas_favorecidas else 'ZONA REGULAR')
        
        if 'N_DEPARTAMENTO' in df_empresas.columns:
            df_empresas['ZONA'] = df_empresas['N_DEPARTAMENTO'].apply(lambda x: 'ZONA NOC Y SUR' if x in zonas_favorecidas else 'ZONA REGULAR')

        df_inscriptos_sin_adherido = df_inscriptos.copy()
        
        programas = {53: "Programa Primer Paso", 51: "Más 26", 54: "CBA Mejora", 55: "Nueva Oportunidad", 57: "Más 26 [2025]"}
        if 'IDETAPA' in df_inscriptos_sin_adherido.columns:
            df_inscriptos_sin_adherido['PROGRAMA'] = df_inscriptos_sin_adherido['IDETAPA'].map(lambda x: programas.get(x, f"Programa {x}"))
        else:
            df_inscriptos_sin_adherido['PROGRAMA'] = "No especificado"
            
        if has_circuitos:
            try:
                if 'ID_LOCALIDAD' in df_circuitos.columns:
                    df_circuitos['ID_LOCALIDAD'] = pd.to_numeric(df_circuitos['ID_LOCALIDAD'], errors='coerce')
                
                if df_inscriptos is not None and not df_inscriptos.empty:
                    if 'ID_LOCALIDAD_GOB' in df_inscriptos.columns and 'ID_LOCALIDAD' in df_circuitos.columns:
                        df_inscriptos = pd.merge(df_inscriptos, df_circuitos, left_on='ID_LOCALIDAD_GOB', right_on='ID_LOCALIDAD', how='left', suffixes=('', '_circuito'))

            except Exception as e:
                st.error(f"Error al procesar datos de circuitos electorales: {str(e)}")

        return df_postulantes_empleo, df_inscriptos_sin_adherido, df_empresas, geojson_data

def render_dashboard(df_postulantes_empleo,df_inscriptos, df_empresas, geojson_data):
    """
    Renderiza el dashboard principal con los datos procesados.
    """
    with st.spinner("Generando visualizaciones..."):
        # Calcular KPIs importantes antes de aplicar filtros
        total_beneficiarios = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'].isin(["BENEFICIARIO"])].shape[0]
        total_beneficiarios_fin = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'].isin(["BENEFICIARIO FIN PROGRAMA"])].shape[0]
        total_beneficiarios_cti = df_inscriptos[df_inscriptos['N_ESTADO_FICHA'] == "BENEFICIARIO- CTI"].shape[0]
        total_general = total_beneficiarios + total_beneficiarios_cti
        
        # Calcular beneficiarios por zona
        beneficiarios_zona_favorecida = df_inscriptos[((df_inscriptos['N_ESTADO_FICHA'].isin(["BENEFICIARIO- CTI"])) | 
                                        (df_inscriptos['BEN_N_ESTADO'].isin(["BENEFICIARIO RETENIDO", "ACTIVO", "BAJA PEDIDO POR EMPRESA"])))&
                                        (df_inscriptos['ZONA'] == 'ZONA NOC Y SUR')].shape[0]
        
        # Mostrar KPIs en la parte superior
        st.markdown('<div class="kpi-section">', unsafe_allow_html=True)
        # Usar la función auxiliar para mostrar KPIs
        kpi_data = [
            {
                "title": "BENEFICIARIOS TOTALES (activos)",
                "value_form": f"{total_general:,}".replace(',', '.'),
                "color_class": "kpi-primary",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS TOTALES", "")
            },
            {
                "title": "BENEFICIARIOS EL (activos)",
                "value_form": f"{total_beneficiarios:,}".replace(',', '.'),
                "color_class": "kpi-secondary",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS EL", "")
            },
            {
                "title": "BENEFICIARIOS COMPLETARON PROGRAMA",
                "value_form": f"{total_beneficiarios_fin:,}".replace(',', '.'),
                "color_class": "kpi-accent-1",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS FIN", "")
            },
            {
                "title": "ZONA FAVORECIDA (activos)",
                "value_form": f"{beneficiarios_zona_favorecida:,}".replace(',', '.'),
                "color_class": "kpi-accent-3",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("ZONA FAVORECIDA", "")
            },
            {
                "title": "BENEFICIARIOS CTI (activos)",
                "value_form": f"{total_beneficiarios_cti:,}".replace(',', '.'),
                "color_class": "kpi-accent-4",
                "tooltip": TOOLTIPS_DESCRIPTIVOS.get("BENEFICIARIOS CTI", "")
            }
        ]
        
        display_kpi_row(kpi_data, num_columns=5)
        st.markdown('</div>', unsafe_allow_html=True)
        

        tabs = st.tabs(["Postulantes", "Inscriptos y Beneficiarios", "Empresas"])
        tab_postulantes = tabs[0]
        tab_beneficiarios = tabs[1]
        tab_empresas = tabs[2]
            
            # Pestaña de postulantes
        with tab_postulantes:
            st.markdown('<div class="section-title">Postulantes EMPLEO +26 [2025]</div>', unsafe_allow_html=True)
            
            # Filtros visuales en dos columnas
            st.markdown('<div class="filter-section">', unsafe_allow_html=True)
            col_filtro1, col_filtro2 = st.columns(2)
            
            # Primera columna: filtro de departamento
            with col_filtro1:
                st.markdown('<div class="filter-label">Departamento:</div>', unsafe_allow_html=True)
                if 'N_DEPARTAMENTO' in df_postulantes_empleo.columns:
                    departamentos = sorted(df_postulantes_empleo['N_DEPARTAMENTO'].dropna().unique())
                    selected_dpto = st.selectbox(
                        "Seleccionar departamento",
                        options=["Todos los departamentos"] + departamentos,
                        label_visibility="collapsed"
                    )
                else:
                    selected_dpto = "Todos los departamentos"

            # Segunda columna: filtro de localidad dependiente del departamento
            with col_filtro2:
                st.markdown('<div class="filter-label">Localidad:</div>', unsafe_allow_html=True)
                if selected_dpto != "Todos los departamentos" and 'N_LOCALIDAD' in df_postulantes_empleo.columns:
                    localidades = sorted(df_postulantes_empleo[df_postulantes_empleo['N_DEPARTAMENTO'] == selected_dpto]['N_LOCALIDAD'].dropna().unique())
                    selected_loc = st.selectbox(
                        "Seleccionar localidad",
                        options=["Todas las localidades"] + localidades,
                        label_visibility="collapsed"
                    )
                elif 'N_LOCALIDAD' in df_postulantes_empleo.columns:
                    localidades = sorted(df_postulantes_empleo['N_LOCALIDAD'].dropna().unique())
                    selected_loc = st.selectbox(
                        "Seleccionar localidad",
                        options=["Todas las localidades"] + localidades,
                        label_visibility="collapsed"
                    )
                else:
                    selected_loc = "Todas las localidades"

            st.markdown('</div>', unsafe_allow_html=True)

            # Aplicar filtros al DataFrame
            df_filtrado = df_postulantes_empleo.copy()
            if selected_dpto != "Todos los departamentos":
                df_filtrado = df_filtrado[df_filtrado['N_DEPARTAMENTO'] == selected_dpto]
            if selected_loc != "Todas las localidades":
                df_filtrado = df_filtrado[df_filtrado['N_LOCALIDAD'] == selected_loc]

            # Mostrar mensaje con el número de registros después de aplicar los filtros
            st.markdown(f'<div class="filter-info">Mostrando {len(df_filtrado)} postulantes</div>', unsafe_allow_html=True)


            # KPIs principales
            total_cuil_unicos = df_filtrado['CUIL'].nunique() if 'CUIL' in df_filtrado.columns else 0
            cantidad_cvs = df_filtrado['ID_DOCUMENTO_CV'].dropna().nunique() if 'ID_DOCUMENTO_CV' in df_filtrado.columns else 0

            kpi_data = [
                {
                    "title": "Postulantes (CUIL únicos)",
                    "value_form": f"{total_cuil_unicos:,}".replace(',', '.'),
                    "color_class": "kpi-primary",
                    "tooltip": "Cantidad total de postulantes únicos (basado en CUIL) después de aplicar filtros."
                },
                {
                    "title": "Cantidad CVs Cargados",
                    "value_form": f"{cantidad_cvs:,}".replace(',', '.'),
                    "color_class": "kpi-accent-2",
                    "tooltip": "Número de currículums vitae únicos que han sido cargados por los postulantes filtrados."
                }
            ]
            display_kpi_row(kpi_data)

            # Gráficos en dos columnas
            col_genero, col_edad = st.columns(2)

            with col_genero:
                st.markdown('<div class="section-title">Distribución por Género</div>', unsafe_allow_html=True)
                if 'SEXO' in df_filtrado.columns and 'CUIL' in df_filtrado.columns and not df_filtrado['SEXO'].dropna().empty:
                    gender_counts = df_filtrado.groupby('SEXO')['CUIL'].nunique().reset_index()
                    gender_counts.columns = ['SEXO', 'CANTIDAD']
                    
                    fig_gender = px.pie(
                        gender_counts, 
                        names='SEXO', 
                        values='CANTIDAD', 
                        title='Postulantes por Género (CUILs únicos)',
                        color_discrete_sequence=px.colors.qualitative.Pastel
                    )
                    fig_gender.update_layout(showlegend=True, title_x=0.5)
                    st.plotly_chart(fig_gender, use_container_width=True)
                else:
                    st.info("No hay datos de género o CUIL disponibles para mostrar.")

            with col_edad:
                st.markdown('<div class="section-title">Distribución por Edades</div>', unsafe_allow_html=True)
                if 'FEC_NACIMIENTO' in df_filtrado.columns and 'CUIL' in df_filtrado.columns:
                    df_edad = df_filtrado[['FEC_NACIMIENTO', 'CUIL']].copy().dropna()
                    if not df_edad.empty:
                        today = datetime.date.today()
                        def calcular_edad(fecha_str):
                            try:
                                fecha = pd.to_datetime(fecha_str, errors='coerce')
                                if pd.isnull(fecha):
                                    return None
                                return today.year - fecha.year - ((today.month, today.day) < (fecha.month, fecha.day))
                            except:
                                return None
                        df_edad['EDAD'] = df_edad['FEC_NACIMIENTO'].apply(calcular_edad)
                        df_edad = df_edad.dropna(subset=['EDAD'])

                        bins = [18, 25, 31, 41, 51, 61, 101]
                        labels = ['18-24', '25-30', '31-40', '41-50', '51-60', '60+']
                        df_edad['RANGO_EDAD'] = pd.cut(df_edad['EDAD'], bins=bins, labels=labels, right=False)
                        
                        edad_cuil_counts = df_edad.groupby('RANGO_EDAD')['CUIL'].nunique().reset_index()
                        edad_cuil_counts.columns = ['Rango de Edad', 'Postulantes Únicos']

                        fig_edades = px.bar(
                            edad_cuil_counts,
                            x='Rango de Edad',
                            y='Postulantes Únicos',
                            title='Postulantes por Rango de Edad',
                            color_discrete_sequence=px.colors.qualitative.Vivid
                        )
                        fig_edades.update_layout(title_x=0.5)
                        st.plotly_chart(fig_edades, use_container_width=True)
                    else:
                        st.info("No hay datos de edad válidos para graficar.")
                else:
                    st.info("No se encontró la columna FEC_NACIMIENTO o CUIL.")

            # Botón de Descarga
            st.markdown('<div class="section-title">Descargar Datos</div>', unsafe_allow_html=True)
            st.write("Usa el siguiente botón para descargar los datos de los postulantes filtrados en formato CSV.")

            @st.cache_data
            def convert_df_to_csv(df):
                # IMPORTANT: Cache the conversion to prevent computation on every rerun
                return df.to_csv(index=False).encode('utf-8')

            csv_data = convert_df_to_csv(df_filtrado)

            st.download_button(
                label="📥 Descargar Tabla de Postulantes (CSV)",
                data=csv_data,
                file_name=f"postulantes_empleo_{datetime.date.today()}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        # Contenido de la pestaña Beneficiarios
        with tab_beneficiarios:
            st.markdown('<div class="section-title">Inscriptos y Beneficiarios de todos lo programas de la gestión</div>', unsafe_allow_html=True)

            df_inscriptos_filtrado = render_tab_filters(df_inscriptos, key_prefix="benef_tab")
            
            # Conteo de ID_FICHA por PROGRAMA y ESTADO_FICHA
            pivot_table = df_inscriptos_filtrado.pivot_table(
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
            if df_inscriptos_filtrado['N_DEPARTAMENTO'].nunique() == 1:
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
        
            st.markdown('<div class="section-title">Empresas adheridas en todos los programas de la gestión</div>', unsafe_allow_html=True)

            show_companies(df_empresas)
            
        
       

def show_companies(df_empresas):
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

    # Filtrar por CUIT único y eliminar duplicados - usar TODAS las columnas disponibles
    columns_to_select = list(df_empresas.columns)

    if 'CUIT' in df_empresas.columns and 'ADHERIDO' in df_empresas.columns:
        # Guardamos la lista original de programas para cada CUIT antes de agrupar
        df_empresas['PROGRAMAS_LISTA'] = df_empresas['ADHERIDO']
        df_empresas['ADHERIDO'] = df_empresas.groupby('CUIT')['ADHERIDO'].transform(lambda x: ', '.join(sorted(set(x))))
    
    # Usar columns_to_select para crear df_display correctamente (sin duplicar columnas)
    df_display = df_empresas[columns_to_select].drop_duplicates(subset='CUIT')
    df_display = df_display.sort_values(by='CUPO', ascending=False).reset_index(drop=True)
    
    # Extraer todos los programas únicos para el filtro multiselect
    programas_unicos = []
    if 'ADHERIDO' in df_display.columns:
        # Extraer todos los programas únicos de la columna ADHERIDO
        todos_programas = df_display['ADHERIDO'].str.split(', ').explode().dropna().unique()
        programas_unicos = sorted(todos_programas)
    
    # Extraer valores únicos para los filtros
    departamentos_unicos = sorted(df_display['N_DEPARTAMENTO'].dropna().unique()) if 'N_DEPARTAMENTO' in df_display.columns else []
    
    # Usar la columna ZONA existente (ya está en el DataFrame)
    
    zonas_unicas = sorted(df_display['ZONA'].dropna().unique()) if 'ZONA' in df_display.columns else []
    
    # Añadir filtros en la pestaña de empresas - 3 filtros en una fila
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
    
    # Filtro 1: Programas
    with col_filtro1:
        if programas_unicos:
            st.markdown('<div class="filter-label">Programa:</div>', unsafe_allow_html=True)
            selected_programas = st.multiselect("Seleccionar programas", options=programas_unicos, default=[], label_visibility="collapsed", key="empresas_programa_filter")
        else:
            selected_programas = []
    
    # Filtro 2: Departamentos
    with col_filtro2:
        if departamentos_unicos:
            st.markdown('<div class="filter-label">Departamento:</div>', unsafe_allow_html=True)
            selected_departamentos = st.multiselect("Seleccionar departamentos", options=departamentos_unicos, default=[], label_visibility="collapsed", key="empresas_depto_filter")
        else:
            selected_departamentos = []
    
    # Filtro 3: Zonas
    with col_filtro3:
        if zonas_unicas:
            st.markdown('<div class="filter-label">Zona:</div>', unsafe_allow_html=True)
            selected_zonas = st.multiselect("Seleccionar zonas", options=zonas_unicas, default=[], label_visibility="collapsed", key="empresas_zona_filter")
        else:
            selected_zonas = []
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Aplicar filtros al dataframe
    df_filtered = df_display.copy()
    
    # Filtrar por programas seleccionados
    if selected_programas:
        # Crear una máscara para filtrar empresas que tengan al menos uno de los programas seleccionados
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
    
    # Filtrar por departamentos seleccionados
    if selected_departamentos and 'N_DEPARTAMENTO' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['N_DEPARTAMENTO'].isin(selected_departamentos)]
    
    # Filtrar por zonas seleccionadas
    if selected_zonas and 'ZONA' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['ZONA'].isin(selected_zonas)]
    
    # Mostrar mensaje con el número de registros después de aplicar los filtros
    filtros_activos = []
    if selected_programas:
        filtros_activos.append(f"Programas: {len(selected_programas)}")
    if selected_departamentos:
        filtros_activos.append(f"Departamentos: {len(selected_departamentos)}")
    if selected_zonas:
        filtros_activos.append(f"Zonas: {len(selected_zonas)}")
    
    filtros_texto = " | ".join(filtros_activos) if filtros_activos else "Sin filtros"
    st.markdown(f'<div class="filter-info">Mostrando {len(df_filtered)} de {len(df_display)} empresas ({filtros_texto})</div>', unsafe_allow_html=True)

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
        if selected_programas:
            df_empresas_original = df_empresas_original[df_empresas_original['PROGRAMAS_LISTA'].isin(selected_programas)]
            
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
        # Usar las columnas especificadas por el usuarioDefinir las columnas a mostrar (usando los nombres reales disponibles en el DataFrame)
        columns_to_select = [col for col in ['N_LOCALIDAD', 'N_DEPARTAMENTO', 'CUIT', 'N_EMPRESA', 
                                           'NOMBRE_TIPO_EMPRESA', 'ADHERIDO', 'CANTIDAD_EMPLEADOS', 
                                           'VACANTES', 'CUPO', 'IMP_GANANCIAS', 'IMP_IVA', 'MONOTRIBUTO',
                                           'INTEGRANTE_SOC', 'EMPLEADOR', 'ACTIVIDAD_MONOTRIBUTO','BENEF', 
                                           'TELEFONO', 'CELULAR', 'MAIL', 'SITIO_WEB', 'TEL_CONTACTO', 
                                           'EMAIL_CONTACTO', 'NOMBRE_FANTASIA', 'ZONA'] 
                           if col in df_filtered.columns]
        
        # Filtrar columnas existentes en el DataFrame
        existing_columns = [col for col in columns_to_select if col in df_display.columns]
        
        st.dataframe(df_filtered[existing_columns], hide_index=True, use_container_width=True)

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
            if 'N_CATEGORIA_EMPLEO' in df_perfil_demanda.columns and 'CUIT' in df_perfil_demanda.columns:
                df_actividad = df_perfil_demanda.groupby('N_CATEGORIA_EMPLEO')['CUIT'].nunique().reset_index()
                df_actividad.columns = ['Actividad Principal', 'Cantidad de Empresas']
                df_actividad = df_actividad.sort_values(by='Cantidad de Empresas', ascending=False).head(10)
                
                fig_actividad = px.pie(
                    df_actividad, 
                    names='Actividad Principal', 
                    values='Cantidad de Empresas',
                    title='Distribución por Actividad Principal (Top 10)',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_actividad.update_layout(showlegend=True, title_x=0.5)
                st.plotly_chart(fig_actividad, use_container_width=True)
            else:
                st.info("No hay datos de tipo de empresa para graficar.")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # --- Visualización 2: Gráfico de Barras por Categoría (Top 10) (en col2) ---
        with col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<h3 style="font-size: 18px; margin-bottom: 15px;">Top 10 - Categorías de Empleo por Nº de Empresas</h3>', unsafe_allow_html=True)

            if 'N_PUESTO_EMPLEO' in df_perfil_demanda.columns and 'CUIT' in df_perfil_demanda.columns:
                categoria_counts = df_perfil_demanda.groupby('N_PUESTO_EMPLEO')['CUIT'].nunique().reset_index()
                categoria_counts.columns = ['Puesto', 'Nº de Empresas']
                
                top_10_categorias = categoria_counts.sort_values(by='Nº de Empresas', ascending=False).head(10)
                
                fig_bar = px.bar(
                    top_10_categorias, 
                    x='Nº de Empresas', 
                    y='Puesto',
                    orientation='h',
                    title='Top 10 Puestos por Empresas (CUITs únicos)',
                    labels={'Nº de Empresas': 'Nº de Empresas (CUITs únicos)', 'Puesto': 'Puesto de Empleo'},
                    color_discrete_sequence=px.colors.qualitative.Vivid
                )
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, title_x=0.5)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No hay datos de puestos de empleo para graficar.")

            st.markdown('</div>', unsafe_allow_html=True)

def show_inscriptions(df_inscriptos, file_date):
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
        
        # Crear un diccionario con los resultados para pasarlo a la función de KPIs
        resultados = {
            "total_match": total_match,
            "total_benef": total_benef,
            "total_validos": total_validos,
            "total_inscriptos": total_inscriptos,
            "total_pendientes": total_pendientes,
            "total_rechazados": total_rechazados,
            "total_empresa_no_apta": total_empresa_no_apta
        }
        
        # Usar la función para crear los KPIs
        kpi_data = create_empleo_kpis(resultados, programa_seleccionado_nombre)
        display_kpi_row(kpi_data)
        
        # Resto del código de visualización con mejoras visuales
        # Aquí puedes añadir más visualizaciones según sea necesario
    
    except Exception as e:
        st.markdown(f"""
            <div class="info-box status-warning">
                <strong>Información:</strong> Se mostrarán los datos disponibles: {str(e)}
            </div>
        """, unsafe_allow_html=True)
