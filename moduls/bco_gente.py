import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row, create_bco_gente_kpis
from utils.styles import COLORES_IDENTIDAD

# Definición unificada de categorías de estado a nivel de módulo
ESTADO_CATEGORIAS = {
    "En Evaluación": ["CREADO","EVALUACIÓN TÉCNICA","COMENZADO"],
    "Rechazados - Bajas": ["RECHAZADO","DESISTIDO","IMPAGO DESISTIDO","BAJA ADMINISTRATIVA"],
    "A Pagar - Convocatoria": ["A PAGAR","A PAGAR CON LOTE","A PAGAR CON BANCO","A PAGAR ENVIADO A SUAF","A PAGAR CON SUAF","MUTUO FIRMADO"],
    "Pagados": ["PAGADO","PRE-FINALIZADO","FINALIZADO","CON PLAN DE CUOTAS","CON PLAN DE CUOTAS CON IMPAGOS","MOROSO ENTRE 3 Y 4 MESES","MOROSO >= 5 MESES"],
    "En proceso de pago": ["PAGO EMITIDO","IMPAGO"]
}

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
        has_geojson_data = geojson_data is not None and not geojson_data.empty
        has_localidad_municipio_data = df_localidad_municipio is not None and not df_localidad_municipio.empty
        
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
                df_global.loc[capital_mask, 'ID_LOCALIDAD'] = 2
        
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
                    
                    st.success("Se ha realizado el cruce de datos con información de deuda y recupero.")
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
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
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
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        return df_filtrado, selected_dpto, selected_loc, selected_linea

def show_bco_gente_dashboard(data, dates):
    """
    Muestra el dashboard de Banco de la Gente.
    
    Args:
        data: Diccionario de dataframes cargados desde GitLab
        dates: Diccionario de fechas de actualización de los archivos
    """
    # Crear diccionario para tooltips de categorías - Definido a nivel global para usar en todas las tablas
    tooltips_categorias = {
        "En Evaluación": ", ".join(ESTADO_CATEGORIAS["En Evaluación"]),
        "Rechazados - Bajas": ", ".join(ESTADO_CATEGORIAS["Rechazados - Bajas"]),
        "A Pagar - Convocatoria": ", ".join(ESTADO_CATEGORIAS["A Pagar - Convocatoria"]),
        "Pagados": ", ".join(ESTADO_CATEGORIAS["Pagados"]),
        "En proceso de pago": ", ".join(ESTADO_CATEGORIAS["En proceso de pago"])
    }
    
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
        if has_recupero_data and df_recupero is not None and not df_recupero.empty:
            with st.spinner("Cargando visualizaciones de recupero..."):
                mostrar_recupero(df_recupero, df_localidad_municipio, geojson_data)
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
    kpi_data = create_bco_gente_kpis(resultados)
    display_kpi_row(kpi_data)

    # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)
     # Nueva tabla: Conteo de Préstamos por Línea y Estado
    st.subheader("Conteo de Préstamos por Línea y Estado")
    
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
    
    # Tabla de estados de préstamos agrupados
    st.subheader("Estados de Préstamos por Categoría")
    
    try:
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
            
            with col1:
                # Multiselect para seleccionar categorías
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
            
            # Agregar información de deuda y recupero si está disponible
            if all(col in df_filtrado_global.columns for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'DEUDA_A_RECUPERAR', 'RECUPERADO']):
                try:
                    # Crear un DataFrame de agregación por departamento y localidad
                    deuda_por_localidad = df_filtrado_global.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD']).agg({
                        'DEUDA_VENCIDA': 'sum',
                        'DEUDA_NO_VENCIDA': 'sum',
                        'DEUDA_A_RECUPERAR': 'sum',
                        'RECUPERADO': 'sum',
                        'MONTO_OTORGADO': 'sum'
                    }).reset_index()
                    
                    # Hacer merge con el pivot_df_filtered
                    pivot_df_filtered = pd.merge(
                        pivot_df_filtered,
                        deuda_por_localidad,
                        on=['N_DEPARTAMENTO', 'N_LOCALIDAD'],
                        how='left'
                    )
                    
                    # Rellenar valores NaN con 0
                    for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'DEUDA_A_RECUPERAR', 'RECUPERADO', 'MONTO_OTORGADO']:
                        pivot_df_filtered[col] = pivot_df_filtered[col].fillna(0)
                    
                    # Calcular totales para las nuevas columnas
                    totales_deuda = {
                        'DEUDA_VENCIDA': pivot_df_filtered['DEUDA_VENCIDA'].sum(),
                        'DEUDA_NO_VENCIDA': pivot_df_filtered['DEUDA_NO_VENCIDA'].sum(),
                        'DEUDA_A_RECUPERAR': pivot_df_filtered['DEUDA_A_RECUPERAR'].sum(),
                        'RECUPERADO': pivot_df_filtered['RECUPERADO'].sum(),
                        'MONTO_OTORGADO': pivot_df_filtered['MONTO_OTORGADO'].sum()
                    }
                    
                    # Actualizar la fila de totales con los nuevos valores
                    totales = pivot_df_filtered[selected_categorias + ['Total']].sum()
                    totales_row = pd.DataFrame([['Total', 'Total'] + totales.values.tolist() + list(totales_deuda.values())], 
                                              columns=['N_DEPARTAMENTO', 'N_LOCALIDAD'] + selected_categorias + ['Total'] + list(totales_deuda.keys()))
                    
                    # Eliminar la última fila (totales anteriores) y agregar la nueva fila de totales
                    pivot_df_filtered = pivot_df_filtered[:-1]
                    pivot_df_filtered = pd.concat([pivot_df_filtered, totales_row], ignore_index=True)
                    
                except Exception as e:
                    st.warning(f"Error al agregar información de deuda: {str(e)}")
            else:
                # Agregar fila de totales sin información de deuda
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
            if all(col in pivot_df_filtered.columns for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'DEUDA_A_RECUPERAR', 'RECUPERADO', 'MONTO_OTORGADO']):
                numeric_columns += ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'DEUDA_A_RECUPERAR', 'RECUPERADO', 'MONTO_OTORGADO']
                
                # Aplicar formato de moneda a las columnas de deuda y recupero
                styled_df = styled_df.format({
                    'DEUDA_VENCIDA': '${:,.2f}',
                    'DEUDA_NO_VENCIDA': '${:,.2f}',
                    'DEUDA_A_RECUPERAR': '${:,.2f}',
                    'RECUPERADO': '${:,.2f}',
                    'MONTO_OTORGADO': '${:,.2f}'
                })
            
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
            
            # Añadir configuración para las columnas de deuda si existen
            if all(col in pivot_df_filtered.columns for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'DEUDA_A_RECUPERAR', 'RECUPERADO', 'MONTO_OTORGADO']):
                column_config.update({
                    "DEUDA_VENCIDA": st.column_config.NumberColumn(
                        "DEUDA VENCIDA",
                        help="Monto de deuda vencida",
                        format="$%.2f"
                    ),
                    "DEUDA_NO_VENCIDA": st.column_config.NumberColumn(
                        "DEUDA NO VENCIDA",
                        help="Monto de deuda no vencida",
                        format="$%.2f"
                    ),
                    "DEUDA_A_RECUPERAR": st.column_config.NumberColumn(
                        "DEUDA A RECUPERAR",
                        help="Suma de deuda vencida y no vencida",
                        format="$%.2f"
                    ),
                    "RECUPERADO": st.column_config.NumberColumn(
                        "RECUPERADO",
                        help="Monto otorgado menos deuda a recuperar",
                        format="$%.2f"
                    ),
                    "MONTO_OTORGADO": st.column_config.NumberColumn(
                        "MONTO OTORGADO",
                        help="Monto total otorgado",
                        format="$%.2f"
                    )
                })
            
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

    # Serie Histórica
    st.subheader("Serie Histórica: Evolución de formularios presentados a lo Largo del Tiempo")
    
    try:
        # Verificar si existe df_recupero y las columnas necesarias
        if df_recupero is None or df_recupero.empty:
            st.info("No hay datos disponibles para la serie histórica.")
        elif 'NRO_SOLICITUD' not in df_recupero.columns or 'FEC_FORM' not in df_recupero.columns:
            st.info("No hay datos disponibles para la serie histórica.")
        else:
            # Asegurarse de que la columna FEC_FORM sea de tipo datetime
            try:
                # Crear una copia para no modificar el dataframe original
                df_fechas = df_recupero.copy()
                
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
                    
                    # Agrupar por mes y año, y contar NRO_SOLICITUD
                    # Crear una columna de año-mes para agrupar
                    df_fechas['AÑO_MES'] = df_fechas['FEC_FORM'].dt.strftime('%Y-%m')
                    
                    # Agrupar por AÑO_MES y contar
                    serie_historica = df_fechas.groupby('AÑO_MES').size().reset_index(name='Cantidad')
                    
                    # Convertir AÑO_MES a datetime para graficar
                    serie_historica['FECHA'] = pd.to_datetime(serie_historica['AÑO_MES'] + '-01')
                    
                    # Ordenar por fecha
                    serie_historica = serie_historica.sort_values('FECHA')
                    
                    # Crear el gráfico
                    fig_historia = px.line(
                        serie_historica, 
                        x='FECHA', 
                        y='Cantidad', 
                        title='Evolución de Formularios por Mes',
                        labels={'Cantidad': 'Cantidad de Formularios', 'FECHA': 'Mes'},
                        markers=True
                    )
                    
                    # Personalizar el diseño del gráfico
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
                    
                    # Mostrar el gráfico
                    st.plotly_chart(fig_historia)
                    
                    # Mostrar tabla de datos
                    with st.expander("Ver datos de la serie histórica"):
                        # Crear una tabla para mostrar los datos
                        tabla_data = serie_historica.copy()
                        tabla_data['Mes-Año'] = tabla_data['FECHA'].dt.strftime('%b %Y')
                        st.dataframe(
                            tabla_data[['Mes-Año', 'Cantidad']].sort_values('FECHA', ascending=False),
                            hide_index=True
                        )
            except Exception:
                # Silenciar errores
                pass
    except Exception:
        # Silenciar errores
        pass
    
    # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)

    # Gráfico de Torta
    st.subheader("Gráfico de Torta: Distribución de demanda de Crédito por Línea de Préstamo")
    grafico_torta = df_filtrado_global.groupby('N_LINEA_PRESTAMO').size().reset_index(name='Cantidad')

    # Colores de la identidad visual
    colores_identidad = COLORES_IDENTIDAD

    fig_torta = px.pie(
        grafico_torta, 
        names='N_LINEA_PRESTAMO', 
        values='Cantidad', 
        title='Distribución de Formularios por Línea de Préstamo',
        color_discrete_sequence=colores_identidad
    )
    
    # Personalizar el diseño del gráfico
    fig_torta.update_traces(
        textposition='inside',
        textinfo='percent+label',
        marker=dict(line=dict(color='#FFFFFF', width=1))
    )
    
    fig_torta.update_layout(
        legend_title="Líneas de Préstamo",
        font=dict(size=12),
        uniformtext_minsize=10,
        uniformtext_mode='hide'
    )
    
    st.plotly_chart(fig_torta)

def mostrar_recupero(df_recupero, df_localidad_municipio, geojson_data):
    """
    Muestra los datos de recupero del Banco de la Gente.
    
    Args:
        df_recupero: DataFrame con datos de recupero
        df_localidad_municipio: DataFrame con datos de departamentos
        geojson_data: Datos GeoJSON para visualización geográfica
    """
    try:
        st.subheader("Análisis de Recupero de Créditos")
        
        # Mostrar información básica
        st.info("Información básica de recupero")
        
        # Crear KPIs simples
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total de Préstamos", f"{len(df_recupero):,}")
        
        with col2:
            if 'MONTO_OTORGADO' in df_recupero.columns:
                monto = df_recupero['MONTO_OTORGADO'].sum()
                st.metric("Monto Total", f"${monto:,.2f}")
            else:
                st.metric("Monto Total", "No disponible")
        
        with col3:
            if 'DEUDA' in df_recupero.columns:
                deuda = df_recupero['DEUDA'].sum()
                st.metric("Deuda Total", f"${deuda:,.2f}")
            else:
                st.metric("Deuda Total", "No disponible")
        
        # Mostrar una tabla simple con los primeros registros
        st.subheader("Muestra de Datos")
        st.dataframe(df_recupero.head(5))
        
    except Exception as e:
        st.error(f"Error en recupero: {str(e)}")
