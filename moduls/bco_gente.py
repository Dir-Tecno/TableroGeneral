import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row, show_last_update
from utils.styles import COLORES_IDENTIDAD, COLOR_PRIMARY, COLOR_SECONDARY, COLOR_ACCENT_1, COLOR_ACCENT_2, COLOR_ACCENT_3, COLOR_ACCENT_4, COLOR_ACCENT_5, COLOR_TEXT_DARK
from utils.kpi_tooltips import ESTADO_CATEGORIAS, TOOLTIPS_DESCRIPTIVOS
from utils.session_helper import safe_session_get, safe_session_set, safe_session_check
from utils.duckdb_utils import DuckDBProcessor

# Inicializar variables de sesión necesarias
if "debug_mode" not in st.session_state:
    st.session_state["debug_mode"] = False
if "selected_categorias" not in st.session_state:
    st.session_state["selected_categorias"] = []
if "selected_lineas_credito" not in st.session_state:
    st.session_state["selected_lineas_credito"] = []

# Crear diccionario para tooltips de categorías (técnico, lista de estados)
tooltips_categorias = {k: ", ".join(v) for k, v in ESTADO_CATEGORIAS.items()}

def create_bco_gente_kpis(resultados, tooltips):
    """
    Crea los KPIs específicos para el módulo Banco de la Gente.
    Cada KPI incluye una clave 'categoria' con el valor exacto de la categoría para facilitar el mapeo y procesamiento posterior.
    
    Args:
        resultados (dict): Diccionario con los resultados de conteo por categoría
        tooltips (dict): Diccionario con los tooltips para cada KPI
    Returns:
        list: Lista de diccionarios con datos de KPI para Banco de la Gente
    """
    kpis = [
        {
            "title": "FORMULARIOS EN EVALUACIÓN",
            "categoria": "En Evaluación",
            "value_form": f"{resultados.get('En Evaluación', 0):,}".replace(',', '.'),
            "value_pers": "0",  # Este valor se actualizará luego con el conteo real de personas únicas
            "color_class": "kpi-primary",
            "tooltip": tooltips.get("En Evaluación")
        },
        {
            "title": "FORMULARIOS A PAGAR / CONVOCATORIA",
            "categoria": "A Pagar - Convocatoria",
            "value_form": f"{resultados.get('A Pagar - Convocatoria', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-3",
            "tooltip": tooltips.get("A Pagar - Convocatoria")
        },
        {
            "title": "FORMULARIOS PAGADOS",
            "categoria": "Pagados",
            "value_form": f"{resultados.get('Pagados', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-2",
            "tooltip": tooltips.get("Pagados")
        },
        {
            "title": "FORMULARIOS EN PROCESO DE PAGO",
            "categoria": "En proceso de pago",
            "value_form": f"{resultados.get('En proceso de pago', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-1",
            "tooltip": tooltips.get("En proceso de pago")
        },
        {
            "title": "FORMULARIOS PAGADOS - FINALIZADOS",
            "categoria": "Pagados-Finalizados",
            "value_form": f"{resultados.get('Pagados-Finalizados', 0):,}".replace(',', '.'),
            "color_class": "kpi-success",
            "tooltip": tooltips.get("Pagados-Finalizados")
        },
        {
            "title": "PAGOS GESTIONADOS",
            "categoria": "PAGOS GESTIONADOS",
            "value_form": f"{resultados.get('PAGOS GESTIONADOS', 0):,}".replace(',', '.'),
            "color_class": "kpi-accent-4",
            "tooltip": tooltips.get("PAGOS GESTIONADOS")
        }
    ]
    return kpis

# --- KPIs de Datos Fiscales ---
def mostrar_kpis_fiscales(df_global):
    """
    Para cada campo fiscal, muestra una tabla con el valor y la cantidad de CUIL únicos asociados,
    filtrando por líneas y categorías igual que mostrar_resumen_creditos.
    """
    if df_global is None or df_global.empty:
        st.warning("No hay datos disponibles para los KPIs fiscales.")
        return

    lineas = ["INICIAR EMPRENDIMIENTO", "POTENCIAR EMPRENDIMIENTO", "L4."]
    categorias = ["Pagados", "Pagados-Finalizados"]
    df_categoria_estados = df_global[
        (df_global["N_LINEA_PRESTAMO"].isin(lineas)) &
        (df_global["CATEGORIA"].isin(categorias))
    ].copy()

    if df_categoria_estados.empty:
        st.info("No se encontraron registros para las líneas y categorías seleccionadas.")
        return

    campos = [
        "IMP_GANANCIAS",
        "IMP_IVA",
        "MONOTRIBUTO",
        "INTEGRANTE_SOC",
        "EMPLEADO",
        "ACTIVIDAD_MONOTRIBUTO"
    ]

    # Mostrar las tablas en 2 filas de 3 columnas
    cols_row1 = st.columns(3)
    cols_row2 = st.columns(3)
    for idx, campo in enumerate(campos):
        col = cols_row1[idx] if idx < 3 else cols_row2[idx-3]
        with col:
            st.markdown(f"<b>{campo.replace('_',' ').title()}</b>", unsafe_allow_html=True)
            if campo not in df_categoria_estados.columns:
                st.info(f"No existe la columna {campo} en los datos.")
                continue
            df_campo = df_categoria_estados[df_categoria_estados[campo].notnull()]
            group = df_campo.groupby(campo)["CUIL"].nunique().reset_index()
            group = group.rename(columns={"CUIL": "CUILs únicos", campo: campo})
            group = group.sort_values("CUILs únicos", ascending=False)
            st.dataframe(group, use_container_width=True, hide_index=True)

# --- RESUMEN DE CREDITOS: Tabla de conteo de campos fiscales para líneas seleccionadas ---
def mostrar_resumen_creditos(df_global):
    """
    Muestra dos gráficos de barras apiladas, uno para cada línea de préstamo ('INICIAR EMPRENDIMIENTO' y 'POTENCIAR EMPRENDIMIENTO'),
    filtrando por CATEGORIA en ['Pagados', 'Pagados-Finalizados'].
    En cada barra: el total de CUIL únicos y el total de CUIL únicos con MONOTRIBUTO not null (apilado).
    """
    if df_global is None or df_global.empty:
        st.warning("No hay datos disponibles en el recupero para el resumen de créditos.")
        return

    # Filtrar por líneas y categorías
    lineas = ["INICIAR EMPRENDIMIENTO", "POTENCIAR EMPRENDIMIENTO"]
    categorias = ["Pagados", "Pagados-Finalizados"]
    df_filtrado = df_global[
        (df_global["N_LINEA_PRESTAMO"].isin(lineas)) &
        (df_global["CATEGORIA"].isin(categorias))
    ].copy()

    if df_filtrado.empty:
        st.info("No se encontraron registros para las líneas y categorías seleccionadas.")
        return

    # Calcular resumen por línea
    resumen = []
    for linea in lineas:
        df_linea = df_filtrado[df_filtrado["N_LINEA_PRESTAMO"] == linea]
        total_cuils = df_linea["CUIL"].nunique()
        cuils_monotributo = df_linea[df_linea["MONOTRIBUTO"].notnull()]["CUIL"].nunique()
        resumen.append({
            "Línea de Crédito": linea,
            "Personas (Total)": total_cuils,
            "Personas con ARCA": cuils_monotributo
        })
    resumen_df = pd.DataFrame(resumen)

    st.markdown("#### Resumen de personas por línea de crédito y con condición ante ARCA")
    import plotly.graph_objects as go
    # Crear los dos gráficos
    figs = []
    for idx, row in resumen_df.iterrows():
        linea = row["Línea de Crédito"]
        total = row["Personas (Total)"]
        con_arca = row["Personas con ARCA"]
        sin_arca = total - con_arca
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Personas con ARCA",
            x=[linea],
            y=[con_arca],
            marker_color="#66c2a5",
            text=[con_arca],
            textposition="inside"
        ))
        fig.add_trace(go.Bar(
            name="Personas sin ARCA",
            x=[linea],
            y=[sin_arca],
            marker_color="#fc8d62",
            text=[sin_arca],
            textposition="inside"
        ))
        fig.update_layout(
            barmode='stack',
            showlegend=True,
            xaxis_title=None,
            yaxis_title="Cantidad de personas",
            title=f"Distribución de personas en {linea}",
            height=350,
            margin=dict(l=10, r=10, t=40, b=10)
        )
        figs.append(fig)

    # Presentar en una sola fila de 3 columnas
    cols = st.columns(3)
    with cols[0]:
        st.markdown(f"**{resumen_df.iloc[0]['Línea de Crédito']}**")
        st.plotly_chart(figs[0], use_container_width=True)
    with cols[1]:
        st.markdown(f"**{resumen_df.iloc[1]['Línea de Crédito']}**")
        st.plotly_chart(figs[1], use_container_width=True)
    with cols[2]:
        st.markdown("**Tabla resumen**")
        st.dataframe(resumen_df, use_container_width=True, hide_index=True)
        import io
        csv_buffer = io.StringIO()
        resumen_df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)
        st.download_button(
            label="Descargar CSV resumen",
            data=csv_buffer.getvalue(),
            file_name="resumen_personas_por_linea.csv",
            mime="text/csv"
        )

# mostrar_resumen_creditos(df_global)

def load_and_preprocess_data(data, is_development=False):
    """
    Carga y preprocesa los datos para el dashboard.
    
    Args:
        data (dict): Diccionario con los datos cargados.
        is_development (bool): True si se está en modo desarrollo.
        
    Returns:
        tuple: (df_global, geojson_data, df_localidad_municipio, df_global_pagados)
    """
    # Función auxiliar para verificar y corregir el DataFrame
    def ensure_dataframe(df):
        """Asegura que el objeto sea un DataFrame y no una Serie"""
        if df is None:
            return pd.DataFrame()
        if isinstance(df, pd.Series):
            return pd.DataFrame([df])
        if not isinstance(df, pd.DataFrame):
            st.warning(f"Tipo de dato inesperado: {type(df)}. Convirtiendo a DataFrame vacío.")
            return pd.DataFrame()
        return df.copy()  # Devolver una copia para evitar modificaciones no deseadas
    with st.spinner("Cargando y procesando datos..."):
        # Extraer los dataframes necesarios y asegurar que sean DataFrames válidos
        df_global = ensure_dataframe(data.get('VT_NOMINA_REP_RECUPERO_X_ANIO.parquet'))
        df_cumplimiento = ensure_dataframe(data.get('VT_CUMPLIMIENTO_FORMULARIOS.parquet'))
        geojson_data = data.get('capa_departamentos_2010.geojson')  # Este es un GeoJSON, no un DataFrame
        df_localidad_municipio = ensure_dataframe(data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt'))
        
        
        has_global_data = not df_global.empty
        has_cumplimiento_data = not df_cumplimiento.empty
        
        # Verificar la estructura del DataFrame para diagnóstico
        if has_global_data and is_development:
            # Importar session_helper para gestionar el modo de depuración
            from utils.session_helper import safe_session_set
            
            # Activar modo debug para mostrar información detallada
            safe_session_set('debug_mode', True)
            
            st.write("Estructura de df_global al inicio:")
            st.write(f"Tipo: {type(df_global)}")
            st.write(f"Columnas: {df_global.columns.tolist()}")
            st.write(f"Tipos de datos: {df_global.dtypes}")
 

        # Agregar columna de CATEGORIA a df_global si está disponible y filtrar solo por 'Pagados' y 'Pagados-Finalizados'
        
        if has_global_data and 'N_ESTADO_PRESTAMO' in df_global.columns:
            # Crear una copia para evitar modificaciones que puedan alterar la estructura
            df_global = df_global.copy()
            
            # Inicializar la columna CATEGORIA con un valor predeterminado
            df_global['CATEGORIA'] = 'Otros'
            
            # Método alternativo para asignar categorías sin usar .loc
            # Crear una función para mapear estados a categorías
            def asignar_categoria(estado):
                for categoria, estados in ESTADO_CATEGORIAS.items():
                    if estado in estados:
                        return categoria
                return 'Otros'
            
            # Aplicar la función a cada fila
            df_global['CATEGORIA'] = df_global['N_ESTADO_PRESTAMO'].apply(asignar_categoria)
            
            # Verificar que df_global sigue siendo un DataFrame después de la asignación
            df_global = ensure_dataframe(df_global)
            

                
            # Reemplazar "L4." por "INICIAR EMPRENDIMIENTO" usando un método alternativo
            df_global['N_LINEA_PRESTAMO'] = df_global['N_LINEA_PRESTAMO'].apply(
                lambda x: "INICIAR EMPRENDIMIENTO" if x == "L4." else x
            )

        # --- Normalizar N_DEPARTAMENTO: dejar solo los válidos, el resto 'OTROS' ---
        departamentos_validos = [
            "CAPITAL",
            "CALAMUCHITA",
            "COLON",
            "CRUZ DEL EJE",
            "GENERAL ROCA",
            "GENERAL SAN MARTIN",
            "ISCHILIN",
            "JUAREZ CELMAN",
            "MARCOS JUAREZ",
            "MINAS",
            "POCHO",
            "PRESIDENTE ROQUE SAENZ PEÑA",
            "PUNILLA",
            "RIO CUARTO",
            "RIO PRIMERO",
            "RIO SECO",
            "RIO SEGUNDO",
            "SAN ALBERTO",
            "SAN JAVIER",
            "SAN JUSTO",
            "SANTA MARIA",
            "SOBREMONTE",
            "TERCERO ARRIBA",
            "TOTORAL",
            "TULUMBA",
            "UNION"
        ]
        if has_global_data and 'N_DEPARTAMENTO' in df_global.columns:
            df_global['N_DEPARTAMENTO'] = df_global['N_DEPARTAMENTO'].apply(lambda x: x if x in departamentos_validos else 'OTROS')

        # Corregir localidades del departamento CAPITAL
        if has_global_data and 'N_DEPARTAMENTO' in df_global.columns and 'N_LOCALIDAD' in df_global.columns:
            # Crear una máscara para identificar registros del departamento CAPITAL
            capital_mask = df_global['N_DEPARTAMENTO'] == 'CAPITAL'
            
            # Aplicar la corrección de localidad
            df_global.loc[capital_mask, 'N_LOCALIDAD'] = 'CORDOBA'
            
            # Si existe la columna ID_LOCALIDAD, corregirla también
            if 'ID_LOCALIDAD' in df_global.columns:
                df_global.loc[capital_mask, 'ID_LOCALIDAD'] = 1
            
            # Añadir columna de ZONA FAVORECIDA
            zonas_favorecidas = [
                'PRESIDENTE ROQUE SAENZ PEÑA', 'GENERAL ROCA', 'RIO SECO', 'TULUMBA', 
                'POCHO', 'SAN JAVIER', 'SAN ALBERTO', 'MINAS', 'CRUZ DEL EJE', 
                'TOTORAL', 'SOBREMONTE', 'ISCHILIN'
            ]
            
            # Crear la columna ZONA
            df_global['ZONA'] = df_global['N_DEPARTAMENTO'].apply(
                lambda x: 'ZONA NOC Y SUR' if x in zonas_favorecidas else 'ZONA REGULAR'
            )
        
            # Renombrar DEUDA como DEUDA_VENCIDA
            df_global  = df_global.rename(columns={'DEUDA': 'DEUDA_VENCIDA'})
                    
            # Convertir columnas numéricas a tipo float
            for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                df_global[col] = pd.to_numeric(df_global[col], errors='coerce')
                   
            
                
            # Rellenar valores NaN con 0 en df_global
            for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                df_global[col] = pd.to_numeric(df_global[col], errors='coerce').fillna(0)
                    
            # Añadir campos calculados a df_global
            df_global['DEUDA_A_RECUPERAR'] = df_global['DEUDA_VENCIDA'] + df_global['DEUDA_NO_VENCIDA']
            df_global['RECUPERADO'] = df_global['MONTO_OTORGADO'] - df_global['DEUDA_A_RECUPERAR']
            
            
                    
            # --- INICIO: Nuevo Merge con df_localidad_municipio ---
            if df_localidad_municipio is not None and not df_localidad_municipio.empty:
                # Definir columnas a traer desde df_localidad_municipio (incluyendo la clave)
                cols_to_merge = [
                            'ID_LOCALIDAD', # Clave del merge (asumimos mismo nombre en ambos DFs)
                            'ID_GOBIERNO_LOCAL',
                            'TIPO', 
                            'Gestion 2023-2027', 
                            'FUERZAS', 
                            'ESTADO', 
                            'LEGISLADOR DEPARTAMENTAL', 
                            'LATITUD', 
                            'LONGITUD'
                ]
                        
                try:
                    # Seleccionar solo las columnas necesarias (incluida la clave)
                    df_localidad_subset = df_localidad_municipio[cols_to_merge].copy()
                    
                    # Realizar el segundo merge (left join) usando la misma clave
                    df_global = pd.merge(
                        df_global,
                        df_localidad_subset,
                        on='ID_LOCALIDAD', # Usar 'on' ya que la clave tiene el mismo nombre
                        how='left'
                    )
                                
                    # --- Limpieza de LATITUD y LONGITUD SOLO después del merge con df_localidad_municipio ---
                    def limpiar_lat_lon(valor):
                        if isinstance(valor, str):
                            # Si tiene más de un punto, eliminar todos menos el último
                            if valor.count('.') > 1:
                                partes = valor.split('.')
                                valor = ''.join(partes[:-1]) + '.' + partes[-1]
                                valor = valor.replace(',', '.')  # Por si viene con coma decimal
                            return valor

                    for col in ['LATITUD', 'LONGITUD']:
                        if col in df_global.columns:
                            df_global[col] = df_global[col].astype(str).apply(limpiar_lat_lon)
                            df_global[col] = pd.to_numeric(df_global[col], errors='coerce')

                except Exception as e_merge2:
                    st.warning(f"Error durante el segundo merge con df_localidad_municipio: {str(e_merge2)}")
                # No es necesario un 'else' aquí, las advertencias ya se mostraron si can_merge es False
            else:
                st.info("df_localidad_municipio no está disponible o está vacío, se omite el segundo cruce.")
            # --- FIN: Nuevo Merge con df_localidad_municipio ---

        
        # Filtrar líneas de préstamo que no deben ser consideradas
        if has_global_data and 'N_LINEA_PRESTAMO' in df_global.columns:
            # Lista de líneas de préstamo a agrupar como 'Otras Lineas'
            lineas_a_agrupar = ["L1", "L3", "L4", "L6"]
            
            # Crear una máscara para identificar las filas con estas líneas
            mask_otras_lineas = df_global['N_LINEA_PRESTAMO'].isin(lineas_a_agrupar)
            
            # Renombrar el valor en la columna 'N_LINEA_PRESTAMO' para esas filas
            df_global.loc[mask_otras_lineas, 'N_LINEA_PRESTAMO'] = "Otras Lineas"
            
            # Ya no se eliminan filas, así que no es necesario re-evaluar has_global_data aquí
            # # Verificar si todavía hay datos después del filtrado
            # has_global_data = not df_global.empty

        
        # Verificar la estructura final para diagnóstico
        if has_global_data and is_development:
            st.write("Estructura final de df_global:")
            st.write(f"Tipo: {type(df_global)}")
            st.write(f"Columnas: {df_global.columns.tolist()}")
            st.write(f"Tipos de datos: {df_global.dtypes}")
            
            # Mostrar información detallada de DataFrames en modo desarrollo
            from utils.ui_components import show_dev_dataframe_info
            show_dev_dataframe_info(df_global, "df_global")
            
            # Restaurar estado anterior de debug_mode si no se quiere mantener
            if not is_development:
                from utils.session_helper import safe_session_set
                safe_session_set('debug_mode', False)
        
        # Convertir cualquier columna que sea Series a valores nativos
        if has_global_data and not df_global.empty:
            for col in df_global.columns:
                try:
                    if len(df_global) > 0 and isinstance(df_global[col].iloc[0], pd.Series):
                        # Si la columna contiene Series, convertirla a valores nativos
                        df_global[col] = df_global[col].apply(lambda x: x.values[0] if isinstance(x, pd.Series) else x)
                except Exception as e:
                    st.warning(f"Error al procesar columna {col}: {str(e)}")
                    # Intentar convertir la columna completa si es una Serie
                    if isinstance(df_global[col], pd.Series):
                        try:
                            df_global[col] = df_global[col].apply(lambda x: x if not isinstance(x, pd.Series) else x.iloc[0] if len(x) > 0 else None)
                        except:
                            pass
                    # Crear un DataFrame adicional que contenga solo las categorías 'Pagados' y 'Pagados-Finalizados'
            # para operaciones específicas que requieren solo estos datos
        categorias_validas = ['Pagados', 'Pagados-Finalizados']
        df_global_pagados = df_global[df_global['CATEGORIA'].isin(categorias_validas)].copy()

        # Display df_global_pagados info after it's assigned
        if is_development:
            show_dev_dataframe_info(df_global_pagados, "df_global_pagados")
        # Realizar el merge con df_cumplimiento directamente en df_global si está disponible
        if has_cumplimiento_data and 'NRO_FORMULARIO' in df_cumplimiento.columns:
            try:
                # Columnas a obtener del DataFrame de cumplimiento
                columnas_cumplimiento = [
                    'NRO_FORMULARIO',
                    'PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'
                ]

                # Verificar que todas las columnas existan
                missing_cols_cumplimiento = [col for col in columnas_cumplimiento if col not in df_cumplimiento.columns]

                if not missing_cols_cumplimiento:
                    # Seleccionar solo las columnas necesarias
                    df_cumplimiento_subset = df_cumplimiento[columnas_cumplimiento].copy()

                    # Convertir columna numérica a tipo float
                    df_cumplimiento_subset['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'] = pd.to_numeric(
                        df_cumplimiento_subset['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'], 
                        errors='coerce'
                    )

                    # Realizar el merge (left join) con df_global_pagados
                    df_global_pagados = pd.merge(
                        df_global_pagados,
                        df_cumplimiento_subset,
                        left_on='NRO_SOLICITUD',  # Clave en df_global_pagados
                        right_on='NRO_FORMULARIO',  # Clave en df_cumplimiento
                        how='left'
                    )
                    # Eliminar la columna duplicada NRO_FORMULARIO si existe
                    if 'NRO_FORMULARIO' in df_global_pagados.columns:
                        df_global_pagados = df_global_pagados.drop('NRO_FORMULARIO', axis=1)
                else:
                    st.warning(f"No se pudo realizar el merge con datos de cumplimiento. Faltan columnas: {', '.join(missing_cols_cumplimiento)}")
            except Exception as e_cumplimiento:
                st.warning(f"Error al realizar el merge con datos de cumplimiento: {str(e_cumplimiento)}")
        else:
            st.info("Los datos de cumplimiento no están disponibles o no contienen la columna NRO_FORMULARIO.")
        # Rellenar valores NaN con 0 en df_global_pagados
        for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
            if col in df_global_pagados.columns:
                df_global_pagados[col] = pd.to_numeric(df_global_pagados[col], errors='coerce').fillna(0)
                
        # Añadir campos calculados a df_global_pagados
        if all(col in df_global_pagados.columns for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA']):
            df_global_pagados['DEUDA_A_RECUPERAR'] = df_global_pagados['DEUDA_VENCIDA'] + df_global_pagados['DEUDA_NO_VENCIDA']
            
        if all(col in df_global_pagados.columns for col in ['MONTO_OTORGADO', 'DEUDA_A_RECUPERAR']):
            df_global_pagados['RECUPERADO'] = df_global_pagados['MONTO_OTORGADO'] - df_global_pagados['DEUDA_A_RECUPERAR']
        
        return df_global, geojson_data, df_localidad_municipio, df_global_pagados

@st.cache_data(ttl=3600)
def load_and_preprocess_data_duckdb(_data, is_development=False):
    """
    Versión optimizada con DuckDB para carga y preprocesamiento de datos del Banco de la Gente.
    Mejora significativa en rendimiento para operaciones de categorización, normalización y JOINs complejos.
    
    Args:
        _data (dict): Diccionario con los datos cargados. El guion bajo evita que Streamlit intente hashear este parámetro.
        is_development (bool): True si se está en modo desarrollo.
        
    Returns:
        tuple: (df_global, geojson_data, df_localidad_municipio, df_global_pagados)
    """
    try:
        with st.spinner("🚀 Procesando datos del Banco de la Gente con DuckDB..."):
            # Extraer los dataframes necesarios
            df_global_raw = _data.get('VT_NOMINA_REP_RECUPERO_X_ANIO.parquet')
            df_cumplimiento_raw = _data.get('VT_CUMPLIMIENTO_FORMULARIOS.parquet')
            geojson_data = _data.get('capa_departamentos_2010.geojson')
            df_localidad_municipio = _data.get('LOCALIDAD CIRCUITO ELECTORAL GEO Y ELECTORES - USAR.txt')
            
            # Verificar disponibilidad de datos principales
            if df_global_raw is None or df_global_raw.empty:
                st.error("No se pudieron cargar los datos principales del Banco de la Gente.")
                return None, None, None, None
            
            # Inicializar DuckDB
            processor = DuckDBProcessor()
            
            # Registrar tablas principales
            processor.register_dataframe("global_raw", df_global_raw)
            
            if df_cumplimiento_raw is not None and not df_cumplimiento_raw.empty:
                processor.register_dataframe("cumplimiento_raw", df_cumplimiento_raw)
                has_cumplimiento = True
            else:
                has_cumplimiento = False
            
            if df_localidad_municipio is not None and not df_localidad_municipio.empty:
                processor.register_dataframe("localidad_municipio", df_localidad_municipio)
                has_localidad = True
            else:
                has_localidad = False
            
            # === PASO 1: Procesar datos principales con categorización y normalización ===
            main_query = """
            SELECT *,
                -- Categorización de estados usando CASE WHEN
                CASE 
                    WHEN N_ESTADO_PRESTAMO IN ('EN EVALUACION', 'EN EVALUACION TECNICA', 'EN EVALUACION CREDITICIA', 
                                              'EN EVALUACION LEGAL', 'EVALUACION TECNICA APROBADA', 'EVALUACION CREDITICIA APROBADA', 
                                              'EVALUACION LEGAL APROBADA', 'EVALUACION TECNICA OBSERVADA', 'EVALUACION CREDITICIA OBSERVADA', 
                                              'EVALUACION LEGAL OBSERVADA', 'EVALUACION TECNICA RECHAZADA', 'EVALUACION CREDITICIA RECHAZADA', 
                                              'EVALUACION LEGAL RECHAZADA', 'SUBSANACION TECNICA', 'SUBSANACION CREDITICIA', 'SUBSANACION LEGAL') 
                    THEN 'En Evaluación'
                    WHEN N_ESTADO_PRESTAMO IN ('A PAGAR', 'CONVOCATORIA') 
                    THEN 'A Pagar - Convocatoria'
                    WHEN N_ESTADO_PRESTAMO = 'PAGADO' 
                    THEN 'Pagados'
                    WHEN N_ESTADO_PRESTAMO IN ('EN PROCESO DE PAGO', 'PROCESO DE PAGO') 
                    THEN 'En proceso de pago'
                    WHEN N_ESTADO_PRESTAMO = 'FINALIZADO' 
                    THEN 'Pagados-Finalizados'
                    WHEN N_ESTADO_PRESTAMO IN ('GESTIONADO', 'GESTIONAR PAGO') 
                    THEN 'PAGOS GESTIONADOS'
                    ELSE 'Otros'
                END as CATEGORIA,
                
                -- Normalización de departamentos
                CASE 
                    WHEN N_DEPARTAMENTO IN ('CAPITAL', 'CALAMUCHITA', 'COLON', 'CRUZ DEL EJE', 'GENERAL ROCA', 
                                           'GENERAL SAN MARTIN', 'ISCHILIN', 'JUAREZ CELMAN', 'MARCOS JUAREZ', 'MINAS', 
                                           'POCHO', 'PRESIDENTE ROQUE SAENZ PEÑA', 'PUNILLA', 'RIO CUARTO', 'RIO PRIMERO', 
                                           'RIO SECO', 'RIO SEGUNDO', 'SAN ALBERTO', 'SAN JAVIER', 'SAN JUSTO', 'SANTA MARIA', 
                                           'SOBREMONTE', 'TERCERO ARRIBA', 'TOTORAL', 'TULUMBA', 'UNION') 
                    THEN N_DEPARTAMENTO 
                    ELSE 'OTROS' 
                END as N_DEPARTAMENTO_NORM,
                
                -- Corrección de localidades para CAPITAL
                CASE 
                    WHEN N_DEPARTAMENTO = 'CAPITAL' THEN 'CORDOBA'
                    ELSE N_LOCALIDAD 
                END as N_LOCALIDAD_NORM,
                
                -- Corrección de ID_LOCALIDAD para CAPITAL
                CASE 
                    WHEN N_DEPARTAMENTO = 'CAPITAL' THEN 1
                    ELSE ID_LOCALIDAD 
                END as ID_LOCALIDAD_NORM,
                
                -- Clasificación de zonas favorecidas
                CASE 
                    WHEN N_DEPARTAMENTO IN ('PRESIDENTE ROQUE SAENZ PEÑA', 'GENERAL ROCA', 'RIO SECO', 'TULUMBA', 
                                           'POCHO', 'SAN JAVIER', 'SAN ALBERTO', 'MINAS', 'CRUZ DEL EJE', 
                                           'TOTORAL', 'SOBREMONTE', 'ISCHILIN') 
                    THEN 'ZONA NOC Y SUR' 
                    ELSE 'ZONA REGULAR' 
                END as ZONA,
                
                -- Agrupación de líneas de préstamo
                CASE 
                    WHEN N_LINEA_PRESTAMO = 'L4.' THEN 'INICIAR EMPRENDIMIENTO'
                    WHEN N_LINEA_PRESTAMO IN ('L1', 'L3', 'L4', 'L6') THEN 'Otras Lineas'
                    ELSE N_LINEA_PRESTAMO 
                END as N_LINEA_PRESTAMO_NORM,
                
                -- Renombrar DEUDA como DEUDA_VENCIDA y convertir a numérico
                DEUDA as DEUDA_VENCIDA,
                DEUDA_NO_VENCIDA as DEUDA_NO_VENCIDA,
                MONTO_OTORGADO as MONTO_OTORGADO
                
            FROM global_raw
            """
            
            df_global_processed = processor.execute_query(main_query)
            
            # === PASO 2: Agregar campos calculados ===
            calc_query = """
            SELECT *,
                DEUDA_VENCIDA + DEUDA_NO_VENCIDA as DEUDA_A_RECUPERAR,
                MONTO_OTORGADO - (DEUDA_VENCIDA + DEUDA_NO_VENCIDA) as RECUPERADO
            FROM df_global_processed
            """
            processor.register_dataframe("df_global_processed", df_global_processed)
            df_global = processor.execute_query(calc_query)

            # Convertir columnas a numérico después de la consulta DuckDB
            for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                if col in df_global.columns:
                    df_global[col] = pd.to_numeric(df_global[col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
            
            # === PASO 3: JOIN con datos de localidad/municipio si está disponible ===
            if has_localidad:
                join_query = """
                SELECT g.*,
                    l.ID_GOBIERNO_LOCAL,
                    l.TIPO,
                    l."Gestion 2023-2027",
                    l.FUERZAS,
                    l.ESTADO,
                    l."LEGISLADOR DEPARTAMENTAL",
                    l.LATITUD,
                    l.LONGITUD
                FROM df_global g
                LEFT JOIN localidad_municipio l ON g.ID_LOCALIDAD_NORM = l.ID_LOCALIDAD
                """
                df_global = processor.execute_query(join_query)
                
                # Limpiar coordenadas geográficas
                if 'LATITUD' in df_global.columns and 'LONGITUD' in df_global.columns:
                    def limpiar_coordenadas(df):
                        for col in ['LATITUD', 'LONGITUD']:
                            if col in df.columns:
                                df[col] = df[col].astype(str).apply(
                                    lambda x: '.'.join(x.split('.')[:-1]) + '.' + x.split('.')[-1] 
                                    if isinstance(x, str) and x.count('.') > 1 else x
                                )
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                        return df
                    df_global = limpiar_coordenadas(df_global)
            
            # === PASO 4: Crear DataFrame de pagados para recupero ===
            pagados_query = """
            SELECT *
            FROM df_global
            WHERE CATEGORIA IN ('Pagados', 'Pagados-Finalizados')
            """
            processor.register_dataframe("df_global", df_global)
            df_global_pagados = processor.execute_query(pagados_query)
            
            # === PASO 5: JOIN con datos de cumplimiento si está disponible ===
            if has_cumplimiento:
                cumplimiento_query = """
                SELECT p.*,
                    c.PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO
                FROM df_global_pagados p
                LEFT JOIN cumplimiento_raw c ON p.NRO_SOLICITUD = c.NRO_FORMULARIO
                """
                processor.register_dataframe("df_global_pagados", df_global_pagados)
                df_global_pagados = processor.execute_query(cumplimiento_query)
            
            # Actualizar nombres de columnas normalizadas en el DataFrame final
            column_mapping = {
                'N_DEPARTAMENTO_NORM': 'N_DEPARTAMENTO',
                'N_LOCALIDAD_NORM': 'N_LOCALIDAD', 
                'ID_LOCALIDAD_NORM': 'ID_LOCALIDAD',
                'N_LINEA_PRESTAMO_NORM': 'N_LINEA_PRESTAMO'
            }
            df_global = df_global.rename(columns=column_mapping)
            df_global_pagados = df_global_pagados.rename(columns=column_mapping)
            
            # Mostrar información detallada de DataFrames en modo desarrollo
            if is_development:
                # Importar módulos necesarios
                from utils.ui_components import show_dev_dataframe_info
                from utils.session_helper import safe_session_set
                
                # Asegurar que debug_mode esté activado para show_dev_dataframe_info
                safe_session_set('debug_mode', True)
                
                # Mostrar información de debug
                st.write("Información de DataFrames procesados con DuckDB - Banco de la Gente")
                show_dev_dataframe_info(df_global, "df_global")
                show_dev_dataframe_info(df_global_pagados, "df_global_pagados")
                
                # Restaurar estado anterior de debug_mode si no se quiere mantener
                if not is_development:
                    safe_session_set('debug_mode', False)
            
            return df_global, geojson_data, df_localidad_municipio, df_global_pagados
            
    except Exception as e:
        st.error(f"Error en procesamiento DuckDB: {str(e)}")
        st.info("🔄 Fallback a procesamiento pandas...")
        return load_and_preprocess_data(_data, is_development)

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
            selected_lineas = st.multiselect("Línea de préstamo:", lineas_prestamo, default=lineas_prestamo, key="bco_linea_filter")
        
        if selected_lineas:
            df_filtrado = df_filtrado[df_filtrado['N_LINEA_PRESTAMO'].isin(selected_lineas)]
        
        
        return df_filtrado, selected_dpto, selected_loc, selected_lineas

def show_bco_gente_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de Banco de la Gente.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
    # Mostrar última actualización al inicio del dashboard
    if dates:
        show_last_update(dates, 'VT_NOMINA_REP_RECUPERO_X_ANIO.parquet')
    
    # Mostrar columnas en modo desarrollo
    if is_development:
        from utils.ui_components import show_dev_dataframe_info
        from utils.session_helper import safe_session_set
        
        # Activar el modo debug para mostrar información detallada
        safe_session_set('debug_mode', True)
        
        # Filtrar el diccionario de datos para evitar objetos de geometría
        filtered_data = {}
        for key, value in data.items():
            # Excluir archivos GeoJSON que causan problemas de representación
            if not key.endswith('.geojson'):
                filtered_data[key] = value
            else:
                # Informar que se ha excluido un archivo GeoJSON
                st.info(f"Archivo GeoJSON excluido de la vista de desarrollo: {key}")
        
        # Mostrar información de los datos filtrados
        show_dev_dataframe_info(filtered_data, modulo_nombre="Banco de la Gente")

    df_global = None
    df_global_pagados = None
    
    # Cargar y preprocesar datos
    use_duckdb = True
    if use_duckdb:
        # Pasando data como _data para evitar problemas de caché en Streamlit
        df_global, geojson_data, df_localidad_municipio, df_global_pagados = load_and_preprocess_data_duckdb(_data=data, is_development=is_development)
    else:
        df_global, geojson_data, df_localidad_municipio, df_global_pagados = load_and_preprocess_data(data, is_development)
    
    # Verificar si los DataFrames principales se cargaron correctamente
    if df_global is None or df_global.empty:
        st.error("No se pudieron cargar los datos principales para el dashboard del Banco de la Gente.")
        return # Exit the function if data is not available

    if is_development:
        st.write("Datos Globales ya cruzados (después de load_and_preprocess_data):")
        if df_global is not None and not df_global.empty: # Asegurarse que df_global existe
            # Mostrar solo información resumida del DataFrame
            st.write(f"Dimensiones del DataFrame: {df_global.shape[0]} filas x {df_global.shape[1]} columnas")
            st.write(f"Columnas disponibles: {', '.join(df_global.columns.tolist()[:20])}{'...' if len(df_global.columns) > 20 else ''}")
            
            # Mostrar solo las primeras 5 filas
            st.write("Primeras 5 filas:")
            if 'geometry' in df_global.columns:
                st.dataframe(df_global.drop(columns=['geometry']).head(5), use_container_width=True)
                df_to_download = df_global.drop(columns=['geometry'])
            else:
                st.dataframe(df_global.head(5), use_container_width=True)
                df_to_download = df_global
                
            # Mover el código de descarga dentro del bloque condicional
            import io
            csv = df_to_download.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar CSV de Datos Globales",
                data=csv,
                file_name="datos_globales.csv",
                mime="text/csv"
            )

    
    # Crear una copia del DataFrame para trabajar con él
    df_filtrado_global = df_global.copy()
    

    
    # Crear pestañas para las diferentes vistas
    tab_global, tab_recupero = st.tabs(["GLOBAL", "RECUPERO"])
    
    with tab_global:
        # Filtros específicos para la pestaña GLOBAL
        if df_filtrado_global is not None and not df_filtrado_global.empty:
            st.markdown('<h3 style="font-size: 18px; margin-top: 0;">Filtros - GLOBAL</h3>', unsafe_allow_html=True)
            
            # Crear tres columnas para los filtros
            col1, col2, col3 = st.columns(3)
            
            # Filtro de departamento en la primera columna
            with col1:
                # DataFrame con LATITUD NO nulo
                df_lat_notnull = df_filtrado_global[df_filtrado_global['LATITUD'].notnull()]
                # DataFrame con LATITUD nulo
                df_lat_null = df_filtrado_global[df_filtrado_global['LATITUD'].isnull()]

                # Departamentos: los de lat_notnull + "Otros" si hay filas en lat_null
                departamentos = sorted(df_lat_notnull['N_DEPARTAMENTO'].dropna().unique().tolist())
                if not df_lat_null.empty:
                    departamentos.append("Otros")
                all_dpto_option = "Todos los departamentos"
                selected_dpto = st.selectbox("Departamento:", [all_dpto_option] + list(departamentos), key="global_dpto_filter")

            # Filtrar por departamento seleccionado
            if selected_dpto != all_dpto_option:
                if selected_dpto == "Otros":
                    df_filtrado_global_tab = df_lat_null.copy()
                    localidades = sorted(df_filtrado_global_tab['N_LOCALIDAD'].dropna().unique())
                else:
                    df_filtrado_global_tab = df_filtrado_global[df_filtrado_global['N_DEPARTAMENTO'] == selected_dpto]
                    # Filtro de localidad (dependiente del departamento)
                    df_latitud_notnull = df_filtrado_global_tab[df_filtrado_global_tab['LATITUD'].notnull()]
                    df_latitud_null = df_filtrado_global_tab[df_filtrado_global_tab['LATITUD'].isnull()]
                    localidades = sorted(
                        pd.concat([
                            df_latitud_notnull['N_LOCALIDAD'].dropna(),
                            df_latitud_null['N_LOCALIDAD'].dropna()
                        ]).unique()
                    )
                all_loc_option = "Todas las localidades"

                # Mostrar filtro de localidad en la segunda columna
                with col2:
                    selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key="global_loc_filter")

                if selected_loc != all_loc_option:
                    df_filtrado_global_tab = df_filtrado_global_tab[df_filtrado_global_tab['N_LOCALIDAD'] == selected_loc]
            else:
                # Si no se seleccionó departamento, mostrar todas las localidades
                localidades = sorted(df_filtrado_global['N_LOCALIDAD'].dropna().unique())
                all_loc_option = "Todas las localidades"
                df_filtrado_global_tab = df_filtrado_global

                # Mostrar filtro de localidad en la segunda columna
                with col2:
                    selected_loc = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key="global_loc_filter")

                if selected_loc != all_loc_option:
                    df_filtrado_global_tab = df_filtrado_global_tab[df_filtrado_global_tab['N_LOCALIDAD'] == selected_loc]
            
            # Filtro de línea de préstamo en la tercera columna
            with col3:
                lineas_prestamo = sorted(df_filtrado_global_tab['N_LINEA_PRESTAMO'].dropna().unique())
                selected_lineas = st.multiselect("Línea de préstamo:", lineas_prestamo, default=lineas_prestamo, key="global_linea_filter")
            
            if selected_lineas:
                df_filtrado_global_tab = df_filtrado_global_tab[df_filtrado_global_tab['N_LINEA_PRESTAMO'].isin(selected_lineas)]
            

            # Mostrar los datos filtrados en la pestaña GLOBAL
            with st.spinner("Cargando visualizaciones globales..."):
                mostrar_global(df_filtrado_global_tab, TOOLTIPS_DESCRIPTIVOS)

            

    with tab_recupero:
        # Filtros específicos para la pestaña RECUPERO
        # Usar df_global_pagados en lugar de df_global para la pestaña de recupero
        # ya que solo necesitamos los préstamos pagados para esta vista
        if df_global_pagados is not None and not df_global_pagados.empty:
            st.markdown('<h3 style="font-size: 18px; margin-top: 0;">Filtros - RECUPERO</h3>', unsafe_allow_html=True)
            
            # Crear una copia del DataFrame para trabajar con él
            df_filtrado_recupero = df_global_pagados.copy()
            
            # Asegurar que df_filtrado_recupero tenga todas las columnas calculadas necesarias
            # Rellenar valores NaN con 0
            for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                if col in df_filtrado_recupero.columns:
                    df_filtrado_recupero[col] = pd.to_numeric(df_filtrado_recupero[col], errors='coerce').fillna(0)
            
            # Calcular DEUDA_A_RECUPERAR si no existe
            if 'DEUDA_A_RECUPERAR' not in df_filtrado_recupero.columns and all(col in df_filtrado_recupero.columns for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA']):
                df_filtrado_recupero['DEUDA_A_RECUPERAR'] = df_filtrado_recupero['DEUDA_VENCIDA'] + df_filtrado_recupero['DEUDA_NO_VENCIDA']
            
            # Calcular RECUPERADO si no existe
            if 'RECUPERADO' not in df_filtrado_recupero.columns and all(col in df_filtrado_recupero.columns for col in ['MONTO_OTORGADO', 'DEUDA_A_RECUPERAR']):
                df_filtrado_recupero['RECUPERADO'] = df_filtrado_recupero['MONTO_OTORGADO'] - df_filtrado_recupero['DEUDA_A_RECUPERAR']
            
            # Crear tres columnas para los filtros
            col1, col2, col3 = st.columns(3)
            
            # Filtro de departamento en la primera columna
            with col1:
                departamentos = sorted(df_filtrado_recupero['N_DEPARTAMENTO'].dropna().unique())
                all_dpto_option = "Todos los departamentos"
                selected_dpto_rec = st.selectbox("Departamento:", [all_dpto_option] + list(departamentos), key="recupero_dpto_filter")
            
            # Filtrar por departamento seleccionado
            if selected_dpto_rec != all_dpto_option:
                df_filtrado_recupero_tab = df_filtrado_recupero[df_filtrado_recupero['N_DEPARTAMENTO'] == selected_dpto_rec]
                # Filtro de localidad (dependiente del departamento)
                localidades = sorted(df_filtrado_recupero_tab['N_LOCALIDAD'].dropna().unique())
                all_loc_option = "Todas las localidades"
                
                # Mostrar filtro de localidad en la segunda columna
                with col2:
                    selected_loc_rec = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key="recupero_loc_filter")
                
                if selected_loc_rec != all_loc_option:
                    df_filtrado_recupero_tab = df_filtrado_recupero_tab[df_filtrado_recupero_tab['N_LOCALIDAD'] == selected_loc_rec]
            else:
                # Si no se seleccionó departamento, mostrar todas las localidades
                localidades = sorted(df_filtrado_recupero['N_LOCALIDAD'].dropna().unique())
                all_loc_option = "Todas las localidades"
                df_filtrado_recupero_tab = df_filtrado_recupero
                
                # Mostrar filtro de localidad en la segunda columna
                with col2:
                    selected_loc_rec = st.selectbox("Localidad:", [all_loc_option] + list(localidades), key="recupero_loc_filter")
                
                if selected_loc_rec != all_loc_option:
                    df_filtrado_recupero_tab = df_filtrado_recupero_tab[df_filtrado_recupero_tab['N_LOCALIDAD'] == selected_loc_rec]
            
            # Filtro de línea de préstamo en la tercera columna
            with col3:
                lineas_prestamo = sorted(df_filtrado_recupero_tab['N_LINEA_PRESTAMO'].dropna().unique())
                all_lineas_option = "Todas las líneas"
                selected_linea_rec = st.selectbox("Línea de préstamo:", [all_lineas_option] + list(lineas_prestamo), key="recupero_linea_filter")
            
            if selected_linea_rec != all_lineas_option:
                df_filtrado_recupero_tab = df_filtrado_recupero_tab[df_filtrado_recupero_tab['N_LINEA_PRESTAMO'] == selected_linea_rec]
            
            # Mostrar los datos de recupero en la pestaña RECUPERO
            with st.spinner("Cargando visualizaciones de recupero..."):
                mostrar_recupero( df_filtrado_recupero_tab, is_development)
        else:
            st.info("No hay datos de recupero disponibles para mostrar.")

def mostrar_global(df_filtrado_global, tooltips_categorias):
    """
    Muestra los datos globales del Banco de la Gente.
    
    Args:
        df_filtrado_global: DataFrame filtrado con datos globales
        tooltips_categorias: Diccionario con tooltips para cada categoría
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
    # Solo una línea de KPIs, mostrando 'formularios / personas únicas' (ej: 1763/1115)
    kpi_data = []
    # Construir KPIs usando create_bco_gente_kpis para mantener colores, títulos y tooltips originales
    kpi_data = create_bco_gente_kpis(resultados, tooltips_categorias)
    for kpi in kpi_data[:]:  # Iterar sobre copia para poder modificar la lista
        categoria = kpi.get("categoria")
        if not categoria:
            continue
        if categoria == "Rechazados - Bajas":
            kpi_data.remove(kpi)
            continue
            
        # Solo calcular el conteo de personas únicas para la categoría "En Evaluación"
        if categoria == "En Evaluación":
            estados = ESTADO_CATEGORIAS.get(categoria, [])
            total_formularios = resultados.get(categoria, 0)
            
            # Calcular personas únicas solo para esta categoría
            if estados:
                mask = df_filtrado_global["N_ESTADO_PRESTAMO"].isin(estados)
                # Verificar cuántas filas cumplen con la condición
                filas_coincidentes = mask.sum()
                
                if filas_coincidentes > 0:
                    # Extraer el subconjunto de datos para análisis
                    df_subset = df_filtrado_global.loc[mask].copy()
                    
                    # Verificar si hay valores no nulos en la columna CUIL y contar personas únicas
                    df_cuil_no_nulos = df_subset.dropna(subset=['CUIL'])
                    
                    # Si hay CUILs no nulos, contar personas únicas; si no, usar el número de filas
                    if not df_cuil_no_nulos.empty:
                        total_personas = df_cuil_no_nulos['CUIL'].nunique()
                    else:
                        # Si todos los CUILs son nulos, usar el número de filas como aproximación
                        total_personas = filas_coincidentes
                else:
                    total_personas = 0
            else:
                total_personas = 0
                
            kpi["value_form"] = total_formularios
            kpi["value_pers"] = f"{total_personas:,}".replace(',', '.')
        # Si en el futuro quieres aplicar a más KPIs, puedes agregar estas claves para otros casos aquí.

    display_kpi_row(kpi_data, num_columns=6)

    # DEBUG VISUAL: Mostrar info de CUIL únicos para 'En Evaluación'
    estados_eval = ESTADO_CATEGORIAS["En Evaluación"]
    mask_eval = df_filtrado_global["N_ESTADO_PRESTAMO"].isin(estados_eval)
    df_eval = df_filtrado_global[mask_eval][["N_ESTADO_PRESTAMO", "CUIL"]]
    cuils_unicos_eval = df_filtrado_global.loc[mask_eval, "CUIL"].nunique()
    st.markdown("<hr>", unsafe_allow_html=True)
   

    # Desglose dinámico de TODOS los N_ESTADO_PRESTAMO agrupados por CATEGORIA_ESTADO
    # Mapeo de categorías a colores según los KPIs
    categoria_colores = {
        "En Evaluación": COLOR_PRIMARY,        # kpi-primary -> #0085c8 (Azul)
        "A Pagar - Convocatoria": COLOR_ACCENT_3, # kpi-accent-3 -> #bccf00 (Verde lima)
        "Pagados": COLOR_ACCENT_2,            # kpi-accent-2 -> #fbbb21 (Amarillo)
        "En proceso de pago": COLOR_ACCENT_1,  # kpi-accent-1 -> #e73446 (Rojo)
        "Pagados-Finalizados": COLOR_ACCENT_4, # kpi-accent-4 -> #8a1e82 (Violeta)
        "Otros": COLOR_TEXT_DARK              # Texto oscuro por defecto
    }
    
    grupos_detalle = []
    for categoria, estados in ESTADO_CATEGORIAS.items():
        if estados:
            estados_detalle = []
            for estado in estados:
                cantidad = int(df_filtrado_global[df_filtrado_global["N_ESTADO_PRESTAMO"] == estado].shape[0])
                estados_detalle.append(f"<b>{estado}:</b> {cantidad}")
            
            # Obtener el color para esta categoría o usar un color por defecto
            color = categoria_colores.get(categoria, COLOR_TEXT_DARK)
            
            # Encerrar cada grupo de estados en un span con el color correspondiente
            categoria_html = f"<span style='color:{color}; padding:0 5px;'><b>{categoria}:</b> {' '.join(estados_detalle)}</span>"
            grupos_detalle.append(categoria_html)
    
    if grupos_detalle:
        detalle_html = "<div style='font-size:13px; margin-bottom:8px; margin-top:6px'>" + " | ".join(grupos_detalle) + "</div>"
        st.markdown(detalle_html, unsafe_allow_html=True)


    # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)
     # Nueva tabla: Conteo de Préstamos por Línea y Estado
    st.subheader("Conteo de Préstamos por Línea y Estado", 
                 help="Muestra el conteo de préstamos por línea y estado, basado en los datos filtrados.")
    try:
        # Verificar que las columnas necesarias existan en el DataFrame
        required_columns = ['N_LINEA_PRESTAMO', 'N_ESTADO_PRESTAMO', 'NRO_SOLICITUD']
        missing_columns = [col for col in required_columns if col not in df_filtrado_global.columns]
    
        if missing_columns:
            st.warning(f"No se pueden mostrar el conteo de préstamos por línea. Faltan columnas: {', '.join(missing_columns)}")
        else:
            # Definir las categorías a mostrar
            categorias_mostrar = ["A Pagar - Convocatoria", "Pagados", "En proceso de pago", "Pagados-Finalizados"]

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
                    tooltip_text = TOOLTIPS_DESCRIPTIVOS.get(categoria, "")
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
    st.subheader("Condición ante ARCA de Préstamos de las líneas de emprendimientos", 
                 help="Muestra la cantidad de personas con condición ante ARCA de los préstamos de las líneas de emprendimientos, estado pagados y finalizados, basado en los datos filtrados.")

    mostrar_resumen_creditos(df_filtrado_global)
    with st.expander("Detalle de condición ante ARCA", expanded=False):
        mostrar_kpis_fiscales(df_filtrado_global)
    # Línea divisoria para separar secciones
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)

    # NUEVA SECCIÓN: Gráficos de Torta Demográficos
    st.subheader("Distribución de Créditos", help="Distribución demográfica de los beneficiarios")
    
    # Crear cuatro columnas para los gráficos: Línea, Sexo, Empleado, Edades
    col_torta_cat, col_torta_sexo=st.columns(2)

    # Gráfico de torta por categoría
    with col_torta_cat:
        try:
            import plotly.express as px  # Importación local para asegurar que px esté definido
            
            df_filtrado_torta = df_filtrado_global[df_filtrado_global['CATEGORIA'].isin(categorias_mostrar)]
            
            # Agrupar el DataFrame filtrado por línea de préstamo
            grafico_torta = df_filtrado_torta.groupby('N_LINEA_PRESTAMO').size().reset_index(name='Cantidad')
            
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
                            title="Distribución por Linea",
                            margin=dict(l=20, r=20, t=30, b=20)
                        )
                st.plotly_chart(fig_torta, use_container_width=True)
        except Exception as e:
            st.error(f"Error al generar el gráfico de categoría: {e}")

    # Gráfico de torta por sexo
    with col_torta_sexo:
        try:
            if 'N_SEXO' in df_filtrado_global.columns:
                # Incluir categorías "Pagados", "En proceso de pago" y "Pagados-Finalizados"
                categorias_incluidas = ['Pagados', 'En proceso de pago', 'Pagados-Finalizados']
                
                # Filtrar por las categorías incluidas y donde N_SEXO no sea nulo
                df_sexo = df_filtrado_global[
                    (df_filtrado_global['CATEGORIA'].isin(categorias_incluidas)) & 
                    (df_filtrado_global['N_SEXO'].notna())
                ].copy()
                
                if df_sexo.empty:
                    st.warning("No hay datos disponibles para el gráfico de sexo después de filtrar NaNs.")
                else:
                    # Agregar una columna que indique la categoría para el hover
                    df_sexo_con_categoria = df_sexo.groupby(['N_SEXO', 'CATEGORIA']).size().reset_index(name='Cantidad')
                    
                    # Agrupar por sexo para el gráfico principal
                    sexo_counts = df_sexo['N_SEXO'].value_counts().reset_index()
                    sexo_counts.columns = ['Sexo', 'Cantidad']
                    
                    if sexo_counts.empty:
                        st.warning("No hay datos para mostrar en el gráfico de sexo.")
                    else:
                        # Crear el gráfico de torta
                        fig_sexo = px.pie(
                            sexo_counts,
                            values='Cantidad',
                            names='Sexo',
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        
                        # Crear un DataFrame con el resumen por sexo y categoría para mostrar en el hover
                        resumen_categorias = {}
                        for sexo in sexo_counts['Sexo'].unique():
                            resumen_categorias[sexo] = {}
                            for categoria in categorias_incluidas:
                                # Filtrar por sexo y categoría
                                count = df_sexo[(df_sexo['N_SEXO'] == sexo) & 
                                                (df_sexo['CATEGORIA'] == categoria)].shape[0]
                                resumen_categorias[sexo][categoria] = count
                        
                        # Crear texto personalizado para cada segmento
                        custom_text = []
                        for sexo in sexo_counts['Sexo']:
                            texto = f"<b>{sexo}</b><br>Total: {sexo_counts[sexo_counts['Sexo']==sexo]['Cantidad'].values[0]}<br>"
                            for categoria in categorias_incluidas:
                                texto += f"{categoria}: {resumen_categorias[sexo][categoria]}<br>"
                            custom_text.append(texto)
                        
                        # Actualizar el gráfico con el texto personalizado
                        fig_sexo.update_traces(
                            textposition='inside',
                            textinfo='percent+label',
                            hovertemplate='%{customdata}',
                            customdata=custom_text
                        )
                        
                        fig_sexo.update_layout(
                            title="Distribución por Sexo (Pagados, En proceso y Finalizados)",
                            margin=dict(l=20, r=20, t=30, b=20)
                        )
                        st.plotly_chart(fig_sexo, use_container_width=True)
            else:
                st.write("Columnas disponibles:", df_filtrado_global.columns.tolist())
                st.warning("La columna 'N_SEXO' no está presente en el DataFrame.")
        except Exception as e:
            st.error(f"Error al generar el gráfico de sexo: {e}")
    
    col_torta_empleado, col_edades = st.columns(2)
    
    # Gráfico de torta por estado de empleo
    with col_torta_empleado:
        try:
            if 'EMPLEADO' in df_filtrado_global.columns:
                df_empleado = df_filtrado_global[
                    (df_filtrado_global['CATEGORIA'] == 'Pagados') & 
                    (df_filtrado_global['EMPLEADO'].notna())
                ].copy()
                if df_empleado.empty:
                    st.warning("No hay datos disponibles para el gráfico de empleo después de filtrar NaNs.")
                else:
                    # Contar valores únicos de EMPLEADO
                    empleado_counts = df_empleado['EMPLEADO'].value_counts().reset_index()
                    empleado_counts.columns = ['Estado de Empleo', 'Cantidad']
                    
                    # Reemplazar valores numéricos por etiquetas descriptivas
                    empleado_counts['Estado de Empleo'] = empleado_counts['Estado de Empleo'].replace({
                        'S': 'Empleado',
                        'N': 'No Empleado'
                    })
                    
                    if empleado_counts.empty:
                        st.warning("No hay datos para mostrar en el gráfico de empleo.")
                    else:
                        fig_empleado = px.pie(
                            empleado_counts,
                            values='Cantidad',
                            names='Estado de Empleo',
                            color_discrete_sequence=px.colors.qualitative.Pastel
                        )
                        fig_empleado.update_traces(
                            textposition='inside',
                            textinfo='percent+label',
                            hoverinfo='label+percent+value',
                            marker=dict(line=dict(color='#FFFFFF', width=1))
                        )
                        fig_empleado.update_layout(
                            title="Distribución por Estado de Empleo EN CREDITOS PAGADOS",
                            margin=dict(l=20, r=20, t=30, b=20)
                        )
                        st.plotly_chart(fig_empleado, use_container_width=True)
            else:
                st.warning("La columna 'EMPLEADO' no está presente en el DataFrame.")
        except Exception as e:
            st.error(f"Error al generar el gráfico de empleo: {e}")

    # Gráfico de distribución de edades con filtro propio de categoría
    with col_edades:
        try:
            import plotly.express as px
            from datetime import datetime
            categorias_estado = list(ESTADO_CATEGORIAS.keys())
            # Filtro solo para el gráfico de edades
            selected_categorias_edades = st.multiselect(
                "Filtrar por Categoría de Estado (solo afecta este gráfico):",
                options=categorias_estado,
                default=categorias_estado,
                key="filtro_categoria_edades"
            )
            if df_filtrado_global is not None and 'FEC_NACIMIENTO' in df_filtrado_global.columns and 'N_ESTADO_PRESTAMO' in df_filtrado_global.columns and 'FEC_FORM' in df_filtrado_global.columns:
                df_edades = df_filtrado_global[['FEC_NACIMIENTO', 'N_ESTADO_PRESTAMO', 'FEC_FORM']].copy()
                # Mapear N_ESTADO_PRESTAMO a CATEGORIA
                df_edades['CATEGORIA'] = 'Otros'
                for categoria, estados in ESTADO_CATEGORIAS.items():
                    mask = df_edades['N_ESTADO_PRESTAMO'].isin(estados)
                    df_edades.loc[mask, 'CATEGORIA'] = categoria
                # Filtrar por las categorías seleccionadas
                if selected_categorias_edades:
                    df_edades = df_edades[df_edades['CATEGORIA'].isin(selected_categorias_edades)]
                # Convertir a datetime y quitar hora
                df_edades['FEC_NACIMIENTO'] = pd.to_datetime(df_edades['FEC_NACIMIENTO'], errors='coerce').dt.date
                df_edades['FEC_FORM'] = pd.to_datetime(df_edades['FEC_FORM'], errors='coerce').dt.date
                # Calcular edad usando FEC_FORM en lugar de la fecha actual
                df_edades['EDAD'] = df_edades.apply(
                    lambda row: row['FEC_FORM'].year - row['FEC_NACIMIENTO'].year - 
                    ((row['FEC_FORM'].month, row['FEC_FORM'].day) < 
                     (row['FEC_NACIMIENTO'].month, row['FEC_NACIMIENTO'].day)) 
                    if pd.notnull(row['FEC_NACIMIENTO']) and pd.notnull(row['FEC_FORM']) else None, 
                    axis=1
                )
                # Definir rangos de edad
                bins = [0, 17, 29, 39, 49, 59, 69, 200]
                labels = ['<18', '18-29', '30-39', '40-49', '50-59', '60-69','70+']
                df_edades['RANGO_EDAD'] = pd.cut(df_edades['EDAD'], bins=bins, labels=labels, right=True)
                conteo_edades = df_edades['RANGO_EDAD'].value_counts(sort=False).reset_index()
                conteo_edades.columns = ['Rango de Edad', 'Cantidad']
                fig_edades = px.bar(
                    conteo_edades,
                    x='Rango de Edad',
                    y='Cantidad',
                    title='Distribución por Rango de Edad (a Fecha de Solicitud)',
                    color='Rango de Edad',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_edades.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_edades, use_container_width=True)
            else:
                st.warning("No hay datos de FEC_NACIMIENTO o N_ESTADO_PRESTAMO disponibles en df_filtrado_global.")
        except Exception as e:
            st.error(f"Error al generar el gráfico de edades: {e}")

    # Línea divisoria para separar secciones
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)
            
    # Tabla de estados de préstamos agrupados
    st.subheader("Estados de Préstamos por Localidad y Categoría de Estados", 
                 help="Muestra el conteo de préstamos agrupados por categorías de estado, "
                      "basado en los datos filtrados. Las categorías agrupa estados del sistema. No considera formularios de baja ni lineas antiguas históricas.")
    try: #Tabla de estados de préstamos agrupados por categoría
        # Verificar que las columnas necesarias existan en el DataFrame
        required_columns = ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'N_ESTADO_PRESTAMO', 'NRO_SOLICITUD','MONTO_OTORGADO']
        missing_columns = [col for col in required_columns if col not in df_filtrado_global.columns]
        
        if missing_columns:
            st.warning(f"No se pueden mostrar los estados de préstamos. Faltan columnas: {', '.join(missing_columns)}")
        else:
            # Filtro específico para esta tabla - Categorías de estado
            categorias_orden = list(ESTADO_CATEGORIAS.keys())
            # Excluir "Rechazados - Bajas" de las categorías disponibles
            if "Rechazados - Bajas" in categorias_orden:
                categorias_orden.remove("Rechazados - Bajas")
            
            # Usar session_state seguro para mantener las categorías seleccionadas
            if not safe_session_check('selected_categorias'):
                safe_session_set('selected_categorias', categorias_orden)
            
            # Obtener líneas de crédito disponibles
            if 'N_LINEA_PRESTAMO' in df_filtrado_global.columns:
                lineas_credito = sorted(df_filtrado_global['N_LINEA_PRESTAMO'].dropna().unique())
            else:
                lineas_credito = []
            
            
            # Inicializar selected_lineas en session_state seguro si no existe
            if not safe_session_check('selected_lineas_credito'):
                safe_session_set('selected_lineas_credito', lineas_credito)
            
            col1, col2 = st.columns([3, 1])
            
            with col1: # Multiselect para seleccionar categorías
                selected_categorias = st.multiselect(
                    "Filtrar por categorías de estado:",
                    options=categorias_orden,
                    default=safe_session_get('selected_categorias', categorias_orden),
                    key="estado_categoria_filter"
                )
            
            with col2: # Multiselect para seleccionar líneas de crédito
                selected_lineas = st.multiselect(
                    "Filtrar por línea de crédito:",
                    options=lineas_credito,
                    default=safe_session_get('selected_lineas_credito', lineas_credito),
                    key="linea_credito_filter"
                )

            # Aplicar filtros al DataFrame para la tabla de Estados de Préstamos por Categoría
            df_categoria_estados = df_filtrado_global.copy()
            
            # Agregar columna de categoría basada en N_ESTADO_PRESTAMO
            df_categoria_estados['CATEGORIA'] = 'Otros'
            for categoria, estados in ESTADO_CATEGORIAS.items():
                mask = df_categoria_estados['N_ESTADO_PRESTAMO'].isin(estados)
                df_categoria_estados.loc[mask, 'CATEGORIA'] = categoria
            
            # --- Filtro de rango de fechas FEC_INICIO_PAGO (solo para categorías que tienen esta fecha) ---
            aplicar_filtro_fecha = st.checkbox('Aplicar filtro por Fecha de Inicio de Pago', value=False, help="Este filtro solo afecta a préstamos que tienen fecha de inicio de pago (principalmente categoría 'Pagados')")
            
            if aplicar_filtro_fecha and 'FEC_INICIO_PAGO' in df_categoria_estados.columns:
                df_categoria_estados['FEC_INICIO_PAGO'] = pd.to_datetime(df_categoria_estados['FEC_INICIO_PAGO'], errors='coerce')
                fechas_validas = df_categoria_estados['FEC_INICIO_PAGO'].dropna().dt.date.unique()
                fechas_validas = sorted(fechas_validas)
                if fechas_validas:
                    min_fecha = fechas_validas[0]
                    max_fecha = fechas_validas[-1]
                    fecha_inicio, fecha_fin = st.select_slider(
                        'Seleccionar rango de Fecha de Inicio de Pago:',
                        options=fechas_validas,
                        value=(min_fecha, max_fecha),
                        key='filtro_fecha_inicio_pago_categoria'
                    )
                    
                    # Crear una máscara para filtrar solo registros con fecha válida en el rango seleccionado
                    mask_fecha = ((df_categoria_estados['FEC_INICIO_PAGO'].dt.date >= fecha_inicio) & 
                                 (df_categoria_estados['FEC_INICIO_PAGO'].dt.date <= fecha_fin))
                    
                    # Crear una máscara para mantener registros sin fecha (NaT)
                    mask_sin_fecha = df_categoria_estados['FEC_INICIO_PAGO'].isna()
                    
                    # Aplicar ambas máscaras para mantener registros que cumplen con el rango de fechas O no tienen fecha
                    df_categoria_estados = df_categoria_estados[mask_fecha | mask_sin_fecha]
            
            # Filtrar por categorías seleccionadas
            if selected_categorias:
                df_categoria_estados = df_categoria_estados[df_categoria_estados['CATEGORIA'].isin(selected_categorias)]
            
            # Filtrar por líneas de crédito seleccionadas
            if selected_lineas:
                df_categoria_estados = df_categoria_estados[df_categoria_estados['N_LINEA_PRESTAMO'].isin(selected_lineas)]
            
            # Asegurarse de que los montos sean numéricos y reemplazar NaN por 0
            if 'MONTO_OTORGADO' in df_categoria_estados.columns:
                df_categoria_estados['MONTO_OTORGADO'] = pd.to_numeric(df_categoria_estados['MONTO_OTORGADO'], errors='coerce').fillna(0)
            
            # Continuar con el agrupamiento solo si hay datos filtrados
            if not df_categoria_estados.empty:
                # Realizar el agrupamiento
                df_grouped = df_categoria_estados.groupby(
                    ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'CATEGORIA', 'N_LINEA_PRESTAMO']
                ).agg({
                    'NRO_SOLICITUD': 'count',
                    'MONTO_OTORGADO': 'sum'
                }).reset_index()
            else:
                st.warning("No hay datos para mostrar con los filtros seleccionados.")

            # Actualizar session_state
            if selected_categorias != safe_session_get('selected_categorias', []):
                safe_session_set('selected_categorias', selected_categorias)
            if selected_lineas != safe_session_get('selected_lineas_credito', []):
                safe_session_set('selected_lineas_credito', selected_lineas)
            
            # Si no se selecciona ninguna categoría, mostrar todas
            if not selected_categorias:
                selected_categorias = categorias_orden
                
            # Crear copia del DataFrame para manipulación
            # Usar @st.cache_data para evitar recalcular si los datos no cambian
            @st.cache_data
            def prepare_categoria_data(df, categorias):
                # La categoría ya está asignada en df_categoria_estados, no necesitamos hacerlo de nuevo
                df_copy = df.copy()

                # Crear pivot table con conteo agrupado por categorías
                pivot_df = df_copy.pivot_table(
                    index=['N_DEPARTAMENTO', 'N_LOCALIDAD'],
                    columns='CATEGORIA',
                    values='NRO_SOLICITUD',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                # Asegurar que todas las categorías seleccionadas estén en la tabla
                for categoria in categorias:
                    if categoria not in pivot_df.columns:
                        pivot_df[categoria] = 0
                
                # Reordenar columnas para mostrar en orden consistente
                return pivot_df.reindex(columns=['N_DEPARTAMENTO', 'N_LOCALIDAD'] + categorias)
            
            # Obtener el DataFrame procesado usando caché
            pivot_df = prepare_categoria_data(df_categoria_estados, categorias_orden)
            
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
            
            styled_df = pivot_df_filtered.style.apply(highlight_total_rows, axis=1)
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # --- Generar DataFrame extendido para descarga (con todas las categorías) ---
            columnas_extra = [
                col for col in ['ID_GOBIERNO_LOCAL','TIPO', 'Gestion 2023-2027', 'FUERZAS', 'ESTADO', 'LEGISLADOR DEPARTAMENTAL'] if col in df_filtrado_global.columns
            ]
            
            # Usar una copia del DataFrame filtrado globalmente para no estar limitado por la selección de categorías de la UI
            df_para_descarga = df_filtrado_global.copy()

            # Aplicar filtro por línea de crédito si está seleccionado
            if selected_lineas:
                df_para_descarga = df_para_descarga[df_para_descarga['N_LINEA_PRESTAMO'].isin(selected_lineas)]

            # Asignar categorías
            df_para_descarga['CATEGORIA'] = 'Otros'
            for categoria, estados in ESTADO_CATEGORIAS.items():
                mask = df_para_descarga['N_ESTADO_PRESTAMO'].isin(estados)
                df_para_descarga.loc[mask, 'CATEGORIA'] = categoria

            # Agrupar para obtener el conteo y la suma de montos
            df_descarga_grouped = df_para_descarga.groupby(
                ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'N_LINEA_PRESTAMO'] + columnas_extra + ['CATEGORIA']
            ).agg({
                'NRO_SOLICITUD': 'count',
                'MONTO_OTORGADO': 'sum'
            }).reset_index()
            
            # Renombrar las columnas para mayor claridad
            df_descarga_grouped = df_descarga_grouped.rename(columns={
                'NRO_SOLICITUD': 'Cantidad',
                'MONTO_OTORGADO': 'Monto Total'
            })
            # --- Botón de descarga Excel con ícono ---
            import io
            excel_buffer = io.BytesIO()
            df_descarga_grouped.to_excel(excel_buffer, index=False)
            fecha_rango_str = ''
            if 'fecha_inicio' in locals() and 'fecha_fin' in locals():
                fecha_rango_str = f"_{fecha_inicio.strftime('%Y%m%d')}_{fecha_fin.strftime('%Y%m%d')}"
            nombre_archivo = f"pagados_x_localidad{fecha_rango_str}.xlsx"
            excel_buffer.seek(0)
            excel_icon = """
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect width="20" height="20" rx="3" fill="#217346"/>
            <path d="M6.5 7.5H8L9.25 10L10.5 7.5H12L10.25 11L12 14.5H10.5L9.25 12L8 14.5H6.5L8.25 11L6.5 7.5Z" fill="white"/>
            </svg>
            """
            st.markdown(f'<span style="vertical-align:middle">{excel_icon}</span> <b>Descargar (Excel)</b>', unsafe_allow_html=True)
            st.download_button(
                label=f"Descargar Excel {nombre_archivo}",
                data=excel_buffer.getvalue(),
                file_name=nombre_archivo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Descargar el agrupado por localidad con id de censo, incluyendo montos totales."
            )
           
    except Exception as e:
        st.warning(f"Error al generar la tabla de estados: {str(e)}")


     # Línea divisoria en gris claro
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)

    # Serie Histórica 
    st.subheader("Serie Histórica de Préstamos", 
                    help="Muestra la cantidad total de solicitud de préstamos, agrupados por mes, dentro del rango de fechas seleccionado. " 
                        "Formularios presentados.") 
    try: 
            if df_filtrado_global is None or df_filtrado_global.empty: 
                st.info("No hay datos de recupero disponibles para la serie histórica.") 
            elif 'FEC_FORM' not in df_filtrado_global.columns: 
                st.warning("La columna 'FEC_FORM' necesaria para la serie histórica no se encuentra en los datos de recupero.") 
            else: 
                # Verificar si existe la columna FEC_INICIO_PAGO
                tiene_fecha_inicio_pago = 'FEC_INICIO_PAGO' in df_filtrado_global.columns
                
                # Preparar DataFrame de fechas de formulario
                df_fechas = df_filtrado_global[['FEC_FORM']].copy()
                df_fechas['FEC_FORM'] = pd.to_datetime(df_fechas['FEC_FORM'], errors='coerce')
                df_fechas.dropna(subset=['FEC_FORM'], inplace=True)
                fecha_actual = datetime.now()
                df_fechas = df_fechas[df_fechas['FEC_FORM'] <= fecha_actual]
                fecha_min_valida = pd.to_datetime('1678-01-01')
                df_fechas_filtrado_rango = df_fechas[df_fechas['FEC_FORM'] >= fecha_min_valida].copy()
                
                # Preparar DataFrame de fechas de inicio de pago si existe la columna
                if tiene_fecha_inicio_pago:
                    df_fechas_pago = df_filtrado_global[['FEC_INICIO_PAGO']].copy()
                    df_fechas_pago['FEC_INICIO_PAGO'] = pd.to_datetime(df_fechas_pago['FEC_INICIO_PAGO'], errors='coerce')
                    df_fechas_pago.dropna(subset=['FEC_INICIO_PAGO'], inplace=True)
                    df_fechas_pago = df_fechas_pago[df_fechas_pago['FEC_INICIO_PAGO'] <= fecha_actual]
                    df_fechas_pago = df_fechas_pago[df_fechas_pago['FEC_INICIO_PAGO'] >= fecha_min_valida].copy()
                    tiene_datos_pago = not df_fechas_pago.empty
                else:
                    tiene_datos_pago = False
                    st.info("La columna 'FEC_INICIO_PAGO' no está disponible para mostrar la segunda serie.")

                if df_fechas_filtrado_rango.empty:
                    st.info("No hay datos disponibles dentro del rango de fechas válido para la serie histórica.")
                else:
                    fecha_min = df_fechas_filtrado_rango['FEC_FORM'].min().date()
                    fecha_max = df_fechas_filtrado_rango['FEC_FORM'].max().date()
                    
                    # Ajustar rango de fechas si hay datos de inicio de pago
                    if tiene_datos_pago:
                        fecha_min_pago = df_fechas_pago['FEC_INICIO_PAGO'].min().date()
                        fecha_max_pago = df_fechas_pago['FEC_INICIO_PAGO'].max().date()
                        fecha_min = min(fecha_min, fecha_min_pago)
                        fecha_max = max(fecha_max, fecha_max_pago)
                    
                    st.caption(f"Rango de fechas disponibles: {fecha_min.strftime('%d/%m/%Y')} - {fecha_max.strftime('%d/%m/%Y')}")

                    start_date = st.date_input("Fecha de inicio:", min_value=fecha_min, max_value=fecha_max, value=fecha_min)
                    end_date = st.date_input("Fecha de fin:", min_value=fecha_min, max_value=fecha_max, value=fecha_max)

                    if start_date > end_date:
                        st.error("La fecha de inicio debe ser anterior a la fecha de fin.")
                    else:
                        # Filtrar datos de formularios por rango de fechas
                        df_fechas_seleccionado = df_fechas_filtrado_rango[
                            (df_fechas_filtrado_rango['FEC_FORM'].dt.date >= start_date) &
                            (df_fechas_filtrado_rango['FEC_FORM'].dt.date <= end_date)
                        ].copy()
                        
                        # Filtrar datos de inicio de pago por rango de fechas (si existen)
                        tiene_datos_pago_filtrados = tiene_datos_pago
                        if tiene_datos_pago_filtrados:
                            df_fechas_pago_seleccionado = df_fechas_pago[
                                (df_fechas_pago['FEC_INICIO_PAGO'].dt.date >= start_date) &
                                (df_fechas_pago['FEC_INICIO_PAGO'].dt.date <= end_date)
                            ].copy()
                            tiene_datos_pago_filtrados = not df_fechas_pago_seleccionado.empty
                        else:
                            tiene_datos_pago_filtrados = False

                        if df_fechas_seleccionado.empty and (not tiene_datos_pago_filtrados):
                            st.info("No hay datos para el período seleccionado.")
                        else:
                            # Preparar serie histórica de formularios
                            if not df_fechas_seleccionado.empty:
                                df_fechas_seleccionado['AÑO_MES'] = df_fechas_seleccionado['FEC_FORM'].dt.to_period('M')
                                serie_historica = df_fechas_seleccionado.groupby('AÑO_MES').size().reset_index(name='Cantidad')
                                serie_historica['FECHA'] = serie_historica['AÑO_MES'].dt.to_timestamp()
                                serie_historica = serie_historica.sort_values('FECHA')
                            
                            # Preparar serie histórica de inicio de pagos
                            if tiene_datos_pago_filtrados:
                                df_fechas_pago_seleccionado['AÑO_MES'] = df_fechas_pago_seleccionado['FEC_INICIO_PAGO'].dt.to_period('M')
                                serie_historica_pago = df_fechas_pago_seleccionado.groupby('AÑO_MES').size().reset_index(name='Cantidad')
                                serie_historica_pago['FECHA'] = serie_historica_pago['AÑO_MES'].dt.to_timestamp()
                                serie_historica_pago = serie_historica_pago.sort_values('FECHA')

                            try:
                                # Crear figura con Plotly Graph Objects para mayor control
                                fig_historia = go.Figure()
                                
                                # Definir colores (con manejo seguro para evitar el error)
                                color_azul = '#1f77b4'  # Color azul por defecto
                                color_rojo = '#d62728'  # Color rojo por defecto
                                
                                # Verificar si COLORES_IDENTIDAD es un diccionario antes de usar .get()
                                if isinstance(COLORES_IDENTIDAD, dict):
                                    color_azul = COLORES_IDENTIDAD.get('azul', color_azul)
                                    color_rojo = COLORES_IDENTIDAD.get('rojo', color_rojo)
                                
                                # Añadir línea de formularios si hay datos
                                if not df_fechas_seleccionado.empty:
                                    fig_historia.add_trace(go.Scatter(
                                        x=serie_historica['FECHA'],
                                        y=serie_historica['Cantidad'],
                                        mode='lines+markers',
                                        name='Formularios Presentados',
                                        line=dict(color=color_azul, width=3),
                                        marker=dict(size=8)
                                    ))
                                
                                # Añadir línea de inicio de pagos si hay datos
                                if tiene_datos_pago_filtrados:
                                    fig_historia.add_trace(go.Scatter(
                                        x=serie_historica_pago['FECHA'],
                                        y=serie_historica_pago['Cantidad'],
                                        mode='lines+markers',
                                        name='Inicio de Pagos',
                                        line=dict(color=color_rojo, width=3),
                                        marker=dict(size=8)
                                    ))
                                
                                # Configurar layout
                                fig_historia.update_layout(
                                    title='Evolución por Mes (Período Seleccionado)',
                                    xaxis_title='Fecha',
                                    yaxis_title='Cantidad',
                                    xaxis_tickformat='%b %Y',
                                    plot_bgcolor='white',
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1
                                    ),
                                    hovermode='x unified'
                                )
                                
                                st.plotly_chart(fig_historia, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error al generar el gráfico de serie histórica: {str(e)}")
                                st.exception(e)  # Muestra el traceback completo para depuración
    
                            with st.expander("Ver datos de la serie histórica"):
                                # Crear DataFrame para resumen anual con ambas métricas
                                resumen_anual = {}
                                
                                # Procesar datos de Formularios Presentados
                                if not df_fechas_seleccionado.empty:
                                    tabla_data_form = serie_historica[['FECHA', 'Cantidad']].copy()
                                    tabla_data_form['Año'] = tabla_data_form['FECHA'].dt.year
                                    tabla_data_form_agrupada = tabla_data_form.groupby('Año', as_index=False)['Cantidad'].sum()
                                    
                                    # Guardar datos de formularios en el diccionario
                                    for _, row in tabla_data_form_agrupada.iterrows():
                                        año = int(row['Año'])
                                        if año not in resumen_anual:
                                            resumen_anual[año] = {'Formularios Presentados': 0, 'Inicio de Pagos': 0}
                                        resumen_anual[año]['Formularios Presentados'] = int(row['Cantidad'])
                                
                                # Procesar datos de Inicio de Pagos
                                if tiene_datos_pago_filtrados:
                                    tabla_data_pago = serie_historica_pago[['FECHA', 'Cantidad']].copy()
                                    tabla_data_pago['Año'] = tabla_data_pago['FECHA'].dt.year
                                    tabla_data_pago_agrupada = tabla_data_pago.groupby('Año', as_index=False)['Cantidad'].sum()
                                    
                                    # Guardar datos de inicio de pagos en el diccionario
                                    for _, row in tabla_data_pago_agrupada.iterrows():
                                        año = int(row['Año'])
                                        if año not in resumen_anual:
                                            resumen_anual[año] = {'Formularios Presentados': 0, 'Inicio de Pagos': 0}
                                        resumen_anual[año]['Inicio de Pagos'] = int(row['Cantidad'])
                                
                                # Custom HTML table con estilos
                                html_table = """
                                    <style>
                                        .serie-table {
                                            width: 100%;
                                            border-collapse: collapse;
                                            margin-bottom: 20px;
                                            font-size: 14px;
                                        }
                                        .serie-table th, .serie-table td {
                                            padding: 8px;
                                            border: 1px solid #ddd;
                                            text-align: right;
                                        }
                                        .serie-table th {
                                            background-color: #0072bb;
                                            color: white;
                                            text-align: center;
                                        }
                                        .serie-table td:first-child {
                                            text-align: left;
                                        }
                                        .serie-table .formularios {
                                            background-color: rgba(31, 119, 180, 0.1);
                                        }
                                        .serie-table .pagos {
                                            background-color: rgba(214, 39, 40, 0.1);
                                        }
                                    </style>
                                """
                                
                                # Crear encabezado de la tabla
                                html_table += '<table class="serie-table"><thead><tr>'
                                html_table += '<th>Año</th><th>Formularios Presentados</th><th>Inicio de Pagos</th></tr></thead><tbody>'
                                
                                # Ordenar años de más reciente a más antiguo
                                for año in sorted(resumen_anual.keys(), reverse=True):
                                    datos = resumen_anual[año]
                                    html_table += f'<tr>'
                                    html_table += f'<td>{año}</td>'
                                    html_table += f'<td class="formularios">{datos["Formularios Presentados"]}</td>'
                                    html_table += f'<td class="pagos">{datos["Inicio de Pagos"]}</td>'
                                    html_table += f'</tr>'

                                html_table += '</tbody></table>'
                                st.markdown(html_table, unsafe_allow_html=True)

                        
    except Exception as e:
        st.error(f"Error inesperado en la sección Serie Histórica: {e}")

def mostrar_recupero(df_filtrado_recupero=None, is_development=False):
    """
    Muestra la sección de recupero de deudas, utilizando datos ya filtrados.
    
    Args:
        df_filtrado_recupero: DataFrame con datos de recupero completos (basado en df_global_pagados).
        is_development: Indica si se está en modo desarrollo.
    """
    # Importar bibliotecas necesarias
    import numpy as np
    st.header("Análisis de Recupero")
    
    # Mostrar df_filtrado_recupero en modo desarrollo siempre al inicio (versión limitada)
    if is_development and df_filtrado_recupero is not None and not df_filtrado_recupero.empty:
        st.subheader("DataFrame de Recupero (Modo Desarrollo - Vista Reducida)")
        
        # Mostrar solo información básica del DataFrame para evitar problemas de tamaño
        st.write(f"Dimensiones del DataFrame: {df_filtrado_recupero.shape[0]} filas x {df_filtrado_recupero.shape[1]} columnas")
        st.write(f"Columnas disponibles: {', '.join(df_filtrado_recupero.columns.tolist())}")
        
        # Mostrar solo las primeras 5 filas en lugar del DataFrame completo
        st.write("Primeras 5 filas:")
        st.dataframe(df_filtrado_recupero.head(5), use_container_width=True)
        
        # Verificar si existe la columna PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO
        if 'PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO' in df_filtrado_recupero.columns:
            st.success("✅ La columna PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO está presente en df_filtrado_recupero")
        else:
            st.error("❌ La columna PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO NO está presente en df_filtrado_recupero")
        
    # Añadir botón para descargar el DataFrame df_filtrado_recupero
    col1, col2 = st.columns([1, 3])
    with col1:
        # Función para convertir el DataFrame a CSV
        def convert_df_to_csv(df):
            return df.to_csv(index=False).encode('utf-8')
        
        # Botón de descarga - solo mostrar si df_filtrado_recupero no es None
        if df_filtrado_recupero is not None and not df_filtrado_recupero.empty:
            csv = convert_df_to_csv(df_filtrado_recupero)
            st.download_button(
                label="⬇️ Descargar datos de recupero",
            data=csv,
            file_name='datos_recupero.csv',
            mime='text/csv',
            help="Descargar el DataFrame completo de recupero en formato CSV"
        )
    
    with col2:
        st.info("El archivo descargado contendrá todos los registros relacionados a 'recupero', incluyendo la columna PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO.")
        
        # Mostrar dimensiones del DataFrame
        if df_filtrado_recupero is not None and not df_filtrado_recupero.empty:
            st.caption(f"Dimensiones del DataFrame: {df_filtrado_recupero.shape[0]:,} filas x {df_filtrado_recupero.shape[1]:,} columnas")
    
    # Agregar una línea divisoria
    st.markdown("---")
        
    # Agregar histograma con curva normal superpuesta para PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO
    if df_filtrado_recupero is not None and 'PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO' in df_filtrado_recupero.columns:
        st.subheader("Análisis de Distribución de Cumplimiento de Formularios")
        st.markdown("<div class='info-box'>Para cuotas pagadas, se calcula la diferencia entre la fecha de vencimiento (FEC_CUOTA) y la fecha de pago (FEC_PAGO), donde un valor positivo indica atraso en el pago y un valor negativo refleja un pago anticipado. En el caso de cuotas vencidas no pagadas, se mide la diferencia entre la fecha de vencimiento y la fecha actual (SYSDATE), representando el atraso acumulado. Las cuotas futuras o sin vencimiento se registran como 0 para no afectar el promedio. A mayor número de días, menor es el cumplimiento del cliente, ya que valores altos señalan demoras prolongadas en los pagos.</div>", unsafe_allow_html=True)
        
        # Crear una copia del DataFrame para trabajar con él
        df_cumplimiento = df_filtrado_recupero.copy()
        
        # Primero asegurarse de que la columna sea numérica y eliminar nulos de PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO
        df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'] = pd.to_numeric(
            df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'], errors='coerce'
        )
        
        # Eliminar valores nulos de la columna de interés antes de filtrar por categoría
        df_cumplimiento = df_cumplimiento.dropna(subset=['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'])
        
        # Filtrar directamente por la categoría "Pagados" si existe la columna CATEGORIA
        if 'CATEGORIA' in df_cumplimiento.columns:
            # Verificar si existe la categoría "Pagados"
            if 'Pagados' in df_cumplimiento['CATEGORIA'].values:
                # Filtrar solo por la categoría "Pagados"
                registros_antes = len(df_cumplimiento)
                df_cumplimiento = df_cumplimiento[df_cumplimiento['CATEGORIA'] == 'Pagados']
                registros_filtrados = registros_antes - len(df_cumplimiento)
                
                # Mostrar información sobre el filtrado
                st.success(f"Análisis limitado a categoría 'Pagados': {len(df_cumplimiento):,} registros")
            else:
                st.warning("La categoría 'Pagados' no existe en los datos. Se usarán todos los registros disponibles.")
        else:
            st.warning("La columna CATEGORIA no está disponible. Se usarán todos los registros disponibles.")
        
        # Mantener la variable total_registros_originales para cálculos posteriores
        total_registros_originales = len(df_filtrado_recupero)
        
        # Agregar opción para filtrar outliers
        col1, col2 = st.columns(2)
        with col1:
            # Agregar opción para filtrar outliers
            filtrar_outliers = st.checkbox(
                "Filtrar valores extremos (outliers)",
                value=True,
                help="Elimina valores extremadamente altos usando el método IQR con umbral conservador (3*IQR)"
            )
        
        # Nota: Los valores negativos representan días adelantados a la fecha de vencimiento de cuotas
        # Son importantes para analizar el cumplimiento, así que los mantenemos
        negativos_filtrados = 0
        
        # Filtrar outliers solo si la opción está activada
        outliers_filtrados = 0
        limite_superior = None
        if filtrar_outliers and len(df_cumplimiento) > 10:  # Necesitamos suficientes datos
            # Filtrar valores extremos (outliers) usando el método IQR
            Q1 = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'].quantile(0.25)
            Q3 = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'].quantile(0.75)
            IQR = Q3 - Q1
            
            # Definir límites para outliers (usando 3*IQR para ser conservadores)
            limite_superior = Q3 + 3 * IQR
            
            # Guardar cantidad antes del filtrado de outliers
            registros_antes = len(df_cumplimiento)
            
            # Filtrar outliers extremos
            df_cumplimiento = df_cumplimiento[df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'] <= limite_superior]
            outliers_filtrados = registros_antes - len(df_cumplimiento)
        
        # Ahora configuramos el slider DESPUÉS de filtrar outliers
        with col2:
            # Obtener valores mínimo y máximo para el slider (incluyendo valores negativos)
            min_dias_raw = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'].min()
            max_dias_raw = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'].max()
            
            # Redondear a enteros para el slider, asegurándonos de incluir todo el rango de datos
            min_dias = int(np.floor(min_dias_raw)) if pd.notna(min_dias_raw) else -30
            max_dias = int(np.ceil(max_dias_raw)) if pd.notna(max_dias_raw) else 365
            
            # Crear slider para filtrar por rango de días
            rango_dias = st.slider(
                "Rango de días de cumplimiento:",
                min_value=min_dias,
                max_value=max_dias,
                value=(min_dias, max_dias),
                step=1,
                help="Valores negativos indican días adelantados al vencimiento (mejor cumplimiento)"
            )
        
        # Aplicar filtro de rango de días (si se ha definido el slider)
        if 'rango_dias' in locals():
            min_rango, max_rango = rango_dias
            df_cumplimiento = df_cumplimiento[
                (df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'] >= min_rango) & 
                (df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'] <= max_rango)
            ]
            
        # Resumen de datos filtrados con información consolidada
        if not df_cumplimiento.empty:
            min_despues = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'].min()
            max_despues = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO'].max()
            
            # Crear un mensaje informativo consolidado
            info_mensaje = f"Datos listos para análisis: {len(df_cumplimiento):,} registros válidos. "
            
            if outliers_filtrados > 0:
                info_mensaje += f"Se filtraron {outliers_filtrados:,} outliers extremos (valores > {limite_superior:.1f} días). "
                
            info_mensaje += f"Rango de días en datos filtrados: {min_despues:.1f} a {max_despues:.1f} días."
            
            st.success(info_mensaje)
            
            # Mostrar advertencia si se filtraron muchos registros
            if (negativos_filtrados + outliers_filtrados) > total_registros_originales * 0.2:  # Si se filtró más del 20%
                st.warning("Se filtraron muchos registros. Los resultados podrían no ser representativos de toda la población.")
        
        if not df_cumplimiento.empty:
            # Importar bibliotecas necesarias
            import plotly.graph_objects as go
            import numpy as np
            from scipy import stats
            
            # Obtener datos para el histograma
            datos = df_cumplimiento['PROMEDIO_DIAS_CUMPLIMIENTO_FORMULARIO']
            
            # Calcular estadísticas descriptivas
            media = datos.mean()
            desv_std = datos.std()
            mediana = datos.median()
            n_registros = len(datos)
            
            # Mostrar estadísticas descriptivas
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Media", f"{media:.1f} días")
            col2.metric("Desviación Estándar", f"{desv_std:.1f} días")
            col3.metric("Mediana", f"{mediana:.1f} días")
            col4.metric("Número de Registros", f"{n_registros:,}")
            
            # Determinar el número de bins para el histograma
            # Regla de Sturges: k = 1 + 3.322 * log10(n)
            n_bins = int(1 + 3.322 * np.log10(n_registros))
            
            # Crear el histograma
            fig = go.Figure()
            
            # Agregar el histograma
            fig.add_trace(go.Histogram(
                x=datos,
                nbinsx=n_bins,
                name='Frecuencia',
                marker_color='rgba(73, 160, 181, 0.7)',
                opacity=0.75
            ))
            
            # Generar puntos para la curva normal teórica
            x_range = np.linspace(max(0, datos.min() - desv_std), datos.max() + desv_std, 1000)
            y_norm = stats.norm.pdf(x_range, media, desv_std)
            
            # Escalar la curva normal para que coincida con la altura del histograma
            # Necesitamos estimar la altura máxima del histograma
            hist_values, bin_edges = np.histogram(datos, bins=n_bins)
            max_height = max(hist_values)
            scaling_factor = max_height / max(y_norm)
            
            # Agregar la curva normal superpuesta
            fig.add_trace(go.Scatter(
                x=x_range,
                y=y_norm * scaling_factor,
                mode='lines',
                name='Curva Normal Teórica',
                line=dict(color='rgba(255, 0, 0, 0.8)', width=2)
            ))
            
            # Personalizar diseño
            fig.update_layout(
                title='Histograma de Días de Cumplimiento con Curva Normal Superpuesta',
                xaxis_title='Días de Cumplimiento (Mayor número = Menor cumplimiento)',
                yaxis_title='Frecuencia',
                legend_title='Distribución',
                height=500,
                hovermode='closest',
                bargap=0.1
            )
            
            # Agregar líneas verticales para la media y mediana
            fig.add_shape(type="line",
                x0=media, y0=0, x1=media, y1=max_height,
                line=dict(color="red", width=2, dash="dash"),
                name="Media"
            )
            
            fig.add_shape(type="line",
                x0=mediana, y0=0, x1=mediana, y1=max_height,
                line=dict(color="green", width=2, dash="dash"),
                name="Mediana"
            )
            
            # Agregar anotaciones para la media y mediana
            fig.add_annotation(
                x=media, y=max_height*0.95,
                text=f"Media: {media:.1f}",
                showarrow=True,
                arrowhead=1,
                ax=40,
                ay=-40
            )
            
            fig.add_annotation(
                x=mediana, y=max_height*0.85,
                text=f"Mediana: {mediana:.1f}",
                showarrow=True,
                arrowhead=1,
                ax=-40,
                ay=-40
            )
            
            # Mostrar gráfico
            st.plotly_chart(fig, use_container_width=True)
            
            
            # Agregar explicación sobre la importancia del análisis
            st.markdown("---")
            st.markdown("**¿Por qué es importante este análisis?**")
            st.markdown("• Permite entender el patrón de cumplimiento de pagos de los beneficiarios")
            st.markdown("• Ayuda a identificar si hay comportamientos atípicos o esperados en los tiempos de pago")
            st.markdown("• Facilita la toma de decisiones basadas en datos sobre políticas de cobro y seguimiento")
            st.write(f"**Conclusión:** La media de días de cumplimiento es de {media:.1f} días, con una desviación estándar de {desv_std:.1f} días. "
                     f"La mediana es de {mediana:.1f} días, lo que significa que el 50% de los casos tienen un tiempo de cumplimiento menor o igual a este valor.")
            
            
        else:
            st.warning("No hay datos válidos de cumplimiento para mostrar en el histograma.")
    
    st.markdown("---")
  
    # --- Nueva Sección: Tabla Agrupada de Pagados (usando datos ya filtrados) ---
    st.subheader("Detalle de Préstamos Pagados por Localidad", help="Muestra la suma de préstamos pagados, no finalizados, con planes de cuotas, por localidad")
    
    # Filtrar solo por la categoría "Pagados" sobre el DataFrame ya filtrado
    df_filtrado_pagados = df_filtrado_recupero[
        (df_filtrado_recupero['CATEGORIA'] == "Pagados")
    ].copy()
    
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