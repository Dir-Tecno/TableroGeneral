import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row, create_bco_gente_kpis
from utils.styles import COLORES_IDENTIDAD
from utils.kpi_tooltips import ESTADO_CATEGORIAS, TOOLTIPS_DESCRIPTIVOS
from utils.ui_components import create_bco_gente_kpis
from utils.data_cleaning import convert_decimal_separator

# Crear diccionario para tooltips de categorías (técnico, lista de estados)
tooltips_categorias = {k: ", ".join(v) for k, v in ESTADO_CATEGORIAS.items()}

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
    df_filtrado = df_global[
        (df_global["N_LINEA_PRESTAMO"].isin(lineas)) &
        (df_global["CATEGORIA"].isin(categorias))
    ].copy()

    if df_filtrado.empty:
        st.info("No se encontraron registros para las líneas y categorías seleccionadas.")
        return

    campos = [
        "IMP_GANANCIAS",
        "IMP_IVA",
        "MONOTRIBUTO",
        "INTEGRANTE_SOC",
        "EMPLEADOR",
        "ACTIVIDAD_MONOTRIBUTO"
    ]

    # Mostrar las tablas en 2 filas de 3 columnas
    cols_row1 = st.columns(3)
    cols_row2 = st.columns(3)
    for idx, campo in enumerate(campos):
        col = cols_row1[idx] if idx < 3 else cols_row2[idx-3]
        with col:
            st.markdown(f"<b>{campo.replace('_',' ').title()}</b>", unsafe_allow_html=True)
            if campo not in df_filtrado.columns:
                st.info(f"No existe la columna {campo} en los datos.")
                continue
            df_campo = df_filtrado[df_filtrado[campo].notnull()]
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

# --- Ejemplo de integración (descomentar para usar en el flujo principal) ---
# mostrar_resumen_creditos(df_global)

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

        # Agregar columna de CATEGORIA a df_recupero si está disponible
        if has_recupero_data and 'N_ESTADO_PRESTAMO' in df_recupero.columns:
            df_recupero['CATEGORIA'] = 'Otros'
            for categoria, estados in ESTADO_CATEGORIAS.items():
                mask = df_recupero['N_ESTADO_PRESTAMO'].isin(estados)
                df_recupero.loc[mask, 'CATEGORIA'] = categoria

        # Check if df_localidad_municipio (likely a string) is not None and not an empty string
        has_localidad_municipio_data = df_localidad_municipio is not None and df_localidad_municipio != "" 
        
        # Renombrar valores en N_LINEA_PRESTAMO
        if has_global_data and 'N_LINEA_PRESTAMO' in df_global.columns:
            # Reemplazar "L4." por "INICIAR EMPRENDIMIENTO"
            df_global['N_LINEA_PRESTAMO'] = df_global['N_LINEA_PRESTAMO'].replace("L4.", "INICIAR EMPRENDIMIENTO")

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
                lambda x: 'ZONA FAVORECIDA' if x in zonas_favorecidas else 'ZONA REGULAR'
            )
        
        # Realizar el cruce entre df_global y df_recupero si ambos están disponibles
        if has_global_data and has_recupero_data and 'NRO_SOLICITUD' in df_recupero.columns:
            try:
                # Verificar si existen las columnas necesarias en df_recupero
                required_columns = ['FEC_NACIMIENTO','CUIL','NRO_SOLICITUD', 'DEUDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO','IMP_GANANCIAS','IMP_IVA','MONOTRIBUTO','INTEGRANTE_SOC','EMPLEADOR','ACTIVIDAD_MONOTRIBUTO']
                missing_columns = [col for col in required_columns if col not in df_recupero.columns]
                
                if not missing_columns:
                    # Seleccionar solo las columnas necesarias de df_recupero para el merge
                    df_recupero_subset = df_recupero[required_columns].copy()
                    
                    # Renombrar DEUDA como DEUDA_VENCIDA
                    df_recupero_subset = df_recupero_subset.rename(columns={'DEUDA': 'DEUDA_VENCIDA'})
                    
                    # Convertir columnas numéricas a tipo float
                    for col in ['DEUDA_VENCIDA', 'DEUDA_NO_VENCIDA', 'MONTO_OTORGADO']:
                        df_recupero_subset[col] = pd.to_numeric(df_recupero_subset[col], errors='coerce')

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
                    
                    # --- INICIO: Nuevo Merge con df_localidad_municipio ---
                    if df_localidad_municipio is not None and not df_localidad_municipio.empty:
                        # Definir columnas a traer desde df_localidad_municipio (incluyendo la clave)
                        cols_to_merge = [
                            'ID_LOCALIDAD', # Clave del merge (asumimos mismo nombre en ambos DFs)
                            'TIPO', 
                            'Gestion 2023-2027', 
                            'FUERZAS', 
                            'ESTADO', 
                            'LEGISLADOR DEPARTAMENTAL', 
                            'LATITUD', 
                            'LONGITUD'
                        ]
                        
                        # Verificar que la columna clave 'ID_LOCALIDAD' exista en ambos DataFrames
                        key_col = 'ID_LOCALIDAD'
                        if key_col not in df_global.columns:
                             st.warning(f"No se encontró la columna clave '{key_col}' en df_global para el cruce con df_localidad_municipio.")
                             can_merge = False
                        elif key_col not in df_localidad_municipio.columns:
                             st.warning(f"No se encontró la columna clave '{key_col}' en df_localidad_municipio para el cruce.")
                             can_merge = False
                        else:
                             can_merge = True

                        # Verificar que todas las columnas *a traer* existan en df_localidad_municipio
                        # (Excluimos la clave que ya verificamos)
                        missing_loc_cols = [col for col in cols_to_merge if col != key_col and col not in df_localidad_municipio.columns]
                        if missing_loc_cols:
                            st.warning(f"No se pudo realizar el cruce con df_localidad_municipio. Faltan columnas en df_localidad_municipio: {', '.join(missing_loc_cols)}")
                            can_merge = False
                        
                        if can_merge:
                            try:
                                # Seleccionar solo las columnas necesarias (incluida la clave)
                                df_localidad_subset = df_localidad_municipio[cols_to_merge].copy()
                                
                                # Realizar el segundo merge (left join) usando la misma clave
                                df_global = pd.merge(
                                    df_global,
                                    df_localidad_subset,
                                    on=key_col, # Usar 'on' ya que la clave tiene el mismo nombre
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
   
    
    # Mostrar columnas en modo desarrollo
    if is_development:
        st.markdown("***")
        st.caption("Información de Desarrollo (Columnas de DataFrames - Bco. Gente)")
        if isinstance(data, dict):
            for name, df_item in data.items(): # Renombrado df a df_item para claridad
                if df_item is not None and not df_item.empty: # Añadido chequeo de no vacío
                    with st.expander(f"Columnas en: `{name}`"):
                        st.write(f"Nombre del DataFrame: {name}")
                        st.write(f"Tipos de datos: {df_item.dtypes}")
                        st.write("Primeras 5 filas:")
                        
                        # Aplicar la corrección aquí para el df_item.head()
                        df_head_display = df_item.head()
                        if 'geometry' in df_head_display.columns:
                            st.dataframe(df_head_display.drop(columns=['geometry']))
                        else:
                            st.dataframe(df_head_display)
                        
                        st.write(f"Total de registros: {len(df_item)}")
                elif df_item is None:
                    st.warning(f"DataFrame '{name}' no cargado (es None).")
                else: # df_item is empty
                    st.info(f"DataFrame '{name}' está vacío.")
        else:
            st.warning("Formato de datos inesperado para Banco de la Gente (se esperaba un diccionario).")
        st.markdown("***")
    
    df_global = None
    df_recupero = None
    
     # Cargar y preprocesar datos
    df_global, df_recupero, geojson_data, df_localidad_municipio, has_global_data, has_recupero_data, has_geojson_data, has_localidad_municipio_data = load_and_preprocess_data(data)
    
    if is_development:
        st.write("Datos Globales ya cruzados (después de load_and_preprocess_data):")
        if df_global is not None and not df_global.empty: # Asegurarse que df_global existe
            if 'geometry' in df_global.columns:
                st.dataframe(df_global.drop(columns=['geometry']))
                df_to_download = df_global.drop(columns=['geometry'])
            else:
                st.dataframe(df_global)
                df_to_download = df_global
        import io
        csv = df_to_download.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Descargar CSV de Datos Globales",
            data=csv,
            file_name="datos_globales.csv",
            mime="text/csv"
        )
    # Verificar que los datos globales existan antes de continuar
    if not has_global_data:
        st.error("No se pudieron cargar los datos globales de Banco de la Gente. Verifique que el archivo 'vt_nomina_rep_dpto_localidad.parquet' exista en el repositorio.")
        return
    
    # Crear una copia del DataFrame para trabajar con él
    df_filtrado_global = df_global.copy()
    
    # Mostrar información de actualización de datos
    if dates and any(dates.values()):
        latest_date = max([d for d in dates.values() if d is not None], default=None)
        if latest_date:
            st.caption(f"Última actualización de datos: {latest_date}")
    
    # Crear pestañas para las diferentes vistas
    tab_global, tab_recupero = st.tabs(["GLOBAL", "RECUPERO"])
    
    with tab_global:
        # Filtros específicos para la pestaña GLOBAL
        if has_global_data:
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
                mostrar_global(df_filtrado_global_tab, TOOLTIPS_DESCRIPTIVOS, df_recupero)
            # --- NUEVA SECCIÓN: Mapa de Monto Otorgado por Localidad (Pagados) ---
            st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)
            st.subheader("Mapa de Monto Otorgado por Localidad (solo préstamos Pagados)")
            try:
                # Filtrar por CATEGORIA == "Pagados" y por línea seleccionada
                df_map = df_global[
                    (df_global["CATEGORIA"] == "Pagados") &
                    (df_global["N_LINEA_PRESTAMO"].isin(selected_lineas))
                ].copy()
                
                # Filtro interactivo por línea de préstamo
                lineas_disponibles = df_global["N_LINEA_PRESTAMO"].dropna().unique().tolist()
                lineas_disponibles.sort()
                selected_lineas = st.multiselect(
                    "Filtrar por Línea de Préstamo:",
                    options=lineas_disponibles,
                    default=lineas_disponibles,
                    key="filtro_linea_prestamo_mapa"
                )

                # Agrupar por localidad y sumar monto otorgado
                df_map_grouped = df_map.groupby([
                    "N_LOCALIDAD", "N_DEPARTAMENTO", "LATITUD", "LONGITUD"
                ], dropna=False)["MONTO_OTORGADO"].sum().reset_index()
                # Limpiar y convertir LATITUD y LONGITUD a float usando la función centralizada
                df_map_grouped = convert_decimal_separator(df_map_grouped, columns=["LATITUD", "LONGITUD"])
                # Reemplazar valores nulos de LATITUD y LONGITUD por las coordenadas de Córdoba Capital
                df_map_grouped["LATITUD"] = df_map_grouped["LATITUD"].fillna(-31.4135000)
                df_map_grouped["LONGITUD"] = df_map_grouped["LONGITUD"].fillna(-64.1810500)
                # Si quedan strings vacíos, convertirlos también
                df_map_grouped.loc[df_map_grouped["LATITUD"].astype(str).str.strip() == '', "LATITUD"] = -31.4135000
                df_map_grouped.loc[df_map_grouped["LONGITUD"].astype(str).str.strip() == '', "LONGITUD"] = -64.1810500
                # Convertir a float por seguridad
                df_map_grouped["LATITUD"] = df_map_grouped["LATITUD"].astype(float)
                df_map_grouped["LONGITUD"] = df_map_grouped["LONGITUD"].astype(float)
                if df_map_grouped.empty:
                    st.info("No hay datos de préstamos pagados con coordenadas para mostrar en el mapa.")
                else:
                    import plotly.express as px
                    col_mapa, col_tabla = st.columns([1, 3])
                    with col_mapa:
                        st.markdown("#### Mapa de Localidades")
                        fig = px.scatter_mapbox(
                            df_map_grouped,
                            lat="LATITUD",
                            lon="LONGITUD",
                            color="MONTO_OTORGADO",
                            size="MONTO_OTORGADO",
                            size_max=40,
                            hover_name="N_LOCALIDAD",
                            hover_data=None,
                            zoom=6,
                            mapbox_style="carto-positron",
                            color_continuous_scale="Viridis",
                            labels={
                                "N_LOCALIDAD": "Localidad",
                                "N_DEPARTAMENTO": "Departamento",
                                "MONTO_OTORGADO": "Monto Otorgado"
                            },
                            custom_data=[
                                "N_LOCALIDAD",
                                "N_DEPARTAMENTO",
                                "MONTO_OTORGADO",
                                "LATITUD",
                                "LONGITUD"
                            ]
                        )
                        fig.update_traces(
                            hovertemplate=
                                "<b>%{customdata[0]}</b><br>" +
                                "Departamento: %{customdata[1]}<br>" +
                                "Monto Otorgado: $%{customdata[2]:,.0f}<extra></extra>"
                        )
                        fig.update_layout(mapbox_center={"lat": -31.4167, "lon": -64.1833})
                        st.plotly_chart(fig, use_container_width=True)
                    with col_tabla:
                        st.markdown("#### Tabla de Localidades por monto otorgado")
                        styled_table = (
                            df_map_grouped[["N_LOCALIDAD", "N_DEPARTAMENTO", "MONTO_OTORGADO"]]
                            .sort_values("MONTO_OTORGADO", ascending=False)
                            .style
                            .background_gradient(cmap="Blues", subset=["MONTO_OTORGADO"])
                            .format({"MONTO_OTORGADO": "{:,.0f}"})
                        )
                        st.dataframe(
                            styled_table,
                            use_container_width=True,
                            hide_index=True
                        )
            except Exception as e:
                st.error(f"Error al generar el mapa de monto otorgado: {e}")
            

    with tab_recupero:
        # Filtros específicos para la pestaña RECUPERO
        if has_global_data and df_global is not None and not df_global.empty:
            st.markdown('<h3 style="font-size: 18px; margin-top: 0;">Filtros - RECUPERO</h3>', unsafe_allow_html=True)
            
            # Crear tres columnas para los filtros
            col1, col2, col3 = st.columns(3)
            
            # Filtro de departamento en la primera columna
            with col1:
                departamentos = sorted(df_filtrado_global['N_DEPARTAMENTO'].dropna().unique())
                all_dpto_option = "Todos los departamentos"
                selected_dpto_rec = st.selectbox("Departamento:", [all_dpto_option] + list(departamentos), key="recupero_dpto_filter")
            
            # Filtrar por departamento seleccionado
            if selected_dpto_rec != all_dpto_option:
                df_filtrado_recupero_tab = df_filtrado_global[df_filtrado_global['N_DEPARTAMENTO'] == selected_dpto_rec]
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
                localidades = sorted(df_filtrado_global['N_LOCALIDAD'].dropna().unique())
                all_loc_option = "Todas las localidades"
                df_filtrado_recupero_tab = df_filtrado_global
                
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
                mostrar_recupero(df_filtrado_recupero_tab, df_localidad_municipio, geojson_data)
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
    kpi_data = create_bco_gente_kpis(resultados, tooltips_categorias)
    # Agregar detalle_html a cada KPI si corresponde
    for kpi in kpi_data:
        categoria = kpi.get("title")
        estados = ESTADO_CATEGORIAS.get(categoria, [])
        total_categoria = resultados.get(categoria, 0)
        # Solo mostrar desglose si hay más de un estado definido y el total es mayor a 0
        if estados and len(estados) > 1:
            conteos_detalle = []
            for estado in estados:
                cantidad = int(df_filtrado_global[df_filtrado_global["N_ESTADO_PRESTAMO"] == estado].shape[0])
                conteos_detalle.append(f"<b>{estado}:</b> {cantidad}")
            detalle_html = "<div style='font-size:13px; color:#555; margin-bottom:0; margin-top:6px'>" + " | ".join(conteos_detalle) + "</div>"
            kpi["detalle_html"] = detalle_html
        else:
            kpi["detalle_html"] = ""
    display_kpi_row(kpi_data)

    # Desglose dinámico de TODOS los N_ESTADO_PRESTAMO agrupados por CATEGORIA_ESTADO
    grupos_detalle = []
    for categoria, estados in ESTADO_CATEGORIAS.items():
        if estados:
            estados_detalle = []
            for estado in estados:
                cantidad = int(df_filtrado_global[df_filtrado_global["N_ESTADO_PRESTAMO"] == estado].shape[0])
                estados_detalle.append(f"<b>{estado}:</b> {cantidad}")
            grupos_detalle.append(" ".join(estados_detalle))
    if grupos_detalle:
        detalle_html = "<div style='font-size:13px; color:#555; margin-bottom:8px; margin-top:6px'>" + " | ".join(grupos_detalle) + "</div>"
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
    
    # Crear tres columnas para los gráficos: Línea, Sexo, Edades
    col_torta_cat, col_torta_sexo, col_edades = st.columns(3)

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
                df_sexo = df_filtrado_global[
                    (df_filtrado_global['CATEGORIA'] == 'Pagados') & 
                    (df_filtrado_global['N_SEXO'].notna())
                ].copy()
                if df_sexo.empty:
                    st.warning("No hay datos disponibles para el gráfico de sexo después de filtrar NaNs.")
                else:
                    sexo_counts = df_sexo['N_SEXO'].value_counts().reset_index()
                    sexo_counts.columns = ['Sexo', 'Cantidad']
                    if sexo_counts.empty:
                        st.warning("No hay datos para mostrar en el gráfico de sexo.")
                    else:
                        fig_sexo = px.pie(
                            sexo_counts,
                            values='Cantidad',
                            names='Sexo',
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        fig_sexo.update_traces(
                            textposition='inside',
                            textinfo='percent+label',
                            hoverinfo='label+percent+value'
                        )
                        fig_sexo.update_layout(
                            title="Distribución por Sexo EN CREDITOS PAGADOS",
                            margin=dict(l=20, r=20, t=30, b=20)
                        )
                        st.plotly_chart(fig_sexo, use_container_width=True)
            else:
                st.write("Columnas disponibles:", df_filtrado_global.columns.tolist())
                st.warning("La columna 'N_SEXO' no está presente en el DataFrame.")
        except Exception as e:
            st.error(f"Error al generar el gráfico de sexo: {e}")

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
            if df_recupero is not None and 'FEC_NACIMIENTO' in df_recupero.columns and 'N_ESTADO_PRESTAMO' in df_recupero.columns:
                df_edades = df_recupero[['FEC_NACIMIENTO', 'N_ESTADO_PRESTAMO']].copy()
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
                # Calcular edad
                hoy = datetime.now().date()
                df_edades['EDAD'] = df_edades['FEC_NACIMIENTO'].apply(lambda x: hoy.year - x.year - ((hoy.month, hoy.day) < (x.month, x.day)) if pd.notnull(x) else None)
                # Definir rangos de edad
                bins = [0, 17, 25, 35, 45, 55, 65, 200]
                labels = ['<18', '18-25', '26-35', '36-45', '46-55', '56-65', '65+']
                df_edades['RANGO_EDAD'] = pd.cut(df_edades['EDAD'], bins=bins, labels=labels, right=True)
                conteo_edades = df_edades['RANGO_EDAD'].value_counts(sort=False).reset_index()
                conteo_edades.columns = ['Rango de Edad', 'Cantidad']
                fig_edades = px.bar(
                    conteo_edades,
                    x='Rango de Edad',
                    y='Cantidad',
                    title='Distribución por Rango de Edad',
                    color='Rango de Edad',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_edades.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_edades, use_container_width=True)
            else:
                st.warning("No hay datos de FEC_NACIMIENTO o N_ESTADO_PRESTAMO disponibles en df_recupero.")
        except Exception as e:
            st.error(f"Error al generar el gráfico de edades: {e}")

    # Línea divisoria para separar secciones
    st.markdown("<hr style='border: 2px solid #cccccc;'>", unsafe_allow_html=True)
            
    # Tabla de estados de préstamos agrupados
    st.subheader("Estados de Préstamos por Categoría", 
                 help="Muestra el conteo de préstamos agrupados por categorías de estado, "
                      "basado en los datos filtrados. Las categorías agrupa estados del sistema. No considera formularios de baja ni lineas antiguas históricas.")
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
            
            # --- Generar DataFrame extendido para descarga (con columnas extra, pero sin renderizarlas en pantalla) ---
            columnas_extra = [
                col for col in ['TIPO', 'Gestion 2023-2027', 'FUERZAS', 'ESTADO', 'LEGISLADOR DEPARTAMENTAL'] if col in df_filtrado_global.columns
            ]
            # Unir las columnas extra al DataFrame original (antes del agrupado)
            df_descarga = df_filtrado_global[
                ['N_DEPARTAMENTO', 'N_LOCALIDAD'] + columnas_extra + ['NRO_SOLICITUD', 'N_ESTADO_PRESTAMO']
            ].copy()
            # Agregar columna de categoría
            df_descarga['CATEGORIA'] = 'Otros'
            for categoria, estados in ESTADO_CATEGORIAS.items():
                mask = df_descarga['N_ESTADO_PRESTAMO'].isin(estados)
                df_descarga.loc[mask, 'CATEGORIA'] = categoria
            # Agrupar para obtener el conteo por las columnas extra y categoría
            df_descarga_grouped = df_descarga.groupby(
                ['N_DEPARTAMENTO', 'N_LOCALIDAD'] + columnas_extra + ['CATEGORIA']
            )['NRO_SOLICITUD'].count().reset_index()
            df_descarga_grouped = df_descarga_grouped.rename(columns={'NRO_SOLICITUD': 'Cantidad'})
            # --- Botón de descarga Excel con ícono ---
            import io
            import base64
            excel_buffer = io.BytesIO()
            df_descarga_grouped.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            excel_icon = """
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect width="20" height="20" rx="3" fill="#217346"/>
            <path d="M6.5 7.5H8L9.25 10L10.5 7.5H12L10.25 11L12 14.5H10.5L9.25 12L8 14.5H6.5L8.25 11L6.5 7.5Z" fill="white"/>
            </svg>
            """
            st.markdown(f'<span style="vertical-align:middle">{excel_icon}</span> <b>Descargar agrupado extendido (Excel)</b>', unsafe_allow_html=True)
            st.download_button(
                label="Descargar Excel",
                data=excel_buffer.getvalue(),
                file_name="estados_prestamos_categoria_extend.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Descargar el agrupado con todas las columnas extra para análisis avanzado."
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
            if df_recupero is None or df_recupero.empty: 
                st.info("No hay datos de recupero disponibles para la serie histórica.") 
            elif 'FEC_FORM' not in df_recupero.columns: 
                st.warning("La columna 'FEC_FORM' necesaria para la serie histórica no se encuentra en los datos de recupero.") 
            else: 
                # Verificar si existe la columna FEC_INICIO_PAGO
                tiene_fecha_inicio_pago = 'FEC_INICIO_PAGO' in df_recupero.columns
                
                # Preparar DataFrame de fechas de formulario
                df_fechas = df_recupero[['FEC_FORM']].copy()
                df_fechas['FEC_FORM'] = pd.to_datetime(df_fechas['FEC_FORM'], errors='coerce')
                df_fechas.dropna(subset=['FEC_FORM'], inplace=True)
                fecha_actual = datetime.now()
                df_fechas = df_fechas[df_fechas['FEC_FORM'] <= fecha_actual]
                fecha_min_valida = pd.to_datetime('1678-01-01')
                df_fechas_filtrado_rango = df_fechas[df_fechas['FEC_FORM'] >= fecha_min_valida].copy()
                
                # Preparar DataFrame de fechas de inicio de pago si existe la columna
                if tiene_fecha_inicio_pago:
                    df_fechas_pago = df_recupero[['FEC_INICIO_PAGO']].copy()
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
                        if tiene_datos_pago:
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
                                tabla_data = serie_historica[['FECHA', 'Cantidad']].copy()
                                tabla_data['Año'] = tabla_data['FECHA'].dt.year
                                tabla_data_agrupada = tabla_data.groupby('Año', as_index=False)['Cantidad'].sum()
                                tabla_data_agrupada = tabla_data_agrupada.sort_values('Año', ascending=False)

                                # Custom HTML table (like the others)
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
                                    </style>
                                """
                                html_table += '<table class="serie-table"><thead><tr>'
                                html_table += '<th>Año</th><th>Cantidad</th></tr></thead><tbody>'
                                for _, row in tabla_data_agrupada.iterrows():
                                    html_table += f'<tr><td>{row["Año"]}</td><td>{int(row["Cantidad"])}</td></tr>'
                                html_table += '</tbody></table>'
                                st.markdown(html_table, unsafe_allow_html=True)

                        
    except Exception as e:
        st.error(f"Error inesperado en la sección Serie Histórica: {e}")

    

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
    st.subheader("Detalle de Préstamos Pagados por Localidad", help="Muestra la suma de préstamos pagados, no finalizados, con planes de cuotas, por localidad")
    
    # Asegurarse de que las columnas necesarias existan en el df_filtrado
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