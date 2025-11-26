import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils.ui_components import display_kpi_row, show_last_update
from utils.styles import COLORES_IDENTIDAD, COLOR_PRIMARY, COLOR_SECONDARY, COLOR_ACCENT_1, COLOR_ACCENT_2, COLOR_ACCENT_3, COLOR_ACCENT_4, COLOR_ACCENT_5, COLOR_TEXT_DARK
from utils.kpi_tooltips import ESTADO_CATEGORIAS, TOOLTIPS_DESCRIPTIVOS
from utils.session_helper import safe_session_get, safe_session_set, safe_session_check

# Inicializar variables de sesión necesarias
if not safe_session_check("selected_categorias"):
    safe_session_set("selected_categorias", [])
if not safe_session_check("selected_lineas_credito"):
    safe_session_set("selected_lineas_credito", [])

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
            group = df_campo.groupby(campo, observed=True)["CUIL"].nunique().reset_index()
            group = group.rename(columns={"CUIL": "CUILs únicos", campo: campo})
            group = group.sort_values("CUILs únicos", ascending=False)
            st.dataframe(group, hide_index=True)

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
        st.plotly_chart(figs[0])
    with cols[1]:
        st.markdown(f"**{resumen_df.iloc[1]['Línea de Crédito']}**")
        st.plotly_chart(figs[1])
    with cols[2]:
        st.markdown("**Tabla resumen**")
        st.dataframe(resumen_df, hide_index=True)
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
            lineas_prestamo = sorted(df_filtrado_global['N_LINEA_PRESTAMO'].dropna().unique())
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
        show_last_update(dates, 'df_global_banco.parquet')
    
    # Mostrar columnas en modo desarrollo
    if is_development:
        from utils.ui_components import show_dev_dataframe_info
        show_dev_dataframe_info(data, modulo_nombre="Banco de la Gente", is_development=is_development)

    df_global = None
    df_global_pagados = None
    
    df_global = data.get('df_global_banco.parquet')
    df_global_pagados = data.get('df_global_pagados.parquet')
    
    
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
            
            # Asegurarse de que df_filtrado_recupero tenga todas las columnas calculadas necesarias
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
            df_filtrado_global.groupby("N_ESTADO_PRESTAMO", observed=True)
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
    # Se removió el detalle expandible de 'Condición ante ARCA' por solicitud
    mostrar_resumen_creditos(df_filtrado_global)
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
            grafico_torta = df_filtrado_torta.groupby('N_LINEA_PRESTAMO', observed=True).size().reset_index(name='Cantidad')
            
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
                st.plotly_chart(fig_torta)
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
                    df_sexo_con_categoria = df_sexo.groupby(['N_SEXO', 'CATEGORIA'], observed=True).size().reset_index(name='Cantidad')
                    
                    # Agrupar por sexo para el gráfico principal
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
                        st.plotly_chart(fig_sexo)
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
                    empleado_counts['Estado de Empleo'] = empleado_counts['Estado de Empleo'].map({
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
                        st.plotly_chart(fig_empleado)
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
                # Convertir a datetime y asegurar tz-naive antes de extraer .date
                df_edades['FEC_NACIMIENTO'] = pd.to_datetime(df_edades['FEC_NACIMIENTO'], format='%d/%m/%Y', errors='coerce')
                if df_edades['FEC_NACIMIENTO'].isna().all():
                    df_edades['FEC_NACIMIENTO'] = pd.to_datetime(df_edades['FEC_NACIMIENTO'], format='%Y-%m-%d', errors='coerce')
                try:
                    if pd.api.types.is_datetime64tz_dtype(df_edades['FEC_NACIMIENTO']):
                        df_edades['FEC_NACIMIENTO'] = df_edades['FEC_NACIMIENTO'].dt.tz_localize(None)
                except Exception:
                    pass
                df_edades['FEC_NACIMIENTO'] = df_edades['FEC_NACIMIENTO'].dt.date

                df_edades['FEC_FORM'] = pd.to_datetime(df_edades['FEC_FORM'], format='%d/%m/%Y', errors='coerce')
                if df_edades['FEC_FORM'].isna().all():
                    df_edades['FEC_FORM'] = pd.to_datetime(df_edades['FEC_FORM'], format='%Y-%m-%d', errors='coerce')
                try:
                    if pd.api.types.is_datetime64tz_dtype(df_edades['FEC_FORM']):
                        df_edades['FEC_FORM'] = df_edades['FEC_FORM'].dt.tz_localize(None)
                except Exception:
                    pass
                df_edades['FEC_FORM'] = df_edades['FEC_FORM'].dt.date
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
                st.plotly_chart(fig_edades)
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
                # Asegurar tz-naive antes de usar .dt.date
                df_categoria_estados['FEC_INICIO_PAGO'] = pd.to_datetime(df_categoria_estados['FEC_INICIO_PAGO'], format='%d/%m/%Y', errors='coerce')
                if df_categoria_estados['FEC_INICIO_PAGO'].isna().all():
                    df_categoria_estados['FEC_INICIO_PAGO'] = pd.to_datetime(df_categoria_estados['FEC_INICIO_PAGO'], format='%Y-%m-%d', errors='coerce')
                try:
                    if pd.api.types.is_datetime64tz_dtype(df_categoria_estados['FEC_INICIO_PAGO']):
                        df_categoria_estados['FEC_INICIO_PAGO'] = df_categoria_estados['FEC_INICIO_PAGO'].dt.tz_localize(None)
                except Exception:
                    pass
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
                    ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'CATEGORIA', 'N_LINEA_PRESTAMO'], observed=True
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
            st.dataframe(styled_df, hide_index=True)
            
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
                ['N_DEPARTAMENTO', 'N_LOCALIDAD', 'N_LINEA_PRESTAMO'] + columnas_extra + ['CATEGORIA'], observed=True
            ).agg({
                'NRO_SOLICITUD': 'count',
                'MONTO_OTORGADO': 'sum'
            }).reset_index()
            
            # Renombrar las columnas para mayor claridad
            df_descarga_grouped = df_descarga_grouped.rename(columns={
                'NRO_SOLICITUD': 'Cantidad',
                'MONTO_OTORGADO': 'Monto Total'
            })
            
            # Convertir columnas datetime con timezone a timezone-naive para Excel
            for col in df_descarga_grouped.columns:
                if pd.api.types.is_datetime64_any_dtype(df_descarga_grouped[col]):
                    try:
                        # Si la columna tiene timezone, removerla
                        if hasattr(df_descarga_grouped[col].dtype, 'tz') and df_descarga_grouped[col].dtype.tz is not None:
                            df_descarga_grouped[col] = df_descarga_grouped[col].dt.tz_localize(None)
                        elif hasattr(df_descarga_grouped[col].dt, 'tz') and df_descarga_grouped[col].dt.tz is not None:
                            df_descarga_grouped[col] = df_descarga_grouped[col].dt.tz_localize(None)
                    except Exception:
                        # Si hay algún error, intentar convertir de forma general
                        try:
                            df_descarga_grouped[col] = pd.to_datetime(df_descarga_grouped[col]).dt.tz_localize(None)
                        except Exception:
                            pass  # Si no se puede convertir, dejar como está
            
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
                df_fechas['FEC_FORM'] = pd.to_datetime(df_fechas['FEC_FORM'], format='%d/%m/%Y', errors='coerce')
                if df_fechas['FEC_FORM'].isna().all():
                    df_fechas['FEC_FORM'] = pd.to_datetime(df_fechas['FEC_FORM'], format='%Y-%m-%d', errors='coerce')
                df_fechas.dropna(subset=['FEC_FORM'], inplace=True)
                # Normalizar timezone: convertir a tz-naive para evitar comparaciones tz-aware vs tz-naive
                try:
                    if pd.api.types.is_datetime64tz_dtype(df_fechas['FEC_FORM']):
                        df_fechas['FEC_FORM'] = df_fechas['FEC_FORM'].dt.tz_localize(None)
                    else:
                        # Intentar eliminar cualquier tz implícita
                        try:
                            if hasattr(df_fechas['FEC_FORM'].dt, 'tz') and df_fechas['FEC_FORM'].dt.tz is not None:
                                df_fechas['FEC_FORM'] = df_fechas['FEC_FORM'].dt.tz_localize(None)
                        except Exception:
                            pass
                except Exception:
                    # Fallback general: intentar reconvertir y eliminar tz
                    try:
                        df_fechas['FEC_FORM'] = pd.to_datetime(df_fechas['FEC_FORM']).dt.tz_localize(None)
                    except Exception:
                        pass
                fecha_actual = datetime.now()
                df_fechas = df_fechas[df_fechas['FEC_FORM'] <= fecha_actual]
                fecha_min_valida = pd.to_datetime('1678-01-01')
                df_fechas_filtrado_rango = df_fechas[df_fechas['FEC_FORM'] >= fecha_min_valida].copy()
                
                # Preparar DataFrame de fechas de inicio de pago si existe la columna
                if tiene_fecha_inicio_pago:
                    df_fechas_pago = df_filtrado_global[['FEC_INICIO_PAGO']].copy()
                    df_fechas_pago['FEC_INICIO_PAGO'] = pd.to_datetime(df_fechas_pago['FEC_INICIO_PAGO'], format='%d/%m/%Y', errors='coerce')
                    if df_fechas_pago['FEC_INICIO_PAGO'].isna().all():
                        df_fechas_pago['FEC_INICIO_PAGO'] = pd.to_datetime(df_fechas_pago['FEC_INICIO_PAGO'], format='%Y-%m-%d', errors='coerce')
                    df_fechas_pago.dropna(subset=['FEC_INICIO_PAGO'], inplace=True)
                    # Normalizar timezone para FEC_INICIO_PAGO
                    try:
                        if pd.api.types.is_datetime64tz_dtype(df_fechas_pago['FEC_INICIO_PAGO']):
                            df_fechas_pago['FEC_INICIO_PAGO'] = df_fechas_pago['FEC_INICIO_PAGO'].dt.tz_localize(None)
                        else:
                            try:
                                if hasattr(df_fechas_pago['FEC_INICIO_PAGO'].dt, 'tz') and df_fechas_pago['FEC_INICIO_PAGO'].dt.tz is not None:
                                    df_fechas_pago['FEC_INICIO_PAGO'] = df_fechas_pago['FEC_INICIO_PAGO'].dt.tz_localize(None)
                            except Exception:
                                pass
                    except Exception:
                        try:
                            df_fechas_pago['FEC_INICIO_PAGO'] = pd.to_datetime(df_fechas_pago['FEC_INICIO_PAGO']).dt.tz_localize(None)
                        except Exception:
                            pass
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
                                serie_historica = df_fechas_seleccionado.groupby('AÑO_MES', observed=True).size().reset_index(name='Cantidad')
                                serie_historica['FECHA'] = serie_historica['AÑO_MES'].dt.to_timestamp()
                                serie_historica = serie_historica.sort_values('FECHA')
                            
                            # Preparar serie histórica de inicio de pagos
                            if tiene_datos_pago_filtrados:
                                df_fechas_pago_seleccionado['AÑO_MES'] = df_fechas_pago_seleccionado['FEC_INICIO_PAGO'].dt.to_period('M')
                                serie_historica_pago = df_fechas_pago_seleccionado.groupby('AÑO_MES', observed=True).size().reset_index(name='Cantidad')
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
                                    xaxis_tickformat='%B %Y',
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
                                
                                st.plotly_chart(fig_historia)
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
                                    tabla_data_form_agrupada = tabla_data_form.groupby('Año', as_index=False, observed=True)['Cantidad'].sum()
                                    
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
                                    tabla_data_pago_agrupada = tabla_data_pago.groupby('Año', as_index=False, observed=True)['Cantidad'].sum()
                                    
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

    # Importar numpy localmente para evitar problemas de alcance
    import numpy as np

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
                plot_bgcolor='white',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
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
            st.plotly_chart(fig)
            
            
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
        df_agrupado = df_filtrado_pagados.groupby(['N_DEPARTAMENTO', 'N_LOCALIDAD'], observed=True).agg(
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
        st.dataframe(df_agrupado)
        
    st.markdown("--- ") # Separador para la siguiente sección
