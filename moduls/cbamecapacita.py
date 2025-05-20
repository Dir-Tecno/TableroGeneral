import streamlit as st
import pandas as pd
import plotly.express as px
from utils.ui_components import display_kpi_row
from utils.data_cleaning import clean_thousand_separator, convert_decimal_separator
import geopandas as gpd
import json

def load_and_preprocess_data(data):
    """
    Carga y preprocesa los datos principales del dashboard CBA ME CAPACITA.
    - Limpia separadores de miles en columnas numéricas.
    - Devuelve los DataFrames listos para usar.
    """
    # Cargar DataFrames principales
    df_postulantes = None
    df_cursos = None
    if isinstance(data, dict):
        df_postulantes = data.get("VT_INSCRIPCIONES_PRG129.parquet")
        df_cursos = data.get("VT_CURSOS_SEDES_GEO.parquet")
    elif isinstance(data, list):
        for df in data:
            if "CUIL" in df.columns:
                df_postulantes = df
            if "ID_PLANIFICACION" in df.columns and "N_CURSO" in df.columns:
                df_cursos = df
    # Limpieza de separador de miles en ambos DataFrames
    df_postulantes = clean_thousand_separator(df_postulantes)
    df_cursos = clean_thousand_separator(df_cursos)

    return df_postulantes, df_cursos

def show_cba_capacita_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de CBA ME CAPACITA.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
    if data is None:
        st.error("No se pudieron cargar los datos de CBA ME CAPACITA.")
        return

    # Mostrar columnas en modo desarrollo
    if is_development:
        st.markdown("***")
        st.caption("Información de Desarrollo (Columnas de DataFrames - CBA Me Capacita)")
        if isinstance(data, dict):
            for name, df_item in data.items(): # Usar df_item para claridad
                if df_item is not None and not df_item.empty:
                    with st.expander(f"Columnas en: `{name}`"):
                        st.write(f"Nombre del DataFrame: {name}")
                        st.write(f"Tipos de datos: {df_item.dtypes}")
                        st.write("Primeras 5 filas:")
                        
                        # Aplicar la corrección aquí para df_item.head()
                        # Esta es la sección que corresponde a la línea 56 del traceback
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
            st.warning("Formato de datos inesperado para CBA Me Capacita (se esperaba un diccionario).")
        st.markdown("***")

    

    # --- Usar función de carga y preprocesamiento ---
    df_postulantes, df_cursos = load_and_preprocess_data(data)

    # Marcador de posición para la implementación real
    st.info("Dashboard de CBA ME CAPACITA en desarrollo.")
    
    # Mostrar información de actualización de datos
    if dates and any(dates.values()):
        latest_date = max([d for d in dates.values() if d is not None], default=None)
        if latest_date:
            st.caption(f"Última actualización de datos: {latest_date}")
    
    # KPIs reales usando VT_INSCRIPCIONES_PRG129.parquet (postulantes) y VT_CURSOS_SEDES_GEO.parquet (cursos)
    total_postulantes = df_postulantes["CUIL"].nunique() if df_postulantes is not None else 0
    cursos_activos = df_cursos["ID_PLANIFICACION"].nunique() if df_cursos is not None else 0
    total_capacitaciones = df_postulantes["ID_CAPACITACION"].nunique() if df_postulantes is not None and "ID_CAPACITACION" in df_postulantes.columns else 0

    def safe_format(val):
        try:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return "0"
            return f"{int(val):,}"
        except Exception:
            return str(val) if val is not None else "0"

    kpi_data = [
        {
            "title": "Postulantes",
            "value_form": f"{total_postulantes:,}".replace(',', '.'),
            "color_class": "kpi-primary",
            "delta": "",
            "delta_color": "#d4f7d4"
        },
        {
            "title": "Cursos Activos",
            "value_form": f"{cursos_activos:,}".replace(',', '.'),
            "color_class": "kpi-secondary",
            "delta": "",
            "delta_color": "#d4f7d4"
        },
        {
            "title": "Capacitaciones Elegidas",
            "value_form": f"{total_capacitaciones:,}".replace(',', '.'),
            "color_class": "kpi-accent-2",
            "delta": "",
            "delta_color": "#d4f7d4"
        }
    ]
    display_kpi_row(kpi_data)

    # Crear pestañas para diferentes vistas
    tab1, tab2 = st.tabs(["Alumnos", "Cursos"])
    
    with tab1:
        st.subheader("Análisis de Alumnos")
        st.write("A la espera de postulaciones y participantes...")
    
    with tab2:
        st.markdown("## Sector Productivos por Departamento")
        # Botón para descargar el Excel con columnas seleccionadas
        df_cursos = None
        if isinstance(data, dict):
            df_cursos = data.get("VT_CURSOS_SEDES_GEO.parquet")
        elif isinstance(data, list):
            for df in data:
                if "N_SEDE" in df.columns and "LATITUD" in df.columns:
                    df_cursos = df
                    break
        # Mostrar botón solo si existe el DataFrame
        if df_cursos is not None:
            import io
            import pandas as pd
            columnas_exportar = [
                "SEPE_CURSOS.FC_OBTENER_NOMBRE_INSTITUCION(INS.CUIT,INS.CUE)",
                "N_CURSO",
                "HORA_INICIO",
                "HORA_FIN",
                "N_SECTOR_PRODUCTIVO",
                "N_SEDE",
                "CONVENIO_MUNICIPIO_COMUNA",
                "N_DEPARTAMENTO",
                "N_LOCALIDAD"
            ]
            # Filtrar solo columnas existentes
            columnas_existentes = [col for col in columnas_exportar if col in df_cursos.columns]
            df_export = df_cursos[columnas_existentes].copy()
            buffer = io.BytesIO()
            df_export.to_excel(buffer, index=False)
            st.download_button(
                label="Descargar Excel de Cursos (columnas seleccionadas)",
                data=buffer.getvalue(),
                file_name="cursos_sector_productivo.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Obtener GeoJSON de departamentos
            geojson_departamentos = None
            if isinstance(data, dict):
                geojson_departamentos = data.get("capa_departamentos_2010.geojson")
            elif isinstance(data, list):
                for df in data:
                    if isinstance(df, dict) and "features" in df:
                        geojson_departamentos = df
                        break

           # Limpiar y convertir LATITUD y LONGITUD
            df_cursos = convert_decimal_separator(df_cursos, columns=["LATITUD", "LONGITUD"])

            # Asegúrate de que los valores sean strings antes de usar .str.extract()
            for col in ["LATITUD", "LONGITUD"]:
                df_cursos[col] = df_cursos[col].astype(str)  # Convertir a string
                df_cursos[col] = df_cursos[col].str.extract(r"(-?\d+\.\d+)").astype(float)
                df_cursos = df_cursos.dropna(subset=["LATITUD", "LONGITUD"])

            # Agrupar y contar para tabla (incluye ID_DEPARTAMENTO para relación con geojson)
            df_agrupado_tabla = df_cursos.groupby([
                "ID_DEPARTAMENTO", "N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO"
            ]).size().reset_index(name="Cantidad")

            # --- NUEVO MAPA: Choropleth por Departamento ---
            if geojson_departamentos is not None and 'ID_DEPARTAMENTO' in df_agrupado_tabla.columns:
                # Sumar cantidad por departamento
                choropleth_data = df_agrupado_tabla.groupby(['ID_DEPARTAMENTO', 'N_DEPARTAMENTO'], as_index=False)["Cantidad"].sum()
                # Agregar columna de sectores productivos agregados por departamento
                sectores_por_depto = df_agrupado_tabla.groupby(['ID_DEPARTAMENTO', 'N_DEPARTAMENTO'])['N_SECTOR_PRODUCTIVO'].apply(lambda x: ', '.join(sorted(set(x)))).reset_index(name='SectoresProductivos')
                choropleth_data = choropleth_data.merge(sectores_por_depto, on=['ID_DEPARTAMENTO', 'N_DEPARTAMENTO'], how='left')
                col_map_depto, col_tabla_depto = st.columns([2, 3])
                with col_map_depto:
                    st.markdown("### Mapa por Departamento (Sectores Productivos)")
                    import plotly.express as px
                    fig_choro = px.choropleth_mapbox(
                        choropleth_data,
                        geojson=geojson_departamentos,
                        locations='ID_DEPARTAMENTO',
                        featureidkey="properties.CODDEPTO",
                        color='Cantidad',
                        hover_name='N_DEPARTAMENTO',
                        hover_data={
                            'Cantidad': True,
                            'SectoresProductivos': True,
                        },
                        mapbox_style="carto-positron",
                        color_continuous_scale="YlGnBu",
                        opacity=0.7,
                        zoom=6,
                        center={"lat":-31.4, "lon":-64.2}, # Córdoba centro aprox
                    )
                    fig_choro.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
                    st.plotly_chart(fig_choro, use_container_width=True)
                with col_tabla_depto:
                    st.markdown("### Tabla Sector Productivo por Departamento")
                    st.dataframe(df_agrupado_tabla[["N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO", "Cantidad"]], use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_agrupado_tabla[["N_DEPARTAMENTO", "N_SECTOR_PRODUCTIVO", "Cantidad"]], use_container_width=True, hide_index=True)

            # Agrupar y contar para mapa de sedes
            df_agrupado_mapa = df_cursos.groupby([
                "N_SEDE", "N_LOCALIDAD", "N_DEPARTAMENTO", "LATITUD", "LONGITUD"
            ]).size().reset_index(name="Cantidad")

            # Mapa de sedes
            col_mapa, col_tabla = st.columns([1, 3])

            with col_mapa:
                st.markdown("### Mapa de Sedes")
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
                st.plotly_chart(fig, use_container_width=True)

            with col_tabla:
                st.markdown("### Cantidad de Cursos por Departamento y Localidad")
                df_cursos_depto_loc = df_cursos.groupby([
                    "N_DEPARTAMENTO", "N_LOCALIDAD"
                ]).size().reset_index(name="Cantidad")
                styled_table = (
                    df_cursos_depto_loc[["N_DEPARTAMENTO", "N_LOCALIDAD", "Cantidad"]].style
                    .background_gradient(cmap="Blues")
                    .format({"Cantidad": "{:,.0f}"})
                )
                st.dataframe(
                    styled_table,
                    use_container_width=True,
                    hide_index=True  # Si tu Streamlit es 1.22 o superior
                )
        else:
            st.warning("No se encontró el DataFrame de cursos con la estructura esperada.")