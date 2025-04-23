import streamlit as st
import pandas as pd
import plotly.express as px
from utils.ui_components import display_kpi_row
import geopandas as gpd
import json

def show_cba_capacita_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de CBA ME CAPACITA.
    
    Args:
        data: Diccionario de dataframes.
        dates: Diccionario con fechas de actualización.
        is_development (bool): True si se está en modo desarrollo.
    """
    try:
        if data is None:
            st.error("No se pudieron cargar los datos de CBA ME CAPACITA.")
            return

        # Mostrar columnas en modo desarrollo
        if is_development:
            st.markdown("***")
            st.caption("Información de Desarrollo (Columnas de DataFrames - CBA Capacita)")
            if isinstance(data, dict):
                for name, df in data.items():
                    if df is not None:
                        with st.expander(f"Columnas en: `{name}`"):
                            st.write(df.columns.tolist())
                    else:
                        st.warning(f"DataFrame '{name}' no cargado o vacío.")
            else:
                st.warning("Formato de datos inesperado para CBA Me Capacita.")
            st.markdown("***")

        # Marcador de posición para la implementación real
        st.info("Dashboard de CBA ME CAPACITA en desarrollo.")
        
        # Mostrar información de actualización de datos
        if dates and any(dates.values()):
            latest_date = max([d for d in dates.values() if d is not None], default=None)
            if latest_date:
                st.caption(f"Última actualización de datos: {latest_date}")
        
        # KPIs reales usando VT_INSCRIPCIONES_PRG129.parquet (postulantes) y VT_CURSOS_SEDES_GEO.parquet (cursos)
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
        total_postulantes = df_postulantes["CUIL"].nunique() if df_postulantes is not None else 0
        cursos_activos = df_cursos["ID_PLANIFICACION"].nunique() if df_cursos is not None else 0
        total_capacitaciones = df_postulantes["ID_CAPACITACION"].nunique() if df_postulantes is not None and "ID_CAPACITACION" in df_postulantes.columns else 0

        kpi_data = [
            {
                "title": "Postulantes",
                "value": f"{total_postulantes:,}",
                "color_class": "kpi-primary",
                "delta": "",
                "delta_color": "#d4f7d4"
            },
            {
                "title": "Cursos Activos",
                "value": f"{cursos_activos:,}",
                "color_class": "kpi-secondary",
                "delta": "",
                "delta_color": "#d4f7d4"
            },
            {
                "title": "Capacitaciones Elegidas",
                "value": f"{total_capacitaciones:,}",
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
            # Gráfico de ejemplo
            chart_data = pd.DataFrame({
                "Localidad": ["Córdoba", "Villa María", "Río Cuarto", "Carlos Paz", "Otros"],
                "Alumnos": [2500, 1200, 800, 600, 578]
            })
            fig = px.bar(chart_data, x="Localidad", y="Alumnos", title="Alumnos por Localidad")
            st.plotly_chart(fig)
            
            # Distribución por edad
            age_data = pd.DataFrame({
                "Rango": ["18-25", "26-35", "36-45", "46-55", "56+"],
                "Cantidad": [1800, 2200, 1000, 500, 178]
            })
            fig = px.pie(age_data, values='Cantidad', names='Rango', title='Distribución por Edad')
            st.plotly_chart(fig)
        
        with tab2:
            st.markdown("## Cursos por Sede, Localidad y Departamento")
            # Obtener el DataFrame correspondiente
            df_cursos = None
            if isinstance(data, dict):
                df_cursos = data.get("VT_CURSOS_SEDES_GEO.parquet")
            elif isinstance(data, list):
                for df in data:
                    if "N_SEDE" in df.columns and "LATITUD" in df.columns:
                        df_cursos = df
                        break
            if df_cursos is not None:
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
                for col in ["LATITUD", "LONGITUD"]:
                    df_cursos[col] = (
                        df_cursos[col]
                        .astype(str)
                        .str.replace(",", ".", regex=False)
                        .str.extract(r"(-?\d+\.\d+)")
                        .astype(float)
                    )
                df_cursos = df_cursos.dropna(subset=["LATITUD", "LONGITUD"])

                # Agrupar y contar para tabla
                df_agrupado_tabla = df_cursos.groupby([
                    "N_DEPARTAMENTO"
                ]).size().reset_index(name="Cantidad")
                st.dataframe(df_agrupado_tabla, use_container_width=True)

                # Agrupar y contar
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
                        # Convertir a dict si es string o GeoDataFrame
                        if isinstance(geojson_departamentos, gpd.GeoDataFrame):
                            geojson_departamentos = json.loads(geojson_departamentos.to_json())
                        elif isinstance(geojson_departamentos, str):
                            geojson_departamentos = json.loads(geojson_departamentos)

                        # Agregar la capa del contorno a las capas existentes (si las hay)
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
                    st.markdown("### Cantidad de Cursos por Departamento")
                    styled_table = (
                        df_agrupado_tabla.style
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
            
    except Exception as e:
        st.error(f"Error al mostrar el dashboard de CBA ME CAPACITA: {str(e)}")