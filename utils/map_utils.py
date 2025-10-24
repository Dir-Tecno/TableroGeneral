import streamlit as st
import pandas as pd
import json
import io

import geopandas as gpd
import plotly.express as px
import folium
from streamlit_folium import folium_static

def load_geojson(geojson_data):
    """
    Carga datos GeoJSON de manera segura, ya sea desde un GeoDataFrame o un diccionario
    
    Args:
        geojson_data: Datos GeoJSON (puede ser un GeoDataFrame, un diccionario o bytes)
        
    Returns:
        dict: Datos GeoJSON en formato diccionario
    """
    try:
        # Si es un GeoDataFrame, convertirlo a diccionario GeoJSON
        if isinstance(geojson_data, gpd.GeoDataFrame):
            return json.loads(geojson_data.to_json())
            
        # Si es bytes, intentar leerlo como GeoDataFrame y luego convertirlo
        elif isinstance(geojson_data, bytes):
            gdf = gpd.read_file(io.BytesIO(geojson_data))
            return json.loads(gdf.to_json())
            
        # Si es un diccionario, verificar que tenga la estructura correcta
        elif isinstance(geojson_data, dict) and 'features' in geojson_data:
            return geojson_data
            
        # Si es una cadena, intentar parsearlo como JSON
        elif isinstance(geojson_data, str):
            return json.loads(geojson_data)
            
        else:
            st.error(f"Formato de datos GeoJSON no reconocido: {type(geojson_data)}")
            return None
    except Exception as e:
        st.warning(f"Error al cargar datos GeoJSON: {e}")
        return None

def create_choropleth_map(df, geojson_data, location_field, color_field, title=None, center=None):
    """
    Crea un mapa coroplético con Plotly Express
    
    Args:
        df: DataFrame con los datos a mostrar
        geojson_data: Datos GeoJSON (puede ser un GeoDataFrame, un diccionario o bytes)
        location_field: Nombre de la columna en df que contiene los identificadores de ubicación
        color_field: Nombre de la columna en df que contiene los valores para colorear
        title: Título del mapa (opcional)
        center: Diccionario con lat y lon para centrar el mapa (opcional)
        
    Returns:
        fig: Figura de Plotly o None si hay un error
    """
    try:
        geojson_dict = load_geojson(geojson_data)
        if geojson_dict is None:
            return None

        if df.empty:
            st.warning("No hay datos para mostrar en el mapa.")
            return None

        # Forzar el uso de ID_DPTO <-> CODDEPTO si ambos existen
        feature_id_key = None
        if 'features' in geojson_dict and geojson_dict['features']:
            sample_props = geojson_dict['features'][0]['properties']
            if 'CODDEPTO' in sample_props and 'ID_DPTO' in df.columns:
                feature_id_key = 'properties.CODDEPTO'
                location_field = 'ID_DPTO'
                # Asegurar que ambos sean string
                df['ID_DPTO'] = df['ID_DPTO'].astype(str)
                for f in geojson_dict['features']:
                    f['properties']['CODDEPTO'] = str(f['properties']['CODDEPTO'])
            else:
                # Fallback automático (como antes)
                first_key = list(sample_props.keys())[0]
                feature_id_key = f'properties.{first_key}'

            st.info(f"Relacionando {location_field} (df) con {feature_id_key} (GeoJSON)")

            if center is None:
                center = {"lat": -31.4, "lon": -64.2}

            fig = px.choropleth_mapbox(
                df,
                geojson=geojson_dict,
                locations=location_field,
                featureidkey=feature_id_key,
                color=color_field,
                color_continuous_scale="Blues",
                mapbox_style="carto-positron",
                zoom=6,
                center=center,
                opacity=0.7,
                labels={color_field: color_field}
            )

            if title:
                fig.update_layout(title=title)

            return fig
        else:
            st.error("El GeoJSON no tiene la estructura esperada (features).")
            return None
    except Exception as e:
        st.error(f"Error al crear el mapa: {str(e)}")
        return None

def display_map(fig):
    """
    Muestra un mapa de Plotly en Streamlit
    
    Args:
        fig: Figura de Plotly
    """
    if fig is not None:
        st.plotly_chart(fig)
    else:
        st.error("No se pudo crear el mapa.")
