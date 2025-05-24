import streamlit as st
import pandas as pd

def show_emprendimientos_dashboard(data, dates, is_development=False):
    """
    Muestra el dashboard de Emprendimientos. Estructura compatible con app.py y los otros módulos.

    Args:
        data: Diccionario de dataframes cargados
        dates: Diccionario con fechas de actualización
        is_development: Booleano, True si está en modo desarrollo
    """
    if data is None or not data:
        st.error("No se pudieron cargar los datos de Emprendimientos.")
        return

    from utils.ui_components import show_dev_dataframe_info  # Mostrar columnas en modo desarrollo
    if is_development:
        show_dev_dataframe_info(data, modulo_nombre="Emprendimientos")

    st.header("Dashboard de Emprendimientos")
    st.markdown("### (En construcción)")
    st.info("Aquí se mostrarán las visualizaciones y KPIs de los programas de emprendimientos una vez definidos los requerimientos de negocio y la estructura de los datos.")
