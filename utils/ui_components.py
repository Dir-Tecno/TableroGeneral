import streamlit as st
import pandas as pd

def show_dev_dataframe_info(data, modulo_nombre="Módulo", info_caption=None):
    """
    Muestra información útil de uno o varios DataFrames en modo desarrollo.
    Args:
        data: pd.DataFrame o dict de DataFrames
        modulo_nombre: str, nombre del módulo
        info_caption: str, texto opcional para el caption
    """
    st.markdown("***")
    st.caption(info_caption or f"Información de Desarrollo ({modulo_nombre})")
    def _show_single(df, name):
        if df is None:
            st.warning(f"DataFrame '{name}' no cargado (es None).")
        elif hasattr(df, 'empty') and df.empty:
            st.info(f"DataFrame '{name}' está vacío.")
        elif hasattr(df, 'head') and hasattr(df, 'columns'):
            with st.expander(f"Columnas en: `{name}`"):
                st.write(f"Nombre del DataFrame: {name}")
                st.write(f"Shape: {df.shape}")
                st.write(f"Columnas: {', '.join(df.columns)}")
                st.write(f"Tipos de datos: {df.dtypes}")
                st.write("Primeras 5 filas:")
                df_head_display = df.head()
                if 'geometry' in df_head_display.columns:
                    st.dataframe(df_head_display.drop(columns=['geometry']))
                else:
                    st.dataframe(df_head_display)
                st.write(f"Total de registros: {len(df)}")
        else:
            st.warning(f"Objeto '{name}' no es un DataFrame válido (tipo: {type(df)})")
    if isinstance(data, dict):
        for name, df in data.items():
            _show_single(df, name)
    else:
        _show_single(data, "DataFrame")
    st.markdown("***")
    
def show_last_update(dates, file_substring, mensaje="Última actualización"):
    """
    Muestra la fecha de última actualización para un archivo específico.
    Args:
        dates: dict con fechas de actualización.
        file_substring: substring para buscar la clave relevante en dates.
        mensaje: texto a mostrar antes de la fecha.
    """
    file_dates = [dates.get(k) for k in dates.keys() if file_substring in k]
    latest_date = file_dates[0] if file_dates else None
    if latest_date:
        latest_date = pd.to_datetime(latest_date)
        try:
            from zoneinfo import ZoneInfo
            latest_date = latest_date.tz_localize('UTC').tz_convert(ZoneInfo('America/Argentina/Buenos_Aires'))
        except Exception:
            latest_date = latest_date - pd.Timedelta(hours=3)
        st.markdown(f"""
            <div style="background-color:#e9ecef; padding:10px; border-radius:5px; margin-bottom:20px; font-size:0.9em;">
                <i class="fas fa-sync-alt"></i> <strong>{mensaje}:</strong> {latest_date.strftime('%d/%m/%Y %H:%M')}
            </div>
        """, unsafe_allow_html=True)

def create_kpi_card(title, color_class="kpi-primary", delta=None, delta_color="#d4f7d4", tooltip=None, detalle_html=None, value_form=None, value_pers=None):
    """
    Crea una tarjeta KPI con un estilo consistente en toda la aplicación.
    
    Args:
        title (str): Título del KPI
        value (str/int/float): Valor principal a mostrar
        color_class (str): Clase CSS para el color de fondo (kpi-primary, kpi-secondary, kpi-accent-1, etc.)
        delta (str/int/float, optional): Valor de cambio a mostrar
        delta_color (str, optional): Color del texto delta
        tooltip (str, optional): Texto explicativo que se mostrará al pasar el cursor
        
    Returns:
        str: HTML formateado para la tarjeta KPI
    """
    # Mostrar el valor según value_form/value_pers si están presentes
    if value_form is not None and value_pers is not None and value_form != value_pers:
        formatted_value = f"{value_form} / {value_pers}"
    elif value_form is not None:
        formatted_value = f"{value_form}"
    elif value_pers is not None:
        formatted_value = f"{value_pers}"
    else:
        formatted_value = "0"
    
    # Agregar atributo title para el tooltip si está presente
    tooltip_attr = f' title="{tooltip}"' if tooltip else ''
    
    # Construir HTML para la tarjeta KPI
    html = f"""
        <div class="kpi-card {color_class}"{tooltip_attr}>
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{formatted_value}</div>
    """
    # Agregar detalle_html si está presente
    if detalle_html:
        html += f'{detalle_html}'
    # Agregar delta si está presente
    if delta is not None:
        # Determinar el símbolo basado en el valor delta
        if isinstance(delta, (int, float)):
            symbol = "↑" if delta >= 0 else "↓"
            delta_text = f"{symbol} {abs(delta):,}"
        else:
            # Si delta es un string, asumimos que ya tiene el formato deseado
            delta_text = delta
        
        html += f'<div style="font-size: 12px; margin-top: 5px; color: {delta_color};">{delta_text}</div>'
    html += "</div>"
    return html


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
        }
    ]
    return kpis

def display_kpi_row(kpi_data, num_columns=5):
    """
    kpi_data puede incluir opcionalmente el campo 'detalle_html' para mostrar debajo del valor principal.
    """
    """
    Muestra una fila de tarjetas KPI.
    
    Args:
        kpi_data (list): Lista de diccionarios con datos de KPI
                         [{"title": "Título", "value": valor, "color_class": "clase-css", "delta": delta, "tooltip": tooltip}, ...]
        num_columns (int): Número de columnas para mostrar los KPIs
    """
    cols = st.columns(num_columns)
    
    for i, kpi in enumerate(kpi_data):
        col_index = i % num_columns
        with cols[col_index]:
            st.markdown(
                create_kpi_card(
                    title=kpi.get("title", ""),
                    color_class=kpi.get("color_class", "kpi-primary"),
                    delta=kpi.get("delta"),
                    delta_color=kpi.get("delta_color", "#d4f7d4"),
                    tooltip=kpi.get("tooltip"),
                    detalle_html=kpi.get("detalle_html"),
                    value_form=kpi.get("value_form"),
                    value_pers=kpi.get("value_pers")
                ),
                unsafe_allow_html=True
            )
