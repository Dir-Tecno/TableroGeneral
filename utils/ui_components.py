import streamlit as st

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
    if value_form is not None and value_pers is not None and value_form > 0 and value_pers > 0 and value_form != value_pers:
        formatted_value = f"{value_form} / {value_pers}"
    elif value_form is not None and value_form > 0:
        formatted_value = f"{value_form}"
    elif value_pers is not None and value_pers > 0:
        formatted_value = f"{value_pers}"
    else:
        formatted_value = str(value_form)
    
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
            "value_form": resultados.get("En Evaluación", 0),
            "color_class": "kpi-primary",
            "tooltip": tooltips.get("En Evaluación")
        },
        {
            "title": "FORMULARIOS A PAGAR / CONVOCATORIA",
            "categoria": "A Pagar - Convocatoria",
            "value_form": resultados.get("A Pagar - Convocatoria", 0),
            "color_class": "kpi-accent-3",
            "tooltip": tooltips.get("A Pagar - Convocatoria")
        },
        {
            "title": "FORMULARIOS PAGADOS",
            "categoria": "Pagados",
            "value_form": resultados.get("Pagados", 0),
            "color_class": "kpi-accent-2",
            "tooltip": tooltips.get("Pagados")
        },
        {
            "title": "FORMULARIOS EN PROCESO DE PAGO",
            "categoria": "En proceso de pago",
            "value_form": resultados.get("En proceso de pago", 0),
            "color_class": "kpi-accent-1",
            "tooltip": tooltips.get("En proceso de pago")
        },
        {
            "title": "FORMULARIOS PAGADOS - FINALIZADOS",
            "categoria": "Pagados-Finalizados",
            "value_form": resultados.get("Pagados-Finalizados", 0),
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
