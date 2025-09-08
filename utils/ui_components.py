import streamlit as st
import pandas as pd
import requests
import datetime
from utils.session_helper import safe_session_get, safe_session_set, safe_session_check, is_session_initialized

# Inicializar variables de sesión necesarias
if "mostrar_form_comentario" not in st.session_state:
    st.session_state["mostrar_form_comentario"] = False
if "campanita_mostrada" not in st.session_state:
    st.session_state["campanita_mostrada"] = False

def show_dev_dataframe_info(data, modulo_nombre="Módulo", info_caption=None, is_development=False):
    """
    Muestra información útil de uno o varios DataFrames en modo desarrollo.
    Args:
        data: pd.DataFrame o dict de DataFrames
        modulo_nombre: str, nombre del módulo
        info_caption: str, texto opcional para el caption
        is_development: bool, indica si estamos en modo desarrollo
    """
    # Mostrar información solo si estamos en modo desarrollo
    if is_development:
        st.write(f"**{info_caption or f'Información de Desarrollo ({modulo_nombre})'}**")
        
        def _show_single(df, name):
            if df is None:
                st.write(f"- DataFrame '{name}' no cargado (es None).")
            elif hasattr(df, 'empty') and df.empty:
                st.write(f"- DataFrame '{name}' está vacío.")
            elif hasattr(df, 'head') and hasattr(df, 'columns'):
                # Crear un expander para este DataFrame
                with st.expander(f"📊 DataFrame: {name} ({df.shape[0]} filas, {df.shape[1]} columnas)", expanded=False):
                    st.write(f"- **Shape**: {df.shape}")
                    st.write(f"- **Columnas**: {', '.join(df.columns)}")
                    
                    # Verificar si es un GeoDataFrame (tiene columna 'geometry')
                    if 'geometry' in df.columns:
                        # Es un GeoDataFrame - mostrar información sin la columna geometry
                        st.write(f"- **Total de registros**: {len(df)}")
                        st.write("- **Muestra de datos (10 primeras filas sin geometría):**")
                        # Crear copia sin geometría para mostrar
                        df_display = df.drop(columns=['geometry']).head(10)
                        st.dataframe(df_display)
                    else:
                        # DataFrame normal - mostrar muestra de datos
                        st.write(f"- **Total de registros**: {len(df)}")
                        st.write("- **Muestra de datos (10 primeras filas):**")
                        st.dataframe(df.head(10))
                    
                    # Mostrar estadísticas básicas para columnas numéricas
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        st.write("- **Estadísticas básicas para columnas numéricas:**")
                        try:
                            stats_df = df[numeric_cols].describe().T
                            st.dataframe(stats_df)
                        except:
                            st.write("  (No se pueden mostrar estadísticas para este DataFrame)")
            else:
                # Mostrar como objeto genérico si no es un DataFrame
                with st.expander(f"🔍 Objeto: {name}", expanded=False):
                    st.write(f"- **Tipo**: {type(df)}")
        
        if isinstance(data, dict):
            for name, df in data.items():
                _show_single(df, name)
        else:
            _show_single(data, "DataFrame")
    
def show_last_update(dates, file_substring, mensaje="Última actualización"):
    """
    Muestra la fecha de última actualización para un archivo específico con zona horaria de Argentina.
    Args:
        dates: dict con fechas de actualización (fechas de commit de GitLab).
        file_substring: substring para buscar la clave relevante en dates.
        mensaje: texto a mostrar antes de la fecha.
    """
    if not dates:
        return
        
    file_dates = [dates.get(k) for k in dates.keys() if file_substring in k]
    latest_date = file_dates[0] if file_dates else None
    
    if latest_date:
        # Convertir a pandas datetime
        latest_date = pd.to_datetime(latest_date)
        
        # Aplicar zona horaria de Argentina (UTC-3)
        try:
            # Intentar usar zoneinfo (Python 3.9+)
            from zoneinfo import ZoneInfo
            if latest_date.tz is None:
                # Si la fecha no tiene zona horaria, asumimos que es UTC
                latest_date = latest_date.tz_localize('UTC')
            # Convertir a hora de Argentina
            latest_date = latest_date.tz_convert(ZoneInfo('America/Argentina/Buenos_Aires'))
        except ImportError:
            # Fallback para versiones anteriores de Python
            try:
                import pytz
                if latest_date.tz is None:
                    latest_date = latest_date.tz_localize('UTC')
                argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
                latest_date = latest_date.tz_convert(argentina_tz)
            except ImportError:
                # Fallback simple: restar 3 horas si no hay zona horaria
                if latest_date.tz is None:
                    latest_date = latest_date - pd.Timedelta(hours=3)
        
        # Formatear la fecha para mostrar
        fecha_formateada = latest_date.strftime('%d/%m/%Y %H:%M')
        
        st.markdown(f"""
            <div style="background-color:#e9ecef; padding:10px; border-radius:5px; margin-bottom:20px; font-size:0.9em;">
                <i class="fas fa-sync-alt"></i> <strong>{mensaje}:</strong> {fecha_formateada} (Hora Argentina)
            </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
            <div style="background-color:#ffecec; padding:10px; border-radius:5px; margin-bottom:20px; font-size:0.9em; color:#a94442;">
                <i class="fas fa-exclamation-circle"></i> <strong>{mensaje}:</strong> No disponible
            </div>
        """, unsafe_allow_html=True)


def enviar_a_slack(mensaje, valoracion):
    """
    Envía un mensaje a Slack con la valoración del usuario.
    
    Args:
        mensaje: El mensaje del usuario
        valoracion: La valoración del 1 al 5
    
    Returns:
        bool: True si el mensaje se envió correctamente, False en caso contrario
    """
    try:
        # URL del webhook de Slack (se obtiene desde secrets)
        try:
            webhook_url = st.secrets["slack"]["webhook_url"]
        except Exception:
            webhook_url = "https://hooks.slack.com/services/your/webhook/url"
            st.warning("No se encontró la URL del webhook de Slack en secrets. Se usará una URL de ejemplo.")
        
        # Crear el mensaje con formato
        estrellas = "⭐" * valoracion
        payload = {
            "text": f"*Nueva valoración del reporte:* {estrellas}\n*Comentario:* {mensaje}"
        }
        
        # Enviar la solicitud POST a Slack
        response = requests.post(webhook_url, json=payload)
        
        # Verificar si la solicitud fue exitosa
        return response.status_code == 200
    except Exception as e:
        st.error(f"Error al enviar a Slack: {str(e)}")
        return False


def render_footer():
    """
    Renderiza un footer optimizado con formulario de comentarios simplificado.
    Mantiene integración con Slack pero con mejor rendimiento.
    """
    st.markdown("""<hr style='margin-top: 50px; margin-bottom: 20px;'>""", unsafe_allow_html=True)
    
    # Footer principal con texto y botón inline
    st.markdown("""
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
            <div style="color: #666; font-size: 0.9em;">
                Realizado con 🧡 por la Dirección de Tecnología y Análisis de Datos del Ministerio de Desarrollo Social y Promoción del Empleo.
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # Usar expander para el formulario de comentarios (más eficiente que session_state)
    with st.expander("💬 Dejar comentario", expanded=False):
        # Formulario simplificado en una sola columna
        comentario = st.text_area("Tu comentario:", height=80, placeholder="Comparte tu opinión sobre este dashboard...")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            valoracion = st.selectbox("Valoración:", options=[1, 2, 3, 4, 5], index=2, format_func=lambda x: "⭐" * x)
        
        with col2:
            enviar = st.button("Enviar", type="primary", use_container_width=True)
        
        if enviar:
            if comentario.strip():
                with st.spinner("Enviando..."):
                    if enviar_a_slack(comentario, valoracion):
                        st.success("¡Comentario enviado!")
                        st.balloons()
                    else:
                        st.error("Error al enviar. Intenta de nuevo.")
            else:
                st.warning("Escribe un comentario.")

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


def show_notification_bell(novedades=None):
    """
    Muestra una campanita con novedades del tablero usando st.expander.
    
    Args:
        novedades (list): Lista de diccionarios con novedades
                         [{"titulo": "Título", "descripcion": "Descripción", "fecha": "YYYY-MM-DD", "modulo": "Nombre del módulo"}, ...]
    """
    try:
        # Verificar explícitamente si la sesión está inicializada antes de cualquier acceso
        if not is_session_initialized():
            # No mostrar la campanita si la sesión no está inicializada
            return
        
        # Evitar duplicación usando un identificador único en session_state seguro
        if safe_session_check("campanita_mostrada"):
            return
        
        # Marcar que ya se mostró la campanita
        safe_session_set("campanita_mostrada", True)
    except Exception:
        # Si hay error al acceder a la sesión, simplemente no mostrar la campanita
        return
    
    if novedades is None:
        # Novedades por defecto si no se proporcionan
        novedades = [
            {
                "titulo": "Fecha de los datos",
                "descripcion": "Se corrigió el dato de la fecha de los datos en todos los módulos",
                "fecha": "2025-08-13",
                "modulo": "Todos"
            },
            {
                "titulo": "Egresados",
                "descripcion": "Se añadió el conteo de egresados en el excel y la tabla de cursos",
                "fecha": "2025-08-13",
                "modulo": "CBA Me Capacita"
            }
        ]
    
    # Filtrar novedades recientes (últimos 7 días)
    hoy = datetime.datetime.now().date()
    novedades_recientes = []
    for novedad in novedades:
        try:
            fecha_novedad = datetime.datetime.strptime(novedad.get("fecha", ""), "%Y-%m-%d").date()
            dias_diferencia = (hoy - fecha_novedad).days
            if dias_diferencia <= 7:  # Novedades de los últimos 7 días
                novedades_recientes.append(novedad)
        except ValueError:
            # Si la fecha no es válida, no incluir en recientes
            pass
    
    # Contar novedades recientes
    num_novedades = len(novedades_recientes)
    
    # Crear un contenedor para el expander
    container = st.container()
    
    # Aplicar CSS para posicionar y estilizar el expander
    st.markdown("""
    <style>
    /* Ocultar el HTML sin procesar */
    .element-container:has(> div.stNotification) {
        display: none;
    }
    
    /* Estilo para posicionar el expander de la campanita */
    .campanita-container div[data-testid="stExpander"] {
        position: absolute;
        top: 70px;
        left: 20px;
        width: 350px;
        z-index: 999;
    }
    
    /* Estilo para el título del expander de la campanita */
    .campanita-container div[data-testid="stExpander"] > div:first-child {
        background-color: white !important;
        border-radius: 20px !important;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2) !important;
    }
    
    /* Estilo para el contenido del expander de la campanita */
    .campanita-container div[data-testid="stExpander"] > details > div {
        background-color: white !important;
        border-radius: 0 0 8px 8px !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1) !important;
    }
    
    /* Estilo para la barra de color en cada novedad */
    .novedad-item {
        margin-bottom: 15px;
        padding-bottom: 10px;
        border-bottom: 1px solid #eee;
    }
    
    /* Estilo para el badge de notificaciones */
    .notification-badge {
        background-color: #ff4b4b;
        color: white;
        border-radius: 50%;
        padding: 0px 6px;
        font-size: 12px;
        margin-left: 5px;
    }
    
    /* Asegurar que el texto del expander se vea correctamente */
    .campanita-container div[data-testid="stExpander"] > div:first-child p {
        font-size: 16px !important;
        font-weight: 500 !important;
        color: #333 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Crear div contenedor para aplicar los estilos específicos
    st.markdown('<div class="campanita-container">', unsafe_allow_html=True)
    
    # Título del expander con campanita y badge
    bell_icon = "🔔"
    # Usamos markdown para el título en lugar de f-string para mejor renderizado
    if num_novedades > 0:
        expander_title = f"{bell_icon} Novedades ({num_novedades})"
    else:
        expander_title = f"{bell_icon} Novedades"
    
    # Crear el expander
    with container.expander(expander_title, expanded=False):
        if num_novedades > 0:
            for novedad in novedades_recientes:
                titulo = novedad.get("titulo", "")
                descripcion = novedad.get("descripcion", "")
                fecha = novedad.get("fecha", "")
                modulo = novedad.get("modulo", "")
                
                # Formatear fecha para mostrar
                try:
                    fecha_obj = datetime.datetime.strptime(fecha, "%Y-%m-%d")
                    fecha_mostrar = fecha_obj.strftime("%d/%m/%Y")
                except ValueError:
                    fecha_mostrar = fecha
                
                # Color según el módulo
                color_modulo = "#0085c8"  # Color por defecto (azul)
                if modulo == "Banco de la Gente":
                    color_modulo = "#0085c8"  # Azul
                elif modulo == "CBA Me Capacita":
                    color_modulo = "#fbbb21"  # Amarillo
                elif modulo == "Programas de Empleo":
                    color_modulo = "#e94235"  # Rojo
                elif modulo == "Emprendimientos":
                    color_modulo = "#34a853"  # Verde
                
                # Mostrar la novedad con barra de color
                st.markdown(f"""
                <div class="novedad-item">
                    <div style="display: flex; align-items: center;">
                        <div style="width: 4px; height: 40px; background-color: {color_modulo}; margin-right: 10px;"></div>
                        <div>
                            <h4 style="margin: 0; color: #333;">{titulo}</h4>
                            <p style="margin: 5px 0 0 0; font-size: 12px; color: #666;">{modulo} · {fecha_mostrar}</p>
                        </div>
                    </div>
                    <p style="margin: 10px 0 0 14px; font-size: 14px; color: #444;">{descripcion}</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No hay novedades recientes")
    
    # Cerrar el div contenedor
    st.markdown('</div>', unsafe_allow_html=True)
