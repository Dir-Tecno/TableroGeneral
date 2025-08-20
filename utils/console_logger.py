import streamlit as st
import json

def log_to_console(message, log_type="info"):
    """
    Envía mensajes a la consola del navegador usando JavaScript.
    
    Args:
        message (str): Mensaje a enviar a la consola
        log_type (str): Tipo de log ('info', 'warn', 'error', 'log')
    """
    # Escapar el mensaje para JavaScript
    escaped_message = json.dumps(str(message))
    
    # Mapear tipos de log
    js_log_type = {
        'info': 'console.info',
        'warn': 'console.warn', 
        'warning': 'console.warn',
        'error': 'console.error',
        'log': 'console.log'
    }.get(log_type.lower(), 'console.log')
    
    # Crear el script JavaScript con timestamp
    js_code = f"""
    <script>
    (function() {{
        const timestamp = new Date().toLocaleTimeString();
        {js_log_type}('[TABLERO ' + timestamp + '] ' + {escaped_message});
    }})();
    </script>
    """
    
    # Ejecutar el JavaScript
    st.markdown(js_code, unsafe_allow_html=True)

def log_data_loading_info(all_data, all_dates, logs):
    """
    Envía toda la información de carga de datos a la consola del navegador.
    
    Args:
        all_data (dict): Diccionario con los datos cargados
        all_dates (dict): Diccionario con las fechas de modificación
        logs (dict): Diccionario con logs de carga (warnings, info)
    """
    # Log de archivos cargados
    if all_data:
        log_to_console(f"Archivos Cargados Exitosamente: {list(all_data.keys())}", "info")
        
        # Log de fechas de modificación
        dates_info = {k: v.strftime('%Y-%m-%d %H:%M:%S') if v else None for k, v in all_dates.items()}
        log_to_console(f"Fechas de Modificación: {dates_info}", "info")
    else:
        log_to_console("ERROR: El diccionario 'all_data' está vacío. La carga de datos falló.", "error")
    
    # Log de advertencias
    if logs and logs.get("warnings"):
        log_to_console("=== ADVERTENCIAS DE CARGA ===", "warn")
        for warning in logs["warnings"]:
            log_to_console(f"⚠️ {warning}", "warn")
    
    # Log de información
    if logs and logs.get("info"):
        log_to_console("=== INFORMACIÓN DE CARGA ===", "info")
        for info in logs["info"]:
            log_to_console(f"ℹ️ {info}", "info")

def log_debug_info(message, data=None):
    """
    Envía información de depuración a la consola del navegador.
    
    Args:
        message (str): Mensaje de depuración
        data (any): Datos adicionales a mostrar (opcional)
    """
    log_to_console(f"[DEBUG] {message}", "log")
    if data is not None:
        log_to_console(f"[DEBUG DATA] {data}", "log")
