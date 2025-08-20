import streamlit as st
import json
from typing import Any, Dict, List, Union

def log_to_browser_console(message: str, level: str = "info", data: Any = None):
    """
    Envía un mensaje a la consola del navegador usando JavaScript.
    
    Args:
        message (str): Mensaje a mostrar en la consola
        level (str): Nivel del log ('info', 'warn', 'error', 'debug')
        data (Any): Datos adicionales para mostrar (opcional)
    """
    # Escapar el mensaje para JavaScript
    safe_message = json.dumps(message)
    
    # Preparar datos adicionales si existen
    data_js = ""
    if data is not None:
        try:
            # Convertir datos a JSON para JavaScript
            if isinstance(data, (dict, list)):
                data_json = json.dumps(data, default=str, ensure_ascii=False)
                data_js = f", {data_json}"
            else:
                data_js = f", {json.dumps(str(data))}"
        except Exception:
            data_js = f", {json.dumps(str(data))}"
    
    # Validar nivel de log
    valid_levels = ['info', 'warn', 'error', 'debug', 'log']
    if level not in valid_levels:
        level = 'info'
    
    # Crear el script JavaScript
    js_code = f"""
    <script>
    console.{level}('[TableroGeneral] {safe_message.strip('"')}'{ data_js });
    </script>
    """
    
    # Ejecutar el JavaScript
    st.markdown(js_code, unsafe_allow_html=True)

def log_loading_info(message: str, data: Any = None):
    """
    Registra información de carga en la consola del navegador.
    
    Args:
        message (str): Mensaje de información de carga
        data (Any): Datos adicionales (opcional)
    """
    log_to_browser_console(f"🔄 CARGA: {message}", "info", data)

def log_loading_warning(message: str, data: Any = None):
    """
    Registra advertencias de carga en la consola del navegador.
    
    Args:
        message (str): Mensaje de advertencia de carga
        data (Any): Datos adicionales (opcional)
    """
    log_to_browser_console(f"⚠️ ADVERTENCIA: {message}", "warn", data)

def log_loading_error(message: str, data: Any = None):
    """
    Registra errores de carga en la consola del navegador.
    
    Args:
        message (str): Mensaje de error de carga
        data (Any): Datos adicionales (opcional)
    """
    log_to_browser_console(f"❌ ERROR: {message}", "error", data)

def log_debug(message: str, data: Any = None):
    """
    Registra mensajes de depuración en la consola del navegador.
    
    Args:
        message (str): Mensaje de depuración
        data (Any): Datos adicionales (opcional)
    """
    log_to_browser_console(f"🐛 DEBUG: {message}", "debug", data)

def log_batch_to_console(logs: Dict[str, List[str]]):
    """
    Envía un lote de logs a la consola del navegador.
    
    Args:
        logs (Dict[str, List[str]]): Diccionario con listas de logs por tipo
                                    Ej: {"info": ["msg1", "msg2"], "warnings": ["warn1"]}
    """
    if not logs:
        return
    
    # Registrar información
    if logs.get("info"):
        for info_msg in logs["info"]:
            log_loading_info(info_msg)
    
    # Registrar advertencias
    if logs.get("warnings"):
        for warning_msg in logs["warnings"]:
            log_loading_warning(warning_msg)
    
    # Registrar errores
    if logs.get("errors"):
        for error_msg in logs["errors"]:
            log_loading_error(error_msg)

def log_data_summary(data_dict: Dict[str, Any], dates_dict: Dict[str, Any] = None):
    """
    Registra un resumen de los datos cargados en la consola del navegador.
    
    Args:
        data_dict (Dict[str, Any]): Diccionario con los datos cargados
        dates_dict (Dict[str, Any]): Diccionario con fechas de modificación (opcional)
    """
    if not data_dict:
        log_loading_error("El diccionario de datos está vacío")
        return
    
    # Resumen de archivos cargados
    files_loaded = list(data_dict.keys())
    log_loading_info(f"Archivos cargados exitosamente: {len(files_loaded)}", files_loaded)
    
    # Información de fechas si está disponible
    if dates_dict:
        formatted_dates = {k: v.strftime('%Y-%m-%d %H:%M:%S') if v else None 
                          for k, v in dates_dict.items()}
        log_loading_info("Fechas de modificación", formatted_dates)
    
    # Información detallada de cada archivo
    for file_name, data in data_dict.items():
        if hasattr(data, 'shape'):
            log_loading_info(f"Archivo: {file_name}", {
                "filas": data.shape[0],
                "columnas": data.shape[1],
                "tipo": str(type(data).__name__)
            })
        elif hasattr(data, '__len__'):
            log_loading_info(f"Archivo: {file_name}", {
                "elementos": len(data),
                "tipo": str(type(data).__name__)
            })
        else:
            log_loading_info(f"Archivo: {file_name}", {
                "tipo": str(type(data).__name__)
            })
