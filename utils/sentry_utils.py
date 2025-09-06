import streamlit as st
import sentry_sdk
import sys
import traceback
from functools import wraps
from contextlib import contextmanager

def init_sentry(dsn=None):
    """
    Inicializa Sentry con configuración estandarizada para el proyecto TableroGeneral.
    Si no se proporciona DSN, se intentará usar el de los secretos de Streamlit.
    
    Args:
        dsn (str, optional): DSN de Sentry. Si es None, se usa st.secrets.
    """
    try:
        if dsn is None:
            # Intentar obtener el DSN de los secretos de Streamlit
            if "sentry" in st.secrets and "dsn" in st.secrets["sentry"]:
                dsn = st.secrets["sentry"]["dsn"]
            elif "sentry_dsn" in st.secrets:
                dsn = st.secrets["sentry_dsn"]
        
        # Si aún no tenemos DSN, mostrar mensaje de error
        if dsn is None:
            st.warning("""
            ⚠️ No se encontró el DSN de Sentry en los secretos. 
            El monitoreo de errores está desactivado.
            
            Para activarlo, configure el DSN en .streamlit/secrets.toml:
            ```
            [sentry]
            dsn = "su_dsn_de_sentry"
            ```
            o
            ```
            sentry_dsn = "su_dsn_de_sentry"
            ```
            """)
            return False
        
        # Inicializar Sentry con configuración óptima para Streamlit
        sentry_sdk.init(
            dsn=dsn,
            send_default_pii=True,
            # Añadir información sobre ambiente y release
            environment=st.secrets.get("sentry_environment", "production"),
            traces_sample_rate=0.2,  # Capturar 20% de las transacciones para performance
            profiles_sample_rate=0.1,  # Capturar 10% de los perfiles
            # Ignorar errores comunes de Streamlit que no necesitan reportarse
            ignore_errors=[
                "StreamlitAPIException",
                "StopException",
                "DuplicateWidgetID",
            ]
        )
        return True
    except Exception as e:
        st.error(f"Error al inicializar Sentry: {str(e)}")
        return False

def set_user_context(user_id=None, email=None, username=None, **kwargs):
    """
    Establece el contexto del usuario para Sentry.
    
    Args:
        user_id (str, optional): ID de usuario.
        email (str, optional): Email del usuario.
        username (str, optional): Nombre de usuario.
        **kwargs: Atributos adicionales para el usuario.
    """
    try:
        if user_id or email or username:
            sentry_sdk.set_user({
                "id": user_id,
                "email": email,
                "username": username,
                **kwargs
            })
    except Exception:
        pass  # Silenciar errores en la configuración de usuario

def add_breadcrumb(category, message, level="info", data=None):
    """
    Añade una miga de pan (breadcrumb) al seguimiento de Sentry.
    
    Args:
        category (str): Categoría de la miga.
        message (str): Mensaje descriptivo.
        level (str): Nivel del mensaje ('info', 'warning', 'error').
        data (dict, optional): Datos adicionales a incluir.
    """
    try:
        sentry_sdk.add_breadcrumb(
            category=category,
            message=message,
            level=level,
            data=data or {}
        )
    except Exception:
        pass  # Silenciar errores en breadcrumbs

def capture_exception(exc=None, extra_data=None):
    """
    Captura explícitamente una excepción en Sentry.
    
    Args:
        exc (Exception, optional): Excepción a capturar. Si es None, se capturará 
                                  la excepción actual en el traceback.
        extra_data (dict, optional): Datos adicionales a incluir en el reporte.
    """
    try:
        if extra_data:
            with sentry_sdk.configure_scope() as scope:
                for key, value in extra_data.items():
                    scope.set_extra(key, value)
        
        if exc:
            sentry_sdk.capture_exception(exc)
        else:
            sentry_sdk.capture_exception()
    except Exception as e:
        # No usar st.error aquí para evitar ciclos
        print(f"Error al capturar excepción en Sentry: {str(e)}")

def capture_message(message, level="info", extra_data=None):
    """
    Captura un mensaje en Sentry.
    
    Args:
        message (str): Mensaje a capturar.
        level (str): Nivel del mensaje ('info', 'warning', 'error').
        extra_data (dict, optional): Datos adicionales a incluir en el reporte.
    """
    try:
        if extra_data:
            with sentry_sdk.configure_scope() as scope:
                for key, value in extra_data.items():
                    scope.set_extra(key, value)
        
        sentry_sdk.capture_message(message, level=level)
    except Exception:
        pass  # Silenciar errores

def set_module_context(module_name):
    """
    Configura el contexto del módulo actual para mejorar los reportes de Sentry.
    
    Args:
        module_name (str): Nombre del módulo (ej: 'bco_gente', 'cbamecapacita').
    """
    try:
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("module", module_name)
    except Exception:
        pass  # Silenciar errores

@contextmanager
def sentry_context_manager(module_name=None, operation=None, extra_data=None):
    """
    Context manager para capturar excepciones en un bloque de código.
    
    Args:
        module_name (str, optional): Nombre del módulo.
        operation (str, optional): Operación que se está realizando.
        extra_data (dict, optional): Datos adicionales a incluir.
    
    Yields:
        None
    """
    try:
        if module_name:
            set_module_context(module_name)
        
        if operation:
            add_breadcrumb(category="operation", message=f"Iniciando: {operation}")
        
        yield
        
        if operation:
            add_breadcrumb(category="operation", message=f"Completado: {operation}")
    except Exception as e:
        data = {"error": str(e)}
        if extra_data:
            data.update(extra_data)
        
        if operation:
            add_breadcrumb(
                category="operation_error",
                message=f"Error en: {operation}",
                level="error",
                data=data
            )
        
        capture_exception(e, extra_data=data)
        raise  # Re-lanzar la excepción para manejo superior

def sentry_wrap(func=None, module_name=None, operation=None):
    """
    Decorador para funciones que captura excepciones automáticamente.
    
    Args:
        func (callable, optional): Función a decorar.
        module_name (str, optional): Nombre del módulo.
        operation (str, optional): Operación que realiza la función.
    
    Returns:
        callable: Función decorada.
    """
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            with sentry_context_manager(module_name=module_name or f.__module__, 
                                      operation=operation or f.__name__):
                return f(*args, **kwargs)
        return wrapped
    
    if func:
        return decorator(func)
    return decorator

# Safe wrapper para st.error que además captura en Sentry
def sentry_error(message, exc=None, extra_data=None):
    """
    Muestra un mensaje de error en Streamlit y lo reporta en Sentry.
    
    Args:
        message (str): Mensaje de error para mostrar.
        exc (Exception, optional): Excepción asociada.
        extra_data (dict, optional): Datos adicionales a incluir.
    """
    st.error(message)
    
    if exc:
        capture_exception(exc, extra_data)
    else:
        capture_message(message, level="error", extra_data=extra_data)
