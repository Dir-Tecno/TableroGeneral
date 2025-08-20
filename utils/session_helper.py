"""
Utilidades para manejo seguro de session_state en Streamlit.
Previene errores de "Tried to use SessionInfo before it was initialized".
"""
import streamlit as st


def safe_session_get(key, default=None):
    """
    Obtiene un valor de session_state de forma segura.
    
    Args:
        key (str): Clave del session_state
        default: Valor por defecto si la clave no existe o hay error
        
    Returns:
        Valor del session_state o default si hay error
    """
    try:
        # Verificar si session_state está disponible
        if hasattr(st, 'session_state'):
            return st.session_state.get(key, default)
        else:
            return default
    except Exception:
        # Si hay cualquier error, devolver el valor por defecto
        return default


def safe_session_set(key, value):
    """
    Establece un valor en session_state de forma segura.
    
    Args:
        key (str): Clave del session_state
        value: Valor a establecer
        
    Returns:
        bool: True si se estableció correctamente, False si hubo error
    """
    try:
        # Verificar si session_state está disponible
        if hasattr(st, 'session_state'):
            st.session_state[key] = value
            return True
        else:
            return False
    except Exception:
        # Si hay cualquier error, no hacer nada
        return False


def safe_session_check(key):
    """
    Verifica si una clave existe en session_state de forma segura.
    
    Args:
        key (str): Clave a verificar
        
    Returns:
        bool: True si la clave existe, False en caso contrario
    """
    try:
        # Verificar si session_state está disponible
        if hasattr(st, 'session_state'):
            return key in st.session_state
        else:
            return False
    except Exception:
        # Si hay cualquier error, asumir que no existe
        return False


def is_session_initialized():
    """
    Verifica si session_state está completamente inicializado.
    
    Returns:
        bool: True si session_state está disponible, False en caso contrario
    """
    try:
        # Intentar acceder a session_state
        if hasattr(st, 'session_state'):
            # Intentar una operación básica para verificar que funciona
            _ = len(st.session_state)
            return True
        else:
            return False
    except Exception:
        return False
