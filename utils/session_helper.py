"""
Utilidades para manejo seguro de session_state en Streamlit.
Previene errores de "Tried to use SessionInfo before it was initialized".
"""
import streamlit as st
from streamlit.runtime.scriptrunner import get_script_run_ctx


def is_session_initialized():
    """
    Verifica si la sesión de Streamlit está inicializada de forma fiable.
    """
    try:
        # get_script_run_ctx() es la forma más robusta de saber si estamos en un script run.
        return get_script_run_ctx() is not None
    except Exception:
        # Si hay algún error, asumimos que la sesión no está lista.
        return False


def safe_session_get(key, default=None):
    """
    Obtiene un valor de st.session_state de forma segura.
    Si la sesión no está lista o la clave no existe, retorna el valor por defecto.
    """
    if is_session_initialized() and hasattr(st, 'session_state') and key in st.session_state:
        return st.session_state[key]
    return default


def safe_session_set(key, value):
    """
    Establece un valor en st.session_state de forma segura.
    Solo lo intenta si la sesión está inicializada.
    """
    if is_session_initialized() and hasattr(st, 'session_state'):
        st.session_state[key] = value


def safe_session_check(key):
    """
    Verifica si una clave existe en st.session_state de forma segura.
    """
    return is_session_initialized() and hasattr(st, 'session_state') and key in st.session_state


def safe_session_delete(key):
    """
    Elimina una clave de st.session_state de forma segura.
    """
    if safe_session_check(key):
        del st.session_state[key]
