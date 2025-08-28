import streamlit as st

URL = "https://reporte-escrituracion.duckdns.org/"

def show_escrituracion_redirect():
    """
    Muestra la leyenda y botón para redirigir al reporte externo.
    Llamar desde el script principal (app.py) cuando corresponda.
    """

    st.markdown(
        f'<div style="text-align:center; margin-top:20px;">'
        f'<a href="{URL}" target="_blank" rel="noopener noreferrer">'
        f'<button style="background:#2980b9;color:#fff;border:none;padding:12px 20px;border-radius:8px;font-size:16px;cursor:pointer;margin-bottom:20px;">'
        f'Abrir reporte de escrituración'
        f'</button>'
        f'</a>'
        f'</div>',
        unsafe_allow_html=True,
    )

# Alias compatible con app.py: aceptar args/kwargs para evitar TypeError cuando app.py pasa (data, dates, is_local)
def show_escrituracion_dashboard(data_for_module=None, dates_for_module=None, is_local=False, *args, **kwargs):
    """
    Alias para compatibilidad: recibe los mismos parámetros que el resto de módulos,
    pero solo muestra la redirección.
    """
    show_escrituracion_redirect()
