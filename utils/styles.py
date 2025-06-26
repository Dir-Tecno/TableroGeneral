import streamlit as st

# Definir colores de la identidad visual como variables globales
COLOR_PRIMARY = "#0085c8"     # Azul institucional
COLOR_SECONDARY = "#00a8e6"   # Celeste
COLOR_ACCENT_1 = "#e73446"    # Rojo
COLOR_ACCENT_2 = "#fbbb21"    # Amarillo
COLOR_ACCENT_3 = "#bccf00"    # Verde lima
COLOR_ACCENT_4 = "#8a1e82"    # Violeta
COLOR_ACCENT_5 = "#ee7326"    # Naranja
COLOR_BG_LIGHT = "#f8f9fa"    # Fondo claro
COLOR_TEXT_DARK = "#333333"   # Texto oscuro

# Lista de colores de identidad para gráficos
COLORES_IDENTIDAD = [
    COLOR_PRIMARY,
    COLOR_SECONDARY,
    COLOR_ACCENT_1,
    COLOR_ACCENT_2,
    COLOR_ACCENT_3,
    COLOR_ACCENT_4,
    COLOR_ACCENT_5
]

def apply_banner():
    """Aplica el banner SVG superior a la aplicación"""
    st.markdown("""
    <div style="width:100%; overflow:hidden; margin-bottom:10px;">
        <svg xmlns="http://www.w3.org/2000/svg" version="1.1" viewBox="0 0 595.28 10.32" width="100%" height="10">
            <defs>
                <style>
                    .cls-1 { fill: #bccf00; }
                    .cls-2 { fill: #00a8e6; }
                    .cls-3 { fill: #0085c8; }
                    .cls-4 { fill: #e73446; }
                    .cls-5 { fill: #8a1e82; }
                    .cls-6 { fill: #fbbb21; }
                    .cls-7 { fill: #ee7326; }
                </style>
            </defs>
            <g>
                <g id="Capa_1">
                    <polygon class="cls-6" points="243.75 10.32 203.81 10.32 185.4 0 236.06 0 243.75 10.32"></polygon>
                    <polygon class="cls-3" points="203.81 10.32 97.28 10.32 108.51 0 185.4 0 203.81 10.32"></polygon>
                    <polygon class="cls-5" points="97.55 10.32 -2.8 10.32 -2.8 0 108.51 0 97.55 10.32"></polygon>
                    <polygon class="cls-4" points="300.53 10.32 243.75 10.32 236.06 0 320.44 0 300.53 10.32"></polygon>
                    <polygon class="cls-4" points="544.42 10.32 509.32 10.32 515.21 0 566.67 0 544.42 10.32"></polygon>
                    <polygon class="cls-1" points="598.77 10.32 544.42 10.32 566.67 0 598.8 0 598.77 10.32"></polygon>
                    <polygon class="cls-7" points="509.32 10.32 403.44 10.32 461.4 0 515.22 0 509.32 10.32"></polygon>
                    <polygon class="cls-2" points="403.44 10.32 353.96 10.32 320.44 0 461.4 0 403.44 10.32"></polygon>
                    <polygon class="cls-6" points="353.95 10.32 300.53 10.32 320.44 0 353.95 10.32"></polygon>
                </g>
            </g>
        </svg>
    </div>
    """, unsafe_allow_html=True)

def apply_styles():
    """Aplica los estilos CSS personalizados a toda la aplicación"""
    st.markdown("""
    <style>
    /* Variables de colores de la identidad visual */
    :root {
        --color-primary: """ + COLOR_PRIMARY + """;
        --color-secondary: """ + COLOR_SECONDARY + """;
        --color-accent-1: """ + COLOR_ACCENT_1 + """;
        --color-accent-2: """ + COLOR_ACCENT_2 + """;
        --color-accent-3: """ + COLOR_ACCENT_3 + """;
        --color-accent-4: """ + COLOR_ACCENT_4 + """;
        --color-accent-5: """ + COLOR_ACCENT_5 + """;
        --color-bg-light: """ + COLOR_BG_LIGHT + """;
        --color-text-dark: """ + COLOR_TEXT_DARK + """;
    }

    /* Estilos generales */
    .main-header {
        font-size: 2.5rem; 
        font-weight: bold; 
        text-align: center; 
        color: var(--color-primary); 
        margin-top: 10px;
        margin-bottom: 20px;
    }
    
    /* Estilo para el contenedor principal */
    .main {
        background-color: var(--color-bg-light);
    }

    /* Estilos para las pestañas principales */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: var(--color-bg-light);
        border-radius: 8px 8px 0 0;
        padding: 0 10px;
        box-shadow: 0 -2px 10px rgba(0,0,0,0.05);
    }

    .stTabs [data-baseweb="tab"] {
        height: 60px; 
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 8px 8px 0 0;
        color: var(--color-text-dark);
        font-weight: 600;
        font-size: 18px; 
        padding: 0 25px; 
        border-bottom: 4px solid transparent; 
        transition: all 0.3s ease;
        letter-spacing: 0.5px; 
        text-transform: uppercase; 
    }

    .stTabs [aria-selected="true"] {
        background-color: white !important;
        color: var(--color-primary) !important;
        border-bottom: 4px solid var(--color-primary) !important; 
        font-weight: 700;
        box-shadow: 0 -4px 10px rgba(0,0,0,0.1); 
    }

    /* Estilos para las sub-pestañas (dentro de los módulos) */
    .stTabs [data-baseweb="tab-list"] [data-baseweb="tab-list"] {
        background-color: white;
        border-radius: 4px;
        padding: 0 5px;
        box-shadow: none;
        border-bottom: 1px solid #eee;
    }

    .stTabs [data-baseweb="tab-list"] [data-baseweb="tab"] {
        height: 40px;
        font-size: 14px;
        font-weight: 500;
        padding: 0 15px;
        border-bottom: 2px solid transparent;
        text-transform: none; 
        letter-spacing: normal; 
    }

    .stTabs [data-baseweb="tab-list"] [aria-selected="true"] {
        background-color: white !important;
        color: var(--color-secondary) !important;
        border-bottom: 2px solid var(--color-secondary) !important;
    }

    /* Estilos para los encabezados de las pestañas */
    .tab-subheader {
        font-size: 24px;
        font-weight: 700;
        color: var(--color-primary);
        margin-bottom: 20px;
        padding-bottom: 10px;
        border-bottom: 2px solid var(--color-accent-2);
    }

    /* Estilos para los contenedores de pestañas */
    .stTabs [data-baseweb="tab-panel"] {
        background-color: white;
        border-radius: 0 0 8px 8px;
        padding: 20px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    }

    /* Colores específicos para cada pestaña principal */
    .tab-color-bco-gente [aria-selected="true"] {
        color: var(--color-primary) !important;
        border-bottom-color: var(--color-primary) !important;
    }

    .tab-color-cba-capacita [aria-selected="true"] {
        color: var(--color-accent-4) !important;
        border-bottom-color: var(--color-accent-4) !important;
    }

    .tab-color-empleo [aria-selected="true"] {
        color: var(--color-accent-3) !important;
        border-bottom-color: var(--color-accent-3) !important;
    }

    /* Estilos globales para KPIs */
    .kpi-container {
        padding: 10px 0 20px 0;
        display: flex;
        gap: 15px;
        flex-wrap: wrap;
    }

    .kpi-card {
        background-color: var(--color-primary);
        color: white;
        padding: 12px 10px;
        margin: 0 2px;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        transition: transform 0.3s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }

    .kpi-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }

    .kpi-title {
        font-size: 13px;
        margin-bottom: 4px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 500;
        line-height: 1.2;
    }

    .kpi-value {
        font-size: 24px;
        font-weight: bold;
        line-height: 1.1;
    }

    /* Variantes de color para KPIs */
    .kpi-primary {
        background-color: var(--color-primary);
    }

    .kpi-secondary {
        background-color: var(--color-secondary);
    }

    .kpi-accent-1 {
        background-color: var(--color-accent-1);
    }

    .kpi-accent-2 {
        background-color: var(--color-accent-2);
    }

    .kpi-accent-3 {
        background-color: var(--color-accent-3);
    }

    .kpi-accent-4 {
        background-color: var(--color-accent-4);
    }

    .kpi-accent-5 {
        background-color: var(--color-accent-5);
    }

    /* Estilos específicos de empleo.py */
    /* General styles */
    .main {
        background-color: #f8f9fa;
        padding: 1rem;
    }

    /* Header styles */
    .dashboard-header {
        background-color: var(--color-primary);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }

    /* Filter container */
    .filter-container {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 20px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        border-left: 4px solid var(--color-secondary);
    }

    /* Card styles */
    .metric-card {
        background-color: white;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        padding: 20px;
        margin-bottom: 15px;
        transition: transform 0.3s ease;
        border-top: 4px solid var(--color-primary);
    }

    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
    }

    .metric-value {
        font-size: 28px;
        font-weight: bold;
        color: var(--color-primary);
    }

    .metric-label {
        font-size: 14px;
        color: #6c757d;
        margin-bottom: 5px;
    }
    
    .metric-subtitle {
        font-size: 12px;
        color: #6c757d;
        margin-top: 5px;
    }

    /* Table styles */
    .styled-table {
        width: 100%;
        border-collapse: collapse;
        margin: 25px 0;
        font-size: 0.9em;
        font-family: sans-serif;
        min-width: 400px;
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
        border-radius: 8px;
        overflow: hidden;
    }

    .styled-table thead tr {
        background-color: var(--color-primary);
        color: #ffffff;
        text-align: left;
    }

    .styled-table th,
    .styled-table td {
        padding: 12px 15px;
    }

    .styled-table tbody tr {
        border-bottom: 1px solid #dddddd;
    }

    .styled-table tbody tr:nth-of-type(even) {
        background-color: #f3f3f3;
    }

    .styled-table tbody tr:last-of-type {
        border-bottom: 2px solid var(--color-primary);
    }

    /* Status indicators */
    .status-success {background-color: #d1e7dd; border-left: 5px solid var(--color-accent-3);}
    .status-info {background-color: #d0e3f1; border-left: 5px solid var(--color-primary);}
    .status-warning {background-color: #fff3cd; border-left: 5px solid var(--color-accent-2);}
    .status-danger {background-color: #f8d7da; border-left: 5px solid var(--color-accent-1);}

    /* Section headers */
    .section-title {
        color: var(--color-primary);
        font-size: 20px;
        font-weight: 600;
        margin: 20px 0 15px 0;
        padding-bottom: 5px;
        border-bottom: 2px solid var(--color-secondary);
    }

    /* Chart container */
    .chart-container {
        background-color: white;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 20px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
    }

    /* Info box */
    .info-box {
        background-color: #e7f5ff;
        border-left: 4px solid var(--color-primary);
        padding: 10px 15px;
        margin-bottom: 15px;
        border-radius: 4px;
        font-size: 14px;
    }
    
    /* Filtros */
    .filter-section {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 20px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        display: flex;
        flex-wrap: wrap;
    }
    
    .filter-label {
        font-size: 14px;
        font-weight: 600;
        color: var(--color-primary);
        margin-bottom: 5px;
    }
    
    .filter-info {
        font-size: 12px;
        color: #6c757d;
        margin-top: 5px;
        margin-bottom: 15px;
        font-style: italic;
    }
    </style>
    """, unsafe_allow_html=True)

def apply_tabs_js():
    """Aplica JavaScript para asignar clases específicas a cada pestaña principal"""
    tabs_css = """
    <script>
    document.addEventListener('DOMContentLoaded', function() {
        // Esperar a que las pestañas se carguen
        setTimeout(function() {
            // Obtener todos los elementos de pestañas
            const tabElements = document.querySelectorAll('[data-baseweb="tab"]');
            
            // Aplicar clases específicas a cada pestaña
            if (tabElements.length >= 3) {
                tabElements[0].classList.add('tab-color-bco-gente');
                tabElements[1].classList.add('tab-color-cba-capacita');
                tabElements[2].classList.add('tab-color-empleo');
            }
        }, 500);
    });
    </script>
    """
    st.markdown(tabs_css, unsafe_allow_html=True)

def setup_page():
    """Configura la página con todos los estilos y elementos visuales"""
    # Aplicar banner
    apply_banner()
    
    # Aplicar estilos
    apply_styles()
    
    # Aplicar JavaScript para colorear pestañas
    apply_tabs_js()
