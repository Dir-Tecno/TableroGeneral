import plotly.graph_objects as go

def apply_base_style(fig, height=380, rotate_x=-35, showlegend=False, y0=True, margin=None, colorbar_thickness=8, colorbar_len=0.30, colorbar_x=1.02, **kwargs):
    """Aplica estilo base a una figura Plotly.

    Args:
        fig: plotly.graph_objects.Figure o plotly.express figura
        height: altura en px
        rotate_x: ángulo en grados para ticks X
        showlegend: mostrar leyenda
        y0: si True fuerza que el eje Y comience en 0 cuando tenga sentido
        margin: dict de márgenes opcional
    """
    if margin is None:
        margin = dict(l=20, r=20, t=60, b=120)

    # Template y márgenes
    fig.update_layout(template='plotly_white', height=height, margin=margin, showlegend=showlegend)

    # Rotar etiquetas del eje X si aplica
    try:
        if rotate_x is not False:
            fig.update_xaxes(tickangle=rotate_x)
    except Exception:
        pass

    # Forzar eje Y a partir de 0 si corresponde
    if y0:
        try:
            # Obtener max actual para ajustar rango
            yvals = []
            for trace in fig.data:
                if hasattr(trace, 'y') and trace.y is not None:
                    # Trace y puede ser numpy array u objeto iterable
                    try:
                        vals = [v for v in trace.y if v is not None]
                    except Exception:
                        vals = []
                    yvals.extend([float(v) for v in vals if isinstance(v, (int, float))])
            if yvals:
                ymax = max(yvals)
                fig.update_yaxes(range=[0, ymax * 1.08])
            else:
                fig.update_yaxes(range=[0, 1])
        except Exception:
            pass

    # Aplicar estilo estándar a colorbars (si existen)
    try:
        # Si la figura usa coloraxis en layout, ajustarlo
        if hasattr(fig.layout, 'coloraxis') and fig.layout.coloraxis is not None:
            try:
                fig.update_layout(coloraxis_colorbar=dict(thickness=colorbar_thickness, len=colorbar_len, x=colorbar_x))
            except Exception:
                pass

        # Recorrer trazas y ajustar colorbar donde aplique
        for trace in fig.data:
            try:
                # Para trazas que tienen atributo colorbar (choropleth, scattermapbox, etc.)
                if hasattr(trace, 'colorbar') and trace.colorbar is not None:
                    trace.colorbar.thickness = colorbar_thickness
                    trace.colorbar.len = colorbar_len
                    # posicionar un poco fuera del gráfico para no superponer
                    try:
                        trace.colorbar.x = colorbar_x
                    except Exception:
                        pass
                # Para trazas con marker.colorbar
                if hasattr(trace, 'marker') and hasattr(trace.marker, 'colorbar') and trace.marker.colorbar is not None:
                    trace.marker.colorbar.thickness = colorbar_thickness
                    trace.marker.colorbar.len = colorbar_len
                    try:
                        trace.marker.colorbar.x = colorbar_x
                    except Exception:
                        pass
            except Exception:
                continue
    except Exception:
        pass


def set_shared_yaxis(figs, pad=0.08):
    """Setea un rango Y común para una lista de figuras Plotly.

    Args:
        figs: lista de figuras
        pad: padding multiplicativo sobre el máximo (por ejemplo 0.08 = +8%)
    """
    max_y = 0
    for fig in figs:
        try:
            for trace in fig.data:
                if hasattr(trace, 'y') and trace.y is not None:
                    try:
                        vals = [v for v in trace.y if v is not None]
                    except Exception:
                        vals = []
                    numeric = [float(v) for v in vals if isinstance(v, (int, float))]
                    if numeric:
                        cand = max(numeric)
                        if cand > max_y:
                            max_y = cand
        except Exception:
            continue

    top = max_y * (1 + pad) if max_y > 0 else 1
    for fig in figs:
        try:
            fig.update_yaxes(range=[0, top])
        except Exception:
            pass
import plotly.graph_objects as go


def apply_base_style(fig, rotate_x=False, showlegend=False, height=400, margin=None, text_inside=False):
    """Aplica un conjunto coherente de estilos a figuras Plotly.

    Args:
        fig: figura de plotly (go.Figure o px.Figure)
        rotate_x (bool): rotar etiquetas del eje X si True
        showlegend (bool): mostrar leyenda
        height (int): altura del gráfico
        margin (dict|None): margenes personalizados
        text_inside (bool): colocar texto dentro de las barras (si aplica)
    """
    if fig is None:
        return fig

    # Template y layout base
    try:
        fig.update_layout(template='plotly_white')
    except Exception:
        pass

    # Márgenes por defecto si no se especifican
    if margin is None:
        margin = dict(l=20, r=20, t=60, b=120)

    try:
        fig.update_layout(height=height, margin=margin, showlegend=showlegend)
    except Exception:
        pass

    # Rotación del eje X para mejorar legibilidad
    try:
        if rotate_x:
            fig.update_xaxes(tickangle=-35, tickfont=dict(size=11))
        else:
            fig.update_xaxes(tickfont=dict(size=11))
    except Exception:
        pass

    # Forzar texto dentro de barras cuando se desea
    if text_inside:
        try:
            fig.update_traces(textposition='inside')
        except Exception:
            pass

    # Quitar líneas de borde en markers/barras si existen
    try:
        fig.update_traces(marker_line_width=0)
    except Exception:
        pass

    return fig
