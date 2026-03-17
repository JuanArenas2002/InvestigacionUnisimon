"""
Servicio de generación de gráficos de publicaciones.
Reutilizable desde FastAPI y scripts standalone.
"""

import io
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Dict

# Configurar backend de matplotlib ANTES de importar pyplot
# (evita errores con tkinter en servidores sin GUI)
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.patches as mpatches
import numpy as np

from extractors.scopus import ScopusExtractor

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# COLORES Y ESTILOS
# ═══════════════════════════════════════════════════════════════════════════════

CHART_COLORS = {
    'bg': "#FFFFFF",
    'panel': "#F6F9FC",
    'border': "#D0D7DE",
    'grid': "#E5EBF0",
    'text': "#24292F",
    'muted': "#57606A",
    'blue': "#0969DA",
    'red': "#D1242F",
    'amber': "#9E6A03",
    'green': "#2DA44E",
}

def configure_matplotlib_styles():
    """Configura el tema de matplotlib"""
    plt.rcParams.update({
        'font.family': 'sans-serif',
        'font.sans-serif': ['Segoe UI', 'Arial', 'DejaVu Sans'],
        'figure.facecolor': CHART_COLORS['bg'],
        'axes.facecolor': CHART_COLORS['panel'],
        'axes.edgecolor': CHART_COLORS['border'],
        'axes.labelcolor': CHART_COLORS['text'],
        'axes.spines.left': True,
        'axes.spines.bottom': True,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'xtick.color': CHART_COLORS['text'],
        'ytick.color': CHART_COLORS['text'],
        'grid.color': CHART_COLORS['grid'],
        'grid.linestyle': '-',
        'grid.linewidth': 0.5,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ═══════════════════════════════════════════════════════════════════════════════

def get_peak(data: List[int], years: List[int]) -> Tuple[int, int]:
    """
    Obtiene el año y índice con más publicaciones.
    Returns: (peak_year, peak_index)
    """
    if not data:
        return years[0], 0
    max_val = max(data)
    max_idx = data.index(max_val)
    return years[max_idx], max_idx


def extract_publications_by_year(
    query: str,
    author_id: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    verbose: bool = True
) -> Tuple[List[int], List[int], Dict, str]:
    """
    Extrae publicaciones de Scopus agrupadas por año.
    
    Args:
        query: Query avanzada de Scopus
        year_from: Filtrar desde este año (opcional)
        year_to: Filtrar hasta este año (opcional)
        verbose: Imprimir información de depuración
    
    Returns:
        (years_list, publications_list, stats_dict, first_author_name)
    """
    
    extractor = ScopusExtractor()
    
    if verbose:
        logger.info(f"Extrayendo datos con query: {query}")
    
    try:
        # Extraer registros de Scopus
        records = extractor.extract(query=query, max_results=None)
    except Exception as e:
        logger.error(f"Error extrayendo de Scopus: {e}")
        raise
    
    if verbose:
        logger.info(f"Total de registros encontrados: {len(records)}")
    
    # Obtener nombre del autor buscando el AU-ID en los registros
    author_name = "Investigador"
    for record in records:
        if record.authors:
            for author in record.authors:
                # Comparar el scopus_id del autor con el AU-ID buscado
                author_scopus_id = author.get('scopus_id', '')
                # Limpiar formato: puede ser "SCOPUS_ID:57193767797" o "57193767797"
                if author_scopus_id:
                    author_scopus_id = author_scopus_id.replace('SCOPUS_ID:', '')
                
                if author_scopus_id == author_id:
                    author_name = author.get('name', 'Investigador')
                    if author_name and author_name != 'Investigador':
                        break
        if author_name != "Investigador":
            break
    
    if verbose:
        logger.info(f"Autor encontrado: {author_name}")
    
    # Agrupar por año con filtro opcional
    pub_by_year = Counter()
    for record in records:
        if record.publication_year:
            year = record.publication_year
            
            # Aplicar filtros de año
            if year_from and year < year_from:
                continue
            if year_to and year > year_to:
                continue
                
            pub_by_year[year] += 1
    
    # Crear series ordenada
    if pub_by_year:
        years = sorted(pub_by_year.keys())
        publications = [pub_by_year[year] for year in years]
    else:
        logger.warning("No se encontraron publicaciones en el rango especificado")
        return [], [], {}, author_name
    
    # Calcular estadísticas
    total = sum(publications)
    stats = {
        'total_publications': total,
        'min_year': min(years),
        'max_year': max(years),
        'avg_per_year': total / len(years) if years else 0,
        'peak_year': get_peak(publications, years)[0],
        'peak_publications': max(publications) if publications else 0,
        'active_years': len(years),
    }
    
    if verbose:
        logger.info(f"Autor: {author_name}")
        logger.info(f"Años: {years}")
        logger.info(f"Publicaciones por año: {publications}")
        logger.info(f"Total: {total} publicaciones")
    
    return years, publications, stats, author_name


def make_investigator_chart(
    fig,
    ax1,
    ax_table,
    years_data: List[int],
    publications_data: List[int],
    bar_peak_note: Optional[str] = None,
    bar_peak_color: str = CHART_COLORS['green'],
    num_years: int = None,
):
    """
    Genera el gráfico de investigador con tabla de datos.
    """
    
    x = np.arange(len(years_data))
    if num_years is None:
        num_years = len(years_data)

    # Detectar el pico y valor máximo
    if sum(publications_data) == 0:
        bar_peak_year, bar_peak_idx = years_data[0], 0
        max_pub_val = 1
    else:
        bar_peak_year, bar_peak_idx = get_peak(publications_data, years_data)
        max_pub_val = max(publications_data)

    # ── Barras ─────────────────────────────────────────────────────────────────
    norm_v = (
        np.array(publications_data) / max_pub_val
        if max_pub_val > 0
        else np.zeros_like(publications_data, dtype=float)
    )
    bar_colors = [plt.cm.Blues(0.25 + 0.50 * v) for v in norm_v]
    if max_pub_val > 0:
        bar_colors[bar_peak_idx] = "#2DA44E"  # Verde para el pico

    # Ancho "profesional" de barras basado en densidad de datos
    # Pocas barras: más anchas (mejor proporción visual)
    # Muchas barras: más estrechas (mejor espaciado)
    if num_years == 1:
        bar_width = 0.38  # Ultra compacta para 1 sola barra
    elif num_years <= 3:
        bar_width = 0.50  # Barras medias-amplias
    elif num_years <= 8:
        bar_width = 0.48  # Barras medias
    elif num_years <= 15:
        bar_width = 0.44  # Barras algo más estrechas
    else:
        bar_width = 0.38  # Barras estrechas para muchos datos

    bars = ax1.bar(
        x,
        publications_data,
        width=bar_width,
        color=bar_colors,
        alpha=0.75,
        zorder=2,
        linewidth=0,
    )

    # ══════════════════════════════════════════════════════════════════════════════
    # MÁRGENES RESPONSIVOS - Elimina espacios en blanco sin necesidad
    # ══════════════════════════════════════════════════════════════════════════════
    # Left/right margins: más compactos con pocos años
    if num_years == 1:
        margin_pct = 0.02  # Mínimo absoluto para 1 año
        y_margin_pct = 0.10  # Pero espacio Y para ver la barra
    elif num_years <= 2:
        margin_pct = 0.04
        y_margin_pct = 0.09
    elif num_years <= 4:
        margin_pct = 0.06
        y_margin_pct = 0.08
    elif num_years <= 8:
        margin_pct = 0.08
        y_margin_pct = 0.08
    else:
        margin_pct = 0.10
        y_margin_pct = 0.08
    
    ax1.margins(x=margin_pct, y=y_margin_pct)
    ax1.set_xlim(-0.5 - margin_pct, len(years_data) - 0.5 + margin_pct)

    # Etiquetas sobre las barras
    for bar, val, yr in zip(bars, publications_data, years_data):
        col = bar_peak_color if yr == bar_peak_year else CHART_COLORS['muted']
        if val > 0:
            text_y_pos = bar.get_height() + (max_pub_val * 0.05)
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                text_y_pos,
                str(val),
                ha="center",
                va="bottom",
                fontsize=8,
                color=col,
                fontweight="bold",
            )

    # ══════════════════════════════════════════════════════════════════════════════
    # CONFIGURACIÓN DE EJES RESPONSIVA
    # ══════════════════════════════════════════════════════════════════════════════
    # Y-axis label: tamaño responsivo
    if num_years >= 15:
        ylabel_size = 9.5
    elif num_years >= 8:
        ylabel_size = 10
    else:
        ylabel_size = 10.5
    
    ax1.set_ylabel("N.° de publicaciones", fontsize=ylabel_size, color=CHART_COLORS['muted'], labelpad=10)
    
    # X-axis ticks: tamaño responsivo
    if num_years >= 15:
        tick_size = 8
    elif num_years >= 8:
        tick_size = 8.5
    else:
        tick_size = 9.5
    
    ax1.set_xticks(x)
    ax1.set_xticklabels(years_data, rotation=0, fontsize=tick_size)
    
    ax1.set_ylim(0, max_pub_val * 1.25 if max_pub_val > 0 else 5)
    ax1.yaxis.set_major_locator(
        ticker.MultipleLocator(
            max(1, int(max_pub_val / 5)) if max_pub_val > 0 else 1
        )
    )
    ax1.grid(axis="y", zorder=0)
    ax1.spines["bottom"].set_color(CHART_COLORS['border'])
    ax1.spines["left"].set_color(CHART_COLORS['border'])
    ax1.tick_params(axis="x", which="both", length=0)

    # Anotación del pico
    if max_pub_val > 0:
        note = bar_peak_note if bar_peak_note else f"  Pico: {bar_peak_year}"
        # Offset dinámico: con pocos años, offset mínimo; con muchos, normal
        if num_years == 1:
            x_offset_ann = 0.4  # Muy cerca de la barra
        elif num_years <= 3:
            x_offset_ann = bar_peak_idx + 0.6 if bar_peak_idx < len(years_data) - 1 else bar_peak_idx - 0.8
        else:
            x_offset_ann = (
                bar_peak_idx + 1.2 if bar_peak_idx < len(years_data) - 2 else bar_peak_idx - 1.5
            )
        y_text_ann = publications_data[bar_peak_idx] + (max_pub_val * 0.1)
        ax1.annotate(
            note,
            xy=(bar_peak_idx, publications_data[bar_peak_idx]),
            xytext=(x_offset_ann, y_text_ann),
            fontsize=8.5,
            color=bar_peak_color,
            arrowprops=dict(arrowstyle="->", color=bar_peak_color, lw=1.2),
            bbox=dict(
                boxstyle="round,pad=0.35",
                facecolor="#E6F4EA",
                edgecolor=bar_peak_color,
                linewidth=0.8,
            ),
        )

    # Leyenda
    leg = [
        mpatches.Patch(facecolor="#388BFD", alpha=0.7, label="Publicaciones"),
    ]
    ax1.legend(
        handles=leg,
        loc="upper left",
        fontsize=9,
        framealpha=0.85,
        facecolor=CHART_COLORS['panel'],
        edgecolor=CHART_COLORS['border'],
        labelcolor=CHART_COLORS['text'],
        handlelength=2,
        borderpad=0.8,
    )

    # ── Tabla ──────────────────────────────────────────────────────────────────
    ax_table.axis("off")

    col_labels = [str(y) for y in years_data]
    cell_data = [
        [str(v) if v > 0 else "—" for v in publications_data],
    ]
    row_labels = ["Publicaciones"]

    cell_colors = []
    row_c = []
    for col_i in range(len(years_data)):
        if max_pub_val > 0:
            intensity = publications_data[col_i] / max_pub_val
            r, g, b, _ = plt.cm.Blues(0.10 + 0.40 * intensity)
            row_c.append((r, g, b, 0.60))
        else:
            row_c.append((*[int(CHART_COLORS['grid'][i : i + 2], 16) / 255 for i in (1, 3, 5)], 1.0))
    cell_colors.append(row_c)

    table = ax_table.table(
        cellText=cell_data,
        rowLabels=row_labels,
        colLabels=col_labels,
        cellColours=cell_colors,
        rowColours=[CHART_COLORS['border']],
        colColours=[CHART_COLORS['border']] * len(col_labels),
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1, 1.8)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(CHART_COLORS['border'])
        cell.set_linewidth(0.5)
        if col == -1:  # Row labels
            cell.set_text_props(color=CHART_COLORS['text'], fontweight="bold", fontsize=8.5)
        elif row == 0:  # Column labels (years)
            cell.set_text_props(color=CHART_COLORS['muted'], fontsize=8)
        else:  # Data cells
            val_text = cell.get_text().get_text()
            if val_text != "—":
                cell.set_text_props(color=CHART_COLORS['blue'], fontweight="bold", fontsize=8.5)
            else:
                cell.set_text_props(color=CHART_COLORS['muted'], fontsize=8.5)


def generate_investigator_chart_file(
    author_id: str,
    affiliation_ids: Optional[List[str]] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    institution_name: str = "Universidad Simón Bolívar",
    figsize: Tuple[float, float] = (16, 10),
    dpi: int = 300,
    output_dir: Path = Path("reports/charts"),
) -> Dict:
    """
    Genera un gráfico y lo guarda en un archivo PNG.
    
    El nombre del investigador se obtiene automáticamente de los registros.
    
    Returns:
        Dict con información del gráfico generado
    """
    
    try:
        logger.info(f"[CHART] ═══════════════════════════════════════════════════════")
        logger.info(f"[CHART] Iniciando generación para AU-ID: {author_id}")
        logger.info(f"[CHART] Parámetros: year_from={year_from}, year_to={year_to}, affiliation_ids={affiliation_ids}")
        
        # Crear directorio de salida
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[CHART] Directorio creado: {output_dir}")
        
        # Construir query
        if affiliation_ids:
            aff_clause = " OR ".join([f"AF-ID ( {aff_id} )" for aff_id in affiliation_ids])
            query = f"AU-ID ( {author_id} ) AND ({aff_clause})"
        else:
            query = f"AU-ID ( {author_id} )"
        
        logger.info(f"[CHART] Query Scopus: {query}")
        
        # Extraer datos (ahora retorna también el nombre del autor)
        logger.info(f"[CHART] Extrayendo publicaciones...")
        years, publications, stats, investigator_name = extract_publications_by_year(
            query=query,
            author_id=author_id,
            year_from=year_from,
            year_to=year_to,
            verbose=True
        )
        
        logger.info(f"[CHART] Datos extraídos: {len(years)} años, {sum(publications)} publicaciones")
        logger.info(f"[CHART] Investigador: {investigator_name}")
        
        if not years:
            logger.error(f"[CHART] No se encontraron publicaciones")
            raise ValueError("No se encontraron publicaciones para la consulta especificada")
        
        # Calcular dimensiones
        num_years = len(years)
        logger.info(f"[CHART] Calculando dimensioning para {num_years} años...")
        
        if num_years == 1:
            fig_width, fig_height = 6.5, 6.0
        elif num_years <= 2:
            fig_width, fig_height = 7.0, 6.0
        elif num_years <= 4:
            fig_width = 7.5 + (num_years - 3) * 0.3
            fig_height = 6.3
        elif num_years <= 7:
            fig_width = 8.2 + (num_years - 5) * 0.35
            fig_height = 6.8
        elif num_years <= 12:
            fig_width = 9.2 + (num_years - 7) * 0.4
            fig_height = 7.3
        elif num_years <= 18:
            fig_width = 11.2 + (num_years - 12) * 0.3
            fig_height = 7.8
        elif num_years <= 25:
            fig_width = 13.0 + (num_years - 18) * 0.2
            fig_height = 8.3
        else:
            fig_width = min(16, 14.4 + (num_years - 25) * 0.05)
            fig_height = min(10, 8.7 + (num_years - 25) * 0.01)
        
        figsize = (fig_width, fig_height)
        logger.info(f"[CHART] Figura: {fig_width:.1f}\" x {fig_height:.1f}\"")
        
        # Height ratios
        if num_years == 1:
            height_ratio, hspace_val = 5.5, 0.12
        elif num_years <= 2:
            height_ratio, hspace_val = 4.5, 0.10
        elif num_years <= 4:
            height_ratio, hspace_val = 4.2, 0.08
        elif num_years <= 7:
            height_ratio, hspace_val = 3.9, 0.07
        elif num_years <= 12:
            height_ratio, hspace_val = 3.6, 0.06
        elif num_years <= 18:
            height_ratio, hspace_val = 3.3, 0.055
        else:
            height_ratio, hspace_val = 3.0, 0.05
        
        logger.info(f"[CHART] Configurando matplotlib...")
        configure_matplotlib_styles()
        
        logger.info(f"[CHART] Creando figura...")
        fig, (ax_chart, ax_table) = plt.subplots(
            2, 1,
            figsize=figsize,
            gridspec_kw={"height_ratios": [height_ratio, 1], "hspace": hspace_val},
            facecolor=CHART_COLORS['bg'],
        )
        
        logger.info(f"[CHART] Renderizando gráfico...")
        make_investigator_chart(
            fig, ax_chart, ax_table,
            years_data=years,
            publications_data=publications,
            num_years=num_years,
        )
        
        # Metadatos
        total_pub = sum(publications)
        min_year, max_year = years[0], years[-1]
        
        fig.text(
            0.065, 0.97,
            f"Publicaciones de {investigator_name}  |  {min_year}–{max_year}",
            fontsize=14, fontweight="bold", color=CHART_COLORS['text'],
            va="bottom",
        )
        
        fig.text(
            0.065, 0.956,
            f"Total de publicaciones: {total_pub}  ·  Fuente: Scopus ID: {author_id}",
            fontsize=9, color=CHART_COLORS['muted'],
            va="bottom",
        )
        
        fig.add_artist(plt.Line2D([0.065, 0.95], [0.951, 0.951], 
                                   transform=fig.transFigure, 
                                   color=CHART_COLORS['border'], linewidth=0.8))
        fig.add_artist(plt.Line2D([0.065, 0.95], [0.015, 0.015], 
                                   transform=fig.transFigure, 
                                   color=CHART_COLORS['border'], linewidth=0.6))
        
        fig.text(0.065, 0.006, f"· {institution_name}", 
                fontsize=8, color=CHART_COLORS['muted'], va="bottom")
        
        # Guardar archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name_slug = investigator_name.lower().replace(" ", "_").replace(",", "").replace(".", "")
        filename = f"grafico_{name_slug}_{timestamp}.png"
        filepath = output_dir / filename
        
        logger.info(f"[CHART] Guardando: {filepath}")
        plt.savefig(str(filepath), dpi=dpi, bbox_inches="tight", facecolor=CHART_COLORS['bg'])
        plt.close(fig)
        logger.info(f"[CHART] Archivo guardado exitosamente")
        
        # Preparar respuesta
        publications_by_year = []
        for year, pub_count in zip(years, publications):
            percentage = (pub_count / total_pub * 100) if total_pub > 0 else 0
            publications_by_year.append({
                'year': year,
                'count': pub_count,
                'percentage': round(percentage, 1)
            })
        
        return {
            'filename': filename,
            'file_path': str(filepath.resolve()),
            'full_path': str(filepath),
            'investigator_name': investigator_name,
            'statistics': {
                'total_publications': total_pub,
                'min_year': min_year,
                'max_year': max_year,
                'avg_per_year': round(stats['avg_per_year'], 2),
                'peak_year': stats['peak_year'],
                'peak_publications': stats['peak_publications'],
                'active_years': stats['active_years'],
                'publications_by_year': publications_by_year,
            },
            'query_used': query,
            'generated_at': datetime.now().isoformat(),
        }
    
    except Exception as e:
        logger.error(f"[CHART] Error fatal: {str(e)}", exc_info=True)
        raise
