"""
api/services/graph_renderer.py
==============================
Servicio profesional de generación de gráficos bibliométricos.

Agnóstico de fuente: toma datos de AuthorData y genera PNG.
Layout idéntico a chart_generator.py pero sin dependencia de Scopus API.

Incluye:
- Header con información del investigador
- KPIs (6 indicadores bibliométricos)
- Gráficos (barras de pubs + línea de citas + CPP/año)
- Tabla de datos por año
- Análisis automático (hallazgos positivos/negativos)
- Footer con institución y fecha

Uso:
    from api.services.data_provider import fetch_author_data
    from api.services.graph_renderer import render_author_chart
    
    author_data = fetch_author_data(db, author_id=1)
    chart_info = render_author_chart(
        author_data=author_data,
        institution_name="Universidad Simón Bolívar",
        output_dir=Path("reports/charts"),
    )
"""

import logging
import statistics
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Tuple, List
from collections import Counter

import numpy as np
import matplotlib
matplotlib.use("Agg")  # Sin entorno gráfico
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker
from matplotlib.patches import FancyBboxPatch

from api.services.data_provider import AuthorData
from api.services.analysis import CampoDisciplinar, generar_hallazgos, dibujar_analisis

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# PALETA DE COLORES
# ══════════════════════════════════════════════════════════════════════════════



# ══════════════════════════════════════════════════════════════════════════════
# PALETA DE COLORES (idéntica a chart_generator.py)
# ══════════════════════════════════════════════════════════════════════════════

_C = dict(
    AZUL_BAR   = "#A8C8E0",
    AZUL_BRD   = "#7AAAC8",
    VERDE_BAR  = "#3A9E6F",
    VERDE_BRD  = "#2D7A56",
    ROJO       = "#DC2626",
    GRIS_800   = "#1F2937",
    GRIS_700   = "#374151",
    GRIS_600   = "#4B5563",
    GRIS_500   = "#6B7280",
    GRIS_400   = "#9CA3AF",
    GRIS_200   = "#E5E7EB",
    GRIS_100   = "#F3F4F6",
    GRIS_50    = "#F9FAFB",
    NARANJA_BG = "#FEF3C7",
    AZUL_KPI   = "#2563EB",
    VERDE_KPI  = "#059669",
)


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def render_author_chart(
    author_data: AuthorData,
    institution_name: str = "Universidad Simón Bolívar",
    output_dir: Path = Path("reports/charts"),
    dpi: int = 180,
    campo: CampoDisciplinar = CampoDisciplinar.CIENCIAS_SALUD,
) -> Dict[str, str]:
    """
    Renderiza un gráfico PNG idéntico a chart_generator.py pero con datos BD.
    
    Incluye: Header + KPIs + Gráficos + Tabla + Análisis + Footer
    
    Args:
        author_data: Objeto AuthorData con datos bibliométricos
        institution_name: Nombre para pie de página
        output_dir: Directorio de salida
        dpi: Resolución del PNG
        campo: Campo disciplinar para contexto
    
    Returns:
        Dict con claves:
            - "filename": nombre del archivo PNG
            - "file_path": ruta relativa
            - "file_size_mb": tamaño en MB
            - "investigator_name": nombre del investigador
    """
    
    logger.info(
        f"[GRAPH v2] Renderizando {author_data.author_name} "
        f"({author_data.total_publications} pubs, {author_data.total_citations} citas)"
    )
    
    # Extraer datos de author_data
    investigador = author_data.author_name
    scopus_id = author_data.source_ids.get("scopus", "N/A")
    fecha_ext = author_data.extraction_date
    rango = author_data.year_range
    años = author_data.yearly_data.years
    pubs = author_data.yearly_data.publications
    cites = author_data.yearly_data.citations
    total_arts = author_data.total_publications
    total_citas = author_data.total_citations
    h_index = author_data.h_index
    cpp = author_data.cpp
    mediana_citas = author_data.median_citations
    pct_citados = author_data.percent_cited
    
    # Calcular CPP por año
    cpp_por_año = [
        round(c / p, 1) if p > 0 else 0.0
        for p, c in zip(pubs, cites)
    ]
    
    # Años especiales
    año_pico = (
        años[cites.index(max(cites))]
        if max(cites) > 0
        else años[0]
    )
    año_max_pub = (
        años[pubs.index(max(pubs))]
        if max(pubs) > 0
        else años[0]
    )
    
    # Construir figura
    fig = _build_figure(
        investigador=investigador,
        scopus_id=scopus_id,
        fecha_ext=fecha_ext,
        rango=rango,
        años=años,
        pubs=pubs,
        cites=cites,
        total_arts=total_arts,
        total_citas=total_citas,
        h_index=h_index,
        cpp=cpp,
        mediana_citas=mediana_citas,
        pct_citados=pct_citados,
        cpp_por_año=cpp_por_año,
        año_pico=año_pico,
        año_max_pub=año_max_pub,
        institution_name=institution_name,
        campo=campo,
    )
    
    # Crear directorio
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generar nombre de archivo
    slug = investigador.lower().replace(" ", "_").replace(".", "").replace("-", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"grafico_{slug}_{timestamp}.png"
    filepath = output_dir / filename
    
    # Guardar PNG
    fig.savefig(
        filepath,
        dpi=dpi,
        bbox_inches="tight",
        facecolor="white",
        edgecolor=_C["GRIS_200"],
    )
    plt.close(fig)
    
    # Estadísticas
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    
    logger.info(f"[GRAPH v2] Gráfico guardado: {filename} ({file_size_mb:.2f} MB)")
    
    return {
        "filename": filename,
        "file_path": str(filepath),
        "file_size_mb": round(file_size_mb, 2),
        "investigator_name": investigador,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUCTOR DE FIGURA (idéntico a chart_generator.py)
# ══════════════════════════════════════════════════════════════════════════════

def _build_figure(
    investigador: str,
    scopus_id: str,
    fecha_ext: str,
    rango: str,
    años: list,
    pubs: list,
    cites: list,
    total_arts: int,
    total_citas: int,
    h_index: int,
    cpp: float,
    mediana_citas: float,
    pct_citados: float,
    cpp_por_año: list,
    año_pico: int,
    año_max_pub: int,
    institution_name: str,
    campo: CampoDisciplinar = CampoDisciplinar.CIENCIAS_SALUD,
) -> plt.Figure:
    """
    Construye la figura matplotlib idéntica a chart_generator._build_figure
    
    Layout:
    - ax_hdr: Header (nombre, rango, meta datos)
    - ax_kpi: 7 KPIs (pubs, citas, h, cpp, mediana, %citados, año pico)
    - ax_chart: Gráficos (barras + línea + CPP/año)
    - ax_tbl: Tabla de datos (pubs/citas/cpp por año)
    - ax_ana: Análisis automático (hallazgos positivos/negativos/notas)
    - ax_ftr: Footer (institución + fecha)
    """
    
    C = _C  # alias
    
    # Canvas
    fig = plt.figure(figsize=(13, 13), facecolor="white")
    fig.patch.set_edgecolor(C["GRIS_200"])
    fig.patch.set_linewidth(1)
    
    gs = fig.add_gridspec(
        6, 1,
        height_ratios=[1.0, 1.0, 5.0, 2.0, 1.4, 0.5],  # Análisis en 1.4
        hspace=0.08,
        left=0.06, right=0.94,
        top=0.96, bottom=0.04,
    )
    
    ax_hdr   = fig.add_subplot(gs[0])
    ax_kpi   = fig.add_subplot(gs[1])
    ax_chart = fig.add_subplot(gs[2])
    ax_tbl   = fig.add_subplot(gs[3])
    ax_ana   = fig.add_subplot(gs[4])  # Análisis
    ax_ftr   = fig.add_subplot(gs[5])
    
    for ax in [ax_hdr, ax_kpi, ax_tbl, ax_ana, ax_ftr]:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
    
    # ════════════════════════════════════════════════════════════════════════
    # 1. HEADER
    # ════════════════════════════════════════════════════════════════════════
    nombre_corto = len(investigador) < 15
    
    if nombre_corto:
        titulo = f"Publicaciones de {investigador}"
        ax_hdr.text(0.02, 0.80, titulo,
                    fontsize=14, fontweight="bold", color=C["GRIS_800"],
                    transform=ax_hdr.transAxes, va="top", ha="left")
        ax_hdr.text(0.98, 0.80, f"{rango}  ",
                    fontsize=12, color=C["GRIS_400"], fontweight="300",
                    transform=ax_hdr.transAxes, va="top", ha="right")
    else:
        ax_hdr.text(0.02, 0.80, f"Publicaciones de {investigador}",
                    fontsize=14, fontweight="bold", color=C["GRIS_800"],
                    transform=ax_hdr.transAxes, va="top", ha="left")
        ax_hdr.text(0.02, 0.58, rango,
                    fontsize=12, color=C["GRIS_400"], fontweight="300",
                    transform=ax_hdr.transAxes, va="top", ha="left")
    
    meta = (f"Publicaciones: {total_arts}   •   "
            f"Citaciones: {total_citas:,}   •   "
            f"Scopus ID: {scopus_id}")
    ax_hdr.text(0.02, 0.40, meta,
                fontsize=8, color=C["GRIS_500"],
                transform=ax_hdr.transAxes, va="top")
    
    ax_hdr.plot([0, 1], [0.05, 0.05], color=C["GRIS_200"], lw=0.8,
                transform=ax_hdr.transAxes, clip_on=False)
    
    # ════════════════════════════════════════════════════════════════════════
    # 2. KPIs (7 indicadores)
    # ════════════════════════════════════════════════════════════════════════
    ax_kpi.add_patch(FancyBboxPatch(
        (0, 0), 1, 1, boxstyle="round,pad=0.01",
        facecolor="#F0F4F9", edgecolor=C["GRIS_200"], linewidth=0.5,
        transform=ax_kpi.transAxes))
    
    kpis = [
        ("PUBLICACIONES", str(total_arts),    rango,                "#2563EB"),
        ("CITACIONES",    f"{total_citas:,}", "total",              "#DC2626"),
        ("H-INDEX",       str(h_index),       "acumulado",          "#059669"),
        ("CPP",           str(cpp),           "citas/pub",          "#7C3AED"),
        ("MEDIANA",       str(mediana_citas), "citas",              "#F59E0B"),
        ("% CITADOS",     f"{pct_citados}%",  "de trabajos",        "#06B6D4"),
        ("AÑO PICO",      str(año_pico),      "máx. citas",         "#0B8511"),
    ]
    
    kpi_w = 1 / len(kpis)
    for i, (lbl, val, sub, color) in enumerate(kpis):
        xc = i * kpi_w + kpi_w / 2
        
        if i > 0:
            ax_kpi.plot([i * kpi_w, i * kpi_w], [0.10, 0.90],
                        color=C["GRIS_200"], lw=0.8,
                        transform=ax_kpi.transAxes, clip_on=False)
        
        ax_kpi.text(xc, 0.85, lbl, ha="center", va="top", fontsize=6.5,
                    color=C["GRIS_600"], fontweight="bold",
                    transform=ax_kpi.transAxes)
        ax_kpi.text(xc, 0.56, val, ha="center", va="center", fontsize=16,
                    color=color, fontweight="bold",
                    transform=ax_kpi.transAxes)
        ax_kpi.text(xc, 0.15, sub, ha="center", va="bottom", fontsize=6,
                    color=C["GRIS_400"], fontweight="normal",
                    transform=ax_kpi.transAxes)
    
    ax_kpi.plot([0, 1], [0.05, 0.05], color=C["GRIS_200"], lw=0.8,
                transform=ax_kpi.transAxes, clip_on=False)
    
    # ════════════════════════════════════════════════════════════════════════
    # 3. GRÁFICOS (barras + línea + CPP/año)
    # ════════════════════════════════════════════════════════════════════════
    ax_chart.margins(y=0.05)
    ax2 = ax_chart.twinx()
    ax3 = ax_chart.twinx()
    ax3.spines["right"].set_position(("outward", 55))
    
    n = len(años)
    x = np.arange(n)
    
    # Ancho dinámico
    if n == 1:
        w = 0.4
        margin_x = 0.8
    elif n <= 3:
        w = 0.5
        margin_x = 0.6
    elif n <= 7:
        w = 0.55
        margin_x = 0.5
    else:
        w = 0.35
        margin_x = 0.3
    
    bar_fc = [C["VERDE_BAR"] if a == año_max_pub else C["AZUL_BAR"] for a in años]
    bar_ec = [C["VERDE_BRD"] if a == año_max_pub else C["AZUL_BRD"] for a in años]
    
    bars = ax_chart.bar(x, pubs, width=w, color=bar_fc,
                        edgecolor=bar_ec, linewidth=0.8, zorder=2)
    
    val_fontsize = 10 if n == 1 else 9 if n <= 5 else 8 if n <= 10 else 7
    for bar, val in zip(bars, pubs):
        ax_chart.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(pubs) * 0.015,
            str(val), ha="center", va="bottom",
            fontsize=val_fontsize, color=C["GRIS_600"], fontweight="500")
    
    marker_size = 6 if n <= 5 else 5 if n <= 10 else 4
    marker_size_cpp = 5 if n <= 5 else 4 if n <= 10 else 3
    
    ax2.plot(x, cites, color=C["ROJO"], linewidth=2,
             marker="o", markersize=marker_size,
             markerfacecolor=C["ROJO"], markeredgecolor="white",
             markeredgewidth=1.5, zorder=3, label="Citaciones")
    
    ax3.plot(x, cpp_por_año, color="#8B5CF6", linewidth=2.5, linestyle="--",
             marker="s", markersize=marker_size_cpp,
             markerfacecolor="#8B5CF6", markeredgecolor="white",
             markeredgewidth=1.2, zorder=2, label="CPP/año")
    
    # Anotación de pico
    idx_pico = cites.index(max(cites)) if max(cites) > 0 else 0
    if max(cites) > 0:
        if n == 1:
            x_text = idx_pico
            y_text = max(cites) * 1.1
        elif idx_pico <= n // 3:
            x_text = idx_pico + 1.5
            y_text = max(cites) * 0.85
        elif idx_pico >= 2 * n // 3:
            x_text = idx_pico - 1.5
            y_text = max(cites) * 0.75
        else:
            x_text = idx_pico
            y_text = max(cites) * 1.05
        
        ax2.annotate(
            f"Máx: {año_pico} ({max(cites)})",
            xy=(idx_pico, max(cites)),
            xytext=(x_text, y_text),
            fontsize=8, color="#DC2626", fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="#FEE2E2",
                      edgecolor="#DC2626", linewidth=1),
            arrowprops=dict(arrowstyle="-|>", color="#DC2626", lw=1.5),
        )
    
    # Escalas
    ax_chart.set_xlim(-margin_x, n - 1 + margin_x)
    ax_chart.set_ylim(0, max(pubs) * 1.35)
    ax2.set_ylim(0, max(cites) * 1.35)
    ax3.set_ylim(0, max(cpp_por_año) * 1.35 if max(cpp_por_año) > 0 else 1.0)
    
    ax_chart.set_xticks(x)
    xtick_fontsize = 11 if n == 1 else 10 if n <= 5 else 9 if n <= 10 else 8
    ax_chart.set_xticklabels([str(a) for a in años], fontsize=xtick_fontsize, 
                             color=C["GRIS_400"])
    
    ax_chart.yaxis.set_major_locator(ticker.MultipleLocator(max(1, max(pubs) // 7)))
    ax_chart.tick_params(axis="y", labelsize=9, labelcolor=C["GRIS_400"])
    ax3.tick_params(axis="y", labelsize=9, labelcolor="#8B5CF6")
    
    cit_step = max(1, round(max(cites) / 6 / 10) * 10) if max(cites) >= 60 else max(1, max(cites) // 6)
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(cit_step))
    ax2.tick_params(axis="y", labelsize=9, labelcolor="#DC2626")
    
    cpp_step = max(0.5, round(max(cpp_por_año) / 5 * 2) / 2) if max(cpp_por_año) > 0 else 0.5
    ax3.yaxis.set_major_locator(ticker.MultipleLocator(cpp_step))
    
    ax_chart.set_ylabel("N.° de publicaciones", fontsize=9, color=C["GRIS_600"], labelpad=8)
    ax2.set_ylabel("N.° de citaciones", fontsize=9, color="#DC2626", labelpad=8)
    ax3.set_ylabel("CPP/año", fontsize=9, color="#8B5CF6", labelpad=8)
    
    ax_chart.spines[["top"]].set_visible(False)
    ax2.spines[["top"]].set_visible(False)
    ax3.spines[["top"]].set_visible(False)
    for sp in ["left", "right", "bottom"]:
        ax_chart.spines[sp].set_color(C["GRIS_200"])
        ax2.spines[sp].set_color(C["GRIS_200"])
        ax3.spines[sp].set_color("#8B5CF6")
    
    ax_chart.grid(axis="y", color=C["GRIS_200"], lw=0.5, zorder=0)
    ax_chart.set_axisbelow(True)
    ax_chart.set_facecolor("white")
    
    # Legendas
    p1 = mpatches.Patch(facecolor="#A8C8E0", edgecolor="#7AAAC8",
                        linewidth=1, label="Publicaciones")
    p2 = plt.Line2D([0], [0], color="#DC2626", linewidth=2.5,
                    marker="o", markersize=6,
                    markerfacecolor="#DC2626", markeredgecolor="white",
                    markeredgewidth=1.5, label="Citaciones")
    p3 = plt.Line2D([0], [0], color="#8B5CF6", linewidth=2.5, linestyle="--",
                    marker="s", markersize=5,
                    markerfacecolor="#8B5CF6", markeredgecolor="white",
                    markeredgewidth=1.2, label="CPP/año")
    ax_chart.legend(handles=[p1, p2, p3], loc="upper left",
                    fontsize=9, frameon=True, framealpha=0.95,
                    edgecolor=C["GRIS_200"], facecolor="white", borderpad=0.8,
                    shadow=False)
    
    # ════════════════════════════════════════════════════════════════════════
    # 4. TABLA DE DATOS (FIX: ARREGLAR SUPERPOSICIÓN)
    # ════════════════════════════════════════════════════════════════════════
    n_years = len(años)
    col_w = 1 / (n_years + 1)
    
    # Dimensiones dinámicas pero GARANTIZANDO ESPACIO PARA ANÁLISIS
    # Máximo: 4 filas * row_h <= 0.75 (dejando 0.15 para análisis)
    max_row_h_allowed = 0.75 / 4  # 0.1875
    
    if n_years <= 4:
        row_h = 0.15  # Caben bien
        font_size_table = 7
        font_size_header = 6.5
        row_label_fontsize = 6
    elif n_years <= 6:
        row_h = 0.13
        font_size_table = 6.5
        font_size_header = 6
        row_label_fontsize = 5.5
    elif n_years <= 9:
        row_h = 0.11
        font_size_table = 5.5
        font_size_header = 5
        row_label_fontsize = 5
    elif n_years <= 12:
        row_h = 0.10
        font_size_table = 5
        font_size_header = 4.5
        row_label_fontsize = 4.5
    else:
        row_h = 0.09
        font_size_table = 4.5
        font_size_header = 4
        row_label_fontsize = 4
    
    # FIJO: Header siempre en 0.90 para garantizar espacio
    header_y_start = 0.90
    
    # Dibujar header (años)
    for j, lbl in enumerate([""] + [str(a) for a in años]):
        ax_tbl.add_patch(FancyBboxPatch(
            (j * col_w, header_y_start), col_w, row_h,
            boxstyle="square,pad=0",
            facecolor=C["GRIS_100"], edgecolor=C["GRIS_200"], linewidth=0.5,
            transform=ax_tbl.transAxes))
        if lbl:
            ax_tbl.text(j * col_w + col_w / 2, header_y_start + row_h / 2, lbl,
                        ha="center", va="center", fontsize=font_size_header,
                        color=C["GRIS_600"], fontweight="bold",
                        transform=ax_tbl.transAxes)
    
    # Filas de datos
    rows = [
        ("Publicaciones", pubs,       C["AZUL_KPI"], "int"),
        ("Citaciones",    cites,      C["ROJO"],     "int"),
        ("CPP/año",       cpp_por_año, "#8B5CF6",    "float"),
    ]
    
    for ri, (lbl, vals, color, val_type) in enumerate(rows):
        ypos = header_y_start - (ri + 1) * row_h
        
        # Etiqueta fila
        ax_tbl.add_patch(FancyBboxPatch(
            (0, ypos), col_w, row_h,
            boxstyle="square,pad=0",
            facecolor=C["GRIS_50"], edgecolor=C["GRIS_200"], linewidth=0.5,
            transform=ax_tbl.transAxes))
        ax_tbl.text(col_w / 2, ypos + row_h / 2, lbl,
                    ha="center", va="center", fontsize=row_label_fontsize,
                    color=C["GRIS_600"], fontweight="bold",
                    transform=ax_tbl.transAxes)
        
        # Celdas de datos
        max_val = max(vals) if vals else 1
        for j, v in enumerate(vals):
            is_peak = v == max_val and v > 0
            if is_peak:
                if lbl == "Citaciones":
                    bg = "#FFE5CC"
                    txc = "#D97706"
                else:
                    bg = "#DBEAFE"
                    txc = "#1E40AF"
            else:
                bg = "white"
                txc = color
            
            ax_tbl.add_patch(FancyBboxPatch(
                ((j + 1) * col_w, ypos), col_w, row_h,
                boxstyle="square,pad=0",
                facecolor=bg, edgecolor=C["GRIS_200"], linewidth=0.5,
                transform=ax_tbl.transAxes))
            
            display_val = f"{v:.1f}" if val_type == "float" else str(int(v))
            adjusted_fontsize = font_size_table if len(display_val) <= 3 else max(font_size_table - 1, 3)
            
            ax_tbl.text((j + 1) * col_w + col_w / 2, ypos + row_h / 2,
                        display_val, ha="center", va="center",
                        fontsize=adjusted_fontsize, color=txc,
                        fontweight="bold" if is_peak else "500",
                        transform=ax_tbl.transAxes)
    
    # ════════════════════════════════════════════════════════════════════════
    # 5. ANÁLISIS AUTOMÁTICO (SIN SUPERPOSICIÓN)
    # ════════════════════════════════════════════════════════════════════════
    positivos, negativos, notas = generar_hallazgos(
        total_arts=total_arts, total_citas=total_citas,
        h_index=h_index, cpp=cpp,
        mediana=mediana_citas, pct_citados=pct_citados,
        años=años, pubs=pubs, cites=cites,
        año_pico=año_pico, año_max_pub=año_max_pub,
        campo=campo,
        db_session=None,  # Usar parámetros por defecto
    )
    # NOTA: Los análisis se renderizarán EN EL PDF, no en la imagen PNG
    # dibujar_analisis(ax_ana, positivos, negativos, C, notas)
    
    # ════════════════════════════════════════════════════════════════════════
    # 6. FOOTER
    # ════════════════════════════════════════════════════════════════════════
    ax_ftr.plot([0, 1], [1, 1], color=C["GRIS_200"], lw=0.8,
                transform=ax_ftr.transAxes, clip_on=False)
    ax_ftr.text(0.01, 0.4, f"• {institution_name}",
                fontsize=8, color=C["GRIS_500"],
                transform=ax_ftr.transAxes, va="center")
    ax_ftr.text(0.99, 0.4, f"Extracción: {fecha_ext}",
                fontsize=7.5, color=C["GRIS_500"], ha="right",
                fontfamily="monospace",
                transform=ax_ftr.transAxes, va="center")
    
    return fig
