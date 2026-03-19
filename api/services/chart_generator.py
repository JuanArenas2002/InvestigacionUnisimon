"""
api/services/chart_generator.py
================================
Servicio de generación de gráficos bibliométricos.
Produce un PNG con el mismo diseño que el reporte HTML institucional.

Uso desde FastAPI (ver router charts.py):
    from api.services.chart_generator import generate_investigator_chart_file

    result = generate_investigator_chart_file(
        author_id       = "57193767797",
        affiliation_ids = ["60106970", "60112687"],
        year_from       = 2015,
        year_to         = 2025,
        institution_name= "Universidad Simón Bolívar",
        output_dir      = Path("reports/charts"),
    )
"""

import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import numpy as np
import matplotlib
matplotlib.use("Agg")                        # sin entorno gráfico (servidor)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker
from matplotlib.patches import FancyBboxPatch

from extractors.scopus import ScopusExtractor
from api.services.analysis import generar_hallazgos, dibujar_analisis, CampoDisciplinar

logger = logging.getLogger(__name__)

# ── Paleta ────────────────────────────────────────────────────────────────────
# Paleta corporativa SUAVE - Diseño minimalista profesional con colores soft
_C = dict(
    # Colores corporativos principales (SUAVES - no saturados)
    AZUL_PRINCIPAL = "#3B82F6",  # Azul suave para títulos, barras normales
    AZUL_KPI = "#60A5FA",        # Azul muy suave para KPI primario
    ROJO = "#F87171",            # Rojo suave para citaciones
    VERDE_EXITO = "#34D399",     # Verde menta suave para máximo, éxito
    
    # Grises (neutros, espacios)
    GRIS_800 = "#1F2937",
    GRIS_600 = "#4B5563",
    GRIS_500 = "#6B7280",
    GRIS_400 = "#9CA3AF",
    GRIS_200 = "#E5E7EB",
    GRIS_100 = "#F3F4F6",
    GRIS_50 = "#F9FAFB",
    
    # Aliases compatibles y borders
    AZUL_BAR = "#3B82F6",
    AZUL_BRD = "#1E40AF",     # Más oscuro para bordes
    VERDE_BAR = "#34D399",
    VERDE_BRD = "#10B981",    # Más oscuro para bordes
    MORADO_CPP = "#A78BFA",   # Púrpura muy suave
)


# ══════════════════════════════════════════════════════════════════════════════
# Función principal — interfaz pública
# ══════════════════════════════════════════════════════════════════════════════

def generate_investigator_chart_file(
    author_id:        str,
    affiliation_ids:  Optional[List[str]] = None,
    year_from:        Optional[int]       = None,
    year_to:          Optional[int]       = None,
    institution_name: str                 = "Universidad Simón Bolívar",
    output_dir:       Path                = Path("reports/charts"),
    dpi:              int                 = 180,
    campo:            CampoDisciplinar    = CampoDisciplinar.CIENCIAS_SALUD,
) -> dict:
    """
    Genera el gráfico PNG de publicaciones y citaciones de un investigador.

    Parámetros
    ----------
    author_id        : AU-ID de Scopus (requerido)
    affiliation_ids  : lista de AF-ID para filtrar por institución (opcional)
    year_from        : año inicial del período (opcional)
    year_to          : año final del período (opcional)
    institution_name : pie de página institucional
    output_dir       : carpeta donde se guarda el PNG
    dpi              : resolución de salida (default 180)
    campo            : campo disciplinar para aplicar umbrales específicos (default CIENCIAS_SALUD)

    Retorna
    -------
    dict con claves:
        investigator_name, scopus_id, filename, file_path,
        statistics, query_used, generated_at
    """

    # ── 1. Obtener datos desde Scopus ─────────────────────────────────────────
    data = _fetch_scopus_data(author_id, affiliation_ids, year_from, year_to)

    investigador = data["investigador"]
    scopus_id    = data["scopus_id"]
    fecha_ext    = data["fecha_ext"]
    rango        = data["rango"]
    años         = data["años"]
    pubs         = data["pubs"]
    cites        = data["cites"]
    df_pubs      = data["df_pubs"]

    if not años:
        raise ValueError(
            f"No se encontraron publicaciones para AU-ID {author_id} "
            f"con los filtros indicados."
        )

    # ── 2. Calcular indicadores ───────────────────────────────────────────────
    import statistics
    
    total_arts  = int(sum(pubs))
    total_citas = int(sum(cites))
    cpp         = round(total_citas / total_arts, 1) if total_arts else 0.0
    citas_sorted = sorted(df_pubs["Citas"].tolist(), reverse=True)
    h_index     = sum(1 for i, c in enumerate(citas_sorted, 1) if c >= i)
    año_pico    = años[cites.index(max(cites))] if max(cites) > 0 else años[0]
    año_max_pub = años[pubs.index(max(pubs))] if max(pubs) > 0 else años[0]
    
    # Nuevos indicadores
    mediana_citas = round(statistics.median(citas_sorted), 1) if citas_sorted else 0.0
    pct_citados = round((len(df_pubs[df_pubs["Citas"] >= 1]) / len(df_pubs)) * 100, 1) if len(df_pubs) > 0 else 0.0
    
    # Calcular CPP por año para tabla
    cpp_por_año = []
    for p, c in zip(pubs, cites):
        cpp_anual = round(c / p, 1) if p > 0 else 0.0
        cpp_por_año.append(cpp_anual)

    statistics = {
        "total_publications": total_arts,
        "min_year": min(años) if años else 0,
        "max_year": max(años) if años else 0,
        "avg_per_year": round(total_arts / len(años), 1) if años else 0,
        "peak_year": año_pico,
        "peak_publications": max(pubs) if pubs else 0,
        "active_years": len(años),
        "publications_by_year": [
            {"year": y, "count": p, "percentage": round((p / total_arts * 100), 1) if total_arts > 0 else 0}
            for y, p in zip(años, pubs)
        ],
    }

    # ── 3. Construir la figura ────────────────────────────────────────────────
    fig = _build_figure(
        investigador  = investigador,
        scopus_id     = scopus_id,
        fecha_ext     = fecha_ext,
        rango         = rango,
        años          = años,
        pubs          = pubs,
        cites         = cites,
        total_arts    = total_arts,
        total_citas   = total_citas,
        h_index       = h_index,
        cpp           = cpp,
        mediana_citas = mediana_citas,
        pct_citados   = pct_citados,
        cpp_por_año   = cpp_por_año,
        año_pico      = año_pico,
        año_max_pub   = año_max_pub,
        institution_name = institution_name,
        campo         = campo,
    )

    # ── 4. Guardar PNG ────────────────────────────────────────────────────────
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    slug      = investigador.lower().replace(" ", "_").replace(".", "").replace("-", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"grafico_{slug}_{timestamp}.png"
    filepath  = output_dir / filename

    fig.savefig(filepath, dpi=dpi, bbox_inches="tight",
                facecolor="white", edgecolor=_C["GRIS_200"])
    plt.close(fig)

    logger.info(f"Gráfico generado: {filepath}")

    # ── 5. Construir query usada ──────────────────────────────────────────────
    af_part = ""
    if affiliation_ids:
        af_parts = " OR ".join(f"AF-ID({a})" for a in affiliation_ids)
        af_part  = f" AND ({af_parts})"
    query_used = f"AU-ID({author_id}){af_part}"

    return {
        "investigator_name": investigador,
        "scopus_id":         scopus_id,
        "filename":          filename,
        "file_path":         str(filepath),
        "statistics":        statistics,
        "query_used":        query_used,
        "generated_at":      datetime.now().isoformat(),
        # Campos adicionales para PDF
        "total_citations": total_citas,
        "h_index": h_index,
        "cpp": cpp,
        "median_citations": mediana_citas,
        "percent_cited": pct_citados,
        "peak_year": año_pico,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Capa de datos  —  extrae desde Scopus usando ScopusExtractor
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_scopus_data(
    author_id:       str,
    affiliation_ids: Optional[List[str]],
    year_from:       Optional[int],
    year_to:         Optional[int],
) -> dict:
    """
    Obtiene publicaciones de Scopus para el AU-ID indicado.
    """
    import pandas as pd
    
    # Construir query Scopus
    if affiliation_ids:
        aff_clause = " OR ".join([f"AF-ID ( {aff_id} )" for aff_id in affiliation_ids])
        query = f"AU-ID ( {author_id} ) AND ({aff_clause})"
    else:
        query = f"AU-ID ( {author_id} )"
    
    logger.info(f"[CHART DATA] Extrayendo con query: {query}")
    
    # Extraer desde Scopus
    extractor = ScopusExtractor()
    records = extractor.extract(query=query, max_results=None)
    
    if not records:
        raise ValueError(f"No se encontraron publicaciones para AU-ID {author_id}")
    
    logger.info(f"[CHART DATA] Registros encontrados: {len(records)}")
    
    # Obtener datos del autor
    investigador = "Investigador"
    scopus_id = author_id
    for record in records:
        if record.authors:
            for author in record.authors:
                author_scopus_id = author.get('scopus_id', '').replace('SCOPUS_ID:', '')
                if author_scopus_id == author_id:
                    investigador = author.get('name', 'Investigador')
                    if investigador and investigador != 'Investigador':
                        break
        if investigador != "Investigador":
            break
    
    # Agrupar por año con citaciones
    pub_by_year = Counter()
    citations_by_year = Counter()
    
    for record in records:
        if record.publication_year:
            year = record.publication_year
            
            # Filtros de año
            if year_from and year < year_from:
                continue
            if year_to and year > year_to:
                continue
            
            pub_by_year[year] += 1
            if record.citation_count > 0:
                citations_by_year[year] += record.citation_count
    
    # Crear series ordenada
    if not pub_by_year:
        raise ValueError("No se encontraron publicaciones en el rango especificado")
    
    años  = sorted(pub_by_year.keys())
    pubs  = [pub_by_year[year] for year in años]
    cites = [citations_by_year.get(year, 0) for year in años]
    
    # Crear DataFrame de publicaciones para cálculo de H-index
    df_pubs = pd.DataFrame([
        {
            'Año': record.publication_year,
            'Citas': record.citation_count or 0,
        }
        for record in records
        if record.publication_year and (not year_from or record.publication_year >= year_from)
                                    and (not year_to or record.publication_year <= year_to)
    ])
    
    fecha_ext = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    y_min = min(años) if años else ""
    y_max = max(años) if años else ""
    rango = f"{y_min} - {y_max}"
    
    return {
        "investigador": investigador,
        "scopus_id":    scopus_id,
        "fecha_ext":    fecha_ext,
        "rango":        rango,
        "años":         años,
        "pubs":         pubs,
        "cites":        cites,
        "df_pubs":      df_pubs,
    }



# ══════════════════════════════════════════════════════════════════════════════
# Motor de renderizado  —  genera la figura Matplotlib
# ══════════════════════════════════════════════════════════════════════════════

def _build_figure(
    investigador, scopus_id, fecha_ext, rango,
    años, pubs, cites,
    total_arts, total_citas, h_index, cpp,
    mediana_citas, pct_citados, cpp_por_año,
    año_pico, año_max_pub,
    institution_name,
    campo: CampoDisciplinar = CampoDisciplinar.CIENCIAS_SALUD,
) -> plt.Figure:

    C = _C  # alias corto

    # ── Canvas ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(13, 11), facecolor="white")  # Más compacto
    fig.patch.set_linewidth(0)  # Sin borde

    gs = fig.add_gridspec(
        5, 1,  # Simplificar: 5 secciones en lugar de 7
        height_ratios=[0.8, 0.9, 5.0, 1.8, 0.4],  # Menos altura total
        hspace=0.10,
        left=0.08, right=0.92,
        top=0.94, bottom=0.06,
    )
    ax_hdr   = fig.add_subplot(gs[0])  # Título simple
    ax_kpi   = fig.add_subplot(gs[1])  # KPIs
    ax_chart = fig.add_subplot(gs[2])  # Gráfico
    ax_tbl   = fig.add_subplot(gs[3])  # Tabla
    ax_ftr   = fig.add_subplot(gs[4])  # Footer

    for ax in [ax_hdr, ax_kpi, ax_tbl, ax_ftr]:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

    # ── Header LIMPIO ──────────────────────────────────────────────────────────
    # Decidir si poner rango en misma línea o en línea separada
    # Solo en misma línea si el nombre es muy corto (< 15 caracteres)
    nombre_corto = len(investigador) < 15
    
    if nombre_corto:
        # Nombre corto: mostrar rango en misma línea
        ax_hdr.text(0.02, 0.80, f"Publicaciones de {investigador}",
                    fontsize=15, fontweight="bold", color=C["AZUL_PRINCIPAL"],
                    transform=ax_hdr.transAxes, va="top", ha="left")
        # Rango al lado con separador
        ax_hdr.text(0.98, 0.80, f"{rango}  ",
                    fontsize=12, color=C["GRIS_400"], fontweight="300",
                    transform=ax_hdr.transAxes, va="top", ha="right")
    else:
        # Nombre largo: rango en línea separada debajo
        ax_hdr.text(0.02, 0.80, f"Publicaciones de {investigador}",
                    fontsize=15, fontweight="bold", color=C["AZUL_PRINCIPAL"],
                    transform=ax_hdr.transAxes, va="top", ha="left")
        # Rango en línea 2
        ax_hdr.text(0.02, 0.58, rango,
                    fontsize=12, color=C["GRIS_400"], fontweight="300",
                    transform=ax_hdr.transAxes, va="top", ha="left")

    meta = (f"Publicaciones: {total_arts}   •   "
            f"Citaciones: {total_citas:,}   •   "
            f"Scopus ID: {scopus_id}")
    ax_hdr.text(0.02, 0.40, meta,
                fontsize=8, color=C["GRIS_600"],
                transform=ax_hdr.transAxes, va="top")

    ax_hdr.plot([0, 1], [0.05, 0.05], color=C["AZUL_PRINCIPAL"], lw=1.2,
                transform=ax_hdr.transAxes, clip_on=False)

    # ── KPIs LIMPIOS ───────────────────────────────────────────────────────────
    # Sin fondo de box, solo texto limpio distribuido
    
    kpis = [
        ("PUBLICACIONES", str(total_arts), C["AZUL_PRINCIPAL"]),
        ("% CITADOS", f"{pct_citados}%", C["VERDE_EXITO"]),
        ("H-INDEX", str(h_index), C["AZUL_KPI"]),
        ("CPP", str(cpp), C["MORADO_CPP"]),
        ("MEDIANA", str(mediana_citas), C["ROJO"]),
        ("CITACIONES", f"{total_citas:,}", C["GRIS_600"]),
    ]
    
    kpi_w = 1 / len(kpis)
    for i, (lbl, val, color) in enumerate(kpis):
        xc = i * kpi_w + kpi_w / 2
        
        # Etiqueta pequeña
        ax_kpi.text(xc, 0.70, lbl, ha="center", va="top", fontsize=7.5,
                    color=C["GRIS_600"], fontweight="bold",
                    transform=ax_kpi.transAxes)
        
        # Valor grande
        ax_kpi.text(xc, 0.35, val, ha="center", va="center", fontsize=18,
                    color=color, fontweight="bold",
                    transform=ax_kpi.transAxes)
    
    # Solo línea divisoria simple (sin box)
    ax_kpi.plot([0, 1], [0.02, 0.02], color=C["GRIS_200"], lw=0.8,
                transform=ax_kpi.transAxes, clip_on=False)

    # ── Gráfico barras + línea + CPP/año ─────────────────────────────────────
    ax_chart.margins(y=0.05)  # Margen vertical para no sobreposicionar tabla
    ax2   = ax_chart.twinx()
    ax3   = ax_chart.twinx()
    ax3.spines["right"].set_position(("outward", 55))  # Desplazar eje CPP/año
    
    n     = len(años)
    x     = np.arange(n)
    
    # Ancho de barra dinámico según cantidad de años
    if n == 1:
        w = 0.4      # Muy estrecho para 1 año
        margin_x = 0.8
    elif n <= 3:
        w = 0.5
        margin_x = 0.6
    elif n <= 7:
        w = 0.55
        margin_x = 0.5
    else:
        w = 0.35     # Más estrecho para muchos años
        margin_x = 0.3

    bar_fc = [C["VERDE_BAR"] if a == año_max_pub else C["AZUL_BAR"]  for a in años]
    bar_ec = [C["VERDE_BRD"] if a == año_max_pub else C["AZUL_BRD"]  for a in años]

    bars = ax_chart.bar(x, pubs, width=w, color=bar_fc,
                        edgecolor=bar_ec, linewidth=0.8, zorder=2)

    # Fontsize dinámico para valores sobre barras
    val_fontsize = 10 if n == 1 else 9 if n <= 5 else 8 if n <= 10 else 7
    for bar, val in zip(bars, pubs):
        ax_chart.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(pubs) * 0.015,
            str(val), ha="center", va="bottom",
            fontsize=val_fontsize, color=C["GRIS_600"], fontweight="500")

    # Tamaño dinámico para marcadores según cantidad de años
    marker_size = 6 if n <= 5 else 5 if n <= 10 else 4
    marker_size_cpp = 5 if n <= 5 else 4 if n <= 10 else 3
    
    ax2.plot(x, cites, color=C["ROJO"], linewidth=2,
             marker="o", markersize=marker_size,
             markerfacecolor=C["ROJO"], markeredgecolor="white",
             markeredgewidth=1.5, zorder=3, label="Citaciones")
    
    # Graficar CPP/año
    ax3.plot(x, cpp_por_año, color="#8B5CF6", linewidth=2.5, linestyle="--",
             marker="s", markersize=marker_size_cpp,
             markerfacecolor="#8B5CF6", markeredgecolor="white",
             markeredgewidth=1.2, zorder=2, label="CPP/año")

    # Anotación pico - dinámico según años
    idx_pico = cites.index(max(cites)) if max(cites) > 0 else 0
    if max(cites) > 0:
        # Posicionar anotación dinámicamente
        if n == 1:
            # Un solo año: poner arriba
            x_text = idx_pico
            y_text = max(cites) * 1.1
        elif idx_pico <= n // 3:
            # Pico en la izquierda: anotar a la derecha
            x_text = idx_pico + 1.5
            y_text = max(cites) * 0.85
        elif idx_pico >= 2 * n // 3:
            # Pico en la derecha: anotar a la izquierda
            x_text = idx_pico - 1.5
            y_text = max(cites) * 0.75
        else:
            # Pico en el medio: anotar arriba
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

    # Escalas y ejes - dinámicas según cantidad de años
    ax_chart.set_xlim(-margin_x, n - 1 + margin_x)
    ax_chart.set_ylim(0, max(pubs)  * 1.35)
    ax2.set_ylim     (0, max(cites) * 1.35)
    ax3.set_ylim     (0, max(cpp_por_año) * 1.35 if max(cpp_por_año) > 0 else 1.0)

    ax_chart.set_xticks(x)
    # Fontsize de etiquetas X dinámico
    xtick_fontsize = 11 if n == 1 else 10 if n <= 5 else 9 if n <= 10 else 8
    ax_chart.set_xticklabels([str(a) for a in años], fontsize=xtick_fontsize, color=C["GRIS_400"])
    
    ax_chart.yaxis.set_major_locator(ticker.MultipleLocator(max(1, max(pubs) // 7)))
    ax_chart.tick_params(axis="y", labelsize=9, labelcolor=C["GRIS_400"])
    ax3.tick_params(axis="y", labelsize=9, labelcolor="#8B5CF6")

    # Escala derecha: múltiplo limpio
    cit_step = max(1, round(max(cites) / 6 / 10) * 10) if max(cites) >= 60 else max(1, max(cites) // 6)
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(cit_step))
    ax2.tick_params(axis="y", labelsize=9, labelcolor="#DC2626")
    
    # Escala CPP/año: paso más fino
    cpp_step = max(0.5, round(max(cpp_por_año) / 5 * 2) / 2) if max(cpp_por_año) > 0 else 0.5
    ax3.yaxis.set_major_locator(ticker.MultipleLocator(cpp_step))

    ax_chart.set_ylabel("N.° de publicaciones", fontsize=9, color=C["GRIS_600"], labelpad=8)
    ax2.set_ylabel     ("N.° de citaciones",     fontsize=9, color="#DC2626",     labelpad=8)
    ax3.set_ylabel     ("CPP/año",               fontsize=9, color="#8B5CF6",     labelpad=8)

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

    # Leyenda mejorada
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

    # ── Tabla de datos ────────────────────────────────────────────────────────
    # Tamaño dinámico basado en número de años
    # CRÍTICO: row_h * 4 debe ser <= 0.90 (espacio disponible)
    n_years = len(años)
    col_w = 1 / (n_years + 1)
    
    # Máximo permitido por fila: 0.90 / 4 = 0.225
    if n_years <= 4:
        row_h = 0.18
        font_size_table = 7
        font_size_header = 6.5
        row_label_fontsize = 6
    elif n_years <= 6:
        row_h = 0.16
        font_size_table = 6.5
        font_size_header = 6
        row_label_fontsize = 5.5
    elif n_years <= 9:
        row_h = 0.14
        font_size_table = 5.5
        font_size_header = 5
        row_label_fontsize = 5
    elif n_years <= 12:
        row_h = 0.13
        font_size_table = 5
        font_size_header = 4.5
        row_label_fontsize = 4.5
    else:
        row_h = 0.12
        font_size_table = 4.5
        font_size_header = 4
        row_label_fontsize = 4
    
    # Calcular header_y_start - mantener la tabla dentro de [0.05, 0.95]
    total_height = 4 * row_h
    # FÓRMULA: header_y_start debe ser >= 3*row_h (para que fila3 no vaya negativa)
    # Y preferiblemente <= 0.95 - total_height para usar el espacio
    min_header_y = 3 * row_h + 0.05  # Fila3 nunca por debajo de 0.05
    max_header_y = 0.95 - total_height  # Ideal para ocupar espacio
    header_y_start = max(min_header_y, max_header_y)
    
    # Si resulta muy bajo, aún hay espacio abajo pero es lo mejor posible
    if header_y_start > 0.90:
        header_y_start = 0.90
    
    for j, lbl in enumerate([""] + [str(a) for a in años]):
        ax_tbl.add_patch(FancyBboxPatch(
            (j * col_w, header_y_start), col_w, row_h,
            boxstyle="square,pad=0",
            facecolor=C["GRIS_100"], edgecolor=C["GRIS_200"], linewidth=0.5,
            transform=ax_tbl.transAxes))
        if lbl:  # No pintar la celda vacía
            ax_tbl.text(j * col_w + col_w / 2, header_y_start + row_h / 2, lbl,
                        ha="center", va="center", fontsize=font_size_header,
                        color=C["GRIS_600"], fontweight="bold",
                        transform=ax_tbl.transAxes)

    # Filas de datos (Publicaciones, Citaciones y CPP/año)
    rows = [
        ("Publicaciones", pubs,       C["AZUL_KPI"], "int"),
        ("Citaciones",    cites,      C["ROJO"],     "int"),
        ("CPP/año",       cpp_por_año, "#8B5CF6",    "float"),
    ]
    
    for ri, (lbl, vals, color, val_type) in enumerate(rows):
        ypos = header_y_start - (ri + 1) * row_h
        
        # Etiqueta de fila (nombre de la métrica)
        ax_tbl.add_patch(FancyBboxPatch(
            (0, ypos), col_w, row_h,
            boxstyle="square,pad=0",
            facecolor=C["GRIS_50"], edgecolor=C["GRIS_200"], linewidth=0.5,
            transform=ax_tbl.transAxes))
        ax_tbl.text(col_w / 2, ypos + row_h / 2, lbl,
                    ha="center", va="center", fontsize=row_label_fontsize,
                    color=C["GRIS_600"], fontweight="bold",
                    transform=ax_tbl.transAxes)
        
        # Celdas de datos (valores por año)
        max_val = max(vals) if vals else 1
        for j, v in enumerate(vals):
            # Colorear el máximo con fondo  
            is_peak = v == max_val and v > 0
            if is_peak:
                if lbl == "Citaciones":
                    bg = "#FFE5CC"  # Naranja claro
                    txc = "#D97706"  # Naranja oscuro
                else:
                    bg = "#DBEAFE"  # Azul claro
                    txc = "#1E40AF"  # Azul oscuro
            else:
                bg = "white"
                txc = color
            
            ax_tbl.add_patch(FancyBboxPatch(
                ((j + 1) * col_w, ypos), col_w, row_h,
                boxstyle="square,pad=0",
                facecolor=bg, edgecolor=C["GRIS_200"], linewidth=0.5,
                transform=ax_tbl.transAxes))
            
            # Formatear valores según tipo
            if val_type == "float":
                # CPP/año: exactamente 1 decimal
                display_val = f"{v:.1f}"
            else:
                # Publicaciones y Citaciones: enteros
                display_val = str(int(v))
            
            # Fontsize dinámico para valores largos
            adjusted_fontsize = font_size_table
            if len(display_val) > 3:
                adjusted_fontsize = max(font_size_table - 1, 3)
            
            ax_tbl.text((j + 1) * col_w + col_w / 2, ypos + row_h / 2,
                        display_val, ha="center", va="center",
                        fontsize=adjusted_fontsize, color=txc, 
                        fontweight="bold" if is_peak else "500",
                        transform=ax_tbl.transAxes)


    # ── Cuadro de análisis automático ─────────────────────────────────────────
    positivos, negativos, notas = generar_hallazgos(
        total_arts=total_arts, total_citas=total_citas,
        h_index=h_index, cpp=cpp,
        mediana=mediana_citas, pct_citados=pct_citados,
        años=años, pubs=pubs, cites=cites,
        año_pico=año_pico, año_max_pub=año_max_pub,
        campo=campo,
    )
    # NOTA: Los análisis se renderizarán EN EL PDF, no en la imagen PNG
    # dibujar_analisis(ax_ana, positivos, negativos, C, notas)


    # ── Footer ────────────────────────────────────────────────────────────────
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

