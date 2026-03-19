"""
api/services/pdf_reporter.py
=============================
Generador de informes PDF detallados con análisis bibliométrico completo.

Incluye:
- Datos del investigador
- Indicadores clave (KPIs)
- Análisis automático (hallazgos positivos/negativos)
- Notas aclaratorias (todo el espacio disponible)
- Datos por año en tabla
- Footer con institución y fecha

Uso:
    from api.services.pdf_reporter import generate_analysis_report
    
    pdf_info = generate_analysis_report(
        investigador="Dr. Juan Pérez",
        kpis={...},
        positivos=[...],
        negativos=[...],
        notas=[...],
        institution_name="Universidad Simón Bolívar",
        output_dir=Path("reports/pdfs"),
    )
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
except ImportError:
    raise ImportError(
        "reportlab no instalado. Instala con: pip install reportlab"
    )

logger = logging.getLogger(__name__)


def generate_analysis_report(
    investigador: str,
    kpis: Dict[str, any],
    positivos: List[str],
    negativos: List[str],
    notas: List[str],
    png_path: Optional[str] = None,
    institution_name: str = "Universidad Simón Bolívar",
    output_dir: Path = Path("reports/pdfs"),
    fecha_ext: str = None,
) -> Dict[str, str]:
    """
    Genera un informe PDF profesional con análisis bibliométrico completo.
    
    Incluye:
    - Gráfico PNG incrustado (si se proporciona)
    - Indicadores clave (KPIs)
    - Análisis automático (hallazgos positivos/negativos)
    - Notas aclaratorias completas
    
    Args:
        investigador: Nombre del investigador
        kpis: Dict con indicadores (pubs, citas, h_index, cpp, mediana, pct_citados, año_pico)
        positivos: Lista de hallazgos positivos
        negativos: Lista de hallazgos negativos
        notas: Lista de notas aclaratorias
        png_path: Ruta al PNG para incrustar en el PDF (opcional)
        institution_name: Nombre de institución para pie de página
        output_dir: Directorio de salida
        fecha_ext: Fecha de extracción (si no se proporciona, se usa fecha actual)
    
    Returns:
        Dict con:
            - "filename": nombre del archivo PDF
            - "file_path": ruta completa del PDF
            - "file_size_mb": tamaño en MB
            - "investigator_name": nombre del investigador
    """
    
    if fecha_ext is None:
        fecha_ext = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(f"[PDF] Generando informe para {investigador}")
    
    # Crear directorio
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Nombre de archivo
    slug = investigador.lower().replace(" ", "_").replace(".", "").replace("-", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"informe_{slug}_{timestamp}.pdf"
    filepath = output_dir / filename
    
    # Crear documento PDF con márgenes amplios para aspecto profesional
    doc = SimpleDocTemplate(
        str(filepath),
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=1*inch,
        bottomMargin=0.75*inch,
    )
    
    # ════════════════════════════════════════════════════════════════════════════
    # PALETA DE COLORES SUAVES Y PROFESIONALES (DISEÑO MODERNO)
    # ════════════════════════════════════════════════════════════════════════════
    COLORES = {
        # Colores suaves modernos
        "principal": colors.HexColor("#6366F1"),         # Índigo suave para encabezados
        "secundario": colors.HexColor("#8B5CF6"),       # Púrpura suave
        "acento": colors.HexColor("#EC4899"),           # Rosa profesional
        "exito": colors.HexColor("#10B981"),            # Verde menta
        "alerta": colors.HexColor("#F59E0B"),           # Ámbar suave
        "texto_oscuro": colors.HexColor("#374151"),     # Gris oscuro para textos
        "texto_claro": colors.HexColor("#6B7280"),      # Gris medio
        "fondo_suave": colors.HexColor("#F9FAFB"),      # Blanco roto muy suave
        "fondo_Card": colors.HexColor("#F3F4F6"),       # Gris muy claro
        "borde_suave": colors.HexColor("#E5E7EB"),      # Borde gris suave
    }
    
    # Estilos profesionales
    styles = getSampleStyleSheet()
    
    # Encabezado institucional
    header_style = ParagraphStyle(
        "Header",
        parent=styles["Normal"],
        fontSize=13,
        textColor=COLORES["principal"],
        fontName="Helvetica-Bold",
        alignment=TA_CENTER,
        spaceAfter=0.02*inch,
    )
    
    subheader_style = ParagraphStyle(
        "Subheader",
        parent=styles["Normal"],
        fontSize=10,
        textColor=COLORES["texto_claro"],
        alignment=TA_CENTER,
        spaceAfter=0.15*inch,
    )
    
    # Título del informe
    titulo_style = ParagraphStyle(
        "CustomTitle",
        parent=styles["Heading1"],
        fontSize=22,
        textColor=COLORES["principal"],
        spaceAfter=0.1*inch,
        fontName="Helvetica-Bold",
        alignment=TA_LEFT,
        borderPadding=10,
    )
    
    # Encabezados de sección — Fondo suave, sin bordes duros
    heading_style = ParagraphStyle(
        "CustomHeading",
        parent=styles["Heading2"],
        fontSize=13,
        textColor=COLORES["principal"],
        spaceAfter=0.15*inch,
        fontName="Helvetica-Bold",
        borderPadding=8,
        backColor=colors.HexColor("#F0F4FF"),  # Fondo índigo muy suave
        borderRadius=2,
        leftIndent=5,
    )
    
    # Texto normal con justificación
    normal_style = ParagraphStyle(
        "CustomNormal",
        parent=styles["Normal"],
        fontSize=10,
        textColor=COLORES["texto_oscuro"],
        spaceAfter=0.12*inch,
        alignment=TA_JUSTIFY,
        leading=14,
    )
    
    # Metadatos
    metadata_style = ParagraphStyle(
        "Metadata",
        parent=styles["Normal"],
        fontSize=9,
        textColor=COLORES["texto_claro"],
        spaceAfter=0.1*inch,
        alignment=TA_LEFT,
    )
    
    # Story (contenido del PDF)
    story = []
    
    # ════════════════════════════════════════════════════════════════════════════
    # ENCABEZADO INSTITUCIONAL
    # ════════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("UNIVERSIDAD SIMÓN BOLÍVAR", header_style))
    story.append(Paragraph("Decanato de Investigación y Desarrollo", subheader_style))
    story.append(Paragraph("─" * 80, metadata_style))
    story.append(Spacer(1, 0.15*inch))
    
    # ════════════════════════════════════════════════════════════════════════════
    # TÍTULO Y FECHA
    # ════════════════════════════════════════════════════════════════════════════
    story.append(Paragraph(f"Informe Bibliométrico: {investigador}", titulo_style))
    story.append(Spacer(1, 0.12*inch))
    
    # Metadatos en cuadro profesional
    metadata_text = f"""
    <b>Institución:</b> {institution_name}<br/>
    <b>Período de análisis:</b> Datos históricos acumulados<br/>
    <b>Fecha de extracción:</b> {fecha_ext} | <b>Generado:</b> {datetime.now().strftime("%d de %B de %Y")}
    """
    story.append(Paragraph(metadata_text, metadata_style))
    story.append(Spacer(1, 0.2*inch))
    
    # ════════════════════════════════════════════════════════════════════════════
    # GRÁFICO PNG INCRUSTADO
    # ════════════════════════════════════════════════════════════════════════════
    if png_path and Path(png_path).exists():
        try:
            img = Image(png_path, width=7*inch, height=5.5*inch)
            story.append(img)
            story.append(Spacer(1, 0.2*inch))
            logger.info(f"[PDF] PNG incrustado: {png_path}")
        except Exception as e:
            logger.warning(f"[PDF] No se pudo incrustar PNG: {e}")
    else:
        if png_path:
            logger.warning(f"[PDF] Archivo PNG no encontrado: {png_path}")
    
    # ════════════════════════════════════════════════════════════════════════════
    # INDICADORES CLAVE (KPIs) — Tabla profesional
    # ════════════════════════════════════════════════════════════════════════════
    story.append(Paragraph("📊 Indicadores Clave (KPIs)", heading_style))
    
    # Tabla de KPIs con formato profesional
    kpi_data = [
        ["INDICADOR", "VALOR", "DESCRIPCIÓN"],
        ["Publicaciones", str(kpis.get("pubs", "N/A")), "Total de artículos publicados"],
        ["Citaciones", f"{kpis.get('citas', 'N/A'):,}" if isinstance(kpis.get('citas'), int) else str(kpis.get('citas', 'N/A')), "Impacto acumulado (número de citas)"],
        ["Índice H", str(kpis.get("h_index", "N/A")), "Mide la consistencia y amplitud del impacto"],
        ["CPP", str(kpis.get("cpp", "N/A")), "Promedio de citas por artículo"],
        ["Mediana de Citas", str(kpis.get("mediana", "N/A")), "Valor central de distribución de citas"],
        ["% de Artículos Citados", str(kpis.get("pct_citados", "N/A")) + "%", "Proporción con visibilidad en la comunidad"],
        ["Año Máximo Impacto", str(kpis.get("año_pico", "N/A")), "Período de mayor producción o citación"],
    ]
    
    kpi_table = Table(kpi_data, colWidths=[1.7*inch, 1.0*inch, 2.3*inch])
    kpi_table.setStyle(TableStyle([
        # Encabezado — Fondo suave
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#6366F1")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
        ("TOPPADDING", (0, 0), (-1, 0), 10),
        
        # Filas alternas — Colores muy suaves
        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
        
        # Bordes suaves
        ("GRID", (0, 0), (-1, -1), 1.0, colors.HexColor("#D1D5DB")),
        ("ALIGN", (0, 1), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTSIZE", (0, 1), (-1, -1), 9.5),
        ("FONTNAME", (1, 1), (1, -1), "Helvetica-Bold"),  # Columna de valores en negrita
        ("TEXTCOLOR", (0, 1), (-1, -1), COLORES["texto_oscuro"]),
        
        # Espaciado interno
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 1), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 0.25*inch))
    
    # ════════════════════════════════════════════════════════════════════════════
    # HALLAZGOS POSITIVOS — Con badge visual suave
    # ════════════════════════════════════════════════════════════════════════════
    if positivos:
        story.append(Paragraph("✓ Hallazgos Positivos (Fortalezas)", heading_style))
        
        # Estilo para hallazgos positivos — Verde suave
        positive_style = ParagraphStyle(
            "Positive",
            parent=normal_style,
            borderColor=colors.HexColor("#10B981"),
            borderWidth=0.5,
            borderPadding=8,
            backColor=colors.HexColor("#F0FDF4"),  # Verde muy suave
            leftIndent=15,
            textColor=COLORES["texto_oscuro"],
        )
        
        for idx, hallazgo in enumerate(positivos, 1):
            story.append(Paragraph(
                f"<b>• {hallazgo}</b>",
                positive_style
            ))
            story.append(Spacer(1, 0.08*inch))
        
        story.append(Spacer(1, 0.15*inch))
    
    # ════════════════════════════════════════════════════════════════════════════
    # HALLAZGOS NEGATIVOS / ÁREAS DE MEJORA — Con badge visual suave
    # ════════════════════════════════════════════════════════════════════════════
    if negativos:
        story.append(Paragraph("⚠ Áreas para Mejorar (Oportunidades)", heading_style))
        
        # Estilo para hallazgos negativos — Ámbar suave
        negative_style = ParagraphStyle(
            "Negative",
            parent=normal_style,
            borderColor=colors.HexColor("#F59E0B"),
            borderWidth=0.5,
            borderPadding=8,
            backColor=colors.HexColor("#FFFBEB"),  # Ámbar muy suave
            leftIndent=15,
            textColor=COLORES["texto_oscuro"],
        )
        
        for idx, hallazgo in enumerate(negativos, 1):
            story.append(Paragraph(
                f"<b>• {hallazgo}</b>",
                negative_style
            ))
            story.append(Spacer(1, 0.08*inch))
        
        story.append(Spacer(1, 0.15*inch))
    
    # ════════════════════════════════════════════════════════════════════════════
    # NOTAS ACLARATORIAS — Con estilo profesional suave
    # ════════════════════════════════════════════════════════════════════════════
    if notas:
        story.append(Paragraph("📌 Notas y Aclaraciones", heading_style))
        
        # Estilo para notas — Gris suave
        note_style = ParagraphStyle(
            "Note",
            parent=normal_style,
            fontSize=9,
            textColor=COLORES["texto_claro"],
            borderColor=colors.HexColor("#D1D5DB"),
            borderWidth=0.5,
            borderPadding=8,
            backColor=colors.HexColor("#FAFBFC"),  # Gris azulado muy suave
            leftIndent=10,
            italic=True,
        )
        
        for idx, nota in enumerate(notas, 1):
            # Eliminar emoji si existe
            contenido = nota[2:].strip() if nota.startswith("Nota:") or len(nota) > 0 and nota[0] in "📌✓⚠" else nota
            story.append(Paragraph(
                f"<b>Nota {idx}:</b> {contenido}",
                note_style
            ))
            story.append(Spacer(1, 0.08*inch))
    
    # ════════════════════════════════════════════════════════════════════════════
    # FOOTER — Pie de página profesional en cada página
    # ════════════════════════════════════════════════════════════════════════════
    # Función de pie de página que se ejecuta en cada página
    def footer_function(canvas, doc):
        """Dibuja el pie de página profesional en cada página"""
        canvas.saveState()
        
        # Línea separadora superior (bien clara en top)
        canvas.setLineWidth(1)
        canvas.setStrokeColor(COLORES["borde_suave"])
        canvas.line(doc.leftMargin, 0.75*inch, doc.width + doc.leftMargin, 0.75*inch)
        
        # INSTITUCIÓN — Texto superior izquierda
        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(COLORES["texto_oscuro"])
        canvas.drawString(doc.leftMargin, 0.60*inch, "Universidad Simón Bolívar")
        
        # DECANATO — Texto más pequeño bajo institución
        canvas.setFont("Helvetica", 6.5)
        canvas.setFillColor(COLORES["texto_claro"])
        canvas.drawString(doc.leftMargin, 0.52*inch, "Decanato de Investigación y Desarrollo")
        
        # FECHA — Línea siguiente completamente separada
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(COLORES["texto_claro"])
        timestamp = datetime.now().strftime("%d de %B de %Y")
        canvas.drawString(doc.leftMargin, 0.42*inch, f"Generado: {timestamp}")
        
        # PÁGINA — Derecha, bien separado
        canvas.drawString(doc.width + doc.leftMargin - 0.9*inch, 0.42*inch, f"Página {doc.page}")
        
        # COPYRIGHT — Línea final
        canvas.setFont("Helvetica-Oblique", 5.5)
        canvas.setFillColor(COLORES["texto_claro"])
        canvas.drawString(
            doc.leftMargin,
            0.28*inch,
            "© 2026 Universidad Simón Bolívar. Análisis bibliométrico Scopus, Web of Science, OpenAlex."
        )
        
        canvas.restoreState()
    
    # ════════════════════════════════════════════════════════════════════════════
    # GENERAR PDF
    # ════════════════════════════════════════════════════════════════════════════
    try:
        doc.build(story, onFirstPage=footer_function, onLaterPages=footer_function)
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"[PDF] Informe generado: {filename} ({file_size_mb:.2f} MB)")
        
        return {
            "filename": filename,
            "file_path": str(filepath),
            "file_size_mb": round(file_size_mb, 2),
            "investigator_name": investigador,
        }
    except Exception as e:
        logger.error(f"[PDF] Error generando PDF: {e}")
        raise
