"""
Exportador de Excel para resultados de cobertura de revistas (Serial Title API).

GENERA archivos .xlsx en memoria usando openpyxl con:
  - Encabezados con estilo
  - Columnas auto-ajustadas
  - Colores condicionales por estado de la revista
  - Hoja de resumen

LEE archivos .xlsx subidos para extraer ISSNs de la primera columna.
"""

import io
import logging
import re
from datetime import datetime
from typing import List

logger_excel = logging.getLogger("excel")

import openpyxl
from openpyxl.styles import (
    Alignment,
    Font,
    PatternFill,
    Border,
    Side,
)
from openpyxl.utils import get_column_letter


# ── Paleta de colores ─────────────────────────────────────────────────────────

COLOR_HEADER_BG   = "1F4E79"   # azul oscuro
COLOR_HEADER_FONT = "FFFFFF"   # blanco
COLOR_ACTIVE      = "C6EFCE"   # verde claro
COLOR_DISCONT     = "FFCCCC"   # rojo claro
COLOR_UNKNOWN     = "FFF2CC"   # amarillo claro
COLOR_ERROR       = "E0E0E0"   # gris
COLOR_ALT_ROW     = "EBF3FB"   # azul muy claro (filas alternadas)

THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)

# ── Columnas del reporte ──────────────────────────────────────────────────────

COLUMNS = [
    ("ISSN",             "issn",            15),
    ("Título",           "title",           45),
    ("Editorial",        "publisher",       30),
    ("Estado",           "status",          14),
    ("¿Descontinuada?",  "is_discontinued", 16),
    ("Desde (año)",      "coverage_from",   13),
    ("Hasta (año)",      "coverage_to",     13),
    ("Áreas temáticas",  "subject_areas",   40),
    ("Source ID Scopus", "source_id",       18),
    ("Error",            "error",           35),
]


# ── Función principal ─────────────────────────────────────────────────────────

def generate_journal_coverage_excel(results: List[dict]) -> bytes:
    """
    Genera un Excel en memoria a partir de los resultados de cobertura.

    Args:
        results: Lista de dicts retornados por SerialTitleExtractor.get_bulk_coverage()

    Returns:
        bytes del archivo .xlsx listo para enviar como respuesta HTTP.
    """
    wb = openpyxl.Workbook()

    # ── Hoja 1: Detalle ───────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Cobertura de Revistas"

    _write_title_row(ws, len(COLUMNS))
    _write_header_row(ws)
    _write_data_rows(ws, results)
    _auto_adjust_columns(ws)

    # ── Hoja 2: Resumen ───────────────────────────────────────────────────────
    ws_summary = wb.create_sheet("Resumen")
    _write_summary_sheet(ws_summary, results)

    # ── Serializar a bytes ────────────────────────────────────────────────────
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer.read()


# ── Helpers privados ──────────────────────────────────────────────────────────

def _write_title_row(ws, num_cols: int):
    """Fila 1: título del reporte."""
    title_cell = ws.cell(row=1, column=1)
    title_cell.value = (
        f"Reporte de Cobertura de Revistas en Scopus  —  "
        f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )
    title_cell.font = Font(bold=True, size=13, color=COLOR_HEADER_BG)
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 22
    ws.merge_cells(
        start_row=1, start_column=1, end_row=1, end_column=num_cols
    )


def _write_header_row(ws):
    """Fila 2: encabezados de columna con estilo."""
    header_fill = PatternFill(
        fill_type="solid", fgColor=COLOR_HEADER_BG
    )
    for col_idx, (header, _, _) in enumerate(COLUMNS, start=1):
        cell = ws.cell(row=2, column=col_idx)
        cell.value = header
        cell.font = Font(bold=True, color=COLOR_HEADER_FONT, size=11)
        cell.fill = header_fill
        cell.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=True
        )
        cell.border = THIN_BORDER
    ws.row_dimensions[2].height = 30


def _write_data_rows(ws, results: List[dict]):
    """Filas de datos a partir de la fila 3."""
    for row_idx, item in enumerate(results, start=3):
        fill_color = _row_fill_color(item, row_idx)
        row_fill = PatternFill(fill_type="solid", fgColor=fill_color)

        for col_idx, (_, key, _) in enumerate(COLUMNS, start=1):
            value = item.get(key)

            # Serialización especial
            if key == "subject_areas" and isinstance(value, list):
                value = " | ".join(value) if value else None
            elif key == "is_discontinued":
                value = "Sí" if value else "No"

            cell = ws.cell(row=row_idx, column=col_idx)
            cell.value = value
            cell.fill = row_fill
            cell.border = THIN_BORDER
            cell.alignment = Alignment(
                horizontal="left", vertical="center", wrap_text=True
            )

        ws.row_dimensions[row_idx].height = 20


def _row_fill_color(item: dict, row_idx: int) -> str:
    """Retorna el color de fondo según estado de la revista."""
    if item.get("error"):
        return COLOR_ERROR
    status = (item.get("status") or "").lower()
    if "discontinued" in status or "inactive" in status:
        return COLOR_DISCONT
    if "active" in status:
        # Alternar tono verde para legibilidad
        return COLOR_ACTIVE if row_idx % 2 == 0 else "D9F0DD"
    return COLOR_ALT_ROW if row_idx % 2 == 0 else "FFFFFF"


def _auto_adjust_columns(ws):
    """Aplica los anchos de columna definidos en COLUMNS."""
    for col_idx, (_, _, width) in enumerate(COLUMNS, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = width


def _write_summary_sheet(ws, results: List[dict]):
    """Hoja de resumen con estadísticas del lote."""
    total     = len(results)
    found     = sum(1 for r in results if not r.get("error") and r.get("title"))
    not_found = sum(1 for r in results if r.get("error") == "Revista no encontrada en Scopus.")
    errors    = sum(1 for r in results if r.get("error") and r.get("error") != "Revista no encontrada en Scopus.")
    active    = sum(1 for r in results if (r.get("status") or "").lower() == "active")
    discont   = sum(1 for r in results if (r.get("status") or "").lower() in ("discontinued", "inactive"))
    unknown   = sum(1 for r in results if (r.get("status") or "").lower() == "unknown" and not r.get("error"))

    header_fill = PatternFill(fill_type="solid", fgColor=COLOR_HEADER_BG)

    rows = [
        ("Métrica",              "Valor"),
        ("Total ISSNs consultados", total),
        ("Revistas encontradas",    found),
        ("No encontradas (404)",    not_found),
        ("Errores de API",          errors),
        ("",                        ""),
        ("Activas",                 active),
        ("Descontinuadas",          discont),
        ("Estado desconocido",      unknown),
        ("",                        ""),
        ("Fecha de generación", datetime.now().strftime("%d/%m/%Y %H:%M")),
    ]

    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 20

    for row_idx, (label, value) in enumerate(rows, start=1):
        cell_a = ws.cell(row=row_idx, column=1, value=label)
        cell_b = ws.cell(row=row_idx, column=2, value=value)
        if row_idx == 1:
            for cell in (cell_a, cell_b):
                cell.font = Font(bold=True, color=COLOR_HEADER_FONT)
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal="center")
                cell.border = THIN_BORDER
        elif label:
            cell_a.font = Font(bold=True)
            cell_a.border = THIN_BORDER
            cell_b.border = THIN_BORDER
            cell_b.alignment = Alignment(horizontal="center")
            if row_idx % 2 == 0:
                cell_a.fill = PatternFill(fill_type="solid", fgColor=COLOR_ALT_ROW)
                cell_b.fill = PatternFill(fill_type="solid", fgColor=COLOR_ALT_ROW)


# ── Lector de ISSNs desde archivo Excel subido ───────────────────────────

def read_issns_from_excel(file_bytes: bytes) -> list[str]:
    """
    Lee un archivo .xlsx y extrae todos los ISSNs de la primera columna (A).

    - Omite la primera fila si parece un encabezado (texto, no ISSN).
    - Acepta ISSNs con o sin guion: '2595-3982' o '25953982'.
    - Elimina duplicados preservando el orden de aparición.
    - Ignora celdas vacías.

    Args:
        file_bytes: Contenido binario del .xlsx.

    Returns:
        Lista de ISSNs únicos como strings.

    Raises:
        ValueError: Si el archivo no es válido o no contiene ISSNs reconocibles.
    """
    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(file_bytes), read_only=True, data_only=True
        )
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo Excel: {e}")

    ws = wb.active
    # Patrón: 7-8 caracteres: 4 dígitos, guion opcional, 3 dígitos + dígito/X
    ISSN_RE = re.compile(r"^\d{4}-?\d{3}[\dXx]$", re.IGNORECASE)

    seen: set[str] = set()
    issns: list[str] = []

    for row_idx, row in enumerate(
        ws.iter_rows(min_col=1, max_col=1, values_only=True), start=1
    ):
        raw = row[0]
        if raw is None:
            continue
        value = str(raw).strip()
        if not value:
            continue

        # Omitir encabezado textual en la primera fila
        normalized = value.replace("-", "")
        if row_idx == 1 and not ISSN_RE.match(value) and not re.match(r"^[\dXx]{7,8}$", normalized, re.I):
            continue

        # Validar: con guion o sin guion (7-8 chars alfanum)
        if ISSN_RE.match(value) or re.match(r"^[\dXx]{7,8}$", normalized, re.I):
            if value not in seen:
                seen.add(value)
                issns.append(value)

    wb.close()

    if not issns:
        raise ValueError(
            "No se encontraron ISSNs válidos en la columna A del archivo. "
            "Asegúrese de que la columna A contenga ISSNs "
            "(formatos aceptados: 2595-3982 ó 25953982)."
        )

    return issns


# ── Lector de Excel de exportación Scopus ────────────────────────────────────

# Mapeo flexible de nombres de columna del export Scopus → clave interna
_SCOPUS_COL_MAP = {
    "title":          ["title"],
    "year":           ["year"],
    "source_title":   ["source title"],
    "issn":           ["issn"],
    "isbn":           ["isbn"],
    "doi":            ["doi"],
    "document_type":  ["document type"],
    "authors":        ["authors"],
    "eid":            ["eid"],
    "language":       ["language of original document", "language"],
    "open_access":    ["open access"],
    "cited_by":       ["cited by"],
    "publisher":      ["publisher"],
}


def _normalize_header(h: str) -> str:
    return str(h).strip().lower()


def read_publications_from_excel(file_bytes: bytes) -> tuple[list[str], list[dict]]:
    """
    Lee un Excel de exportación de Scopus y retorna los datos de publicaciones.

    Requiere que la primera fila sea encabezados.
    Columnas clave detectadas automáticamente (case-insensitive):
      Title, Year, Source title, ISSN, DOI, Document Type, Authors, EID…

    Args:
        file_bytes: Contenido binario del .xlsx.

    Returns:
        Tuple (headers: list[str], rows: list[dict]):
          - headers: nombres originales de columna en orden
          - rows: cada fila como dict con TODOS los valores originales,
                  MÁS claves internas normalizadas (title, year, issn,
                  source_title, doi, document_type, authors)

    Raises:
        ValueError: Si el archivo no es válido o no tiene encabezados.
    """
    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo Excel: {e}")

    ws = wb.active
    headers: list[str] = []
    rows: list[dict] = []

    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:
            headers = [str(c).strip() if c is not None else f"Col_{i}"
                       for i, c in enumerate(row)]
            continue

        # Skip filas completamente vacías
        if all(c is None for c in row):
            continue

        row_dict: dict = {}
        # Guardar todos los valores originales
        for col_name, val in zip(headers, row):
            row_dict[col_name] = val if val is not None else ""

        # Agregar claves internas normalizadas
        norm_headers = {_normalize_header(h): h for h in headers}
        for internal_key, candidates in _SCOPUS_COL_MAP.items():
            for cand in candidates:
                orig_col = norm_headers.get(cand)
                if orig_col:
                    row_dict[f"__{internal_key}"] = row_dict.get(orig_col, "")
                    break
            else:
                row_dict[f"__{internal_key}"] = ""

        rows.append(row_dict)

    wb.close()

    if not headers:
        raise ValueError("El archivo Excel no tiene encabezados en la primera fila.")
    if not rows:
        raise ValueError("El archivo Excel no contiene filas de datos.")

    return headers, rows


# ── Generador del Excel de verificación de cobertura ─────────────────────────

# Columnas nuevas que se añaden al reporte (hoja principal, sin áreas temáticas)
_COVERAGE_NEW_COLS = [
    ("Revista en Scopus",         "journal_found",        10),
    ("Título oficial (Scopus)",   "scopus_journal_title", 40),
    ("Editorial (Scopus)",        "scopus_publisher",     28),
    ("Estado revista",            "journal_status",       16),
    ("Periodos de cobertura",     "coverage_periods_str", 34),
    ("¿En cobertura?",            "in_coverage",          26),
]

# Palabras clave para detectar columnas de autores → van a la hoja "Autores"
_AUTHOR_COL_KEYWORDS = ("author", "affiliat", "correspondence")

COLOR_IN_COV      = "C6EFCE"   # verde
COLOR_OUT_COV     = "FFCCCC"   # rojo
COLOR_NO_DATA     = "FFF2CC"   # amarillo
COLOR_NOT_FOUND   = "E0E0E0"   # gris


def _is_author_col(col_name: str) -> bool:
    """True si el encabezado corresponde a una columna de autores/afiliaciones."""
    norm = _normalize_header(col_name)
    return any(kw in norm for kw in _AUTHOR_COL_KEYWORDS)


def _write_sheet_header(ws, all_headers: list[str], title_text: str):
    """Escribe la fila de título (fila 1) y la fila de encabezados (fila 2)."""
    num_cols = len(all_headers)
    header_fill = PatternFill(fill_type="solid", fgColor=COLOR_HEADER_BG)

    title_cell = ws.cell(row=1, column=1, value=title_text)
    title_cell.font = Font(bold=True, size=12, color="FFFFFF")
    title_cell.fill = header_fill
    ws.row_dimensions[1].height = 22
    if num_cols > 1:
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=num_cols)

    for col_idx, col_name in enumerate(all_headers, start=1):
        cell = ws.cell(row=2, column=col_idx, value=col_name)
        cell.font = Font(bold=True, color=COLOR_HEADER_FONT, size=10)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER
    ws.row_dimensions[2].height = 28


def generate_publications_coverage_excel(
    headers: list[str],
    rows: list[dict],
) -> bytes:
    """
    Genera un Excel de verificación de cobertura con 3 hojas:
      1. "Cobertura" – columnas del artículo/revista (sin autores) + columnas de cobertura
      2. "Autores"   – DOI + Título + todas las columnas de autores/afiliaciones
      3. "Resumen"   – estadísticas del cruce

    Args:
        headers: Columnas originales del archivo fuente (en orden).
        rows:    Filas enriquecidas por SerialTitleExtractor.check_publications_coverage().

    Returns:
        bytes del .xlsx listo para enviar como respuesta HTTP.
    """
    wb = openpyxl.Workbook()

    # Separar columnas de autores de columnas de artículo/revista
    article_headers = [h for h in headers if not _is_author_col(h)]
    author_headers  = [h for h in headers if _is_author_col(h)]

    # Columnas de cobertura que se agregan a la hoja principal
    cov_col_names = [col[0] for col in _COVERAGE_NEW_COLS]

    # Pre-calcular texto de periodos para cada fila
    for row in rows:
        periods: list = row.get("coverage_periods") or []
        if periods:
            parts = []
            for s, e in periods:
                parts.append(str(s) if s == e else f"{s}–{e}")
            row["coverage_periods_str"] = "  |  ".join(parts)
        else:
            # fallback: si no hay lista pero sí hay from/to
            cf = row.get("coverage_from")
            ct = row.get("coverage_to")
            if cf and ct:
                row["coverage_periods_str"] = f"{cf}–{ct}"
            elif cf:
                row["coverage_periods_str"] = f"{cf}–actual"
            else:
                row["coverage_periods_str"] = "—"

    # ── Hoja 1: Cobertura ─────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Cobertura"

    main_headers = article_headers + cov_col_names
    _write_sheet_header(
        ws, main_headers,
        f"Verificación de Cobertura Scopus  —  "
        f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}  —  "
        f"{len(rows)} publicaciones",
    )

    new_col_start = len(article_headers) + 1  # columna donde empiezan las de cobertura

    for row_idx, row in enumerate(rows, start=3):
        in_cov = str(row.get("in_coverage", ""))
        row_fill_color = _coverage_row_color(in_cov, row.get("journal_found", False), row_idx)
        row_fill = PatternFill(fill_type="solid", fgColor=row_fill_color)

        # Columnas del artículo/revista (sin autores)
        for col_idx, col_name in enumerate(article_headers, start=1):
            val = row.get(col_name, "")
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

        # Columnas nuevas de cobertura (todas con color)
        for extra_idx, (col_label, col_key, _) in enumerate(_COVERAGE_NEW_COLS):
            col_idx = new_col_start + extra_idx
            raw_val = row.get(col_key)
            if col_key == "journal_found":
                val = "Sí" if raw_val else "No"
            elif raw_val is None or raw_val == "":
                val = "—"
            else:
                val = raw_val

            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.fill = row_fill
            cell.border = THIN_BORDER
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            if col_key == "in_coverage":
                cell.font = Font(bold=True)

        ws.row_dimensions[row_idx].height = 18

    # Anchos hoja Cobertura
    for col_idx, col_name in enumerate(article_headers, start=1):
        col_letter = get_column_letter(col_idx)
        norm = _normalize_header(col_name)
        if "title" in norm and "source" not in norm:
            ws.column_dimensions[col_letter].width = 50
        elif "source title" in norm:
            ws.column_dimensions[col_letter].width = 32
        elif norm in ("year", "volume", "issue", "cited by", "art. no.", "page start", "page end"):
            ws.column_dimensions[col_letter].width = 9
        elif norm in ("doi", "link", "eid"):
            ws.column_dimensions[col_letter].width = 42
        elif norm in ("issn", "isbn", "coden"):
            ws.column_dimensions[col_letter].width = 14
        else:
            ws.column_dimensions[col_letter].width = 18

    for extra_idx, (_, _, width) in enumerate(_COVERAGE_NEW_COLS):
        col_letter = get_column_letter(new_col_start + extra_idx)
        ws.column_dimensions[col_letter].width = width

    ws.freeze_panes = "C3"   # fijar primeras 2 columnas (título + año) y fila de encabezados

    # ── Hoja 2: Autores ───────────────────────────────────────────────────────
    if author_headers:
        ws_auth = wb.create_sheet("Autores")

        # Columnas de referencia: DOI y Título (si existen en article_headers)
        ref_cols = [h for h in article_headers
                    if _normalize_header(h) in ("doi", "title", "year")]
        auth_sheet_headers = ref_cols + author_headers

        _write_sheet_header(
            ws_auth, auth_sheet_headers,
            f"Detalle de Autores y Afiliaciones  —  {len(rows)} publicaciones",
        )

        for row_idx, row in enumerate(rows, start=3):
            for col_idx, col_name in enumerate(auth_sheet_headers, start=1):
                val = row.get(col_name, "")
                cell = ws_auth.cell(row=row_idx, column=col_idx, value=val)
                cell.border = THIN_BORDER
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
            ws_auth.row_dimensions[row_idx].height = 30  # más alto por texto largo

        # Anchos hoja Autores
        for col_idx, col_name in enumerate(auth_sheet_headers, start=1):
            col_letter = get_column_letter(col_idx)
            norm = _normalize_header(col_name)
            if norm == "doi":
                ws_auth.column_dimensions[col_letter].width = 40
            elif norm == "title":
                ws_auth.column_dimensions[col_letter].width = 42
            elif norm == "year":
                ws_auth.column_dimensions[col_letter].width = 8
            elif "full name" in norm or "with affiliation" in norm:
                ws_auth.column_dimensions[col_letter].width = 60
            elif "id" in norm:
                ws_auth.column_dimensions[col_letter].width = 30
            else:
                ws_auth.column_dimensions[col_letter].width = 50

        ws_auth.freeze_panes = "D3"

    # Letras de columnas de cobertura (calculadas a partir de new_col_start)
    # _COVERAGE_NEW_COLS: [0] Revista en Scopus, [3] Estado revista, [5] ¿En cobertura?
    col_revista   = get_column_letter(new_col_start)      # Revista en Scopus
    col_status    = get_column_letter(new_col_start + 3)  # Estado revista
    col_cobertura = get_column_letter(new_col_start + 5)  # ¿En cobertura?
    last_data_row = 2 + len(rows)                          # row 1=titulo, 2=headers, 3..N=datos

    # ── Hoja 3: Descontinuadas ────────────────────────────────────────────────
    # Scopus puede retornar 'Discontinued', 'Inactive' o derivar el estado como 'Discontinued'
    _DISC_STATUSES = {"discontinued", "inactive"}

    # Log de diagnóstico: mostrar distribución de journal_status en los rows
    status_counts: dict[str, int] = {}
    for r in rows:
        s = str(r.get("journal_status", "") or "").strip()
        status_counts[s] = status_counts.get(s, 0) + 1
    logger_excel.info(f"[Excel] Distribución journal_status: {status_counts}")

    discontinued_rows = [
        r for r in rows
        if str(r.get("journal_status", "")).strip().lower() in _DISC_STATUSES
    ]
    logger_excel.info(f"[Excel] Filas descontinuadas encontradas: {len(discontinued_rows)} / {len(rows)}")
    if discontinued_rows:
        ws_disc = wb.create_sheet("Descontinuadas")
        _write_sheet_header(
            ws_disc, main_headers,
            f"Revistas Descontinuadas en Scopus  —  "
            f"{len(discontinued_rows)} publicaciones  —  "
            f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        )
        for row_idx, row in enumerate(discontinued_rows, start=3):
            in_cov = str(row.get("in_coverage", ""))
            row_fill_color = _coverage_row_color(in_cov, row.get("journal_found", False), row_idx)
            row_fill = PatternFill(fill_type="solid", fgColor=row_fill_color)

            for col_idx, col_name in enumerate(article_headers, start=1):
                val = row.get(col_name, "")
                cell = ws_disc.cell(row=row_idx, column=col_idx, value=val)
                cell.border = THIN_BORDER
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)

            for extra_idx, (col_label, col_key, _) in enumerate(_COVERAGE_NEW_COLS):
                col_idx = new_col_start + extra_idx
                raw_val = row.get(col_key)
                if col_key == "journal_found":
                    val = "Sí" if raw_val else "No"
                elif raw_val is None or raw_val == "":
                    val = "—"
                else:
                    val = raw_val
                cell = ws_disc.cell(row=row_idx, column=col_idx, value=val)
                cell.fill = row_fill
                cell.border = THIN_BORDER
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                if col_key == "in_coverage":
                    cell.font = Font(bold=True)
            ws_disc.row_dimensions[row_idx].height = 18

        # Anchos hoja Descontinuadas (mismos que Cobertura)
        for col_idx, col_name in enumerate(article_headers, start=1):
            col_letter = get_column_letter(col_idx)
            norm = _normalize_header(col_name)
            if "title" in norm and "source" not in norm:
                ws_disc.column_dimensions[col_letter].width = 50
            elif "source title" in norm:
                ws_disc.column_dimensions[col_letter].width = 32
            elif norm in ("year", "volume", "issue", "cited by", "art. no.", "page start", "page end"):
                ws_disc.column_dimensions[col_letter].width = 9
            elif norm in ("doi", "link", "eid"):
                ws_disc.column_dimensions[col_letter].width = 42
            elif norm in ("issn", "isbn", "coden"):
                ws_disc.column_dimensions[col_letter].width = 14
            else:
                ws_disc.column_dimensions[col_letter].width = 18
        for extra_idx, (_, _, width) in enumerate(_COVERAGE_NEW_COLS):
            col_letter = get_column_letter(new_col_start + extra_idx)
            ws_disc.column_dimensions[col_letter].width = width
        ws_disc.freeze_panes = "C3"

    # ── Hoja 4: Resumen ───────────────────────────────────────────────────────
    ws_sum = wb.create_sheet("Resumen")
    _write_publications_summary(ws_sum, col_revista, col_status, col_cobertura, last_data_row)

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer.read()


def _coverage_row_color(in_cov: str, found: bool, row_idx: int) -> str:
    """Color de fondo para la fila según el resultado de cobertura."""
    if not found:
        return COLOR_NOT_FOUND
    if in_cov == "Sí":
        return COLOR_IN_COV if row_idx % 2 == 0 else "D9F0DD"
    if in_cov.startswith("No"):
        return COLOR_OUT_COV if row_idx % 2 == 0 else "FFE0E0"
    return COLOR_NO_DATA if row_idx % 2 == 0 else "FFFBE6"


def _write_publications_summary(
    ws,
    col_revista: str,
    col_status: str,
    col_cobertura: str,
    last_data_row: int,
):
    """
    Hoja de resumen con fórmulas COUNTIF/COUNTIFS que apuntan a la hoja 'Cobertura'.
    Al modificar valores manualmente en esa hoja, los totales se actualizan solos.

    Args:
        col_revista:    Letra de la columna 'Revista en Scopus' en la hoja Cobertura.
        col_status:     Letra de la columna 'Estado revista' en la hoja Cobertura.
        col_cobertura:  Letra de la columna '¿En cobertura?' en la hoja Cobertura.
        last_data_row:  Última fila con datos en la hoja Cobertura.
    """
    header_fill  = PatternFill(fill_type="solid", fgColor=COLOR_HEADER_BG)
    data_range_r = f"'Cobertura'!{col_revista}{3}:{col_revista}{last_data_row}"
    data_range_s = f"'Cobertura'!{col_status}{3}:{col_status}{last_data_row}"
    data_range_c = f"'Cobertura'!{col_cobertura}{3}:{col_cobertura}{last_data_row}"

    # (etiqueta, fórmula_o_valor,  color_fill)
    rows_def = [
        # ─ Encabezado ─────────────────────────────────────────────────────
        ("Métrica",                              "Valor",                    "__header"),
        # ─ Totales ────────────────────────────────────────────────────
        ("Total publicaciones analizadas",        f"=COUNTA({data_range_c})",  None),
        ("Revistas encontradas en Scopus",        f'=COUNTIF({data_range_r},"S\u00ed")',    None),
        ("   de las cuales: activas",             f'=COUNTIF(\'Cobertura\'!{get_column_letter_offset(col_revista, 3)}{3}:{get_column_letter_offset(col_revista, 3)}{last_data_row},"Active")', None),
        ("Revistas NO encontradas",               f'=COUNTIF({data_range_r},"No")',   None),
        ("",                                      "",                         None),
        # ─ Cobertura por estado ───────────────────────────────────────
        ("✓  Publicación EN cobertura",            f'=COUNTIF({data_range_c},"S\u00ed")',             COLOR_IN_COV),
        ("✗  ANTES de cobertura",                 f'=COUNTIF({data_range_c},"No (antes de cobertura)")',       COLOR_OUT_COV),
        ("✗  DESPUÉS de cobertura",               f'=COUNTIF({data_range_c},"No (despu\u00e9s de cobertura)")',     COLOR_OUT_COV),
        ("✗  LAGUNA de cobertura",                f'=COUNTIF({data_range_c},"No (laguna de cobertura)")',       COLOR_OUT_COV),
        ("?  Sin datos suficientes",               f'=COUNTIF({data_range_c},"Sin datos")',                      COLOR_NO_DATA),
        ("",                                      "",                         None),
        # ─ % cobertura ────────────────────────────────────────────────────
        ("% en cobertura (sobre total)",          '=IFERROR(B7/B2,0)',         None),
        ("",                                      "",                         None),        # ─ Revistas descontinuadas ────────────────────────────────────────────
        ("Revistas descontinuadas en Scopus",
             f'=COUNTIF({data_range_s},"Discontinued")+COUNTIF({data_range_s},"Inactive")',
             COLOR_DISCONT),
        ("   con publicación dentro de cobertura",
             f'=COUNTIFS({data_range_s},"Discontinued",{data_range_c},"S\u00ed")'
             f'+COUNTIFS({data_range_s},"Inactive",{data_range_c},"S\u00ed")',
             COLOR_DISCONT),
        ("",                                      "",                         None),        # ─ Metadatos ────────────────────────────────────────────────────
        ("Fecha generación",                     datetime.now().strftime("%d/%m/%Y %H:%M"), None),
    ]

    ws.column_dimensions["A"].width = 42
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 46

    # Encabezado de la tercera columna (leyenda de valores válidos)
    legend_header = ws.cell(row=1, column=3, value="Valores válidos en '¿En cobertura?'")
    legend_header.font  = Font(bold=True, color=COLOR_HEADER_FONT)
    legend_header.fill  = PatternFill(fill_type="solid", fgColor=COLOR_HEADER_BG)
    legend_header.alignment = Alignment(horizontal="center")
    legend_header.border = THIN_BORDER

    legend_items = [
        ("Sí",                         COLOR_IN_COV),
        ("No (antes de cobertura)",     COLOR_OUT_COV),
        ("No (después de cobertura)",   COLOR_OUT_COV),
        ("No (laguna de cobertura)",    COLOR_OUT_COV),
        ("Sin datos",                   COLOR_NO_DATA),
        ("—",                          COLOR_NOT_FOUND),
    ]

    for row_idx, (label, formula, color) in enumerate(rows_def, start=1):
        ca = ws.cell(row=row_idx, column=1, value=label)
        cb = ws.cell(row=row_idx, column=2, value=formula)

        if color == "__header":
            for c in (ca, cb):
                c.font      = Font(bold=True, color=COLOR_HEADER_FONT)
                c.fill      = PatternFill(fill_type="solid", fgColor=COLOR_HEADER_BG)
                c.alignment = Alignment(horizontal="center")
                c.border    = THIN_BORDER
        elif label:
            ca.font   = Font(bold=True)
            ca.border = THIN_BORDER
            cb.border = THIN_BORDER
            cb.alignment = Alignment(horizontal="center")
            # Formato porcentaje para la fila de %
            if "%)" in label or "%" in label:
                cb.number_format = "0.0%"
            if color:
                fill = PatternFill(fill_type="solid", fgColor=color)
                ca.fill = fill
                cb.fill = fill

        # Leyenda en columna C (solo las primeras filas)
        if row_idx == 1:
            pass  # ya escrito arriba
        elif 1 <= row_idx - 1 <= len(legend_items):
            leg_val, leg_color = legend_items[row_idx - 2] if row_idx >= 2 and (row_idx - 2) < len(legend_items) else ("", None)
            cl = ws.cell(row=row_idx, column=3, value=leg_val)
            cl.border = THIN_BORDER
            cl.alignment = Alignment(horizontal="center")
            if leg_color:
                cl.fill = PatternFill(fill_type="solid", fgColor=leg_color)

    # Nota al pie
    note_row = len(rows_def) + 2
    note = ws.cell(
        row=note_row, column=1,
        value=(
            "⚠ Puede cambiar manualmente los valores en la columna '¿En cobertura?' "
            "de la hoja 'Cobertura' y este resumen se actualizará automáticamente."
        )
    )
    note.font      = Font(italic=True, color="666666", size=9)
    note.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=note_row, start_column=1, end_row=note_row, end_column=3)
    ws.row_dimensions[note_row].height = 30


def get_column_letter_offset(col_letter: str, offset: int) -> str:
    """Devuelve la letra de columna desplazada 'offset' posiciones desde col_letter."""
    col_idx = 0
    for ch in col_letter.upper():
        col_idx = col_idx * 26 + (ord(ch) - ord('A') + 1)
    return get_column_letter(col_idx + offset)
