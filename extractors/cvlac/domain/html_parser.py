"""
Parseo puro de la estructura HTML de perfiles CVLAC (Minciencias Colombia).

CVLAC no tiene API REST pública. Sus perfiles son páginas HTML con tablas
organizadas por secciones de producción bibliográfica.

NOTA DE FRAGILIDAD: La estructura HTML de CVLAC puede cambiar si Minciencias
actualiza el portal. Esta implementación se basa en la estructura conocida a
2025 y puede requerir actualización ante cambios del portal.

Estructura típica de la página CVLAC:
  <h3>Artículos publicados</h3>
  <table>
    <tr>
      <td>Título del artículo</td>
      <td>Nombre de la revista</td>
      <td>... año, ISSN, etc. ...</td>
    </tr>
  </table>

Este módulo solo lee y transforma HTML — sin HTTP ni I/O de disco.
"""

import logging
import re
from typing import List, Optional

logger = logging.getLogger(__name__)

# Secciones de producción bibliográfica que se parsean,
# mapeadas a su tipo de publicación estándar.
SECTIONS_TO_PARSE = [
    ("Artículos publicados",              "article"),
    ("Libros publicados",                 "book"),
    ("Capítulos de libro publicados",     "book-chapter"),
    ("Documentos de trabajo",             "working-paper"),
    ("Otra producción bibliográfica",     "other"),
]


def extract_row_data(cells, pub_type: str, cvlac_code: str) -> Optional[dict]:
    """
    Extrae datos de una fila <tr> de la tabla de productos CVLAC.

    La estructura de columnas varía por sección, pero típicamente contiene:
      cells[0] → Título del producto
      cells[1] → Revista / Editorial / Fuente
      Resto    → Texto con año, ISSN, DOI, etc. mezclados

    Estrategia de extracción:
      - Año: regex (19|20)\\d{2} en cualquier celda.
      - ISSN: regex \\d{4}-\\d{3}[\\dxX] en cualquier celda.
      - DOI: busca <a href="...doi.org..."> en las celdas.
      - Título/revista: posición fija (cells[0] y cells[1]).

    Args:
        cells: Lista de elementos <td> de BeautifulSoup.
        pub_type: Tipo de publicación de la sección padre (ej: 'article').
        cvlac_code: Código CVLAC del investigador, para generar un ID único.

    Returns:
        Dict con las claves: cvlac_product_id, title, year, type, issn,
        doi, journal, authors. None si la fila no tiene título.
    """
    # Extraer texto plano de cada celda
    text_content = [cell.get_text(strip=True) for cell in cells]

    # El título es siempre la primera celda
    title = text_content[0] if text_content else None

    # --- Año: buscar en todas las celdas ---
    year = None
    for text in text_content:
        year_match = re.search(r'(19|20)\d{2}', text)
        if year_match:
            year = int(year_match.group())
            break

    # --- ISSN: formato XXXX-XXXN (con dígito o X al final) ---
    issn = None
    for text in text_content:
        issn_match = re.search(r'\d{4}-\d{3}[\dxX]', text)
        if issn_match:
            issn = issn_match.group()
            break

    # --- DOI: buscar enlace a doi.org en las celdas ---
    doi = None
    for cell in cells:
        link = cell.find("a", href=re.compile(r"doi\.org", re.IGNORECASE))
        if link:
            doi = link.get("href", "").strip()
            break

    # Generar un ID único basado en el código del investigador y el hash del título
    product_id = f"cvlac_{cvlac_code}_{hash(title) if title else 'unknown'}"

    return {
        "cvlac_product_id": product_id,
        "title":            title,
        "year":             year,
        "type":             pub_type,
        "issn":             issn,
        "doi":              doi,
        "journal":          text_content[1] if len(text_content) > 1 else None,
        # CVLAC muestra el perfil de un solo autor → la autoría se infiere
        # del código consultado, no de la página.
        "authors":          [],
    }


def parse_section(
    soup,
    section_title: str,
    pub_type: str,
    cvlac_code: str,
    year_from: Optional[int],
    year_to: Optional[int],
) -> List[dict]:
    """
    Parsea una sección específica de la página CVLAC y devuelve
    los productos encontrados como lista de dicts crudos.

    Lógica:
      1. Busca el encabezado de texto que coincida con section_title.
      2. Busca la <table> inmediatamente siguiente.
      3. Itera las <tr> y extrae datos con extract_row_data.
      4. Filtra por rango de años si se especifica.

    Args:
        soup: Objeto BeautifulSoup del perfil CVLAC completo.
        section_title: Texto del encabezado de sección (ej: 'Artículos publicados').
        pub_type: Tipo de publicación para etiquetar los registros.
        cvlac_code: Código del investigador, para IDs únicos.
        year_from: Año mínimo de filtro (None = sin límite inferior).
        year_to:   Año máximo de filtro (None = sin límite superior).

    Returns:
        Lista de dicts crudos de productos, filtrados por año.
        Lista vacía si no se encuentra la sección o no hay productos.
    """
    records = []

    # Buscar el encabezado de la sección en el HTML
    header = soup.find(string=re.compile(section_title, re.IGNORECASE))
    if not header:
        logger.debug(f"[CVLAC] Sección '{section_title}' no encontrada en perfil {cvlac_code}.")
        return records

    # La tabla de productos sigue al encabezado
    table = header.find_next("table")
    if not table:
        logger.debug(f"[CVLAC] Sin tabla después de '{section_title}' en perfil {cvlac_code}.")
        return records

    rows = table.find_all("tr")
    for row in rows:
        try:
            cells = row.find_all("td")
            if len(cells) < 2:
                # Fila sin suficientes columnas (encabezado o separador)
                continue

            raw = extract_row_data(cells, pub_type, cvlac_code)
            if not raw or not raw.get("title"):
                continue

            # Filtrar por rango de años
            year = raw.get("year")
            if year_from and year and year < year_from:
                continue
            if year_to and year and year > year_to:
                continue

            records.append(raw)

        except Exception as e:
            logger.debug(f"[CVLAC] Error parseando fila en sección '{section_title}': {e}")
            continue

    logger.debug(
        f"[CVLAC] Sección '{section_title}' en {cvlac_code}: {len(records)} productos."
    )
    return records
