"""
Lógica de dominio para cobertura de revistas en Scopus.

Encapsula las reglas de negocio puras relacionadas con la cobertura
de revistas científicas en el índice Scopus:
  - Construcción de periodos de cobertura desde yearly-data.
  - Derivación del estado (Active/Inactiva/Discontinued) de la revista.
  - Verificación de si un año de publicación cae dentro de la cobertura.
  - Utilidades de normalización de ISSNs.

No hace ninguna llamada HTTP ni I/O de disco. Toda la lógica es pura
y testeable sin red ni filesystem.
"""

import re
import logging
from datetime import datetime
from typing import List, Tuple, Optional, Dict

logger = logging.getLogger(__name__)

# Mapa de códigos de estado de Scopus a etiquetas legibles
STATUS_MAP: Dict[str, str] = {
    "d":            "Discontinued",
    "discontinued": "Discontinued",
    "inactive":     "Discontinued",
    "n":            "Active",
    "a":            "Active",
    "active":       "Active",
}


def clean_issn(value: str) -> str:
    """
    Normaliza un ISSN: quita espacios y guiones. Retorna '' si no es válido.

    Un ISSN válido tiene 7 u 8 caracteres alfanuméricos (dígitos + posible X).

    Args:
        value: ISSN en cualquier formato (con o sin guión, con espacios).

    Returns:
        ISSN limpio sin guiones ni espacios, o '' si no tiene formato válido.

    Ejemplos:
        '2595-3982' → '25953982'
        '1234-567X' → '1234567X'
        'no-issn'   → ''
    """
    if not value:
        return ""
    cleaned = str(value).strip().replace("-", "").replace(" ", "")
    if re.match(r"^[\dXx]{7,8}$", cleaned, re.IGNORECASE):
        return cleaned
    return ""


def split_issns(raw: str) -> List[str]:
    """
    Divide un campo ISSN que puede contener varios valores separados por
    punto y coma ('; '), coma o espacio. Retorna solo los ISSNs válidos,
    deduplicados.

    Args:
        raw: String con uno o varios ISSNs mezclados con separadores.

    Returns:
        Lista de ISSNs limpios y únicos.

    Ejemplo:
        '14220067; 16616596' → ['14220067', '16616596']
    """
    if not raw:
        return []
    parts = re.split(r"[;,\s]+", str(raw).strip())
    result = []
    seen: set = set()
    for part in parts:
        clean = clean_issn(part)
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def title_similarity(a: str, b: str) -> float:
    """
    Calcula la similitud de Jaccard de tokens entre dos títulos de revistas.

    Se usa para validar que el título devuelto por Scopus corresponde
    al título buscado, evitando falsos positivos.

    Args:
        a: Primer título (ej: el título buscado por el usuario).
        b: Segundo título (ej: el título devuelto por Scopus).

    Returns:
        Score de Jaccard entre 0.0 (completamente distintos) y 1.0 (idénticos).
    """
    def _tokens(s: str) -> set:
        return set(re.sub(r'[^\w]', ' ', (s or '').lower()).split())

    t1, t2 = _tokens(a), _tokens(b)
    if not t1 or not t2:
        return 0.0
    return len(t1 & t2) / len(t1 | t2)


def is_issn_format(value: str) -> bool:
    """
    Detecta si un string tiene formato de ISSN (7-8 caracteres alfanuméricos).

    Se usa en get_bulk_coverage para distinguir automáticamente ISSNs
    de nombres de revista en la lista de entrada.

    Args:
        value: String a evaluar.

    Returns:
        True si tiene formato ISSN, False si parece un nombre de revista.
    """
    clean = value.strip().replace("-", "")
    return bool(re.match(r"^[\dXx]{7,8}$", clean))


def build_coverage_periods(
    yearly_info: list,
    declared_end_year: Optional[int] = None,
) -> Tuple[List[Tuple[int, int]], Optional[int], Optional[int]]:
    """
    Construye la lista de periodos de cobertura desde los datos anuales
    del Serial Title API de Scopus.

    Scopus no devuelve los periodos de cobertura explícitamente. La fuente
    real son los años con publicationCount > 0 en yearly-data.info. Esta
    función agrupa esos años en rangos consecutivos (periodos).

    Lógica:
      1. Filtra años con publicationCount > 0 de yearly_info.
      2. Agrupa años consecutivos en rangos (start, end).
      3. Si no hay datos anuales, usa coverageStartYear/EndYear como fallback.
      4. Si declared_end_year > último período calculado, extiende el último.

    Args:
        yearly_info: Lista de dicts de yearly-data.info del JSON de Scopus.
                     Cada dict tiene '@year' y 'publicationCount'.
        declared_end_year: Valor de coverageEndYear declarado por Scopus.
                           Puede ser mayor que el último año con datos
                           (Scopus no carga datos del año en curso hasta meses después).

    Returns:
        Tupla (coverage_periods, coverage_from, coverage_to):
          - coverage_periods: Lista de (start, end) en orden cronológico.
          - coverage_from: Año de inicio de la primera cobertura, o None.
          - coverage_to:   Año de fin de la última cobertura, o None.
    """
    # Filtrar y ordenar años con publicaciones
    active_years: List[int] = sorted(
        int(y["@year"])
        for y in (yearly_info or [])
        if isinstance(y, dict)
        and y.get("@year")
        and int(y.get("publicationCount") or 0) > 0
    )

    periods_set: set = set()

    if active_years:
        # Agrupar años consecutivos en rangos
        start = active_years[0]
        prev  = active_years[0]
        for yr in active_years[1:]:
            if yr == prev + 1:
                prev = yr
            else:
                periods_set.add((start, prev))
                start = yr
                prev  = yr
        periods_set.add((start, prev))

    coverage_periods: List[Tuple[int, int]] = sorted(periods_set, key=lambda t: t[0])

    coverage_from = coverage_periods[0][0]  if coverage_periods else None
    coverage_to   = coverage_periods[-1][1] if coverage_periods else None

    # Extender el último período si declared_end_year es mayor
    # (Scopus puede estar 1-2 años atrás en sus datos anuales)
    if declared_end_year and (coverage_to is None or declared_end_year > coverage_to):
        coverage_to = declared_end_year
        if coverage_periods:
            last_start, last_end = coverage_periods[-1]
            if declared_end_year > last_end:
                coverage_periods[-1] = (last_start, declared_end_year)

    return coverage_periods, coverage_from, coverage_to


def derive_status(
    explicit_status: Optional[str],
    coverage_to: Optional[int],
) -> str:
    """
    Deriva el estado de la revista (Active/Inactiva/Discontinued/Unknown)
    a partir del estado explícito de Scopus y el año de fin de cobertura.

    Scopus no siempre devuelve el estado explícitamente. En ese caso
    se infiere del año de fin de cobertura relativo al año actual.

    Reglas:
      - Si Scopus da estado explícito: se normaliza con STATUS_MAP.
      - Si coverage_to >= año_actual - 2: probablemente Active
        (Scopus puede estar rezagado 1-2 años).
      - Si coverage_to < año_actual - 2: "Inactiva" (no confirmada como
        Discontinued por Scopus, pero sin publicaciones recientes).
      - Sin información: "Unknown".

    Args:
        explicit_status: Estado tal como lo devuelve Scopus (puede ser
                         código corto 'd'/'n'/'a' o string completo).
        coverage_to: Año de fin de cobertura calculado.

    Returns:
        Estado normalizado: 'Active', 'Inactiva', 'Discontinued' o 'Unknown'.
    """
    current_year = datetime.now().year

    if explicit_status:
        normalized = STATUS_MAP.get(str(explicit_status).strip().lower())
        if normalized:
            return normalized
        # Si no está en el mapa, devolver tal cual (puede ser un valor nuevo)
        return str(explicit_status)

    if coverage_to:
        if coverage_to >= current_year - 2:
            return "Active"
        return "Inactiva"

    return "Unknown"


def check_year_in_coverage(
    pub_year: int,
    coverage_periods: List[Tuple[int, int]],
    coverage_from: Optional[int],
    coverage_to: Optional[int],
) -> str:
    """
    Determina si el año de publicación cae dentro de la cobertura de Scopus.

    Maneja múltiples periodos de cobertura (revistas con lagunas históricas)
    y extiende el límite superior para revistas activas con datos rezagados.

    Args:
        pub_year: Año de publicación del artículo.
        coverage_periods: Lista de (start, end) de cobertura de la revista.
        coverage_from: Año de inicio del primer período.
        coverage_to:   Año de fin del último período.

    Returns:
        Uno de los siguientes strings:
          'Sí'                         → año dentro de cobertura
          'No (antes de cobertura)'    → año anterior al inicio
          'No (después de cobertura)'  → año posterior al fin
          'No (laguna de cobertura)'   → año entre períodos (hueco)
          'Sin datos'                  → sin información de cobertura
    """
    if not pub_year:
        return "Sin datos"

    current_year = datetime.now().year

    if coverage_periods:
        last_start, last_end = coverage_periods[-1]
        # Extender el techo efectivo para revistas activas con datos rezagados
        eff_last = max(last_end, current_year) if last_end >= current_year - 2 else last_end

        if any(s <= pub_year <= e for s, e in coverage_periods):
            return "Sí"
        if last_end < pub_year <= eff_last:
            return "Sí"
        if pub_year < coverage_periods[0][0]:
            return "No (antes de cobertura)"
        if pub_year > eff_last:
            return "No (después de cobertura)"
        # Está entre períodos pero en una laguna
        return "No (laguna de cobertura)"

    # Fallback: usar coverage_from y coverage_to si no hay períodos detallados
    if coverage_from and coverage_to:
        eff_to = max(coverage_to, current_year) if coverage_to >= current_year - 2 else coverage_to
        if coverage_from <= pub_year <= eff_to:
            return "Sí"
        if pub_year < coverage_from:
            return "No (antes de cobertura)"
        return "No (después de cobertura)"

    if coverage_from and not coverage_to:
        return "Sí" if pub_year >= coverage_from else "No (antes de cobertura)"

    return "Sin datos"


def parse_entry_json(issn: str, data: dict) -> dict:
    """
    Parsea el JSON de respuesta del Serial Title API de Scopus y retorna
    un dict normalizado con los datos de cobertura de la revista.

    Estructura del JSON de Scopus ENHANCED:
      data['serial-metadata-response']['entry'][0]
        - 'dc:title': nombre de la revista
        - 'source-id': ID interno de Scopus
        - 'coverageStartYear' / 'coverageEndYear': años declarados
        - 'yearly-data': {'info': [{@year, publicationCount}...]}
        - 'prism:issn' / 'prism:eIssn': ISSNs del entry
        - 'subject-area': [{@abbrev, $}...]
        - 'sourceRecordStatus' / '@status': estado

    Args:
        issn: El identificador buscado (ISSN, E-ISSN, nombre o source_id).
        data: JSON de respuesta del Serial Title API de Scopus.

    Returns:
        Dict normalizado con claves: issn, identifier_type, resolved_issn,
        resolved_eissn, title, source_id, publisher, status, is_discontinued,
        coverage_from, coverage_to, coverage_periods, subject_areas, error.
        Si no hay datos, retorna {'issn': issn, 'error': '...'}.
    """
    entry_list = (
        data.get("serial-metadata-response", {})
            .get("entry", [])
    )
    if not entry_list:
        return {"issn": issn, "error": "Sin datos en la respuesta de Scopus."}

    entry = entry_list[0]

    # ── Periodos de cobertura ─────────────────────────────────────────
    yearly_info = (entry.get("yearly-data") or {}).get("info") or []
    if isinstance(yearly_info, dict):
        yearly_info = [yearly_info]

    # Año fin declarado por Scopus (puede superar el último año con datos)
    try:
        declared_end = int(entry["coverageEndYear"]) if entry.get("coverageEndYear") else None
    except (ValueError, TypeError):
        declared_end = None

    coverage_periods, coverage_from, coverage_to = build_coverage_periods(
        yearly_info=yearly_info,
        declared_end_year=declared_end,
    )

    # ── Estado ────────────────────────────────────────────────────────
    explicit_status = (
        entry.get("sourceRecordStatus")
        or entry.get("@status")
        or entry.get("status")
    )
    if isinstance(explicit_status, dict):
        explicit_status = explicit_status.get("$")

    status = derive_status(explicit_status, coverage_to)
    is_discontinued = status.lower() in ("inactive", "inactiva", "discontinued")

    logger.info(
        f"  parse_entry ISSN={issn}: explicit_status={explicit_status!r} "
        f"coverage_to={coverage_to} → status={status!r}"
    )

    # ── Editorial ─────────────────────────────────────────────────────
    publisher = (
        entry.get("dc:publisher")
        or entry.get("publisher")
        or entry.get("prism:publisher")
    )
    if isinstance(publisher, dict):
        publisher = publisher.get("$")

    # ── Áreas temáticas ───────────────────────────────────────────────
    subject_areas = []
    for area in entry.get("subject-area", []) or []:
        if isinstance(area, dict):
            abbr = area.get("@abbrev", "")
            name = area.get("$", "")
            subject_areas.append(f"{abbr}: {name}" if abbr else name)

    # ── ISSNs reales del entry ────────────────────────────────────────
    def _entry_issn(field: str) -> Optional[str]:
        """Extrae y limpia un campo ISSN del entry, manejando dict/list/str."""
        raw = entry.get(field)
        if isinstance(raw, dict):
            raw = raw.get("$") or raw.get("#text")
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        return clean_issn(str(raw)) if raw else None

    resolved_issn  = _entry_issn("prism:issn")  or _entry_issn("prism:isbn")
    resolved_eissn = _entry_issn("prism:eIssn") or _entry_issn("prism:e-issn")

    # ── Tipo de identificador buscado ─────────────────────────────────
    clean_search = issn.strip().replace("-", "")
    if clean_search == (resolved_eissn or ""):
        detected_type = "eissn"
    elif clean_search == (resolved_issn or ""):
        detected_type = "issn"
    elif len(clean_search) == 8:
        detected_type = "eissn"
    else:
        detected_type = "issn"

    return {
        "issn":             issn,
        "identifier_type":  detected_type,
        "resolved_issn":    resolved_issn,
        "resolved_eissn":   resolved_eissn,
        "title":            entry.get("dc:title"),
        "source_id":        entry.get("source-id"),
        "publisher":        publisher,
        "status":           status,
        "is_discontinued":  is_discontinued,
        "coverage_from":    coverage_from,
        "coverage_to":      coverage_to,
        "coverage_periods": coverage_periods,
        "subject_areas":    subject_areas,
        "error":            None,
    }
