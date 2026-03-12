"""
Helpers de identificadores para el pipeline de cobertura Scopus.

Contiene las expresiones regulares, constantes y funciones auxiliares
para extraer/normalizar EIDs, DOIs y demás identificadores de las filas
del Excel de entrada.
"""
import re as _re


# ── Expresiones regulares para EID ──────────────────────────────────────────

_EID_FROM_URL    = _re.compile(r"[?&]eid=(2-s2\.0-[^&\s]+)", _re.IGNORECASE)
_EID_FROM_PATH   = _re.compile(r"/pages/publications/(\d+)",  _re.IGNORECASE)
_EID_FROM_RECORD = _re.compile(r"[?&]eid=([^&\s]+)",          _re.IGNORECASE)


# Valores considerados "vacíos" en el Excel de salida (placeholders decorativos)
_EMPTY_PLACEHOLDERS = frozenset({
    "—", "-", "–", "n/a", "na", "none", "null", "sin datos", "no encontrada"
})


def _resolve_eid(row: dict) -> str:
    """
    Devuelve el EID directo o lo extrae del Link si la columna EID está vacía.

    Maneja tres formatos de URL de Scopus:
      - ?eid=2-s2.0-XXXXX          → EID completo en query param
      - /pages/publications/NNNNN  → número puro, se prefija con '2-s2.0-'
      - Otros ?eid=XXX             → se usa tal cual
    Ignora valores placeholder del Excel de salida ('—', etc.).
    """
    eid = str(row.get("__eid", "") or "").strip()
    if eid and eid.lower() not in _EMPTY_PLACEHOLDERS:
        return eid
    link = str(row.get("__link", "") or "").strip()
    if link:
        m = _EID_FROM_PATH.search(link)
        if m:
            return f"2-s2.0-{m.group(1)}"
        m = _EID_FROM_RECORD.search(link)
        if m:
            return m.group(1).strip()
    return ""


def _clean_id(v) -> str:
    """Normaliza un valor de identificador: elimina placeholders del Excel de salida."""
    s = str(v or "").strip()
    return "" if s.lower() in _EMPTY_PLACEHOLDERS else s


def _build_pub_entry(row: dict, *, include_prev: bool = True) -> dict:
    """
    Construye el dict de publicación para check_publications_coverage.

    Si include_prev=False se omiten los _prev_* (fuerza re-consulta).
    Lee nombres de columna tanto del formato Scopus original como del
    formato de salida del propio reporte (para re-procesamiento).
    Normaliza los valores '—' que el Excel de salida escribe en celdas vacías.
    """
    def _g(*keys) -> str:
        for k in keys:
            v = row.get(k)
            s = str(v or "").strip()
            if s and s.lower() not in _EMPTY_PLACEHOLDERS:
                return s
        return ""

    pub = {
        "issn":         ";".join(filter(None, [
            _clean_id(row.get("__issn",  "")),
            _clean_id(row.get("__eissn", "")),
        ])),
        "isbn":         _clean_id(row.get("__isbn", "")),
        "doi":          _clean_id(row.get("__doi", "")),
        "eid":          _resolve_eid(row),
        "source_title": _g("__source_title"),
        "year":         row.get("__year"),
        "title":        _g("__title"),
    }
    if include_prev:
        pub.update({
            "_prev_in_coverage":         _g("¿En cobertura?"),
            "_prev_journal_found":        _g("Revista en Scopus", "En Scopus"),
            "_prev_journal_status":       _g("Estado revista"),
            "_prev_scopus_journal_title": _g("Título oficial (Scopus)", "Revista (Scopus)"),
            "_prev_scopus_publisher":     _g("Editorial (Scopus)", "Editorial"),
            "_prev_coverage_periods_str": _g("Periodos de cobertura", "Periodos cobertura"),
        })
    else:
        pub.update({
            "_prev_in_coverage":         "",
            "_prev_journal_found":        "",
            "_prev_journal_status":       "",
            "_prev_scopus_journal_title": "",
            "_prev_scopus_publisher":     "",
            "_prev_coverage_periods_str": "",
        })
    return pub
