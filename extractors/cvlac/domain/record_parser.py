"""
Conversión de items normalizados de Metrik CvLAC a campos de StandardRecord.

Recibe el dict ya limpio producido por metrik_service._normalize_produccion()
y lo transforma al contrato del sistema (StandardRecord).

Campos de entrada (produccion item):
    cc, autor_principal, tipo, subtipo, titulo, revista,
    anio (int), doi (str|None), editorial (opcional), autores (list)

Campos de salida (fields dict para StandardRecord):
    source_id, doi, title, publication_year, publication_type,
    source_journal, issn, authors, institutional_authors, raw_data
"""

import hashlib
from typing import Dict, List, Optional

# Mapeo subtipo CvLAC → publication_type estándar del sistema
_SUBTIPO_TO_TYPE: Dict[str, str] = {
    "artículos":  "article",
    "articulos":  "article",
}


def build_authors(
    names: List[str],
    is_institutional: bool = False,
    cedula: Optional[str] = None,
) -> List[Dict]:
    """
    Convierte una lista de nombres a la estructura estándar de autores.

    Args:
        names:            Lista de nombres de autores.
        is_institutional: True si se marcan como autores institucionales.
        cedula:           Cédula de ciudadanía. Si se provee, se incluye en el
                          dict para que el adapter la pase al campo cedula del
                          Author de dominio, habilitando búsqueda por cédula
                          en _upsert_author (más fiable que fuzzy por nombre).

    Returns:
        Lista de dicts {name, orcid, is_institutional, cedula?}.
    """
    result = []
    for name in names:
        if not name:
            continue
        entry: Dict = {
            "name": name,
            "orcid": None,
            "is_institutional": is_institutional,
        }
        if cedula:
            entry["cedula"] = cedula
        result.append(entry)
    return result


def parse_raw(item: dict, investigador: Optional[dict] = None) -> dict:
    """
    Convierte un item de produccion[] (formato Metrik normalizado) a los
    campos necesarios para StandardRecord.

    Args:
        item:         Dict normalizado de un producto (de metrik_service).
        investigador: Dict del investigador (cc, nombre, categoria, etc.)
                      Usado como contexto para institutional_authors.

    Returns:
        Dict con claves listas para construir un StandardRecord.
    """
    investigador = investigador or {}

    titulo = item.get("titulo") or ""
    cc = item.get("cc") or investigador.get("cc") or "unknown"

    # ID único estable: fuente + cédula + md5 del título (hash() de Python no es determinístico entre reinicios)
    titulo_hash = hashlib.md5(titulo.encode("utf-8", errors="replace")).hexdigest()[:8]
    source_id = f"metrik_{cc}_{titulo_hash}"

    # Tipo de publicación: mapear subtipo al estándar del sistema
    subtipo_raw = (item.get("subtipo") or "").strip().lower()
    pub_type = _SUBTIPO_TO_TYPE.get(subtipo_raw) or item.get("tipo") or "other"

    # Autor principal → institucional, con cédula para lookup exacto
    autor_principal = item.get("autor_principal")
    nombre_institucional = autor_principal or investigador.get("nombre")
    if nombre_institucional:
        institutional_authors = build_authors(
            [nombre_institucional], is_institutional=True, cedula=cc
        )
    else:
        institutional_authors = []

    # Coautores de la lista autores[] — sin cédula
    autores_list = item.get("autores") or []
    inst_names = {a["name"] for a in institutional_authors}
    coauthors = build_authors(
        [n for n in autores_list if n not in inst_names],
        is_institutional=False,
    )

    # authors = institucional primero + coautores sin repetir.
    # El engine lee _parsed_authors para crear los vínculos autor↔publicación;
    # si el institucional no está aquí, nunca se linkea.
    all_authors = institutional_authors + coauthors

    return {
        "source_id":             source_id,
        "doi":                   item.get("doi"),          # ya es None si vacío
        "title":                 titulo or None,
        "publication_year":      item.get("anio"),         # ya es int o None
        "publication_type":      pub_type,
        "source_journal":        item.get("revista"),
        "issn":                  None,                     # Metrik no expone ISSN
        "authors":               all_authors,
        "institutional_authors": institutional_authors,
        "raw_data": {
            **item,
            "_investigador": investigador,
        },
    }
