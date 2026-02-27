"""
Utilidades compartidas del API.
"""


def build_source_url(source_name: str, source_id: str, doi: str = None) -> str:
    """
    Construye la URL pública a la ficha del producto en cada base de datos.
    """
    if not source_id:
        return ""

    source_id = str(source_id).strip()

    if source_name == "openalex":
        if source_id.startswith("https://"):
            return source_id
        return f"https://openalex.org/{source_id}"

    if source_name == "scopus":
        if source_id.startswith("2-s2.0-"):
            return f"https://www.scopus.com/record/display.uri?eid={source_id}&origin=resultslist"
        return f"https://www.scopus.com/record/display.uri?eid=2-s2.0-{source_id}&origin=resultslist"

    if source_name == "wos":
        return f"https://www.webofscience.com/wos/woscc/full-record/{source_id}"

    if source_name == "cvlac":
        return f"https://scienti.minciencias.gov.co/cvlac/visualizador/generarCurriculoCv.do?cod_rh={source_id}"

    return ""
