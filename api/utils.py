"""
Utilidades compartidas del API.
"""


def get_clean_source_id(source_name: str, source_id: str) -> str:
    """
    Devuelve el ID limpio de la plataforma (sin prefijo de URL base).
    El frontend construye la URL completa a partir de este ID.
    """
    if not source_id:
        return ""

    source_id = str(source_id).strip()

    if source_name == "openalex":
        # Los IDs de OpenAlex a veces se almacenan como URL completa
        if source_id.startswith("https://openalex.org/"):
            return source_id[len("https://openalex.org/"):]
        if source_id.startswith("https://"):
            return source_id  # URL desconocida, devolver tal cual
        return source_id

    return source_id
