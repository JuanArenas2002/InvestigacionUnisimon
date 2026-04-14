"""
Servicio de extracción de perfiles CvLAC desde la API JSON de Metrik Unisimon.

A diferencia del scraper de Minciencias (que parsea HTML), este servicio
consume el endpoint REST de Metrik que devuelve JSON estructurado:

    GET https://metrik.unisimon.edu.co/scienti/cvlac/{cc_investigador}

Responsabilidades:
  - Consumir el endpoint y validar la respuesta JSON.
  - Normalizar los campos del investigador: cc, nombre, categoria, nacionalidad.
  - Normalizar cada producto de produccion[]: tipo, subtipo, titulo, revista,
    anio (int), doi (None si vacío), editorial, autores (list desde CSV).
  - Aplicar reglas de limpieza: ignorar "N/A", trim, DOI vacío → None.

No accede a base de datos ni crea StandardRecord — devuelve dicts limpios
listos para serializar como JSON.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional

import requests
import urllib3
from unidecode import unidecode

logger = logging.getLogger(__name__)

METRIK_BASE_URL = "https://metrik.unisimon.edu.co/scienti/cvlac"
_NA_VALUES = {"n/a", "na", "null", "none", "-", ""}

# Únicos subtipos aceptados (sin tildes, minúsculas — comparación robusta).
# "Artículos" → unidecode → "articulos"
_SUBTIPO_ACEPTADO = "articulos"

# Metrik Unisimon usa un certificado de CA institucional no reconocido por
# el store de Python. Se desactiva la verificación SSL solo para este host.
# InsecureRequestWarning se suprime para no contaminar los logs de la API.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ──────────────────────────────────────────────────────────────
# Helpers de limpieza
# ──────────────────────────────────────────────────────────────

def _clean(value: Any) -> Optional[str]:
    """
    Devuelve el valor como string limpio, o None si es vacío / N/A.

    Args:
        value: Cualquier valor del JSON crudo.

    Returns:
        String con strip(), o None si el valor es N/A / vacío.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _NA_VALUES:
        return None
    return text


def _to_int(value: Any) -> Optional[int]:
    """
    Convierte el año a entero, devuelve None si no es convertible.

    Args:
        value: Valor del campo año (puede venir como string o número).

    Returns:
        Entero o None.
    """
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def _split_authors(value: Any) -> List[str]:
    """
    Convierte una cadena de autores separada por coma a lista limpia.

    Regla: cada nombre se hace strip + title-case si estaba en MAYÚSCULAS
    completas (convención frecuente en CvLAC); de lo contrario se conserva.

    Args:
        value: String tipo "PEREZ JUAN, LOPEZ MARIA" o lista ya parseada.

    Returns:
        Lista de strings, sin vacíos.
    """
    if not value:
        return []
    if isinstance(value, list):
        return [a.strip() for a in value if str(a).strip()]

    parts = str(value).split(",")
    result = []
    for part in parts:
        name = part.strip()
        if not name or name.lower() in _NA_VALUES:
            continue
        # Si todo el nombre está en mayúsculas, convertir a title-case
        if name.isupper():
            name = name.title()
        result.append(name)
    return result


def _clean_doi(value: Any) -> Optional[str]:
    """
    Normaliza el DOI: vacío / N/A → None, de lo contrario strip().

    Args:
        value: Valor crudo del campo doi.

    Returns:
        DOI limpio o None.
    """
    cleaned = _clean(value)
    if not cleaned:
        return None
    # Conservar el DOI tal cual; solo limpiar espacios
    return cleaned


# ──────────────────────────────────────────────────────────────
# Normalizadores de dominio
# ──────────────────────────────────────────────────────────────

def _normalize_investigador(raw: dict, cc: str) -> dict:
    """
    Extrae y limpia los campos del investigador desde el JSON crudo.

    Intenta variantes de nombre de campo (mayúsculas, minúsculas, con/sin
    acento) para tolerar diferencias entre versiones del API.

    Args:
        raw: Dict raíz del JSON devuelto por el endpoint Metrik.
        cc:  Cédula del investigador usada en la petición (fallback).

    Returns:
        Dict con: cc, nombre, categoria, nacionalidad.
        Campos sin valor se omiten (no se incluyen con None).
    """
    def get(*keys):
        for k in keys:
            v = _clean(raw.get(k))
            if v is not None:
                return v
        return None

    investigador = {}

    cc_val = get("cc", "cedula", "cod_rh", "id") or cc
    if cc_val:
        investigador["cc"] = cc_val

    nombre = get("nombre", "Nombre", "name", "nombres", "nombre_completo")
    if nombre:
        investigador["nombre"] = nombre

    categoria = get("categoria", "Categoria", "categoria_investigador", "category")
    if categoria:
        investigador["categoria"] = categoria

    nacionalidad = get("nacionalidad", "Nacionalidad", "nationality", "pais")
    if nacionalidad:
        investigador["nacionalidad"] = nacionalidad

    return investigador


def _normalize_produccion(items: List[Any], cc: str) -> List[dict]:
    """
    Normaliza la lista de productos de producción del investigador.

    Aplica todas las reglas de limpieza:
      - Ignora valores "N/A".
      - DOI vacío → None.
      - anio → int.
      - autores → list (split por coma).
      - Campos vacíos se omiten del dict resultado.

    Args:
        items: Lista cruda del campo produccion[] del JSON.
        cc:    Cédula del investigador (se usa como fallback para el campo cc).

    Returns:
        Lista de dicts normalizados, uno por producto.
    """
    result = []

    for item in items:
        if not isinstance(item, dict):
            continue

        # ── Filtro de subtipo: solo "Artículos" ──────────────────────────
        # Buscar el campo en sus variantes de capitalización
        subtipo_raw = (
            item.get("Subtipo")
            or item.get("subtipo")
            or item.get("SubTipo")
            or ""
        )
        subtipo_norm = unidecode(str(subtipo_raw)).strip().lower()
        if subtipo_norm != _SUBTIPO_ACEPTADO:
            logger.debug(
                f"[MetrikCvLAC] Subtipo ignorado: '{subtipo_raw}'"
            )
            continue
        # ─────────────────────────────────────────────────────────────────

        def get(*keys):
            for k in keys:
                v = item.get(k)
                if v is not None:
                    cleaned = _clean(v)
                    if cleaned is not None:
                        return cleaned
            return None

        producto: dict = {}

        # cc del producto (puede estar en el item o heredarse del investigador)
        cc_val = _clean(item.get("cc") or item.get("cedula")) or cc
        if cc_val:
            producto["cc"] = cc_val

        autor_principal = get(
            "autor_principal", "AutorPrincipal", "autor", "first_author"
        )
        if autor_principal:
            producto["autor_principal"] = autor_principal

        tipo = get("tipo", "Tipo", "tipo_producto", "type", "publication_type")
        if tipo:
            producto["tipo"] = tipo

        subtipo = get("subtipo", "Subtipo", "subtipo_producto", "subtype")
        if subtipo:
            producto["subtipo"] = subtipo

        titulo = get("titulo", "Titulo", "title", "titulo_producto", "Título")
        if titulo:
            producto["titulo"] = titulo

        revista = get(
            "revista", "Revista", "journal", "fuente", "source", "nombre_revista"
        )
        if revista:
            producto["revista"] = revista

        # Año: obligatoriamente int
        anio_raw = item.get("anio") or item.get("año") or item.get("year") or item.get("Año")
        anio = _to_int(anio_raw)
        if anio is not None:
            producto["anio"] = anio

        # DOI: None si vacío
        doi = _clean_doi(
            item.get("doi") or item.get("DOI") or item.get("Doi")
        )
        producto["doi"] = doi  # siempre presente (None explícito)

        editorial = get(
            "editorial", "Editorial", "publisher", "edicion", "Edición", "edition"
        )
        if editorial:
            producto["editorial"] = editorial

        # Autores: split por coma
        autores_raw = (
            item.get("autores")
            or item.get("Autores")
            or item.get("authors")
            or item.get("coautores")
        )
        producto["autores"] = _split_authors(autores_raw)

        result.append(producto)

    return result


# ──────────────────────────────────────────────────────────────
# Punto de entrada público
# ──────────────────────────────────────────────────────────────

def fetch_profile(cc_investigador: str, timeout: int = 30) -> dict:
    """
    Descarga y normaliza el perfil CvLAC desde el API Metrik Unisimon.

    Flujo:
      1. GET https://metrik.unisimon.edu.co/scienti/cvlac/{cc_investigador}
      2. Valida que la respuesta sea JSON con estructura reconocible.
      3. Normaliza investigador y produccion[].
      4. Devuelve dict limpio listo para serializar.

    Args:
        cc_investigador: Cédula de ciudadanía del investigador.
        timeout:         Timeout de la petición HTTP en segundos.

    Returns:
        Dict con estructura:
        {
            "investigador": { cc, nombre, categoria, nacionalidad },
            "produccion":   [ { cc, autor_principal, tipo, subtipo, titulo,
                                revista, anio, doi, editorial, autores }, ... ]
        }

    Raises:
        requests.HTTPError: Si el servidor devuelve un código de error HTTP.
        ValueError:         Si la respuesta no es JSON válido o falta
                            el campo produccion.
        requests.Timeout:   Si la petición supera el timeout.
        requests.ConnectionError: Si no se puede conectar al servidor.
    """
    cc = str(cc_investigador).strip()
    url = f"{METRIK_BASE_URL}/{cc}"

    logger.info(f"[MetrikCvLAC] Consultando: {url}")

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "BiblioReconciler/1.0 (metrik-cvlac-client)",
    })
    # verify=False porque Metrik Unisimon usa CA institucional no reconocida
    # por el store de certificados de Python / Windows.
    session.verify = False

    response = session.get(url, timeout=timeout)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise requests.HTTPError(
            f"[MetrikCvLAC] HTTP {response.status_code} al consultar cc={cc}: {e}",
            response=response,
        )

    try:
        raw = response.json()
    except Exception:
        raise ValueError(
            f"[MetrikCvLAC] La respuesta para cc={cc} no es JSON válido. "
            f"Primeros 200 chars: {response.text[:200]}"
        )

    # Tolerar respuesta como lista (algunos endpoints envuelven en [])
    if isinstance(raw, list):
        raw = raw[0] if raw else {}

    if not isinstance(raw, dict):
        raise ValueError(
            f"[MetrikCvLAC] Formato inesperado para cc={cc}: {type(raw)}"
        )

    investigador = _normalize_investigador(raw, cc)

    # produccion puede estar en distintas claves
    produccion_raw = (
        raw.get("produccion")
        or raw.get("Produccion")
        or raw.get("produccion_bibliografica")
        or raw.get("products")
        or []
    )

    if not isinstance(produccion_raw, list):
        logger.warning(
            f"[MetrikCvLAC] 'produccion' no es lista para cc={cc} "
            f"(tipo: {type(produccion_raw)}). Se devuelve vacío."
        )
        produccion_raw = []

    produccion = _normalize_produccion(produccion_raw, cc)

    logger.info(
        f"[MetrikCvLAC] cc={cc}: "
        f"{len(produccion)} productos normalizados."
    )

    return {
        "investigador": investigador,
        "produccion": produccion,
    }
