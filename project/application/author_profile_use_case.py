"""
Caso de uso: Edición controlada del perfil básico de un autor.

Reglas de negocio:
  - Nombre: solo puede venir de una fuente ya vinculada al autor.
  - Fuente externa: URL parseada → ID extraído → sin conflicto con otro autor.
  - ORCID: formato válido + sin conflicto.
  - Todo cambio queda en audit log (delegado al repositorio).
"""

import re
import logging
import concurrent.futures
from typing import Dict, List, Optional, Tuple

from project.domain.ports.author_repository import AuthorRepositoryPort
from project.domain.value_objects.orcid import ORCID

logger = logging.getLogger("project.application")

# ── Patrones de URL por fuente ────────────────────────────────────────────────
# Cada entrada: (patrón regex, grupo que contiene el ID)
_URL_PATTERNS: Dict[str, re.Pattern] = {
    "cvlac": re.compile(r"cod_rh=(\w+)", re.IGNORECASE),
    "openalex": re.compile(r"openalex\.org/(A\d+)", re.IGNORECASE),
    "scopus": re.compile(r"authorId=(\d+)", re.IGNORECASE),
    "google_scholar": re.compile(r"user=([\w-]+)", re.IGNORECASE),
    "orcid": re.compile(r"orcid\.org/([\d]{4}-[\d]{4}-[\d]{4}-[\dX]{4})", re.IGNORECASE),
}

KNOWN_SOURCES = list(_URL_PATTERNS.keys())

# URLs base para construir profile_url desde un ID
_PROFILE_URL_TEMPLATES: Dict[str, str] = {
    "cvlac": "https://scienti.minciencias.gov.co/cvlac/visualizador/generateCurriculoCvLac.do?cod_rh={id}",
    "openalex": "https://openalex.org/{id}",
    "scopus": "https://www.scopus.com/authid/detail.uri?authorId={id}",
    "google_scholar": "https://scholar.google.com/citations?user={id}",
    "orcid": "https://orcid.org/{id}",
    "wos": None,  # no tiene URL pública estándar
}


def _build_profile_url(source: str, external_id: str) -> Optional[str]:
    template = _PROFILE_URL_TEMPLATES.get(source)
    if template:
        return template.format(id=external_id)
    return None


def _parse_url(source: str, url: str) -> Optional[str]:
    """Extrae el ID del autor desde la URL según la fuente. Retorna None si no matchea."""
    pattern = _URL_PATTERNS.get(source)
    if not pattern:
        return None
    m = pattern.search(url)
    return m.group(1) if m else None


class AuthorProfileUseCase:
    """
    Orquesta la edición segura del perfil de un autor.
    Solo delega persistencia al repositorio — la lógica de validación vive aquí.
    """

    def __init__(self, repo: AuthorRepositoryPort) -> None:
        self._repo = repo

    # ── 1. Opciones de nombre ─────────────────────────────────────────────────

    def get_name_options(self, author_id: int) -> dict:
        author = self._repo.get_author_by_id(author_id)
        if not author:
            raise ValueError(f"Autor {author_id} no encontrado")

        options = self._repo.get_author_name_options(author_id)
        return {
            "author_id": author_id,
            "current_name": author["name"],
            "options": options,
        }

    # ── 2. Actualizar nombre ──────────────────────────────────────────────────

    def update_name(self, author_id: int, source: str, value: str) -> dict:
        author = self._repo.get_author_by_id(author_id)
        if not author:
            raise ValueError(f"Autor {author_id} no encontrado")

        # Validar que la fuente esté vinculada.
        # ORCID se almacena en author.orcid, no en external_ids.
        external_ids = author.get("external_ids") or {}
        orcid = author.get("orcid")
        source_is_orcid = source == "orcid"

        if source_is_orcid:
            if not orcid:
                raise ValueError("ORCID no vinculado a este autor.")
        elif source not in external_ids:
            raise ValueError(
                f"Fuente '{source}' no vinculada al autor. "
                f"Fuentes disponibles: {list(external_ids.keys())}"
            )

        # Validar que el valor provenga de la fuente indicada.
        # Estrategia:
        #   1. Intentar caché BD (get_author_name_options).
        #   2. Si la caché no tiene nombres para esa fuente (p.ej. CvLAC sin
        #      registros con cvlac_code coincidente), hacer fetch en vivo.
        #   3. ORCID no tiene tabla propia → siempre en vivo.
        if source_is_orcid:
            live = _live_orcid(orcid)
            live_name = live["name"] if live else None
            if not live_name or value.lower() != live_name.lower():
                raise ValueError(
                    f"El nombre '{value}' no coincide con el nombre en ORCID "
                    f"({live_name!r}). Usa el valor exacto devuelto por la fuente."
                )
        else:
            options = self._repo.get_author_name_options(author_id)
            source_names = {opt["name"].lower() for opt in options if opt["source"] == source}

            if not source_names:
                # Caché vacía para esta fuente → validar contra la API externa
                live_opt = _fetch_live_for_source(source, author)
                if live_opt:
                    source_names = {live_opt["name"].lower()}

            if value.lower() not in source_names:
                raise ValueError(
                    f"El nombre '{value}' no coincide con ningún nombre "
                    f"disponible en '{source}'."
                )

        return self._repo.update_author_name(author_id, value, source)

    # ── 3. Listar vínculos de fuente ──────────────────────────────────────────

    def get_source_links(self, author_id: int) -> dict:
        if not self._repo.get_author_by_id(author_id):
            raise ValueError(f"Autor {author_id} no encontrado")
        links = self._repo.get_author_source_links(author_id)
        return {"author_id": author_id, "links": links}

    # ── 4. Actualizar vínculo de fuente ───────────────────────────────────────

    def update_source_link(self, author_id: int, source: str, profile_url: str) -> dict:
        if not self._repo.get_author_by_id(author_id):
            raise ValueError(f"Autor {author_id} no encontrado")

        if source not in _URL_PATTERNS:
            raise ValueError(
                f"Fuente '{source}' no soportada para vinculación por URL. "
                f"Soportadas: {list(_URL_PATTERNS.keys())}"
            )

        external_id = _parse_url(source, profile_url)
        if not external_id:
            raise ValueError(
                f"No se pudo extraer el ID de autor de la URL para fuente '{source}'. "
                f"Ejemplo de URL válida: {_build_profile_url(source, '<ID>')}"
            )

        # Verificar que no esté vinculado a otro autor
        conflict_id = self._repo.check_source_id_conflict(source, external_id, author_id)
        if conflict_id:
            raise ValueError(
                f"El perfil de {source} (ID: {external_id}) ya está vinculado "
                f"al autor con id={conflict_id}. Contacte al administrador si es un error."
            )

        return self._repo.update_author_source_link(author_id, source, external_id)

    # ── 5. Desvincular fuente ─────────────────────────────────────────────────

    def remove_source_link(self, author_id: int, source: str) -> dict:
        author = self._repo.get_author_by_id(author_id)
        if not author:
            raise ValueError(f"Autor {author_id} no encontrado")
        external_ids = author.get("external_ids") or {}
        if source not in external_ids:
            raise ValueError(f"Fuente '{source}' no está vinculada a este autor")
        return self._repo.remove_author_source_link(author_id, source)

    # ── 6. Actualizar ORCID ───────────────────────────────────────────────────

    def update_orcid(self, author_id: int, orcid: str) -> dict:
        if not self._repo.get_author_by_id(author_id):
            raise ValueError(f"Autor {author_id} no encontrado")

        orcid = orcid.strip()
        if not ORCID.validate(orcid):
            raise ValueError(
                f"ORCID '{orcid}' no tiene formato válido. "
                "Debe ser: 0000-0001-2345-6789 (cuatro grupos de 4 dígitos)"
            )

        # Verificar conflicto
        conflict_id = self._repo.check_source_id_conflict("orcid", orcid, author_id)
        if conflict_id:
            raise ValueError(
                f"El ORCID '{orcid}' ya está asignado al autor con id={conflict_id}."
            )

        return self._repo.update_author_orcid(author_id, orcid)

    # ── 7. Nombres en tiempo real desde APIs externas ────────────────────────────

    def get_name_options_live(self, author_id: int) -> dict:
        """
        Consulta cada API externa vinculada al autor para obtener el nombre
        tal como aparece en esa plataforma en este momento.

        Las llamadas se ejecutan en paralelo; si una fuente falla se omite
        sin interrumpir el resto.
        """
        author = self._repo.get_author_by_id(author_id)
        if not author:
            raise ValueError(f"Autor {author_id} no encontrado")

        external_ids = author.get("external_ids") or {}
        cedula = author.get("cedula")
        orcid = author.get("orcid")

        fetchers = {}

        if cedula and external_ids.get("cvlac"):
            fetchers["cvlac"] = lambda: _live_cvlac(cedula, external_ids["cvlac"])

        if external_ids.get("openalex"):
            oa_id = external_ids["openalex"]
            fetchers["openalex"] = lambda: _live_openalex(oa_id)

        if external_ids.get("scopus"):
            sc_id = external_ids["scopus"]
            fetchers["scopus"] = lambda: _live_scopus(sc_id)

        if orcid:
            _orcid = orcid
            fetchers["orcid"] = lambda: _live_orcid(_orcid)

        options = []
        if fetchers:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                future_map = {pool.submit(fn): src for src, fn in fetchers.items()}
                for future in concurrent.futures.as_completed(future_map, timeout=20):
                    src = future_map[future]
                    try:
                        result = future.result(timeout=15)
                        if result:
                            options.append(result)
                    except concurrent.futures.TimeoutError:
                        logger.warning(f"[live-names] Timeout en fuente '{src}'")
                    except Exception as exc:
                        logger.warning(f"[live-names] Error en fuente '{src}': {exc}")

        return {
            "author_id": author_id,
            "current_name": author["name"],
            "options": options,
        }


# ── Helpers: un fetcher por fuente ───────────────────────────────────────────

def _live_cvlac(cedula: str, ext_id: str) -> Optional[dict]:
    """Obtiene el nombre del investigador desde la API Metrik CvLAC."""
    try:
        from extractors.cvlac.application.metrik_service import fetch_profile
        profile = fetch_profile(cc_investigador=cedula)
        nombre = (profile.get("investigador") or {}).get("nombre")
        if nombre:
            return {
                "source": "cvlac",
                "name": nombre,
                "profile_url": _build_profile_url("cvlac", ext_id),
            }
    except Exception as exc:
        logger.warning(f"[live-names] CvLAC cc={cedula}: {exc}")
    return None


def _live_openalex(openalex_id: str) -> Optional[dict]:
    """Obtiene el display_name del autor desde la API de OpenAlex."""
    try:
        from pyalex import Authors
        aid = openalex_id.strip()
        if not aid.startswith("https://openalex.org/"):
            aid = f"https://openalex.org/{aid}"
        author_data = Authors()[aid]
        display_name = author_data.get("display_name")
        if display_name:
            return {
                "source": "openalex",
                "name": display_name,
                "profile_url": _build_profile_url("openalex", openalex_id),
            }
    except Exception as exc:
        logger.warning(f"[live-names] OpenAlex id={openalex_id}: {exc}")
    return None


def _live_scopus(scopus_id: str) -> Optional[dict]:
    """Obtiene el nombre del autor desde Scopus Author Search API (AU-ID)."""
    try:
        import xml.etree.ElementTree as ET
        from extractors.scopus.infrastructure.http_client import create_session
        from config import scopus_config

        if not scopus_config.api_key:
            return None

        session = create_session(
            config=scopus_config,
            api_key=scopus_config.api_key,
            inst_token=scopus_config.inst_token,
        )
        resp = session.get(
            "https://api.elsevier.com/content/search/author",
            params={"query": f"AU-ID({scopus_id})"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "dc":   "http://purl.org/dc/elements/1.1/",
        }
        root = ET.fromstring(resp.content)
        entries = root.findall("atom:entry", ns)
        if entries:
            name = entries[0].findtext("dc:title", namespaces=ns)
            if name:
                return {
                    "source": "scopus",
                    "name": name,
                    "profile_url": _build_profile_url("scopus", scopus_id),
                }
    except Exception as exc:
        logger.warning(f"[live-names] Scopus id={scopus_id}: {exc}")
    return None


def _live_orcid(orcid: str) -> Optional[dict]:
    """Obtiene el nombre del autor desde la API pública de ORCID."""
    try:
        import requests as _requests
        orcid_clean = orcid.strip()
        resp = _requests.get(
            f"https://pub.orcid.org/v3.0/{orcid_clean}/person",
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            name_data = resp.json().get("name") or {}
            given  = (name_data.get("given-names")  or {}).get("value", "")
            family = (name_data.get("family-name") or {}).get("value", "")
            full   = f"{given} {family}".strip()
            if full:
                return {
                    "source": "orcid",
                    "name": full,
                    "profile_url": f"https://orcid.org/{orcid_clean}",
                }
    except Exception as exc:
        logger.warning(f"[live-names] ORCID {orcid}: {exc}")
    return None


def _fetch_live_for_source(source: str, author: dict) -> Optional[dict]:
    """
    Hace fetch en vivo para una fuente específica usando los IDs del autor.
    Usado como fallback cuando la caché BD no tiene opciones para esa fuente.
    """
    external_ids = author.get("external_ids") or {}
    ext_id = external_ids.get(source)

    if source == "cvlac":
        cedula = author.get("cedula")
        if cedula and ext_id:
            return _live_cvlac(cedula, ext_id)

    elif source == "openalex":
        if ext_id:
            return _live_openalex(ext_id)

    elif source == "scopus":
        if ext_id:
            return _live_scopus(ext_id)

    return None
