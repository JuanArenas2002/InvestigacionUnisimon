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
from typing import Dict, List, Optional, Tuple

from project.ports.repository_port import RepositoryPort

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

_ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-[\dX]{4}$")

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

    def __init__(self, repo: RepositoryPort) -> None:
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

        # Validar que la fuente esté vinculada
        external_ids = author.get("external_ids") or {}
        if source not in external_ids:
            raise ValueError(
                f"Fuente '{source}' no vinculada al autor. "
                f"Fuentes disponibles: {list(external_ids.keys())}"
            )

        # Validar que el valor provenga de opciones reales de esa fuente
        options = self._repo.get_author_name_options(author_id)
        source_names = {opt["name"].lower() for opt in options if opt["source"] == source}
        if value.lower() not in source_names:
            raise ValueError(
                f"El nombre '{value}' no proviene de la fuente '{source}'. "
                f"Opciones disponibles: {[o['name'] for o in options if o['source'] == source]}"
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
        if not _ORCID_RE.match(orcid):
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
