"""
Router: Edición controlada del perfil básico de un autor.

Todos los cambios se validan en la capa de aplicación — el investigador
no tiene acceso directo a los campos; los valores vienen de las fuentes
vinculadas o se verifican contra ellas.

Prefijo: /authors/id/{author_id}
Tag: Perfil de Autor
"""

from fastapi import APIRouter, HTTPException

from api.schemas.authors import (
    NameOptionsResponse,
    UpdateNameRequest,
    SourceLinksResponse,
    UpdateSourceLinkRequest,
    UpdateOrcidRequest,
    AuthorRead,
)
from project.application.author_profile_use_case import AuthorProfileUseCase
from project.infrastructure.persistence.postgres_repository import PostgresRepository

router = APIRouter(prefix="/authors", tags=["Perfil de Autor"])

# Instancia compartida (stateless)
_repo = PostgresRepository()
_use_case = AuthorProfileUseCase(_repo)


def _handle(fn, *args, **kwargs):
    """Convierte ValueError del dominio en 400/404 de HTTP."""
    try:
        return fn(*args, **kwargs)
    except ValueError as exc:
        msg = str(exc)
        status = 404 if "no encontrado" in msg else 400
        raise HTTPException(status_code=status, detail=msg)


# ── 1. Opciones de nombre ─────────────────────────────────────────────────────

@router.get(
    "/id/{author_id}/name-options",
    response_model=NameOptionsResponse,
    summary="Nombres disponibles desde fuentes vinculadas",
    description=(
        "Retorna el nombre actual del autor y las opciones de nombre "
        "extraídas de cada fuente vinculada (CvLAC, OpenAlex, Scopus, etc.). "
        "El investigador puede seleccionar uno para actualizar su perfil."
    ),
)
def get_name_options(author_id: int):
    return _handle(_use_case.get_name_options, author_id)


# ── 2. Actualizar nombre ──────────────────────────────────────────────────────

@router.patch(
    "/id/{author_id}/name",
    response_model=dict,
    summary="Actualizar nombre del autor (validado contra fuente)",
    description=(
        "Actualiza el nombre del autor. El valor debe provenir exactamente "
        "de la fuente indicada y dicha fuente debe estar vinculada al autor. "
        "Queda registrado en el audit log."
    ),
)
def update_name(author_id: int, body: UpdateNameRequest):
    return _handle(_use_case.update_name, author_id, body.source, body.value)


# ── 3. Listar vínculos de fuente ──────────────────────────────────────────────

@router.get(
    "/id/{author_id}/source-links",
    response_model=SourceLinksResponse,
    summary="Vínculos a fuentes externas del autor",
    description=(
        "Lista todas las fuentes soportadas con su estado de vinculación "
        "(linked=true/false), el ID externo actual y la URL de perfil."
    ),
)
def get_source_links(author_id: int):
    return _handle(_use_case.get_source_links, author_id)


# ── 4. Vincular / actualizar fuente ───────────────────────────────────────────

@router.patch(
    "/id/{author_id}/source-link",
    response_model=SourceLinksResponse,
    summary="Vincular perfil externo al autor (verificado)",
    description=(
        "El investigador pega la URL de su perfil en la fuente. "
        "El endpoint extrae el ID, verifica que no esté asignado a otro autor "
        "y actualiza el vínculo. Queda registrado en el audit log.\n\n"
        "**Ejemplos de URL válidas:**\n"
        "- CvLAC: `https://scienti.minciencias.gov.co/cvlac/visualizador/generateCurriculoCvLac.do?cod_rh=0001234567`\n"
        "- OpenAlex: `https://openalex.org/A1234567890`\n"
        "- Scopus: `https://www.scopus.com/authid/detail.uri?authorId=12345678900`\n"
        "- Google Scholar: `https://scholar.google.com/citations?user=ABC123`\n"
        "- ORCID: `https://orcid.org/0000-0001-2345-6789`"
    ),
)
def update_source_link(author_id: int, body: UpdateSourceLinkRequest):
    return _handle(_use_case.update_source_link, author_id, body.source, body.profile_url)


# ── 5. Desvincular fuente ─────────────────────────────────────────────────────

@router.delete(
    "/id/{author_id}/source-link/{source}",
    response_model=SourceLinksResponse,
    summary="Desvincular fuente externa del autor",
    description=(
        "Elimina el vínculo entre el autor y la fuente indicada. "
        "El cambio queda registrado en el audit log."
    ),
)
def remove_source_link(author_id: int, source: str):
    return _handle(_use_case.remove_source_link, author_id, source)


# ── 6. Actualizar ORCID ───────────────────────────────────────────────────────

@router.patch(
    "/id/{author_id}/orcid",
    response_model=dict,
    summary="Actualizar ORCID del autor (validado)",
    description=(
        "Actualiza el ORCID del autor. Valida formato estándar "
        "(0000-0001-2345-6789) y verifica que no esté asignado a otro autor. "
        "Queda registrado en el audit log."
    ),
)
def update_orcid(author_id: int, body: UpdateOrcidRequest):
    return _handle(_use_case.update_orcid, author_id, body.orcid)
