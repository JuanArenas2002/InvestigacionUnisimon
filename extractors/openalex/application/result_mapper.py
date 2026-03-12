from config import institution
from extractors.base import normalize_doi

from ..domain.author_names import classify_institutionality


EMPTY_OA_ROW = {
    "oa_encontrado": False,
    "oa_confianza": None,
    "oa_metodo": None,
    "oa_work_id": None,
    "oa_titulo": None,
    "oa_año": None,
    "oa_doi": None,
    "oa_tipo": None,
    "oa_revista": None,
    "oa_issn": None,
    "oa_issn_todos": None,
    "oa_editorial": None,
    "oa_open_access": None,
    "oa_status_oa": None,
    "oa_citas": None,
    "oa_idioma": None,
    "oa_url": None,
    "oa_autores": None,
    "oa_autor_institucional": None,
    "oa_autores_institucionales": None,
}


def flatten_result(orig: dict, oa: dict | None, method: str | None) -> dict:
    out = dict(orig)
    if not oa:
        out.update(EMPTY_OA_ROW)
        return out

    primary_loc = oa.get("primary_location") or {}
    source = primary_loc.get("source") or {}
    open_access = oa.get("open_access") or {}
    authors_txt, inst_names, has_inst = classify_institutionality(
        oa.get("authorships") or [],
        institution.ror_id,
    )
    verify_flag = bool(method and method.endswith("_verificar"))

    out.update({
        "oa_encontrado": True,
        "oa_confianza": "verificar" if verify_flag else "confirmado",
        "oa_metodo": method,
        "oa_work_id": oa.get("id"),
        "oa_titulo": oa.get("title"),
        "oa_año": oa.get("publication_year"),
        "oa_doi": normalize_doi(str(oa.get("doi") or "")),
        "oa_tipo": oa.get("type"),
        "oa_revista": source.get("display_name"),
        "oa_issn": source.get("issn_l"),
        "oa_issn_todos": "; ".join(source.get("issn") or []) or None,
        "oa_editorial": source.get("host_organization_name"),
        "oa_open_access": open_access.get("is_oa"),
        "oa_status_oa": open_access.get("oa_status"),
        "oa_citas": oa.get("cited_by_count", 0),
        "oa_idioma": oa.get("language"),
        "oa_url": primary_loc.get("landing_page_url") or oa.get("doi") or oa.get("id"),
        "oa_autores": authors_txt or None,
        "oa_autor_institucional": has_inst,
        "oa_autores_institucionales": "; ".join(inst_names) or None,
    })
    return out