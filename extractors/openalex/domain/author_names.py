import re

from extractors.base import normalize_author_name


_LOWERCASE_PARTICLES = {
    "de", "del", "la", "las", "los", "y", "da", "das", "do", "dos",
    "di", "du", "van", "von", "der", "den",
}


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _smart_case_token(token: str) -> str:
    if not token:
        return ""
    if re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]\.??", token):
        return token[0].upper() + ("." if token.endswith(".") else "")
    lowered = token.lower()
    if lowered in _LOWERCASE_PARTICLES:
        return lowered
    if "'" in token:
        return "'".join(_smart_case_token(part) for part in token.split("'"))
    if token.isupper() or token.islower():
        return token[:1].upper() + token[1:].lower()
    return token


def normalize_author_display_name(name: str) -> str:
    cleaned = normalize_author_name(name)
    if not cleaned:
        return ""
    return " ".join(_smart_case_token(token) for token in cleaned.split())


def extract_author_display_names(authorships: list[dict] | None) -> list[str]:
    names = [
        normalize_author_display_name((authorship.get("author") or {}).get("display_name") or "")
        for authorship in (authorships or [])
    ]
    return _dedupe_preserve_order([name for name in names if name])


def extract_institutional_author_names(authorships: list[dict] | None, ror_id: str) -> list[str]:
    names: list[str] = []
    for authorship in (authorships or []):
        institutions = authorship.get("institutions") or []
        if not any(inst.get("ror") == ror_id for inst in institutions):
            continue
        name = normalize_author_display_name((authorship.get("author") or {}).get("display_name") or "")
        if name:
            names.append(name)
    return _dedupe_preserve_order(names)


def classify_institutionality(authorships: list[dict] | None, ror_id: str) -> tuple[str | None, list[str], bool | str]:
    all_names = extract_author_display_names(authorships)
    inst_names = extract_institutional_author_names(authorships, ror_id)
    if inst_names:
        has_inst: bool | str = True
    elif authorships:
        has_inst = "verificar"
    else:
        has_inst = False
    authors_txt = "; ".join(all_names) or None
    return authors_txt, inst_names, has_inst