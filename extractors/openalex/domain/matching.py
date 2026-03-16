import re
import unicodedata
from typing import Any


_DASH_CHARS_RE = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\-]+")


_TITLE_STOPWORDS = frozenset({
    "de", "del", "la", "el", "los", "las", "un", "una", "unos", "unas",
    "en", "y", "o", "a", "para", "por", "con", "sin", "sobre", "entre",
    "the", "an", "of", "in", "and", "or", "for", "to", "is", "are",
    "its", "with", "from", "at", "by", "on", "as",
})


def normalize_title(title: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(title or "").lower())
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    no_punct = re.sub(r"[^\w\s]", " ", no_accents)
    clean = re.sub(r"\s+", " ", no_punct).strip()
    tokens = [word for word in clean.split() if word not in _TITLE_STOPWORDS]
    return " ".join(tokens)


def sanitize_title(title: str) -> str:
    return str(title or "").strip().lstrip("¿¡").strip().lower()


def title_query_variants(title: str) -> list[str]:
    """
    Genera variantes para consulta en OpenAlex cuando hay diferencias de
    indexación por puntuación y separadores (coma, guion, etc.).

        Orden de prioridad:
            1) cadena sin puntuación (segura para search_filter)
            2) frase clave compacta (2-3 tokens distintivos)
            3) texto saneado original
            4) cadena con guiones compactados (sin espacios alrededor)
    """
    base = re.sub(r"\s+", " ", sanitize_title(title)).strip()
    if not base:
        return []

    punct_relaxed = re.sub(r"[^\w\s]", " ", base)
    punct_relaxed = re.sub(r"\s+", " ", punct_relaxed).strip()
    variants: list[str] = []
    if punct_relaxed:
        variants.append(punct_relaxed)

    norm = normalize_title(base)
    norm_tokens = [t for t in norm.split() if t]
    if len(norm_tokens) >= 3:
        key_tokens = [norm_tokens[0], norm_tokens[1], norm_tokens[-1]]
        keyphrase = " ".join(dict.fromkeys(key_tokens))
        if keyphrase and keyphrase not in variants:
            variants.append(keyphrase)

    if base and base not in variants:
        variants.append(base)

    dash_compact = _DASH_CHARS_RE.sub("-", base)
    dash_compact = re.sub(r"\s*-\s*", "-", dash_compact)
    dash_compact = re.sub(r"\s+", " ", dash_compact).strip()
    if dash_compact and dash_compact not in variants:
        variants.append(dash_compact)

    return variants


def truncate_title_for_search(title: str, max_words: int = 10) -> str:
    normalized = normalize_title(title)
    words = normalized.split()
    if len(words) <= max_words:
        return normalized
    return " ".join(words[:max_words])


def title_bigrams_for_search(title: str, max_bigrams: int = 3) -> list[str]:
    """
    Genera bigramas de términos significativos para fallback de recall.

    Ejemplo: "relacion universidad empresa america" ->
             ["relacion universidad", "universidad empresa", ...]
    """
    normalized = normalize_title(title)
    tokens = [t for t in normalized.split() if len(t) >= 4]
    if len(tokens) < 2:
        return []

    bigrams: list[str] = []
    for i in range(len(tokens) - 1):
        bg = f"{tokens[i]} {tokens[i + 1]}"
        if bg not in bigrams:
            bigrams.append(bg)
        if len(bigrams) >= max_bigrams:
            break
    return bigrams


def normalize_issn(raw: str) -> str:
    clean = re.sub(r"[^0-9Xx]", "", str(raw or "")).upper()
    if len(clean) == 8:
        return clean[:4] + "-" + clean[4:]
    match = re.match(r"^(\d{4}-[\dX]{4})$", str(raw or "").strip().upper())
    return match.group(1) if match else ""


def best_match(query_title: str, candidates: list[dict], year: int | None = None, *, min_score: float = 80.0, logger: Any = None) -> dict | None:
    if not candidates:
        return None
    try:
        from rapidfuzz import fuzz
    except ImportError:
        return dict(candidates[0]) if candidates else None

    best_score = 0.0
    best_work = None
    qt_lower = sanitize_title(query_title)
    qt_norm = normalize_title(query_title)

    for work in candidates:
        cand_title = str(work.get("title") or "")
        cand_raw = sanitize_title(cand_title)
        cand_norm = normalize_title(cand_title)
        title_score = max(
            fuzz.token_sort_ratio(qt_lower, cand_raw),
            fuzz.token_sort_ratio(qt_norm, cand_norm),
        )

        work_year = work.get("publication_year")
        if year and work_year:
            diff = abs(int(work_year) - int(year))
            if diff == 0:
                year_score = 100
            elif diff == 1:
                year_score = 60
            elif diff == 2:
                year_score = 25
            elif diff == 3:
                year_score = 10
            else:
                year_score = 0
        else:
            year_score = 0

        composite = title_score * 0.85 + year_score * 0.15
        if composite > best_score:
            best_score = composite
            best_work = work

    if best_score >= min_score and best_work is not None:
        if logger is not None:
            best_year = best_work.get("publication_year")
            best_title_score = max(
                fuzz.token_sort_ratio(qt_lower, sanitize_title(str(best_work.get("title") or ""))),
                fuzz.token_sort_ratio(qt_norm, normalize_title(str(best_work.get("title") or ""))),
            )
            logger.debug(
                f"[OpenAlexEnricher] Match OK (umbral {min_score:.0f}%) — "
                f"título: {best_title_score:.0f}% | "
                f"año: {best_year} {'✓' if year and best_year and int(best_year)==int(year) else '~'} | "
                f"compuesto: {best_score:.1f}%"
            )
        return dict(best_work)

    if logger is not None:
        logger.debug(
            f"[OpenAlexEnricher] Sin match — mejor compuesto: {best_score:.1f}% < umbral {min_score:.0f}%"
        )
    return None


def best_match_loose(query_title: str, candidates: list[dict], min_title_score: float = 78.0, logger: Any = None) -> dict | None:
    if not candidates:
        return None
    try:
        from rapidfuzz import fuzz
    except ImportError:
        return dict(candidates[0]) if candidates else None

    qt_lower = sanitize_title(query_title)
    qt_norm = normalize_title(query_title)
    best_score = 0.0
    best_work = None

    for work in candidates:
        cand_title = str(work.get("title") or "")
        score = max(
            fuzz.token_sort_ratio(qt_lower, sanitize_title(cand_title)),
            fuzz.token_sort_ratio(qt_norm, normalize_title(cand_title)),
        )
        if score > best_score:
            best_score = score
            best_work = work

    if best_score >= min_title_score and best_work is not None:
        if logger is not None:
            logger.debug(
                f"[OpenAlexEnricher] Verificar match (título: {best_score:.0f}%) '{query_title[:55]}'"
            )
        return dict(best_work)
    return None