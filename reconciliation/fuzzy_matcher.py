"""
Motor de Fuzzy Matching para reconciliación bibliográfica.

Compara registros cuando no hay coincidencia por DOI exacto.
Usa una combinación ponderada de:
  - Similitud de título (peso: 0.55)
  - Coincidencia de año (peso: 0.20)
  - Similitud de autores (peso: 0.25)

Produce un score final de 0 a 100.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from rapidfuzz import fuzz
from unidecode import unidecode

from config import reconciliation_config as rc_config

logger = logging.getLogger(__name__)


# =============================================================
# RESULTADO DE COMPARACIÓN
# =============================================================

@dataclass
class FuzzyMatchResult:
    """Resultado detallado de una comparación fuzzy"""
    title_score: float = 0.0
    year_match: bool = False
    year_score: float = 0.0
    author_score: float = 0.0
    combined_score: float = 0.0
    is_match: bool = False
    match_type: str = "no_match"  # fuzzy_high_confidence, fuzzy_combined, manual_review, no_match

    def to_dict(self) -> dict:
        return {
            "title_score": round(self.title_score, 2),
            "year_match": self.year_match,
            "year_score": round(self.year_score, 2),
            "author_score": round(self.author_score, 2),
            "combined_score": round(self.combined_score, 2),
            "is_match": self.is_match,
            "match_type": self.match_type,
        }


# =============================================================
# NORMALIZADORES
# =============================================================

def normalize_for_comparison(text: str) -> str:
    """
    Normalización profunda para comparación fuzzy:
    - minúsculas
    - sin diacríticos
    - solo alfanuméricos y espacios
    - espacios múltiples colapsados
    """
    if not text:
        return ""
    text = str(text).lower().strip()
    text = unidecode(text)
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# Palabras vacías que no aportan información para distinguir títulos académicos
_ACADEMIC_STOPWORDS = frozenset({
    'a', 'an', 'the', 'of', 'in', 'on', 'at', 'to', 'for', 'with',
    'and', 'or', 'but', 'by', 'from', 'as', 'into', 'about',
    'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did',
    'not', 'no', 'nor', 'its', 'their', 'our',
    'this', 'that', 'these', 'those', 'it',
    'between', 'among', 'within', 'through', 'using', 'based', 'via',
    'new', 'use', 'used', 'study', 'analysis', 'review', 'approach',
})


def _get_significant_words(text: str) -> frozenset:
    """
    Extrae palabras significativas de un título (sin stopwords, longitud >= 3).
    Usado para verificar que dos títulos compartan suficiente vocabulario antes
    de calcular el score fuzzy.
    """
    normalized = normalize_for_comparison(text)
    return frozenset(
        w for w in normalized.split()
        if len(w) >= 3 and w not in _ACADEMIC_STOPWORDS
    )


def normalize_author_name(name: str) -> str:
    """
    Normaliza nombre de autor para comparación.
    "García López, Juan Carlos" → "garcia lopez juan carlos"
    """
    if not name:
        return ""
    name = str(name).lower().strip()
    name = unidecode(name)
    name = re.sub(r'[^a-z\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _author_token_set(authors_text: str) -> frozenset:
    """
    Extrae el conjunto de todos los tokens de una cadena de autores.
    Normaliza y descarta puntuación, manteniendo todas las palabras.

    Esto permite comparar "García, Juan" con "Juan García" correctamente
    porque ambas producen el mismo conjunto: {"garcia", "juan"}.

    Ejemplos:
        "García López, Juan"  → frozenset({"garcia", "lopez", "juan"})
        "Juan García López"   → frozenset({"garcia", "lopez", "juan"})
        "Smith J.; López M."  → frozenset({"smith", "j", "lopez", "m"})
    """
    if not authors_text:
        return frozenset()
    normalized = normalize_author_name(authors_text)
    # Eliminar comas y punto y coma residuales tras normalizar
    normalized = re.sub(r'[,;]', ' ', normalized)
    return frozenset(t for t in normalized.split() if t)


def extract_author_surnames(authors_text: str) -> List[str]:
    """
    Extrae apellidos de una cadena de autores.

    NOTA: para comparación robusta prefiere _author_token_set,
    que no asume qué posición ocupa el apellido.
    """
    if not authors_text:
        return []

    normalized = normalize_author_name(authors_text)
    # Dividir solo por punto y coma (cada autor es una entrada)
    parts = re.split(r'\s*;\s*', normalized)

    surnames = []
    for part in parts:
        # Si hay coma, el apellido está antes (formato "Apellido, Nombre")
        if ',' in part:
            surname_part = part.split(',')[0].strip()
        else:
            # Sin coma: tomar la última palabra como apellido (heurística)
            words = part.strip().split()
            surname_part = words[-1] if words else ""
        if surname_part:
            surnames.append(surname_part)

    return surnames


# =============================================================
# COMPARADORES INDIVIDUALES
# =============================================================

def compare_titles(title_a: str, title_b: str) -> float:
    """
    Compara dos títulos normalizados.
    Usa token_sort_ratio para manejar orden diferente de palabras.

    Incluye un guard de solapamiento: si los títulos no comparten suficientes
    palabras significativas, devuelve 0 sin calcular scores fuzzy. Esto evita
    que títulos completamente distintos con alta similitud léxica superficial
    sean considerados candidatos a merge.

    Returns:
        Score de 0 a 100
    """
    if not title_a or not title_b:
        return 0.0

    norm_a = normalize_for_comparison(title_a)
    norm_b = normalize_for_comparison(title_b)

    if not norm_a or not norm_b:
        return 0.0

    # Guard: solapamiento mínimo de palabras significativas
    words_a = _get_significant_words(title_a)
    words_b = _get_significant_words(title_b)
    min_overlap = rc_config.min_title_word_overlap
    if words_a and words_b and len(words_a & words_b) < min_overlap:
        return 0.0

    # Usar múltiples métricas y tomar la mejor
    ratio = fuzz.ratio(norm_a, norm_b)
    token_sort = fuzz.token_sort_ratio(norm_a, norm_b)
    token_set = fuzz.token_set_ratio(norm_a, norm_b)

    # Ponderar: token_sort es más robusto para títulos académicos
    # (las palabras pueden estar en diferente orden entre fuentes)
    score = max(
        ratio * 0.3 + token_sort * 0.5 + token_set * 0.2,
        token_sort  # Si token_sort es muy alto, confiar en él
    )

    return min(score, 100.0)


def compare_years(
    year_a: Optional[int],
    year_b: Optional[int],
    tolerance: int = None,
) -> Tuple[bool, float]:
    """
    Compara años de publicación.

    Returns:
        (coinciden, score)
        - Si coinciden exactamente: (True, 100.0)
        - Si están dentro de tolerancia: (True, score proporcional)
        - Si falta alguno: (True, 50.0) — no penalizar por dato faltante
    """
    tolerance = tolerance if tolerance is not None else rc_config.year_tolerance

    if year_a is None or year_b is None:
        # Si falta el año en alguna fuente, no penalizar
        return True, 50.0

    diff = abs(year_a - year_b)

    if diff == 0:
        return True, 100.0
    elif diff <= tolerance:
        # Score proporcional a la diferencia
        score = 100.0 * (1 - diff / (tolerance + 1))
        return True, score
    else:
        return False, 0.0


def compare_authors(
    authors_a: str,
    authors_b: str,
    orcids_a: Optional[frozenset] = None,
    orcids_b: Optional[frozenset] = None,
) -> float:
    """
    Compara autores bibliográficos con cascada de prioridad:

    1. ORCID (cuando disponible) — identificador único, máxima confianza.
    2. Token-set Jaccard — maneja nombres invertidos:
       "García, Juan" ≡ "Juan García" porque ambos producen el mismo token set.
    3. Fuzzy completo (token_sort_ratio) — fallback para variaciones tipográficas.

    Args:
        authors_a / authors_b : cadenas de autores (separadas por ";")
        orcids_a / orcids_b   : conjuntos de ORCID ya extraídos (opcional).
                                 Si se pasan, se evalúan ANTES del fuzzy.

    Returns:
        Score de 0 a 100.
    """
    # ── 0. Datos vacíos → neutro ──────────────────────────────────────────
    if not authors_a or not authors_b:
        return 50.0

    norm_a = normalize_author_name(authors_a)
    norm_b = normalize_author_name(authors_b)
    if not norm_a or not norm_b:
        return 50.0

    # ── 1. ORCID primero ──────────────────────────────────────────────────
    if orcids_a and orcids_b:
        common = orcids_a & orcids_b
        total = max(len(orcids_a), len(orcids_b))
        orcid_score = (len(common) / total) * 100 if total > 0 else 0.0
        if orcid_score > 0:
            # Al menos un ORCID coincide → retornar score alto directamente.
            # Mezclamos con token fuzzy para no penalizar si faltan ORCIDs
            # en algunos autores del mismo paper.
            token_score = _token_jaccard(authors_a, authors_b)
            return orcid_score * 0.70 + token_score * 0.30

    # ── 2. Token-set Jaccard (maneja nombres invertidos) ──────────────────
    token_score = _token_jaccard(authors_a, authors_b)

    # ── 3. Fuzzy completo con token_sort (maneja reorden de palabras) ─────
    fuzzy_score = float(fuzz.token_sort_ratio(norm_a, norm_b))

    # Combinar: Jaccard domina si hay buena coincidencia, fuzzy actúa de red
    if token_score >= 60:
        return token_score * 0.65 + fuzzy_score * 0.35
    else:
        return token_score * 0.35 + fuzzy_score * 0.65


def _token_jaccard(authors_a: str, authors_b: str) -> float:
    """Similitud Jaccard sobre el conjunto de tokens de ambas cadenas."""
    tokens_a = _author_token_set(authors_a)
    tokens_b = _author_token_set(authors_b)
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    return (intersection / union * 100) if union > 0 else 0.0


# =============================================================
# COMPARACIÓN COMPLETA (SCORE COMBINADO)
# =============================================================

def compare_records(
    title_a: str,
    year_a: Optional[int],
    authors_a: str,
    title_b: str,
    year_b: Optional[int],
    authors_b: str,
) -> FuzzyMatchResult:
    """
    Comparación completa entre dos registros bibliográficos.
    Combina título, año y autores con pesos configurables.

    Args:
        title_a, year_a, authors_a: Datos del registro A
        title_b, year_b, authors_b: Datos del registro B

    Returns:
        FuzzyMatchResult con scores detallados y decisión
    """
    result = FuzzyMatchResult()

    # 1. Comparar títulos
    result.title_score = compare_titles(title_a, title_b)

    # 2. Comparar años
    result.year_match, result.year_score = compare_years(year_a, year_b)

    # 3. Comparar autores
    result.author_score = compare_authors(authors_a, authors_b)

    # REGLA DURA: si el año no coincide y se requiere, rechazar
    if rc_config.year_must_match and not result.year_match:
        result.combined_score = 0
        result.is_match = False
        result.match_type = "no_match"
        return result

    # 4. Score combinado ponderado
    result.combined_score = (
        result.title_score * rc_config.weight_title
        + result.year_score * rc_config.weight_year
        + result.author_score * rc_config.weight_authors
    )

    # 5. Clasificar el resultado
    if result.title_score >= rc_config.title_high_confidence and result.year_match:
        # Título casi idéntico + año coincide → alta confianza
        result.is_match = True
        result.match_type = "fuzzy_high_confidence"

    elif result.combined_score >= rc_config.combined_threshold:
        # Score combinado supera umbral → match
        result.is_match = True
        result.match_type = "fuzzy_combined"

    elif result.combined_score >= rc_config.manual_review_threshold:
        # Zona gris → marcar para revisión manual
        result.is_match = False
        result.match_type = "manual_review"

    else:
        result.is_match = False
        result.match_type = "no_match"

    return result
