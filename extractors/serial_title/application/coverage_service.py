"""
Servicio de cobertura de revistas: orquestación de lookup masivo y enriquecimiento.

Separa la lógica de "cómo y en qué orden intentar identificar una revista"
del parseo JSON (dominio) y de los detalles HTTP y caché (infraestructura).

Estrategia de identificación (de más a menos confiable):
  1. EID  → Abstract Retrieval API → source_id → Serial Title API
  2. ISSN/E-ISSN → Serial Title API directamente
  3. ISBN (para series) → Serial Title API
  4. DOI  → Abstract Retrieval API → source_id/ISSN → Serial Title API
  5. Nombre de revista → Serial Title Search (con validación Jaccard ≥ 0.30)

La deduplicación por source_id evita N llamadas al Serial Title API cuando
N artículos pertenecen a la misma revista.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from extractors.serial_title._exceptions import SerialTitleAPIError
from extractors.serial_title.domain.journal_coverage import (
    clean_issn, split_issns, is_issn_format,
    check_year_in_coverage,
)
from extractors.serial_title.infrastructure.disk_cache import _dcache_get, _dcache_set

logger = logging.getLogger(__name__)

# Valores que indican "sin datos válidos" en columnas previas del Excel
_SKIP_VALUES = {"", "sin datos", "no encontrada", "—"}


def _has_prev_data(pub: dict) -> bool:
    """
    Retorna True si la publicación ya tiene datos de cobertura válidos
    de una ejecución anterior (presentes en columnas _prev_* del Excel).

    Se usa para evitar re-consultar revistas que ya fueron procesadas
    en una exportación previa, ahorrando cuota de API.

    Args:
        pub: Dict de una publicación que puede tener columnas _prev_*.

    Returns:
        True si tiene datos previos válidos, False si debe consultarse.
    """
    in_cov = str(pub.get("_prev_in_coverage") or "").strip().lower()
    found  = str(pub.get("_prev_journal_found") or "").strip().lower()
    return in_cov not in _SKIP_VALUES and found in ("sí", "si", "true", "1")


def build_journal_keys(publications: List[dict]) -> Tuple[Dict[str, dict], int]:
    """
    Identifica los journals únicos a consultar a partir de una lista de publicaciones.

    Agrupa las publicaciones por identificador de revista (en orden de prioridad:
    EID → ISSN → ISBN → DOI → nombre) para deduplicar consultas al Serial Title API.

    Args:
        publications: Lista de dicts de publicaciones. Cada dict puede tener:
                      eid, issn, isbn, doi, source_title, y columnas _prev_*.

    Returns:
        Tupla (journal_keys, skipped_prev):
          - journal_keys: Dict {key → {type, value, fallbacks...}} de journals únicos.
          - skipped_prev: Número de publicaciones ya con datos previos (no se consultan).
    """
    journal_keys: Dict[str, dict] = {}
    skipped_prev = 0

    for pub in publications:
        if _has_prev_data(pub):
            skipped_prev += 1
            continue

        issns = split_issns(pub.get("issn", ""))
        isbn  = clean_issn(pub.get("isbn", ""))
        doi   = (pub.get("doi") or "").strip()
        eid   = (pub.get("eid") or "").strip()
        src   = (pub.get("source_title") or "").strip()

        if eid:
            # PRIORIDAD MÁXIMA: EID → Abstract Retrieval → source_id → Serial Title
            key = f"eid:{eid.lower()}"
            if key not in journal_keys:
                journal_keys[key] = {
                    "type":           "eid",
                    "value":          eid,
                    "issn_fallback":  issns[0] if issns else (isbn or ""),
                    "doi_fallback":   doi,
                    "title_fallback": src,
                }
        elif issns:
            for issn in issns:
                key = f"issn:{issn}"
                if key not in journal_keys:
                    journal_keys[key] = {
                        "type":           "issn",
                        "value":          issn,
                        "doi_fallback":   doi,
                        "title_fallback": src,
                    }
                else:
                    # Enriquecer fallbacks de entradas existentes
                    entry = journal_keys[key]
                    if not entry.get("doi_fallback") and doi:
                        entry["doi_fallback"] = doi
                    if not entry.get("title_fallback") and src:
                        entry["title_fallback"] = src
        elif isbn:
            key = f"issn:{isbn}"
            if key not in journal_keys:
                journal_keys[key] = {
                    "type":           "issn",
                    "value":          isbn,
                    "doi_fallback":   doi,
                    "title_fallback": src,
                }
        elif doi:
            key = f"doi:{doi.lower()}"
            if key not in journal_keys:
                journal_keys[key] = {
                    "type":           "doi",
                    "value":          doi,
                    "title_fallback": src,
                }
        elif src:
            # ÚLTIMO RECURSO: búsqueda por nombre de revista
            key = f"title:{src.lower()}"
            if key not in journal_keys:
                journal_keys[key] = {"type": "title", "value": src}

    return journal_keys, skipped_prev


def enrich_publication(pub: dict, journal_cache: Dict[str, dict]) -> dict:
    """
    Enriquece una publicación individual con los datos de cobertura de su revista.

    Busca en journal_cache usando la misma jerarquía de identificadores
    (EID → ISSN → ISBN → DOI → nombre) que se usó en build_journal_keys.
    Aplica check_year_in_coverage del dominio para determinar si el año
    de publicación cae dentro de la cobertura de Scopus.

    Args:
        pub:           Dict de la publicación a enriquecer.
        journal_cache: Dict {key → journal_info} ya consultado.

    Returns:
        Dict de la publicación con campos de cobertura añadidos:
          scopus_journal_title, scopus_publisher, journal_status,
          coverage_from, coverage_to, coverage_periods, journal_found,
          journal_subject_areas, in_coverage, coverage_error, resolved_issn/eissn.
    """
    row = dict(pub)

    # Restaurar datos previos si ya existen
    if _has_prev_data(pub):
        row.update({
            "scopus_journal_title":  pub.get("_prev_scopus_journal_title"),
            "scopus_publisher":      pub.get("_prev_scopus_publisher"),
            "journal_status":        pub.get("_prev_journal_status") or "Unknown",
            "coverage_from":         None,
            "coverage_to":           None,
            "coverage_periods":      [],
            "coverage_periods_str":  pub.get("_prev_coverage_periods_str") or "—",
            "journal_found":         True,
            "journal_subject_areas": None,
            "in_coverage":           pub.get("_prev_in_coverage") or "Sin datos",
            "coverage_error":        None,
        })
        return row

    # Buscar en caché con la misma jerarquía de claves
    issns = split_issns(pub.get("issn", ""))
    isbn  = clean_issn(pub.get("isbn", ""))
    doi   = (pub.get("doi") or "").strip()
    eid   = (pub.get("eid") or "").strip()
    src   = (pub.get("source_title") or "").strip()

    journal_info = None
    if eid:
        journal_info = journal_cache.get(f"eid:{eid.lower()}")
    if journal_info is None and issns:
        for issn in issns:
            candidate = journal_cache.get(f"issn:{issn}")
            if candidate and not candidate.get("error"):
                journal_info = candidate
                break
        if journal_info is None and issns:
            journal_info = journal_cache.get(f"issn:{issns[0]}")
    if journal_info is None and isbn:
        journal_info = journal_cache.get(f"issn:{isbn}")
    if journal_info is None and doi:
        journal_info = journal_cache.get(f"doi:{doi.lower()}")
    if journal_info is None and src:
        journal_info = journal_cache.get(f"title:{src.lower()}")

    if journal_info and not journal_info.get("error"):
        try:
            pub_year = int(pub.get("year") or 0)
        except (ValueError, TypeError):
            pub_year = 0

        in_cov = check_year_in_coverage(
            pub_year=pub_year,
            coverage_periods=journal_info.get("coverage_periods") or [],
            coverage_from=journal_info.get("coverage_from"),
            coverage_to=journal_info.get("coverage_to"),
        )

        had_issn = bool(issns or isbn)
        row.update({
            "scopus_journal_title":   journal_info.get("title"),
            "scopus_publisher":       journal_info.get("publisher"),
            "journal_status":         journal_info.get("status", "Unknown"),
            "coverage_from":          journal_info.get("coverage_from"),
            "coverage_to":            journal_info.get("coverage_to"),
            "coverage_periods":       journal_info.get("coverage_periods", []),
            "journal_found":          True,
            "journal_found_via":      journal_info.get("_found_via", "issn"),
            "coverage_error":         None,
            "journal_subject_areas":  " | ".join(journal_info.get("subject_areas") or []) or None,
            "in_coverage":            in_cov,
            # Consolidar ISSN/E-ISSN resueltos solo si la publicación no los tenía
            "resolved_issn":          journal_info.get("resolved_issn") or "" if not had_issn else "",
            "resolved_eissn":         journal_info.get("resolved_eissn") or "" if not had_issn else "",
        })
    else:
        row.update({
            "scopus_journal_title":  None,
            "scopus_publisher":      None,
            "journal_status":        "No encontrada",
            "coverage_from":         None,
            "coverage_to":           None,
            "coverage_periods":      [],
            "journal_found":         False,
            "journal_subject_areas": None,
            "in_coverage":           "Sin datos",
            "resolved_issn":         "",
            "resolved_eissn":        "",
            "coverage_error": (
                journal_info.get("error") if journal_info
                else "ISSN/título no disponible"
            ),
        })

    return row
