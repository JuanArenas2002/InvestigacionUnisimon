"""
Servicio para extraer el h-index de autores desde Scopus.
Procesa múltiples autores en paralelo.
"""

import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import pandas as pd
import requests

from config import scopus_config

logger = logging.getLogger("scopus_h_index")


def _parse_author_entry(entry: object) -> dict:
    """Normaliza la respuesta de author-retrieval-response a un dict plano."""
    if isinstance(entry, list):
        entry = entry[0] if entry else {}
    return entry if isinstance(entry, dict) else {}


def _get_metrics(author_id: str, headers: dict) -> dict:
    """Llama view=METRICS y devuelve h_index + conteos."""
    url = f"{scopus_config.base_url}/author/author_id/{author_id}"
    resp = requests.get(url, headers=headers, timeout=scopus_config.timeout,
                        params={"view": "METRICS"})
    resp.raise_for_status()
    entry = _parse_author_entry(resp.json().get("author-retrieval-response", {}))
    core = entry.get("coredata", {})
    return {
        "h_index":        int(entry.get("h-index", 0) or 0),
        "document_count": int(core.get("document-count", 0) or 0),
        "citation_count": int(core.get("citation-count", 0) or 0),
        "cited_by_count": int(core.get("cited-by-count", 0) or 0),
        "coauthor_count": int(core.get("coauthor-count", 0) or 0),
    }


def _get_profile(author_id: str, headers: dict) -> dict:
    """Llama view=ENHANCED y devuelve nombre, institución, ORCID, áreas, rango."""
    url = f"{scopus_config.base_url}/author/author_id/{author_id}"
    resp = requests.get(url, headers=headers, timeout=scopus_config.timeout,
                        params={"view": "ENHANCED"})
    resp.raise_for_status()
    entry = _parse_author_entry(resp.json().get("author-retrieval-response", {}))
    profile = entry.get("author-profile", {})

    pref = profile.get("preferred-name", {})
    name = f"{pref.get('given-name', '')} {pref.get('surname', '')}".strip() \
           or pref.get("indexed-name", "")

    inst = ""
    aff_list = (profile.get("affiliation-current") or {}).get("affiliation", [])
    if isinstance(aff_list, dict):
        aff_list = [aff_list]
    if aff_list:
        ip = aff_list[0].get("ip-doc", {})
        inst = ip.get("afdispname") or ip.get("sort-name", "")

    areas_raw = entry.get("subject-areas", {}).get("subject-area", [])
    if isinstance(areas_raw, dict):
        areas_raw = [areas_raw]
    areas = list(dict.fromkeys(
        a.get("$", "") or a.get("@abbrev", "")
        for a in areas_raw if isinstance(a, dict)
    ))[:8]

    pub_range = profile.get("publication-range", {})
    orcid = profile.get("orcid", "") or ""

    return {
        "name":      name,
        "inst":      inst,
        "orcid":     orcid,
        "areas":     ", ".join(areas),
        "year_from": pub_range.get("@start", ""),
        "year_to":   pub_range.get("@end", ""),
    }


def get_author_h_index(author_id: str) -> Dict:
    """Obtiene h-index + perfil completo de un autor desde Scopus."""
    if not scopus_config.api_key:
        return {
            "author_id": author_id, "status": "error", "h_index": None,
            "error": "SCOPUS_API_KEY no configurada",
        }

    headers = {
        "Accept": "application/json",
        "X-ELS-APIKey": scopus_config.api_key,
    }
    if scopus_config.inst_token:
        headers["X-ELS-Insttoken"] = scopus_config.inst_token

    try:
        metrics = _get_metrics(author_id, headers)
        profile = _get_profile(author_id, headers)
        return {
            "author_id": author_id,
            "status": "success",
            "error": None,
            **metrics,
            **profile,
        }
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        msg_map = {401: "No autorizado (API key inválida)", 404: "Autor no encontrado",
                   429: "Rate limit alcanzado"}
        return {
            "author_id": author_id, "status": "error", "h_index": None,
            "error": msg_map.get(code, f"Error HTTP {code}"),
        }
    except Exception as exc:
        logger.error(f"Error al obtener h-index para {author_id}: {exc}")
        return {"author_id": author_id, "status": "error", "h_index": None, "error": str(exc)}


class ScopusHIndexService:
    """Procesa múltiples autores y obtiene sus h-index en paralelo."""

    _ID_COLS = {"author_id", "scopus_id", "scopus_author_id", "id"}

    def __init__(self, max_workers: int = 3):
        self.max_workers = max_workers

    @staticmethod
    def _clean_id(val) -> str:
        """Convierte float-IDs (57193767797.0) a string limpio (57193767797)."""
        try:
            return str(int(float(str(val).strip())))
        except (ValueError, OverflowError):
            return str(val).strip()

    def _extract_ids(self, df: "pd.DataFrame") -> List[str]:
        for col in df.columns:
            if col.strip().lower() in self._ID_COLS:
                return [self._clean_id(v) for v in df[col].dropna().unique()]
        return [self._clean_id(v) for v in df.iloc[:, 0].dropna().unique()]

    def process_author_ids(self, file_bytes: bytes) -> List[Dict]:
        df = pd.read_excel(io.BytesIO(file_bytes))
        author_ids = [aid for aid in self._extract_ids(df) if aid]

        if not author_ids:
            raise ValueError("No se encontraron IDs de autores en el archivo")

        logger.info(f"Procesando {len(author_ids)} autores...")

        results: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(get_author_h_index, aid): aid for aid in author_ids}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    aid = futures[future]
                    logger.error(f"Error procesando {aid}: {exc}")
                    results.append({"author_id": aid, "status": "error",
                                    "h_index": None, "error": str(exc)})

        return results
