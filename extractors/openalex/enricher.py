"""Enriquecedor de publicaciones vía PyAlex / OpenAlex API."""

import logging
import socket
import time
from pathlib import Path
from typing import List

import pyalex
from pyalex import Works

from config import openalex_config, institution
from extractors.base import normalize_doi
from .application.result_mapper import flatten_result
from .application.stages.catalog_search import (
    enrich_by_issn as stage_enrich_by_issn,
    enrich_by_source_name as stage_enrich_by_source_name,
)
from .application.stages.title_search import (
    enrich_by_title as stage_enrich_by_title,
    enrich_by_title_only as stage_enrich_by_title_only,
)
from .domain.matching import (
    best_match,
    best_match_loose,
    normalize_issn,
    normalize_title,
    sanitize_title,
    truncate_title_for_search,
)
from .infrastructure.excel_reader import read_excel_bytes, read_excel_path
from ._rate_limit import OpenAlexRateLimitError, extract_retry_after

logger = logging.getLogger(__name__)


class  OpenAlexEnricher:
    """
    Enriquece un listado de publicaciones consultando OpenAlex a través de PyAlex.

    Acepta un Excel con (al menos) las columnas:
        Título  |  Año  |  doi

    Estrategia de búsqueda por orden de prioridad:
      1. Lote DOI  — hasta MAX_BATCH DOIs por petición (filtro OR).
                     Rápido y exacto. Un solo HTTP request por lote.
      2. Sin DOI   — búsqueda full-text por título + filtro por año.
                     Se elige el candidato con mayor similitud usando
                     RapidFuzz (token_sort_ratio, umbral MIN_SCORE %).

    Uso típico::

        enricher = OpenAlexEnricher()

        # Desde un archivo Excel
        rows = enricher.enrich_from_excel("publicaciones.xlsx")

        # Desde bytes (útil dentro de FastAPI)
        rows = enricher.enrich_from_excel_bytes(file_bytes)

        # Guardar resultado
        enricher.save_to_excel(rows, "resultado_openalex.xlsx")

    Campos OpenAlex añadidos a cada fila (prefijo ``oa_``):
        oa_encontrado, oa_metodo, oa_work_id, oa_titulo, oa_año, oa_doi,
        oa_tipo, oa_revista, oa_issn, oa_editorial, oa_open_access,
        oa_status_oa, oa_citas, oa_idioma, oa_url, oa_autores
    """

    MAX_BATCH      = 25    # DOIs por petición (25 para evitar URLs demasiado largas)
    MIN_SCORE      = 80.0  # umbral score compuesto (título 85% + año 15%) — búsqueda general
    MIN_SCORE_ISSN = 68.0  # umbral permisivo cuando ISSN ya confirma la revista
    MIN_SCORE_SOURCE = 70.0  # umbral para coincidencia por nombre de revista

    # Campos que se piden a OpenAlex (reduce tamaño de respuesta)
    _SELECT = [
        "id", "doi", "title", "publication_year", "type",
        "primary_location", "open_access", "authorships",
        "cited_by_count", "language",
    ]

    def __init__(self, email: str = None):
        try:
            import pyalex as _pyalex
            from pyalex import Works as _Works
            self._pyalex = _pyalex
            self._Works  = _Works
        except ImportError:
            raise ImportError(
                "PyAlex no está instalado.  Ejecuta:  pip install pyalex"
            )

        self.email   = email or institution.contact_email
        self.api_key = openalex_config.api_key

        pyalex.config.email                = self.email
        pyalex.config.api_key              = self.api_key or None
        pyalex.config.max_retries          = 2
        pyalex.config.retry_backoff_factor = 0.5
        # 429 se maneja manualmente — NO dejar que urllib3 espere el Retry-After
        # (puede ser miles de segundos → la app parecería congelada)
        pyalex.config.retry_http_codes     = [500, 502, 503, 504]

        # Timeout global de socket para que las peticiones nunca cuelguen
        socket.setdefaulttimeout(30)

        key_status = f"key={'***' + self.api_key[-4:] if self.api_key else 'no configurada'}"
        logger.info(f"[OpenAlexEnricher] Iniciado — polite pool: {self.email} | {key_status}")

    # ── API pública ──────────────────────────────────────────────────────────

    def enrich_from_excel(self, file_path) -> list[dict]:
        """
        Lee un Excel (.xlsx) por ruta y devuelve las filas enriquecidas.

        Args:
            file_path: str o Path al archivo .xlsx.

        Returns:
            Lista de dicts con los campos originales + campos ``oa_*``.
        """
        rows = self._read_excel_path(Path(file_path))
        return self.enrich(rows)

    def enrich_from_excel_bytes(self, file_bytes: bytes) -> list[dict]:
        """
        Igual que ``enrich_from_excel`` pero acepta los bytes del archivo
        directamente (útil en endpoints FastAPI con UploadFile).
        """
        rows = self._read_excel_bytes(file_bytes)
        return self.enrich(rows)

    def enrich(self, publications: list[dict]) -> list[dict]:
        """
        Enriquece una lista de dicts que contienen al menos:
            - clave ``titulo`` / ``Título`` / ``title``
            - clave ``año``    / ``Año``    / ``year``
            - clave ``doi``    / ``DOI``

        Returns:
            La misma lista con campos ``oa_*`` añadidos a cada dict.
        """
        n = len(publications)
        logger.info(f"[OpenAlexEnricher] Enriqueciendo {n} publicaciones…")

        oa_map: list[dict | None] = [None] * n
        method: list[str | None]  = [None] * n

        # Cache intra-ejecución: evita rellamar a la API si el mismo
        # título+año aparece más de una vez en el input (duplicados).
        # Clave: (title_norm, year)  Valor: (oa_dict | None, method_str | None)
        self._search_cache: dict[tuple, tuple] = {}

        with_doi    = [(i, p) for i, p in enumerate(publications) if self._doi(p)]
        without_doi = [(i, p) for i, p in enumerate(publications) if not self._doi(p)]

        logger.info(
            f"[OpenAlexEnricher] Con DOI: {len(with_doi)} | "
            f"Sin DOI: {len(without_doi)}"
        )

        # 1. Búsqueda por lotes de DOI (precisa y rápida)
        try:
            self._enrich_by_doi_batch(oa_map, method, with_doi)
        except OpenAlexRateLimitError as e:
            logger.error(f"[OpenAlexEnricher] RATE LIMIT ALCANZADO — {e}")
            raise

        # 2. Fallback por título: registros CON doi que el lote no resolvió
        doi_not_found = [(i, p) for i, p in with_doi if oa_map[i] is None]
        if doi_not_found:
            logger.info(
                f"[OpenAlexEnricher] Fallback título para {len(doi_not_found)} "
                f"con DOI no resuelto…"
            )
            try:
                self._enrich_by_title(oa_map, method, doi_not_found)
            except OpenAlexRateLimitError as e:
                logger.error(f"[OpenAlexEnricher] RATE LIMIT en fallback título — {e}")
                raise

        # 3. Búsqueda por título para los que no tienen DOI
        try:
            self._enrich_by_title(oa_map, method, without_doi)
        except OpenAlexRateLimitError as e:
            logger.error(f"[OpenAlexEnricher] RATE LIMIT ALCANZADO durante búsqueda por título — {e}")
            raise

        # 4. Fallback por ISSN + título: para los que siguen sin resolver y tienen ISSN
        still_not_found = [
            (i, p) for i, p in enumerate(publications)
            if oa_map[i] is None and self._issns(p)
        ]
        if still_not_found:
            logger.info(
                f"[OpenAlexEnricher] Fallback ISSN+título para "
                f"{len(still_not_found)} publicaciones sin resolver…"
            )
            try:
                self._enrich_by_issn(oa_map, method, still_not_found)
            except OpenAlexRateLimitError as e:
                logger.error(f"[OpenAlexEnricher] RATE LIMIT en fallback ISSN — {e}")
                raise

        # 5. Fallback por nombre de revista + título: sin ISSN pero con columna 'revista'
        still_not_found2 = [
            (i, p) for i, p in enumerate(publications)
            if oa_map[i] is None and self._revista(p) and not self._issns(p)
        ]
        if still_not_found2:
            logger.info(
                f"[OpenAlexEnricher] Fallback nombre-revista para "
                f"{len(still_not_found2)} publicaciones sin resolver…"
            )
            try:
                self._enrich_by_source_name(oa_map, method, still_not_found2)
            except OpenAlexRateLimitError as e:
                logger.error(f"[OpenAlexEnricher] RATE LIMIT en fallback nombre-revista — {e}")
                raise

        # 6. Último recurso: búsqueda solo por título sin filtro de año
        #    (para los que siguen sin resolverse y tienen título)
        last_resort = [
            (i, p) for i, p in enumerate(publications)
            if oa_map[i] is None and self._title(p)
        ]
        if last_resort:
            logger.info(
                f"[OpenAlexEnricher] Último recurso (solo título) para "
                f"{len(last_resort)} publicaciones…"
            )
            try:
                self._enrich_by_title_only(oa_map, method, last_resort)
            except OpenAlexRateLimitError as e:
                logger.error(f"[OpenAlexEnricher] RATE LIMIT en último recurso — {e}")
                raise

        found = sum(1 for x in oa_map if x is not None)
        logger.info(
            f"[OpenAlexEnricher] Encontrados: {found}/{n} "
            f"({found / n * 100:.1f}%)"
        )

        return [
            self._flatten(pub, oa_map[i], method[i])
            for i, pub in enumerate(publications)
        ]

    def save_to_excel(self, enriched_rows: list[dict], output_path) -> Path:
        """
        Guarda los resultados enriquecidos en un Excel (.xlsx).

        Las columnas originales (Título, Año, doi) van primero; los campos
        OpenAlex (oa_*) van después con encabezado azul diferenciado.

        Args:
            enriched_rows: lista devuelta por ``enrich()`` / ``enrich_from_excel()``.
            output_path:   str o Path donde guardar el .xlsx.

        Returns:
            Path del archivo guardado.
        """
        try:
            import openpyxl
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.utils import get_column_letter
        except ImportError:
            raise ImportError("openpyxl requerido: pip install openpyxl")

        if not enriched_rows:
            raise ValueError("No hay filas para guardar.")

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Enriquecimiento OpenAlex"

        headers = list(enriched_rows[0].keys())

        # ── Fila 1: título del reporte ───────────────────────────────────────
        from datetime import datetime as _dt
        _hdr_fill = PatternFill(fill_type="solid", fgColor="1F4E79")
        _oa_fill  = PatternFill(fill_type="solid", fgColor="16537e")
        _hdr_font = Font(bold=True, color="FFFFFF", size=10)

        title_cell = ws.cell(
            row=1, column=1,
            value=(
                f"Enriquecimiento OpenAlex  —  "
                f"{len(enriched_rows)} publicaciones  —  "
                f"Generado: {_dt.now().strftime('%d/%m/%Y %H:%M')}"
            ),
        )
        title_cell.font  = Font(bold=True, size=12, color="FFFFFF")
        title_cell.fill  = _hdr_fill
        ws.row_dimensions[1].height = 22
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(headers))

        # ── Fila 2: encabezados ─────────────────────────────────────────────
        for ci, h in enumerate(headers, start=1):
            cell = ws.cell(row=2, column=ci, value=h)
            cell.fill      = _oa_fill if h.startswith("oa_") else _hdr_fill
            cell.font      = _hdr_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        ws.row_dimensions[2].height = 28

        # ── Filas de datos ───────────────────────────────────────────────────
        _found_fill  = PatternFill(fill_type="solid", fgColor="EBF3FB")
        _notfnd_fill = PatternFill(fill_type="solid", fgColor="F5F5F5")
        _yes_fill    = PatternFill(fill_type="solid", fgColor="C6EFCE")
        _no_fill     = PatternFill(fill_type="solid", fgColor="FFCCCC")

        for ri, row in enumerate(enriched_rows, start=3):
            found    = row.get("oa_encontrado", False)
            base_fll = _found_fill if found else _notfnd_fill
            for ci, h in enumerate(headers, start=1):
                val  = row.get(h, "")
                if isinstance(val, bool):
                    val = "Sí" if val else "No"
                cell = ws.cell(row=ri, column=ci, value=val if val is not None else "")
                cell.alignment = Alignment(
                    horizontal="left", vertical="center",
                    wrap_text=(h in ("oa_autores", "oa_titulo", "titulo")),
                )
                if h == "oa_encontrado":
                    cell.fill = _yes_fill if found else _no_fill
                    cell.font = Font(bold=True)
                elif h == "oa_open_access":
                    cell.fill = _yes_fill if val == "Sí" else base_fll
                else:
                    cell.fill = base_fll
            ws.row_dimensions[ri].height = 16

        # ── Anchos de columna ────────────────────────────────────────────────
        _widths = {
            "titulo":        50, "oa_titulo":  50, "oa_autores":   46,
            "doi":           38, "oa_doi":     38, "oa_url":       38,
            "oa_work_id":    34, "oa_revista": 34, "oa_editorial": 28,
            "oa_encontrado": 14, "oa_metodo":  14, "oa_tipo":      16,
            "oa_year":        8, "año":         8, "oa_status_oa": 14,
        }
        for ci, h in enumerate(headers, start=1):
            w = _widths.get(h, 18 if h.startswith("oa_") else 14)
            ws.column_dimensions[get_column_letter(ci)].width = w

        ws.freeze_panes = "A3"

        output_path = Path(output_path)
        wb.save(output_path)
        logger.info(f"[OpenAlexEnricher] Excel guardado: {output_path}")
        return output_path

    # ── Internos: búsqueda ───────────────────────────────────────────────────

    def _enrich_by_doi_batch(
        self,
        oa_map:  list,
        method:  list,
        indexed: list[tuple[int, dict]],
    ) -> None:
        """Resuelve DOIs en lotes — una sola petición por lote."""
        import requests as _req

        for batch_start in range(0, len(indexed), self.MAX_BATCH):
            batch = indexed[batch_start : batch_start + self.MAX_BATCH]

            doi_to_idx: dict[str, int] = {}
            doi_full_list: list[str]   = []
            for idx, pub in batch:
                d = self._doi(pub)
                if not d:
                    continue
                full = d if d.startswith("https://") else f"https://doi.org/{d}"
                doi_to_idx[d]   = idx
                doi_full_list.append(full)

            if not doi_full_list:
                continue

            n_lote     = len(doi_full_list)
            lote_label = f"{batch_start + 1}–{batch_start + len(batch)}"
            logger.info(f"[OpenAlexEnricher] Lote DOI {lote_label}: {n_lote} DOIs")

            try:
                query_obj = (
                    self._Works()
                    .select(self._SELECT)
                    .filter_or(doi=doi_full_list)
                )
                url_preview = query_obj.url[:120]
                logger.info(
                    f"[OpenAlexEnricher] → GET {url_preview}"
                    f"{'...' if len(query_obj.url) > 120 else ''}"
                )
                works = query_obj.get(per_page=min(n_lote, 200))
                logger.info(
                    f"[OpenAlexEnricher] ← Lote {lote_label}: "
                    f"{len(works)} works recibidos"
                )
                found_in_batch = 0
                for work in works:
                    w_doi = normalize_doi(str(work.get("doi") or ""))
                    if w_doi in doi_to_idx:
                        i = doi_to_idx[w_doi]
                        oa_map[i] = dict(work)
                        method[i] = "doi"
                        found_in_batch += 1
                logger.info(
                    f"[OpenAlexEnricher] ✓ Lote {lote_label}: "
                    f"{found_in_batch}/{n_lote} coincidencias"
                )
                time.sleep(0.1)

            except OpenAlexRateLimitError:
                raise
            except (socket.timeout, _req.exceptions.Timeout) as exc:
                logger.error(
                    f"[OpenAlexEnricher] TIMEOUT en lote {lote_label} — "
                    f"la API no respondió en 30s ({exc})"
                )
            except _req.exceptions.ConnectionError as exc:
                logger.error(
                    f"[OpenAlexEnricher] ERROR DE CONEXIÓN en lote {lote_label}: {exc}"
                )
            except _req.exceptions.HTTPError as exc:
                retry_after = extract_retry_after(exc)
                if retry_after is not None:
                    raise OpenAlexRateLimitError(retry_after)
                logger.warning(
                    f"[OpenAlexEnricher] HTTP error en lote {lote_label}: {exc}"
                )
            except Exception as exc:
                retry_after = extract_retry_after(exc)
                if retry_after is not None:
                    raise OpenAlexRateLimitError(retry_after)
                logger.warning(
                    f"[OpenAlexEnricher] Lote DOI {lote_label} falló "
                    f"({type(exc).__name__}: {exc}); reintentando individualmente…"
                )
                for full_doi, idx in {
                    (d if d.startswith("https://") else f"https://doi.org/{d}"): i
                    for i, pub in batch
                    for d in [self._doi(pub)] if d
                }.items():
                    try:
                        logger.debug(f"[OpenAlexEnricher]   → DOI individual: {full_doi}")
                        work = self._Works().select(self._SELECT)[full_doi]
                        oa_map[idx] = dict(work)
                        method[idx] = "doi"
                        time.sleep(0.12)
                    except Exception as e2:
                        retry_after2 = extract_retry_after(e2)
                        if retry_after2 is not None:
                            raise OpenAlexRateLimitError(retry_after2)
                        logger.debug(
                            f"[OpenAlexEnricher] DOI individual no encontrado "
                            f"'{full_doi}': {e2}"
                        )

    def _enrich_by_title(
        self,
        oa_map:  list,
        method:  list,
        indexed: list[tuple[int, dict]],
    ) -> None:
        stage_enrich_by_title(self, oa_map, method, indexed, logger)

    def _enrich_by_issn(
        self,
        oa_map:  list,
        method:  list,
        indexed: list[tuple[int, dict]],
    ) -> None:
        stage_enrich_by_issn(self, oa_map, method, indexed, logger)

    def _best_match(
        self,
        query_title: str,
        candidates: list,
        year: int | None = None,
        *,
        min_score: float | None = None,
    ) -> dict | None:
        threshold = min_score if min_score is not None else self.MIN_SCORE
        return best_match(
            query_title,
            candidates,
            year,
            min_score=threshold,
            logger=logger,
        )

    def _enrich_by_source_name(
        self,
        oa_map:  list,
        method:  list,
        indexed: list[tuple[int, dict]],
    ) -> None:
        stage_enrich_by_source_name(self, oa_map, method, indexed, logger)

    def _enrich_by_title_only(
        self,
        oa_map:  list,
        method:  list,
        indexed: list[tuple[int, dict]],
    ) -> None:
        stage_enrich_by_title_only(self, oa_map, method, indexed, logger)

    @classmethod
    def _normalize_title(cls, title: str) -> str:
        return normalize_title(title)

    @staticmethod
    def _sanitize_title(title: str) -> str:
        return sanitize_title(title)

    def _best_match_loose(
        self,
        query_title: str,
        candidates: list,
        min_title_score: float = 78.0,
    ) -> dict | None:
        return best_match_loose(
            query_title,
            candidates,
            min_title_score=min_title_score,
            logger=logger,
        )

    @classmethod
    def _truncate_title_for_search(cls, title: str, max_words: int = 10) -> str:
        return truncate_title_for_search(title, max_words=max_words)

    @staticmethod
    def _normalize_issn(raw: str) -> str:
        return normalize_issn(raw)

    # ── Internos: transformación ─────────────────────────────────────────────

    def _flatten(self, orig: dict, oa: dict | None, met: str | None) -> dict:
        return flatten_result(orig, oa, met)

    # ── Internos: lectura de Excel ───────────────────────────────────────────
    def _read_excel_path(self, path: Path) -> list[dict]:
        return read_excel_path(path, logger)

    def _read_excel_bytes(self, file_bytes: bytes) -> list[dict]:
        return read_excel_bytes(file_bytes, logger)

    # ── Internos: helpers de extracción ─────────────────────────────────────

    def _doi(self, pub: dict) -> str:
        raw = str(
            pub.get("doi") or pub.get("DOI") or pub.get("Doi") or ""
        ).strip()
        return normalize_doi(raw) if raw else ""

    def _title(self, pub: dict) -> str:
        return str(
            pub.get("titulo") or pub.get("Título") or pub.get("título") or
            pub.get("title")  or pub.get("Title")  or ""
        ).strip()

    def _year(self, pub: dict) -> int | None:
        raw = (
            pub.get("año") or pub.get("Año") or
            pub.get("year") or pub.get("Year")
        )
        try:
            return int(raw) if raw else None
        except (ValueError, TypeError):
            return None

    def _issn(self, pub: dict) -> str:
        """Extrae el primer ISSN válido del dict (compatibilidad con código antiguo)."""
        lst = self._issns(pub)
        return lst[0] if lst else ""

    def _issns(self, pub: dict) -> list[str]:
        """
        Extrae todos los ISSNs presentes en el registro.
        El campo puede contener varios separados por ';', ',' o espacio.
        """
        raw = str(pub.get("issn") or "").strip()
        if not raw or raw in ("None", "nan", ""):
            return []
        import re
        parts = re.split(r"[;,\s]+", raw)
        result = []
        for p in parts:
            n = self._normalize_issn(p.strip())
            if n and n not in result:
                result.append(n)
        return result

    def _revista(self, pub: dict) -> str:
        """Extrae el nombre de la revista del registro."""
        raw = str(
            pub.get("revista") or pub.get("Revista") or
            pub.get("journal") or pub.get("Journal") or ""
        ).strip()
        return raw if raw not in ("", "None", "nan") else ""








