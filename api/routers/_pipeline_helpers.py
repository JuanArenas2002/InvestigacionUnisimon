"""
Funciones auxiliares del pipeline de cobertura Scopus.

Se separan aquí para ser compartidas entre los dos endpoints:
  - /scopus/check-publications-coverage   (primer análisis completo)
  - /scopus/reprocess-coverage            (re-procesar publicaciones fallidas)
"""
import logging

logger = logging.getLogger("pipeline")


def _enrich_discontinued_with_openalex(rows_local: list[dict]) -> None:
    """Adjunta datos de openalex_records (por DOI) a filas con revistas descontinuadas."""
    from db.session import get_session
    from db.models import OpenalexRecord as _OARecord

    _DISC = {"discontinued", "inactive", "inactiva"}

    def _ndoi(d: str) -> str:
        d = (d or "").strip().lower()
        d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
        return d.split()[0] if d else ""

    disc_dois = list({
        _ndoi(str(row.get("__doi") or ""))
        for row in rows_local
        if str(row.get("journal_status", "")).strip().lower() in _DISC
        and row.get("__doi")
    })
    disc_dois = [d for d in disc_dois if d]

    if not disc_dois:
        logger.info("[check-coverage] OpenAlex cruce: ningún DOI descontinuado para consultar.")
        return

    session = get_session()
    try:
        oa_records = session.query(_OARecord).filter(
            _OARecord.doi.in_(disc_dois)
        ).all()
        openalex_map: dict[str, dict] = {}
        for rec in oa_records:
            key = _ndoi(rec.doi or "")
            if key:
                openalex_map[key] = {
                    "oa_work_id":     rec.openalex_work_id or "",
                    "oa_title":       rec.title or "",
                    "oa_year":        rec.publication_year,
                    "oa_authors":     rec.authors_text or "",
                    "oa_journal":     rec.source_journal or "",
                    "oa_issn":        rec.issn or "",
                    "oa_open_access": (
                        "Sí" if rec.is_open_access is True
                        else ("No" if rec.is_open_access is False else "")
                    ),
                    "oa_oa_status":   rec.oa_status or "",
                    "oa_citations":   rec.citation_count if rec.citation_count is not None else 0,
                    "oa_url":         rec.url or "",
                }
        logger.info(
            f"[check-coverage] OpenAlex cruce: {len(disc_dois)} DOIs descontinuados "
            f"→ {len(openalex_map)} coincidencias en BD"
        )
        for row in rows_local:
            key = _ndoi(str(row.get("__doi") or ""))
            if key in openalex_map:
                row["_openalex"] = openalex_map[key]
    except Exception as exc:
        logger.warning(f"[check-coverage] Cruce OpenAlex BD falló: {exc}")
    finally:
        session.close()


def _rescue_not_found_via_openalex(rows_local: list[dict], extractor_inst) -> None:
    """
    Para cada fila con journal_found=False y DOI:
      1. Busca el registro en openalex_records (BD local) por DOI.
      2. Para DOIs que la BD no tiene → llama OpenAlex API directamente por DOI
         (GET /works/https://doi.org/{doi}) sin guardar en BD.
      3. Usa el ISSN o source_journal que OpenAlex tiene registrado.
      4. Re-consulta Scopus Serial Title API con ese dato.
      5. Si lo encuentra, actualiza la fila con toda la info de cobertura.
    """
    from db.session import get_session
    from db.models import OpenalexRecord as _OARecord
    from extractors.serial_title import SerialTitleAPIError as _STErr
    from config import institution as _inst
    import re as _re
    import pyalex as _pyalex
    from pyalex import Works as _Works
    _pyalex.config.email           = getattr(_inst, "contact_email", "") or "api@openalex.org"
    _pyalex.config.max_retries     = 3
    _pyalex.config.retry_backoff_factor = 0.5

    def _ndoi(d: str) -> str:
        d = (d or "").strip().lower()
        d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
        return d.split()[0] if d else ""

    def _clean_issn(s: str) -> str:
        return _re.sub(r"[^0-9Xx]", "", (s or "")).upper()

    def _compute_in_coverage(pub_year: int, journal_info: dict) -> str:
        """Replica la lógica de check_publications_coverage para ¿En cobertura?"""
        import datetime as _dt
        _cy = _dt.datetime.now().year
        periods: list = journal_info.get("coverage_periods") or []
        cf = journal_info.get("coverage_from")
        ct = journal_info.get("coverage_to")
        # Si la revista sigue activa (coverage_to reciente), Scopus puede
        # estar 1-2 años atrás en sus datos. Extender el techo efectivo.
        _ect = max(ct, _cy) if (ct and ct >= _cy - 2) else ct
        if not pub_year:
            return "Sin datos"
        if pub_year and periods:
            _last = periods[-1][1]
            _eff_last = max(_last, _cy) if _last >= _cy - 2 else _last
            if any(s <= pub_year <= e for s, e in periods) or (_last < pub_year <= _eff_last):
                return "Sí"
            elif pub_year < periods[0][0]:
                return "No (antes de cobertura)"
            elif pub_year > _eff_last:
                return "No (después de cobertura)"
            else:
                return "No (laguna de cobertura)"
        elif cf and _ect:
            if cf <= pub_year <= _ect:
                return "Sí"
            elif pub_year < cf:
                return "No (antes de cobertura)"
            return "No (después de cobertura)"
        elif cf:
            return "Sí" if pub_year >= cf else "No (antes de cobertura)"
        return "Sin datos"

    # 1. Recoger DOIs de filas no encontradas
    not_found_rows = [
        r for r in rows_local
        if not r.get("journal_found") and r.get("__doi")
    ]
    if not not_found_rows:
        logger.info("[rescue-oa] No hay filas sin encontrar con DOI. Nada que rescatar.")
        return

    dois_needed = list({_ndoi(str(r.get("__doi") or "")) for r in not_found_rows})
    dois_needed = [d for d in dois_needed if d]
    logger.info(
        f"[rescue-oa] {len(not_found_rows)} filas sin encontrar → "
        f"consultando {len(dois_needed)} DOIs en OpenAlex BD..."
    )

    # 2. Consultar BD OpenAlex
    session = get_session()
    try:
        oa_records = session.query(_OARecord).filter(
            _OARecord.doi.in_(dois_needed)
        ).all()
    except Exception as exc:
        logger.warning(f"[rescue-oa] Error consultando BD OpenAlex: {exc}")
        return
    finally:
        session.close()

    # Construir mapa doi → (issn, source_journal)
    oa_map: dict[str, tuple[str, str]] = {}
    for rec in oa_records:
        key = _ndoi(rec.doi or "")
        if key:
            oa_map[key] = (rec.issn or "", rec.source_journal or "")

    logger.info(
        f"[rescue-oa] OpenAlex BD devolvió {len(oa_map)} registros de {len(dois_needed)} buscados."
    )

    # 2b. Para DOIs que la BD no tiene → consultar OpenAlex API vía PyAlex.
    #     Works()[doi_url] hace GET /works/{doi_url} y maneja retry/rate-limit.
    dois_missing_from_db = [d for d in dois_needed if d not in oa_map]

    if dois_missing_from_db:
        logger.info(
            f"[rescue-oa] {len(dois_missing_from_db)} DOIs no están en BD → "
            f"consultando OpenAlex API (PyAlex)..."
        )
        for doi_key in dois_missing_from_db:
            try:
                doi_url = f"https://doi.org/{doi_key}"
                work = _Works()[doi_url]
                primary_loc = work.get("primary_location") or {}
                source      = primary_loc.get("source") or {}
                oa_issn     = source.get("issn_l") or ""
                oa_journal  = source.get("display_name") or ""
                if not oa_issn:
                    issn_list = source.get("issn") or []
                    oa_issn = issn_list[0] if issn_list else ""
                oa_map[doi_key] = (oa_issn, oa_journal)
                logger.debug(
                    f"[rescue-oa] DOI {doi_key} → issn={oa_issn!r} revista={oa_journal!r}"
                )
            except Exception as exc:
                logger.debug(f"[rescue-oa] DOI {doi_key} → no encontrado en OpenAlex: {exc}")
                continue

        n_api_hits = sum(1 for d in dois_missing_from_db if d in oa_map)
        logger.info(
            f"[rescue-oa] OpenAlex API: {n_api_hits}/{len(dois_missing_from_db)} DOIs resueltos."
        )

    # 3. Para cada doi con datos de OpenAlex (BD + API), re-intentar Scopus Serial Title
    # ── Pre-resolución con deduplicación y caché de disco ────────────────────
    # Recolectar journals únicos a consultar antes de iterar sobre las filas.
    # Deduplica por ISSN (no por tuple ISSN+título) para minimizar llamadas.
    # Aprovecha la caché en disco del paso 3 (ya consultados = hit instantáneo).
    from extractors.serial_title import _dcache_get as _st_dcache_get, _dcache_set as _st_dcache_set

    # clave → journal_info  (None = consultado pero no encontrado)
    _rescue_jcache: dict[str, dict | None] = {}

    def _rescue_resolve_issn(issn: str) -> "dict | None":
        key = f"issn:{issn}"
        if key in _rescue_jcache:
            return _rescue_jcache[key]
        cached = _st_dcache_get(key)
        if cached is not None:
            _rescue_jcache[key] = cached
            return cached
        try:
            res = extractor_inst.get_journal_coverage(issn)
            if not res.get("error"):
                _st_dcache_set(key, res)
                _rescue_jcache[key] = res
                return res
        except _STErr:
            pass
        _rescue_jcache[key] = None
        return None

    def _rescue_resolve_title(title: str) -> "dict | None":
        key = f"title:{title.lower()}"
        if key in _rescue_jcache:
            return _rescue_jcache[key]
        cached = _st_dcache_get(key)
        if cached is not None:
            _rescue_jcache[key] = cached
            return cached
        try:
            res = extractor_inst.search_journal_by_title(title)
            if not res.get("error"):
                _st_dcache_set(key, res)
                _rescue_jcache[key] = res
                return res
        except _STErr:
            pass
        _rescue_jcache[key] = None
        return None

    # Deduplicar por ISSN único (no por tuple issn+título)
    _unique_issns: set[str] = set()
    _unique_titles: set[str] = set()   # solo cuando no hay ISSN válido
    for _r in not_found_rows:
        _dk = _ndoi(str(_r.get("__doi") or ""))
        if _dk not in oa_map:
            continue
        _ui = _clean_issn(oa_map[_dk][0])
        _ut = (oa_map[_dk][1] or "").strip()
        if _ui and len(_ui) >= 7:
            # Solo ISSN — los títulos se usarán como fallback en el per-row loop
            # solo si el ISSN falla; no los pre-resolvemos aquí para evitar
            # duplicar ~N llamadas a la API por cada ISSN ya cubierto.
            _unique_issns.add(_ui)
        elif _ut:
            # Sin ISSN válido → único camino es buscar por título
            _unique_titles.add(_ut)

    logger.info(
        f"[rescue-oa] Pre-resolviendo {len(_unique_issns)} ISSNs únicos + "
        f"{len(_unique_titles)} títulos únicos (de {len(not_found_rows)} filas)..."
    )
    _done = 0
    for _ui in _unique_issns:
        _rescue_resolve_issn(_ui)
        _done += 1
        if _done % 10 == 0 or _done == len(_unique_issns):
            logger.info(f"[rescue-oa] Pre-resolución ISSNs: {_done}/{len(_unique_issns)}")
    for _ut in _unique_titles:
        _rescue_resolve_title(_ut)
    logger.info(f"[rescue-oa] Pre-resolución completa. Iniciando actualización de filas...")

    rescued = 0
    for row in not_found_rows:
        doi_key = _ndoi(str(row.get("__doi") or ""))
        if doi_key not in oa_map:
            continue

        oa_issn, oa_src_journal = oa_map[doi_key]
        journal_info: dict | None = None
        used_via = ""

        # Intento A: ISSN de OpenAlex (desde caché pre-construida)
        issn_clean = _clean_issn(oa_issn)
        if issn_clean and len(issn_clean) >= 7:
            res = _rescue_resolve_issn(issn_clean)
            if res:
                journal_info = res
                used_via = "openalex→scopus(issn)"
                logger.debug(
                    f"[rescue-oa] DOI={doi_key}: encontrado vía ISSN OpenAlex ({issn_clean})"
                )

        # Intento B: nombre de revista de OpenAlex (desde caché pre-construida)
        if journal_info is None and oa_src_journal.strip():
            res = _rescue_resolve_title(oa_src_journal.strip())
            if res:
                journal_info = res
                used_via = "openalex→scopus(título)"
                logger.debug(
                    f"[rescue-oa] DOI={doi_key}: encontrado vía título OpenAlex ('{oa_src_journal}')"
                )

        if journal_info is None:
            continue

        # 4. Actualizar la fila con la info de cobertura recién obtenida
        try:
            pub_year = int(row.get("__year") or 0)
        except (ValueError, TypeError):
            pub_year = 0

        _areas = journal_info.get("subject_areas") or []
        _areas_str = " | ".join(_areas) if isinstance(_areas, list) else str(_areas or "")

        row["journal_found"]         = True
        row["journal_found_via"]     = used_via
        row["scopus_journal_title"]  = journal_info.get("title") or oa_src_journal
        row["scopus_publisher"]      = journal_info.get("publisher") or ""
        row["journal_status"]        = journal_info.get("status") or ""
        row["coverage_from"]         = journal_info.get("coverage_from")
        row["coverage_to"]           = journal_info.get("coverage_to")
        row["coverage_periods"]      = journal_info.get("coverage_periods") or []
        row["journal_subject_areas"] = _areas_str
        row["resolved_issn"]         = journal_info.get("resolved_issn") or oa_issn
        row["resolved_eissn"]        = journal_info.get("resolved_eissn") or ""
        row["in_coverage"]           = _compute_in_coverage(pub_year, journal_info)
        row["_openalex"] = {
            "oa_work_id":     "",
            "oa_title":       row.get("__title") or "",
            "oa_year":        pub_year,
            "oa_authors":     "",
            "oa_journal":     oa_src_journal,
            "oa_issn":        oa_issn,
            "oa_open_access": "",
            "oa_oa_status":   "",
            "oa_citations":   0,
            "oa_url":         "",
        }
        rescued += 1

    logger.info(
        f"[rescue-oa] Rescatadas {rescued}/{len(not_found_rows)} publicaciones "
        f"usando datos OpenAlex → Scopus Serial Title."
    )