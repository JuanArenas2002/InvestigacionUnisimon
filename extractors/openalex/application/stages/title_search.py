import socket
import time

from ..._rate_limit import OpenAlexRateLimitError, extract_retry_after


def enrich_by_title(enricher, oa_map: list, method: list, indexed: list[tuple[int, dict]], logger) -> None:
    total = len(indexed)
    for seq, (idx, pub) in enumerate(indexed, start=1):
        title = enricher._title(pub)
        year = enricher._year(pub)
        if not title:
            logger.debug(f"[OpenAlexEnricher] Título vacío en fila {idx}, saltando")
            continue
        if seq % 25 == 1:
            logger.info(
                f"[OpenAlexEnricher] Por título: {seq}/{total} "
                f"(encontrados: {sum(1 for m in method if m and 'titulo' in m)})"
            )
        try:
            title_norm = enricher._normalize_title(title)
            title_clean = enricher._sanitize_title(title)
            title_variants = enricher._title_query_variants(title)
            primary_query_title = title_variants[0] if title_variants else title_clean
            secondary_query_title = title_variants[1] if len(title_variants) > 1 else None

            cache_key = (title_norm, year)
            if hasattr(enricher, "_search_cache") and cache_key in enricher._search_cache:
                cached_oa, cached_method = enricher._search_cache[cache_key]
                if cached_oa is not None:
                    oa_map[idx] = cached_oa
                    method[idx] = cached_method
                    logger.debug(f"[OpenAlexEnricher]   ✓ Cache hit '{title[:55]}'")
                continue

            all_seen: list = []
            query = enricher._Works().select(enricher._SELECT).search_filter(title=primary_query_title)
            if year:
                query = query.filter(publication_year=year)
            candidates = query.get(per_page=15)
            all_seen.extend(candidates)
            best = enricher._best_match(title, candidates, year)

            # Variante 1b: mismo título con estrategia guion/espacio alterna
            if best is None and secondary_query_title:
                q1b = enricher._Works().select(enricher._SELECT).search_filter(title=secondary_query_title)
                if year:
                    q1b = q1b.filter(publication_year=year)
                candidates1b = q1b.get(per_page=15)
                all_seen.extend(candidates1b)
                best = enricher._best_match(title, candidates1b, year)
                if best:
                    time.sleep(0.1)

            if best is None and title_norm != title_clean:
                query2 = enricher._Works().select(enricher._SELECT).search_filter(title=title_norm)
                if year:
                    query2 = query2.filter(publication_year=year)
                candidates2 = query2.get(per_page=15)
                all_seen.extend(candidates2)
                best = enricher._best_match(title, candidates2, year)
                if best:
                    time.sleep(0.1)

            if best is None and all_seen:
                early = enricher._best_match_loose(title, all_seen, min_title_score=88.0)
                if early:
                    has_doi_early = bool(enricher._doi(pub))
                    oa_map[idx] = early
                    method[idx] = "titulo_fallback_doi_verificar" if has_doi_early else "titulo_verificar"
                    logger.debug(
                        f"[OpenAlexEnricher]   ~ Early exit '{title[:55]}' "
                        f"(título ≥88% en pool R1/R2, R3+R4 omitidos)"
                    )
                    time.sleep(0.1)
                    continue

            candidates3: list = []
            if best is None:
                q3 = enricher._Works().select(enricher._SELECT).search(primary_query_title)
                if year:
                    q3 = q3.filter(publication_year=year)
                candidates3 = q3.get(per_page=15)
                all_seen.extend(candidates3)
                best = enricher._best_match(title, candidates3, year)
                if best:
                    time.sleep(0.1)

            if best is None and year and candidates3:
                candidates4 = enricher._Works().select(enricher._SELECT).search(primary_query_title).get(per_page=15)
                all_seen.extend(candidates4)
                best = enricher._best_match(title, candidates4, year)
                if best:
                    time.sleep(0.1)

            # Fallback final: bigramas de términos clave (ej. "universidad empresa")
            if best is None:
                for bg in enricher._title_bigrams_for_search(title, max_bigrams=3):
                    q_bg = enricher._Works().select(enricher._SELECT).search_filter(title=bg)
                    if year:
                        q_bg = q_bg.filter(publication_year=year)
                    candidates_bg = q_bg.get(per_page=25)
                    all_seen.extend(candidates_bg)
                    best = enricher._best_match(title, candidates_bg, year)

                    # Si el año del Excel está desfasado, intenta el mismo bigrama sin año.
                    if best is None and year:
                        candidates_bg_ny = (
                            enricher._Works()
                            .select(enricher._SELECT)
                            .search_filter(title=bg)
                            .get(per_page=25)
                        )
                        all_seen.extend(candidates_bg_ny)
                        best = enricher._best_match(title, candidates_bg_ny, year)

                    if best:
                        logger.debug(f"[OpenAlexEnricher]   ✓ Bigram fallback '{bg}'")
                        time.sleep(0.1)
                        break

            has_doi = bool(enricher._doi(pub))
            if best:
                oa_map[idx] = best
                method[idx] = "titulo_fallback_doi" if has_doi else "titulo"
                logger.debug(
                    f"[OpenAlexEnricher]   ✓ '{title[:60]}' → encontrado "
                    f"({'fallback' if has_doi else 'sin doi'})"
                )
            else:
                best_v = enricher._best_match_loose(title, all_seen)
                if best_v:
                    oa_map[idx] = best_v
                    method[idx] = "titulo_fallback_doi_verificar" if has_doi else "titulo_verificar"
                    logger.debug(
                        f"[OpenAlexEnricher]   ~ '{title[:60]}' → VERIFICAR "
                        f"(título similar, datos difieren)"
                    )

            if hasattr(enricher, "_search_cache"):
                enricher._search_cache[cache_key] = (oa_map[idx], method[idx])

            time.sleep(0.15)

        except OpenAlexRateLimitError:
            raise
        except (socket.timeout, Exception) as exc:
            retry_after = extract_retry_after(exc)
            if retry_after is not None:
                raise OpenAlexRateLimitError(retry_after)
            logger.debug(
                f"[OpenAlexEnricher] Búsqueda título falló "
                f"'{title[:50]}': {type(exc).__name__}: {exc}"
            )


def enrich_by_title_only(enricher, oa_map: list, method: list, indexed: list[tuple[int, dict]], logger) -> None:
    total = len(indexed)
    for seq, (idx, pub) in enumerate(indexed, start=1):
        title = enricher._title(pub)
        year = enricher._year(pub)
        if not title:
            continue
        if seq % 25 == 1:
            logger.info(f"[OpenAlexEnricher] Último recurso (solo título): {seq}/{total}")
        title_norm = enricher._normalize_title(title)
        title_clean = enricher._sanitize_title(title)
        title_variants = enricher._title_query_variants(title)
        primary_query_title = title_variants[0] if title_variants else title_clean
        secondary_query_title = title_variants[1] if len(title_variants) > 1 else None
        title_short = enricher._truncate_title_for_search(title)
        try:
            candidates = enricher._Works().select(enricher._SELECT).search_filter(title=primary_query_title).get(per_page=15)
            best = enricher._best_match(title, candidates, year, min_score=88.0)

            if best is None and secondary_query_title:
                candidates_alt = enricher._Works().select(enricher._SELECT).search_filter(title=secondary_query_title).get(per_page=15)
                candidates = list(candidates) + list(candidates_alt)
                best = enricher._best_match(title, candidates_alt, year, min_score=88.0)

            candidates2: list = []
            if best is None and title_short and title_short != title_norm and len(candidates) < 8:
                candidates2 = enricher._Works().select(enricher._SELECT).search_filter(title=title_short).get(per_page=15)
                best = enricher._best_match(title, candidates2, year, min_score=88.0)

            candidates3: list = []
            if best is None:
                candidates3 = enricher._Works().select(enricher._SELECT).search(primary_query_title).get(per_page=15)
                best = enricher._best_match(title, candidates3, year, min_score=88.0)

            candidates_bg_all: list = []
            if best is None:
                for bg in enricher._title_bigrams_for_search(title, max_bigrams=3):
                    q_bg = enricher._Works().select(enricher._SELECT).search_filter(title=bg)
                    if year:
                        q_bg = q_bg.filter(publication_year=year)
                    c_bg = q_bg.get(per_page=25)
                    candidates_bg_all.extend(c_bg)
                    best = enricher._best_match(title, c_bg, year, min_score=88.0)

                    if best is None and year:
                        c_bg_ny = (
                            enricher._Works()
                            .select(enricher._SELECT)
                            .search_filter(title=bg)
                            .get(per_page=25)
                        )
                        candidates_bg_all.extend(c_bg_ny)
                        best = enricher._best_match(title, c_bg_ny, year, min_score=88.0)

                    if best:
                        logger.debug(f"[OpenAlexEnricher] ✓ Último recurso bigrama '{bg}'")
                        break

            if best:
                oa_map[idx] = best
                method[idx] = "titulo_solo"
                logger.debug(f"[OpenAlexEnricher] ✓ Último recurso '{title[:60]}'")
            else:
                all_candidates = list(candidates) + list(candidates2) + list(candidates3) + list(candidates_bg_all)
                best_v = enricher._best_match_loose(title, all_candidates)
                if best_v:
                    oa_map[idx] = best_v
                    method[idx] = "titulo_solo_verificar"
                    logger.debug(f"[OpenAlexEnricher] ~ Último recurso VERIFICAR '{title[:55]}'")

            time.sleep(0.2)

        except OpenAlexRateLimitError:
            raise
        except Exception as exc:
            retry_after = extract_retry_after(exc)
            if retry_after is not None:
                raise OpenAlexRateLimitError(retry_after)
            logger.debug(f"[OpenAlexEnricher] Último recurso falló '{title[:50]}': {exc}")