import time

from ..._rate_limit import OpenAlexRateLimitError, extract_retry_after


def enrich_by_issn(enricher, oa_map: list, method: list, indexed: list[tuple[int, dict]], logger) -> None:
    total = len(indexed)
    for seq, (idx, pub) in enumerate(indexed, start=1):
        issns = enricher._issns(pub)
        title = enricher._title(pub)
        year = enricher._year(pub)
        if not issns or not title:
            continue
        if seq % 25 == 1:
            logger.info(f"[OpenAlexEnricher] ISSN fallback: {seq}/{total}")
        title_clean = enricher._sanitize_title(title)
        best: dict | None = None
        matched_issn = ""

        for issn_raw in issns:
            if best:
                break
            issn_clean = enricher._normalize_issn(issn_raw)
            if not issn_clean:
                continue
            try:
                q = (
                    enricher._Works()
                    .select(enricher._SELECT)
                    .filter(**{"primary_location.source.issn": issn_clean})
                    .search_filter(title=title_clean)
                )
                if year:
                    q = q.filter(publication_year=year)
                candidates_a = q.get(per_page=10)
                best = enricher._best_match(title, candidates_a, year, min_score=enricher.MIN_SCORE_ISSN)

                candidates_b: list = []
                if best is None and year:
                    q_ny = (
                        enricher._Works()
                        .select(enricher._SELECT)
                        .filter(**{"primary_location.source.issn": issn_clean})
                        .search_filter(title=title_clean)
                    )
                    candidates_b = q_ny.get(per_page=10)
                    best = enricher._best_match(title, candidates_b, year, min_score=enricher.MIN_SCORE_ISSN)
                    if best:
                        time.sleep(0.1)

                if best is None and (candidates_a or candidates_b):
                    pool_ab = list(candidates_a) + list(candidates_b)
                    early_v = enricher._best_match_loose(title, pool_ab)
                    if early_v:
                        oa_map[idx] = early_v
                        method[idx] = "issn_verificar"
                        logger.debug(
                            f"[OpenAlexEnricher] ~ ISSN early exit '{issn_clean}' "
                            f"'{title[:50]}' (pool A+B, C/C_ny omitidos)"
                        )
                        break

                candidates_c: list = []
                if best is None:
                    q_loc = (
                        enricher._Works()
                        .select(enricher._SELECT)
                        .filter(**{"locations.source.issn": issn_clean})
                        .search_filter(title=title_clean)
                    )
                    if year:
                        q_loc = q_loc.filter(publication_year=year)
                    candidates_c = q_loc.get(per_page=10)
                    best = enricher._best_match(title, candidates_c, year, min_score=enricher.MIN_SCORE_ISSN)
                    if best:
                        time.sleep(0.1)

                if best is None:
                    all_issn_seen = list(candidates_a) + list(candidates_b) + list(candidates_c)
                    best_v = enricher._best_match_loose(title, all_issn_seen)
                    if best_v:
                        oa_map[idx] = best_v
                        method[idx] = "issn_verificar"
                        logger.debug(f"[OpenAlexEnricher] ~ ISSN VERIFICAR '{issn_clean}' '{title[:50]}'")
                        break

                if best:
                    matched_issn = issn_clean

                time.sleep(0.12)

            except OpenAlexRateLimitError:
                raise
            except Exception as exc:
                retry_after = extract_retry_after(exc)
                if retry_after is not None:
                    raise OpenAlexRateLimitError(retry_after)
                logger.debug(f"[OpenAlexEnricher] ISSN fallback falló issn={issn_clean} '{title[:50]}': {exc}")

        if best:
            oa_map[idx] = best
            method[idx] = "issn"
            logger.debug(f"[OpenAlexEnricher] ✓ ISSN match '{matched_issn}' '{title[:55]}'")

        time.sleep(0.05)


def enrich_by_source_name(enricher, oa_map: list, method: list, indexed: list[tuple[int, dict]], logger) -> None:
    total = len(indexed)
    for seq, (idx, pub) in enumerate(indexed, start=1):
        revista = enricher._revista(pub)
        title = enricher._title(pub)
        year = enricher._year(pub)
        if not revista or not title:
            continue
        if seq % 25 == 1:
            logger.info(f"[OpenAlexEnricher] Fallback revista: {seq}/{total}")
        title_clean = enricher._sanitize_title(title)
        try:
            q = (
                enricher._Works()
                .select(enricher._SELECT)
                .filter(**{"primary_location.source.display_name.search": revista})
                .search_filter(title=title_clean)
            )
            if year:
                q = q.filter(publication_year=year)
            candidates = q.get(per_page=10)
            best = enricher._best_match(title, candidates, year, min_score=enricher.MIN_SCORE_SOURCE)

            candidates_ny: list = []
            if best is None and year:
                q_ny = (
                    enricher._Works()
                    .select(enricher._SELECT)
                    .filter(**{"primary_location.source.display_name.search": revista})
                    .search_filter(title=title_clean)
                )
                candidates_ny = q_ny.get(per_page=10)
                best = enricher._best_match(title, candidates_ny, year, min_score=enricher.MIN_SCORE_SOURCE)
                if best:
                    time.sleep(0.1)

            if best:
                oa_map[idx] = best
                method[idx] = "revista"
                logger.debug(f"[OpenAlexEnricher] ✓ Revista match '{revista[:40]}' '{title[:50]}'")
            else:
                all_candidates = list(candidates) + list(candidates_ny)
                best_v = enricher._best_match_loose(title, all_candidates)
                if best_v:
                    oa_map[idx] = best_v
                    method[idx] = "revista_verificar"
                    logger.debug(f"[OpenAlexEnricher] ~ Revista VERIFICAR '{revista[:35]}' '{title[:45]}'")

            time.sleep(0.15)

        except OpenAlexRateLimitError:
            raise
        except Exception as exc:
            retry_after = extract_retry_after(exc)
            if retry_after is not None:
                raise OpenAlexRateLimitError(retry_after)
            logger.debug(f"[OpenAlexEnricher] Fallback revista falló revista='{revista[:30]}' '{title[:40]}': {exc}")