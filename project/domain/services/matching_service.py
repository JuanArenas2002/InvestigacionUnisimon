from typing import Iterable, List

from reconciliation.fuzzy_matcher import compare_authors, compare_titles, compare_years

from project.domain.models.publication import Publication


class MatchingService:
    """Asigna una clave canonica y score de match a cada publicacion."""

    def match(self, publications: Iterable[Publication]) -> List[Publication]:
        pubs = list(publications)
        for idx, publication in enumerate(pubs):
            publication.canonical_key = publication.identity_key()
            publication.match_type = "identity"
            publication.match_score = 100.0

            if idx == 0:
                continue

            previous = pubs[idx - 1]
            title_score = compare_titles(publication.title or "", previous.title or "")
            year_match, year_score = compare_years(publication.publication_year, previous.publication_year)
            author_score = compare_authors(publication.authors_text or "", previous.authors_text or "")

            if title_score >= 90 and year_match:
                publication.match_type = "fuzzy_candidate"
                publication.match_score = (title_score * 0.55) + (year_score * 0.20) + (author_score * 0.25)
        return pubs
