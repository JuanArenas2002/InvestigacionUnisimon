from typing import Iterable, List, Optional

from reconciliation.fuzzy_matcher import compare_authors, compare_titles, compare_years

from project.domain.models.author import Author
from project.domain.models.publication import Publication


class MatchingService:
    """
    Asigna clave canonica y score de match a cada publicacion.

    Cascada de comparacion de autores:
      1. ORCID exacto (maxima confianza, sin ambiguedad)
      2. Token-set Jaccard (nombres invertidos / variantes de formato)
      3. Fuzzy de cadena completa (fallback tipografico)
    """

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
            year_match, year_score = compare_years(
                publication.publication_year, previous.publication_year
            )
            author_score = self._compare_authors(publication, previous)

            if title_score >= 90 and year_match:
                publication.match_type = "fuzzy_candidate"
                publication.match_score = (
                    title_score * 0.55
                    + year_score * 0.20
                    + author_score * 0.25
                )
        return pubs

    @staticmethod
    def _compare_authors(pub_a: Publication, pub_b: Publication) -> float:
        """
        Compara los autores de dos publicaciones.

        Prioridad:
          1. Conjuntos de ORCID (si al menos una publicacion tiene ORCIDs)
          2. Fuzzy de authors_text con token-set Jaccard (nombres invertidos OK)
        """
        orcids_a = frozenset(a.orcid for a in pub_a.authors if a.orcid)
        orcids_b = frozenset(a.orcid for a in pub_b.authors if a.orcid)

        return compare_authors(
            pub_a.authors_text or "",
            pub_b.authors_text or "",
            orcids_a=orcids_a or None,
            orcids_b=orcids_b or None,
        )
