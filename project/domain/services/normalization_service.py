from typing import Iterable, List

from shared.normalizers import normalize_text, normalize_year

from project.domain.models.publication import Publication
from project.domain.value_objects.doi import DOI


class NormalizationService:
    """Normaliza metadatos para comparacion y persistencia consistente."""

    def normalize_publication(self, publication: Publication) -> Publication:
        doi_vo = DOI.parse(publication.doi)
        publication.doi = doi_vo.value if doi_vo else None
        publication.publication_year = normalize_year(publication.publication_year)
        publication.normalized_title = normalize_text(publication.title or "") or None

        author_names = [a.name.strip() for a in publication.authors if a.name and a.name.strip()]
        publication.authors_text = "; ".join(author_names) if author_names else None
        publication.normalized_authors = (
            normalize_text(publication.authors_text) if publication.authors_text else None
        )
        return publication

    def normalize_batch(self, publications: Iterable[Publication]) -> List[Publication]:
        return [self.normalize_publication(pub) for pub in publications]
