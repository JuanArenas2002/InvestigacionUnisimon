"""
Tests para servicios de dominio: DeduplicationService, NormalizationService, MatchingService.

Ejecutar:
    pytest tests/project/test_domain_services.py -v

Sin dependencias externas — prueba la logica pura de dominio.
"""

import pytest

from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.domain.services.deduplication_service import DeduplicationService
from project.domain.services.normalization_service import NormalizationService


# ──────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────────────────────────────────────


def make_pub(
    source_name: str = "test",
    doi: str | None = None,
    title: str = "Sample Publication",
    year: int | None = 2023,
    authors: list[Author] | None = None,
    source_id: str | None = None,
) -> Publication:
    return Publication(
        source_name=source_name,
        source_id=source_id,
        doi=doi,
        title=title,
        publication_year=year,
        authors=authors or [],
    )


# ──────────────────────────────────────────────────────────────────────────────
# DEDUPLICATION SERVICE
# ──────────────────────────────────────────────────────────────────────────────


class TestDeduplicationService:
    def setup_method(self):
        self.svc = DeduplicationService()

    def test_empty_list(self):
        assert self.svc.deduplicate([]) == []

    def test_single_item(self):
        pub = make_pub(doi="10.1234/test")
        result = self.svc.deduplicate([pub])
        assert len(result) == 1

    def test_removes_doi_duplicate(self):
        pub1 = make_pub(doi="10.1234/test", source_id="A")
        pub2 = make_pub(doi="10.1234/test", source_id="B")
        result = self.svc.deduplicate([pub1, pub2])
        assert len(result) == 1

    def test_keeps_different_dois(self):
        pub1 = make_pub(doi="10.1234/a")
        pub2 = make_pub(doi="10.1234/b")
        result = self.svc.deduplicate([pub1, pub2])
        assert len(result) == 2

    def test_selects_record_with_more_authors(self):
        authors = [Author(name="Juan Garcia"), Author(name="Maria Lopez")]
        pub_poor = make_pub(doi="10.1234/test", source_id="A")
        pub_rich = make_pub(doi="10.1234/test", source_id="B", authors=authors)
        result = self.svc.deduplicate([pub_poor, pub_rich])
        assert len(result) == 1
        assert len(result[0].authors) == 2

    def test_title_year_dedup_without_doi(self):
        pub1 = make_pub(title="Machine Learning Review", year=2022)
        pub2 = make_pub(title="Machine Learning Review", year=2022)
        # Identity key usa title normalizado — necesitamos normalizar primero
        # Con normalized_title None usa title raw
        result = self.svc.deduplicate([pub1, pub2])
        # Ambas tienen la misma identity_key -> se deduplicaran
        assert len(result) == 1

    def test_different_years_not_dedup(self):
        pub1 = make_pub(title="Machine Learning Review", year=2021)
        pub2 = make_pub(title="Machine Learning Review", year=2022)
        result = self.svc.deduplicate([pub1, pub2])
        assert len(result) == 2

    def test_preserves_all_unique(self):
        pubs = [
            make_pub(doi=f"10.1234/{i}", title=f"Paper {i}")
            for i in range(10)
        ]
        result = self.svc.deduplicate(pubs)
        assert len(result) == 10


# ──────────────────────────────────────────────────────────────────────────────
# NORMALIZATION SERVICE
# ──────────────────────────────────────────────────────────────────────────────


class TestNormalizationService:
    def setup_method(self):
        self.svc = NormalizationService()

    def test_normalizes_doi(self):
        pub = make_pub(doi="https://doi.org/10.1234/test")
        result = self.svc.normalize_publication(pub)
        # DOI normalizado no debe incluir el prefijo URL
        assert result.doi is not None
        assert "doi.org" not in result.doi

    def test_doi_none_stays_none(self):
        pub = make_pub(doi=None)
        result = self.svc.normalize_publication(pub)
        assert result.doi is None

    def test_normalizes_title(self):
        pub = make_pub(title="  The EFFECTS of Climate Change  ")
        result = self.svc.normalize_publication(pub)
        assert result.normalized_title is not None
        assert result.normalized_title == result.normalized_title.lower()

    def test_authors_text_joined(self):
        authors = [
            Author(name="Garcia, J."),
            Author(name="Lopez, M."),
            Author(name="Torres, A."),
        ]
        pub = make_pub(authors=authors)
        result = self.svc.normalize_publication(pub)
        assert result.authors_text == "Garcia, J.; Lopez, M.; Torres, A."

    def test_authors_text_none_when_no_authors(self):
        pub = make_pub(authors=[])
        result = self.svc.normalize_publication(pub)
        assert result.authors_text is None

    def test_skips_authors_without_name(self):
        authors = [Author(name="Garcia, J."), Author(name="")]
        pub = make_pub(authors=authors)
        result = self.svc.normalize_publication(pub)
        assert "Garcia, J." in result.authors_text
        # El autor sin nombre no debe aparecer
        assert ";;" not in result.authors_text

    def test_normalize_batch(self):
        pubs = [make_pub(doi=f"10.1234/{i}") for i in range(5)]
        results = self.svc.normalize_batch(pubs)
        assert len(results) == 5

    def test_year_normalization(self):
        pub = make_pub(year=2020)
        result = self.svc.normalize_publication(pub)
        assert result.publication_year == 2020

    def test_normalized_authors_text(self):
        authors = [Author(name="García Perez, José")]
        pub = make_pub(authors=authors)
        result = self.svc.normalize_publication(pub)
        assert result.normalized_authors is not None


# ──────────────────────────────────────────────────────────────────────────────
# PUBLICATION IDENTITY KEY
# ──────────────────────────────────────────────────────────────────────────────


class TestPublicationIdentityKey:
    def test_doi_key(self):
        pub = make_pub(doi="10.1234/test")
        assert pub.identity_key() == "doi:10.1234/test"

    def test_title_year_key_when_no_doi(self):
        pub = make_pub(doi=None, title="My Paper", year=2023)
        key = pub.identity_key()
        assert key.startswith("title:")
        assert "2023" in key

    def test_normalized_title_used_if_available(self):
        pub = make_pub(doi=None, title="My Paper", year=2023)
        pub.normalized_title = "my paper"
        key = pub.identity_key()
        assert "my paper" in key

    def test_doi_takes_priority_over_title(self):
        pub = make_pub(doi="10.9999/xyz", title="Some Title", year=2020)
        assert pub.identity_key() == "doi:10.9999/xyz"
