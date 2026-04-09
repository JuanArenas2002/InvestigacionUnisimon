"""
Tests para la mejora de comparacion de autores:
  - Nombres invertidos (Apellido, Nombre  ≡  Nombre Apellido)
  - ORCID como primera prioridad
  - Cascada correcta en MatchingService

Ejecutar:
    pytest tests/project/test_author_matching.py -v
"""

import pytest

from reconciliation.fuzzy_matcher import compare_authors, _author_token_set, _token_jaccard
from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.domain.services.matching_service import MatchingService
from project.domain.services.normalization_service import NormalizationService


# ──────────────────────────────────────────────────────────────────────────────
# _author_token_set
# ──────────────────────────────────────────────────────────────────────────────


class TestAuthorTokenSet:
    def test_simple_name(self):
        tokens = _author_token_set("Juan García")
        assert "juan" in tokens
        assert "garcia" in tokens

    def test_inverted_name_same_tokens(self):
        tokens_a = _author_token_set("García, Juan")
        tokens_b = _author_token_set("Juan García")
        assert tokens_a == tokens_b

    def test_multiple_authors(self):
        tokens = _author_token_set("García, Juan; López, María")
        assert "garcia" in tokens
        assert "juan" in tokens
        assert "lopez" in tokens
        assert "maria" in tokens

    def test_accents_removed(self):
        tokens = _author_token_set("Martínez Pérez, José")
        assert "martinez" in tokens
        assert "perez" in tokens
        assert "jose" in tokens

    def test_empty_string(self):
        assert _author_token_set("") == frozenset()


# ──────────────────────────────────────────────────────────────────────────────
# compare_authors — nombres invertidos
# ──────────────────────────────────────────────────────────────────────────────


class TestCompareAuthorsInvertedNames:
    def test_same_name_different_order(self):
        score = compare_authors("García, Juan", "Juan García")
        assert score >= 85, f"Nombres invertidos deben tener score alto, got {score}"

    def test_same_name_with_semicolon_separator(self):
        score = compare_authors(
            "García López, Juan; Torres, María",
            "Juan García López; María Torres",
        )
        assert score >= 80, f"Score esperado >= 80, got {score}"

    def test_different_authors_low_score(self):
        score = compare_authors("Smith, John", "González, Carlos")
        assert score < 50, f"Autores distintos deben tener score bajo, got {score}"

    def test_empty_first_is_neutral(self):
        score = compare_authors("", "García, Juan")
        assert score == 50.0

    def test_empty_both_is_neutral(self):
        score = compare_authors("", "")
        assert score == 50.0

    def test_accented_names(self):
        score = compare_authors("Martínez, José", "Jose Martinez")
        assert score >= 85, f"Tildes no deben penalizar, got {score}"

    def test_compound_surnames(self):
        score = compare_authors("García López, Juan Carlos", "Juan Carlos Garcia Lopez")
        assert score >= 85, f"Apellidos compuestos invertidos, got {score}"


# ──────────────────────────────────────────────────────────────────────────────
# compare_authors — ORCID como primera prioridad
# ──────────────────────────────────────────────────────────────────────────────


class TestCompareAuthorsOrcidPriority:
    ORCID_A = "0000-0001-2345-6789"
    ORCID_B = "0000-0002-9876-5432"

    def test_shared_orcid_gives_high_score(self):
        orcids_a = frozenset({self.ORCID_A})
        orcids_b = frozenset({self.ORCID_A})  # mismo ORCID
        score = compare_authors(
            "García, J.",
            "J. Garcia",
            orcids_a=orcids_a,
            orcids_b=orcids_b,
        )
        assert score >= 80, f"ORCID compartido debe dar score alto, got {score}"

    def test_different_orcids_falls_back_to_fuzzy(self):
        orcids_a = frozenset({self.ORCID_A})
        orcids_b = frozenset({self.ORCID_B})  # ORCIDs distintos
        # Mismos nombres pero ORCIDs diferentes → usa fuzzy del texto
        score_with_orcid = compare_authors(
            "García, Juan",
            "Juan García",
            orcids_a=orcids_a,
            orcids_b=orcids_b,
        )
        score_without_orcid = compare_authors("García, Juan", "Juan García")
        # Con ORCIDs distintos el score se basa en fuzzy solamente
        # Los nombres son iguales así que debería seguir siendo alto
        assert score_with_orcid >= 50

    def test_no_orcid_uses_fuzzy(self):
        score = compare_authors("García, Juan", "Juan García", orcids_a=None, orcids_b=None)
        assert score >= 85

    def test_partial_orcid_match(self):
        """Un autor con ORCID compartido de 2 totales."""
        orcids_a = frozenset({self.ORCID_A, self.ORCID_B})
        orcids_b = frozenset({self.ORCID_A})  # solo comparten uno
        score = compare_authors(
            "García J.; López M.",
            "Garcia J.",
            orcids_a=orcids_a,
            orcids_b=orcids_b,
        )
        assert score > 0


# ──────────────────────────────────────────────────────────────────────────────
# MatchingService — integración ORCID
# ──────────────────────────────────────────────────────────────────────────────


class TestMatchingServiceOrcid:
    def setup_method(self):
        self.svc = MatchingService()
        self.norm = NormalizationService()

    def _make_pub(self, title, year, authors, doi=None):
        pub = Publication(
            source_name="test",
            doi=doi,
            title=title,
            publication_year=year,
            authors=authors,
        )
        return self.norm.normalize_publication(pub)

    def test_same_orcid_boosts_match(self):
        """Dos pubs con mismo autor (ORCID compartido) deben obtener match."""
        shared_orcid = "0000-0001-1111-2222"
        pub_a = self._make_pub(
            title="Effects of exercise on cognitive function",
            year=2023,
            authors=[Author(name="García, Juan", orcid=shared_orcid)],
        )
        pub_b = self._make_pub(
            title="Effects of exercise on cognitive function",
            year=2023,
            authors=[Author(name="Juan García", orcid=shared_orcid)],
        )
        result = self.svc.match([pub_a, pub_b])
        assert result[1].match_type in ("fuzzy_candidate", "identity")

    def test_inverted_name_authors_still_matches(self):
        """Dos pubs con el mismo autor en distinto orden de nombre/apellido."""
        pub_a = self._make_pub(
            title="Climate change impact on biodiversity",
            year=2022,
            authors=[Author(name="Martínez López, Carlos")],
        )
        pub_b = self._make_pub(
            title="Climate change impact on biodiversity",
            year=2022,
            authors=[Author(name="Carlos Martinez Lopez")],
        )
        result = self.svc.match([pub_a, pub_b])
        # El score del segundo registro debe ser alto (fuzzy_candidate)
        assert result[1].match_score is not None
        assert result[1].match_score > 50

    def test_compare_authors_static_method(self):
        """_compare_authors usa ORCID cuando está disponible."""
        orcid = "0000-0003-9999-8888"
        pub_a = Publication(
            source_name="a",
            title="T",
            authors=[Author(name="Smith, J.", orcid=orcid)],
            authors_text="Smith, J.",
        )
        pub_b = Publication(
            source_name="b",
            title="T",
            authors=[Author(name="J. Smith", orcid=orcid)],
            authors_text="J. Smith",
        )
        score = MatchingService._compare_authors(pub_a, pub_b)
        assert score >= 70, f"Mismo ORCID debe dar score alto, got {score}"
