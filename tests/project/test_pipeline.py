"""
Tests del IngestPipeline con fuentes y repositorio mockeados.

Demuestra que:
- El pipeline funciona SIN FastAPI
- Las fuentes pueden ser mockeadas (SourcePort)
- El repositorio puede ser mockeado (RepositoryPort)
- Cada etapa es testeable por separado

Ejecutar:
    pytest tests/project/test_pipeline.py -v
"""

from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from project.application.ingest_pipeline import IngestPipeline, PipelineResult
from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.domain.services.deduplication_service import DeduplicationService
from project.domain.services.matching_service import MatchingService
from project.domain.services.normalization_service import NormalizationService
from project.domain.ports.publication_repository import PublicationRepositoryPort
from project.domain.ports.source_port import SourcePort


# ──────────────────────────────────────────────────────────────────────────────
# MOCKS
# ──────────────────────────────────────────────────────────────────────────────


class MockSource(SourcePort):
    """Fuente falsa que devuelve publicaciones predefinidas."""

    SOURCE_NAME = "mock"

    def __init__(self, records: List[Publication], name: str = "mock") -> None:
        self._records = records
        self._name = name

    @property
    def source_name(self) -> str:
        return self._name

    def fetch_records(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        **kwargs,
    ) -> List[Publication]:
        records = self._records
        if max_results:
            records = records[:max_results]
        return records


class FailingSource(SourcePort):
    """Fuente que siempre lanza una excepcion."""

    SOURCE_NAME = "failing"

    @property
    def source_name(self) -> str:
        return self.SOURCE_NAME

    def fetch_records(self, **kwargs) -> List[Publication]:
        raise ConnectionError("Fuente no disponible")


class MockRepository(PublicationRepositoryPort):
    """Repositorio en memoria para tests."""

    def __init__(self) -> None:
        self.saved_authors: List[Publication] = []
        self.saved_source: Dict[str, List[Publication]] = {}
        self.upserted: List[Publication] = []

    def save_authors(self, publications: List[Publication]) -> int:
        self.saved_authors.extend(publications)
        return sum(len(p.authors) for p in publications)

    def save_source_records(self, records_by_source: Dict[str, List[Publication]]) -> int:
        self.saved_source = records_by_source
        return sum(len(v) for v in records_by_source.values())

    def upsert_canonical_publications(self, publications: List[Publication]) -> int:
        self.upserted.extend(publications)
        return len(publications)

    def list_publications(self, limit: int = 100, offset: int = 0) -> List[dict]:
        return [{"id": i, "title": p.title} for i, p in enumerate(self.upserted)]


# ──────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────────────────────────────────────


def make_publications(n: int = 3, source: str = "mock") -> List[Publication]:
    return [
        Publication(
            source_name=source,
            source_id=f"ID_{i}",
            doi=f"10.1234/{source}_{i}",
            title=f"Publication {i} from {source}",
            publication_year=2020 + i,
            authors=[Author(name=f"Autor {i}")],
        )
        for i in range(n)
    ]


def build_pipeline(
    sources: List[SourcePort],
    repository: RepositoryPort | None = None,
) -> IngestPipeline:
    return IngestPipeline(
        sources=sources,
        repository=repository or MockRepository(),
        deduplication_service=DeduplicationService(),
        normalization_service=NormalizationService(),
        matching_service=MatchingService(),
    )


# ──────────────────────────────────────────────────────────────────────────────
# TESTS: PIPELINE COMPLETO
# ──────────────────────────────────────────────────────────────────────────────


class TestIngestPipelineRun:
    def test_returns_pipeline_result(self):
        source = MockSource(make_publications(3))
        pipeline = build_pipeline([source])
        result = pipeline.run(persist=False)
        assert isinstance(result, PipelineResult)

    def test_collected_count_correct(self):
        source = MockSource(make_publications(5))
        pipeline = build_pipeline([source])
        result = pipeline.run(persist=False)
        assert result.collected == 5

    def test_by_source_tracked(self):
        pubs_a = make_publications(2, "source_a")
        pubs_b = make_publications(3, "source_b")
        pipeline = build_pipeline([
            MockSource(pubs_a, "source_a"),
            MockSource(pubs_b, "source_b"),
        ])
        result = pipeline.run(persist=False)
        assert result.by_source["source_a"] == 2
        assert result.by_source["source_b"] == 3

    def test_persist_false_does_not_call_repository(self):
        repo = MockRepository()
        pipeline = build_pipeline([MockSource(make_publications(2))], repo)
        pipeline.run(persist=False)
        assert repo.saved_source == {}
        assert repo.upserted == []
        assert repo.saved_authors == []

    def test_persist_true_calls_repository(self):
        repo = MockRepository()
        pipeline = build_pipeline([MockSource(make_publications(3))], repo)
        result = pipeline.run(persist=True)
        assert result.source_saved == 3
        assert result.canonical_upserted > 0
        assert result.authors_saved >= 0  # puede ser 0 si las pubs no tienen autores

    def test_persist_true_saves_authors(self):
        """save_authors se llama con el conjunto completo recolectado."""
        from project.domain.models.author import Author as DomainAuthor
        authors = [DomainAuthor(name="García, Juan"), DomainAuthor(name="López, M.")]
        pubs = [Publication(source_name="test", source_id="1", authors=authors)]
        repo = MockRepository()
        pipeline = build_pipeline([MockSource(pubs, "test")], repo)
        result = pipeline.run(persist=True)
        assert result.authors_saved == 2
        assert len(repo.saved_authors) == 1  # 1 publication pasada a save_authors

    def test_empty_sources_produces_zero_results(self):
        pipeline = build_pipeline([MockSource([])])
        result = pipeline.run(persist=False)
        assert result.collected == 0
        assert result.deduplicated == 0

    def test_failing_source_captured_in_errors(self):
        pipeline = build_pipeline([FailingSource()])
        result = pipeline.run(persist=False)
        assert "failing" in result.errors
        assert result.collected == 0

    def test_failing_source_does_not_abort_other_sources(self):
        good_pubs = make_publications(4)
        pipeline = build_pipeline([
            FailingSource(),
            MockSource(good_pubs, "good"),
        ])
        result = pipeline.run(persist=False)
        assert "failing" in result.errors
        assert result.collected == 4
        assert result.by_source["good"] == 4

    def test_max_results_respected_per_source(self):
        source = MockSource(make_publications(10))
        pipeline = build_pipeline([source])
        result = pipeline.run(max_results=3, persist=False)
        assert result.collected <= 3

    def test_no_fastapi_dependency(self):
        """El pipeline no importa ni usa FastAPI — validacion de aislamiento."""
        import project.application.ingest_pipeline as module
        source_code = open(module.__file__).read()
        assert "fastapi" not in source_code.lower()


# ──────────────────────────────────────────────────────────────────────────────
# TESTS: ETAPAS INDIVIDUALES
# ──────────────────────────────────────────────────────────────────────────────


class TestPipelineStages:
    def setup_method(self):
        self.pipeline = build_pipeline([MockSource([])])

    def test_collect_stage(self):
        source = MockSource(make_publications(4), "test")
        pipeline = build_pipeline([source])
        collected, errors = pipeline.collect()
        assert len(collected["test"]) == 4
        assert errors == {}

    def test_collect_returns_errors_dict(self):
        pipeline = build_pipeline([FailingSource()])
        collected, errors = pipeline.collect()
        assert "failing" in errors
        assert isinstance(errors["failing"], str)

    def test_deduplicate_stage(self):
        pubs = make_publications(3)
        # Agregar duplicado del primero
        duplicate = Publication(
            source_name="mock",
            source_id="ID_0",
            doi="10.1234/mock_0",  # mismo doi que pubs[0]
            title="Publication 0 from mock",
            publication_year=2020,
        )
        result = self.pipeline.deduplicate(pubs + [duplicate])
        assert len(result) == 3  # duplicado eliminado

    def test_normalize_stage(self):
        pubs = make_publications(3)
        result = self.pipeline.normalize(pubs)
        for pub in result:
            assert pub.normalized_title is not None

    def test_match_stage(self):
        pubs = make_publications(3)
        normalized = self.pipeline.normalize(pubs)
        matched = self.pipeline.match(normalized)
        for pub in matched:
            assert pub.canonical_key is not None
            assert pub.match_score is not None

    def test_enrich_stage(self):
        pubs = make_publications(2)
        normalized = self.pipeline.normalize(pubs)
        matched = self.pipeline.match(normalized)
        enriched = self.pipeline.enrich(matched)
        for pub in enriched:
            assert "pipeline" in pub.raw_data

    def test_enrich_adds_pipeline_metadata(self):
        pub = make_publications(1)[0]
        pub.canonical_key = "doi:10.1234/mock_0"
        pub.match_type = "identity"
        pub.match_score = 100.0
        enriched = self.pipeline.enrich([pub])
        assert enriched[0].raw_data["pipeline"]["canonical_key"] == "doi:10.1234/mock_0"
        assert enriched[0].raw_data["pipeline"]["match_type"] == "identity"
