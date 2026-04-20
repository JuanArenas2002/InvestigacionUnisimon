"""
EJEMPLO: Como agregar una nueva fuente al sistema.

Este archivo sirve como documentacion ejecutable (doctest) y guia practica.
Para agregar una nueva fuente bibliografica solo necesitas:

1. Crear `project/infrastructure/sources/mi_fuente_adapter.py`
2. Implementar SourcePort con SOURCE_NAME unico
3. El SourceRegistry la detecta automaticamente via autodiscover()
4. No necesitas modificar ningun otro archivo

Ejecutar:
    pytest tests/project/test_new_source_example.py -v
"""

from typing import List, Optional

from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.domain.ports.source_port import SourcePort
from project.registry.source_registry import SourceRegistry
from project.application.ingest_pipeline import IngestPipeline
from project.domain.services.deduplication_service import DeduplicationService
from project.domain.services.matching_service import MatchingService
from project.domain.services.normalization_service import NormalizationService


# ──────────────────────────────────────────────────────────────────────────────
# PASO 1: Implementar SourcePort
# ──────────────────────────────────────────────────────────────────────────────
#
# En produccion este archivo estaria en:
#   project/infrastructure/sources/pubmed_adapter.py
#


class PubMedAdapter(SourcePort):
    """
    Adapter ejemplo para PubMed.

    Para usarlo en produccion:
    1. Mover a project/infrastructure/sources/pubmed_adapter.py
    2. Implementar fetch_records() llamando a la API real de PubMed
    3. El SourceRegistry lo detectara automaticamente
    """

    SOURCE_NAME = "pubmed"

    @property
    def source_name(self) -> str:
        return self.SOURCE_NAME

    def fetch_records(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        **kwargs,
    ) -> List[Publication]:
        # En produccion: llamar a la API de PubMed
        # https://www.ncbi.nlm.nih.gov/books/NBK25501/
        #
        # from extractors.pubmed.extractor import PubMedExtractor
        # extractor = PubMedExtractor()
        # records = extractor.extract(...)
        # return [self._to_publication(r) for r in records]

        # Para este test, devolvemos datos ficticios
        return [
            Publication(
                source_name=self.SOURCE_NAME,
                source_id="PMID_12345",
                pmid="12345",
                doi="10.1234/pubmed.001",
                title="Effects of Exercise on Cognitive Function",
                publication_year=2023,
                source_journal="Journal of Neuroscience",
                authors=[
                    Author(
                        name="Smith, J.",
                        orcid="0000-0001-2345-6789",
                        is_institutional=True,
                        external_ids={"pubmed": "PMID_12345"},
                    )
                ],
                citation_count=15,
            )
        ]


# ──────────────────────────────────────────────────────────────────────────────
# PASO 2: Registrar manualmente (o via autodiscover si esta en sources/)
# ──────────────────────────────────────────────────────────────────────────────


class TestNewSourceIntegration:
    """Verifica que una nueva fuente se integra sin modificar codigo existente."""

    def test_new_source_implements_port(self):
        adapter = PubMedAdapter()
        assert isinstance(adapter, SourcePort)
        assert adapter.source_name == "pubmed"

    def test_new_source_returns_publications(self):
        adapter = PubMedAdapter()
        records = adapter.fetch_records(year_from=2020, max_results=10)
        assert isinstance(records, list)
        assert all(isinstance(r, Publication) for r in records)

    def test_new_source_publications_have_source_name(self):
        adapter = PubMedAdapter()
        records = adapter.fetch_records()
        for record in records:
            assert record.source_name == "pubmed"

    def test_register_manually_in_registry(self):
        """Registro manual sin autodiscover."""
        registry = SourceRegistry()
        registry._register_from_module(
            type("FakeModule", (), {"PubMedAdapter": PubMedAdapter})
        )
        assert "pubmed" in registry.source_names
        instance = registry.create("pubmed")
        assert isinstance(instance, PubMedAdapter)

    def test_pipeline_uses_new_source(self):
        """Pipeline completo con la nueva fuente — sin FastAPI, sin BD."""
        from tests.project.test_pipeline import MockRepository

        pipeline = IngestPipeline(
            sources=[PubMedAdapter()],
            repository=MockRepository(),
            deduplication_service=DeduplicationService(),
            normalization_service=NormalizationService(),
            matching_service=MatchingService(),
        )

        result = pipeline.run(persist=False)

        assert result.collected >= 1
        assert result.by_source.get("pubmed", 0) >= 1
        assert result.deduplicated >= 1
        assert result.normalized >= 1
        assert result.matched >= 1

    def test_new_source_does_not_affect_other_sources(self):
        """Agregar una nueva fuente no rompe las existentes."""
        from tests.project.test_pipeline import MockSource, MockRepository, make_publications

        existing_source = MockSource(make_publications(3), "existing")
        new_source = PubMedAdapter()

        pipeline = IngestPipeline(
            sources=[existing_source, new_source],
            repository=MockRepository(),
            deduplication_service=DeduplicationService(),
            normalization_service=NormalizationService(),
            matching_service=MatchingService(),
        )

        result = pipeline.run(persist=False)
        assert "existing" in result.by_source
        assert "pubmed" in result.by_source
        assert result.collected == 3 + 1  # 3 existentes + 1 pubmed
