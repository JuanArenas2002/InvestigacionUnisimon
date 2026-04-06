from typing import List

from project.application.ingest_pipeline import IngestPipeline
from project.domain.services.deduplication_service import DeduplicationService
from project.domain.services.matching_service import MatchingService
from project.domain.services.normalization_service import NormalizationService
from project.infrastructure.persistence.postgres_repository import PostgresRepository
from project.registry.source_registry import SourceRegistry


def build_source_registry() -> SourceRegistry:
    return SourceRegistry().autodiscover()


def build_repository() -> PostgresRepository:
    return PostgresRepository()


def build_pipeline(source_names: List[str]) -> IngestPipeline:
    registry = build_source_registry()
    sources = registry.create_many(source_names)

    return IngestPipeline(
        sources=sources,
        repository=build_repository(),
        deduplication_service=DeduplicationService(),
        normalization_service=NormalizationService(),
        matching_service=MatchingService(),
    )
