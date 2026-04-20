from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from project.domain.models.publication import Publication
from project.domain.services.deduplication_service import DeduplicationService
from project.domain.services.matching_service import MatchingService
from project.domain.services.normalization_service import NormalizationService
from project.domain.ports.publication_repository import PublicationRepositoryPort
from project.domain.ports.source_port import SourcePort


@dataclass(slots=True)
class PipelineResult:
    collected: int
    deduplicated: int
    normalized: int
    matched: int
    enriched: int
    authors_saved: int
    source_saved: int
    canonical_upserted: int
    by_source: Dict[str, int] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class IngestPipeline:
    """Caso de uso principal ETL: collect -> deduplicate -> normalize -> match -> enrich."""

    def __init__(
        self,
        sources: List[SourcePort],
        repository: PublicationRepositoryPort,
        deduplication_service: DeduplicationService,
        normalization_service: NormalizationService,
        matching_service: MatchingService,
    ) -> None:
        self.sources = sources
        self.repository = repository
        self.deduplication_service = deduplication_service
        self.normalization_service = normalization_service
        self.matching_service = matching_service

    def run(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        source_kwargs: Optional[Dict[str, dict]] = None,
        persist: bool = True,
    ) -> PipelineResult:
        source_kwargs = source_kwargs or {}

        collected_by_source, errors = self.collect(
            year_from=year_from,
            year_to=year_to,
            max_results=max_results,
            source_kwargs=source_kwargs,
        )

        collected = [pub for pubs in collected_by_source.values() for pub in pubs]
        deduplicated = self.deduplicate(collected)
        normalized = self.normalize(deduplicated)
        matched = self.match(normalized)
        enriched = self.enrich(matched)

        authors_saved = 0
        source_saved = 0
        canonical_upserted = 0
        if persist:
            # Guardar autores primero, desde el conjunto completo recolectado
            # (antes de dedup), para no perder autores de registros descartados.
            authors_saved = self.repository.save_authors(collected)
            source_saved = self.repository.save_source_records(collected_by_source)
            canonical_upserted = self.repository.upsert_canonical_publications(enriched)

        return PipelineResult(
            collected=len(collected),
            deduplicated=len(deduplicated),
            normalized=len(normalized),
            matched=len(matched),
            enriched=len(enriched),
            authors_saved=authors_saved,
            source_saved=source_saved,
            canonical_upserted=canonical_upserted,
            by_source={name: len(rows) for name, rows in collected_by_source.items()},
            errors=errors,
        )

    def collect(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        source_kwargs: Optional[Dict[str, dict]] = None,
    ) -> tuple[Dict[str, List[Publication]], Dict[str, str]]:
        source_kwargs = source_kwargs or {}
        collected: Dict[str, List[Publication]] = {}
        errors: Dict[str, str] = {}

        for source in self.sources:
            kwargs = source_kwargs.get(source.source_name, {})
            try:
                collected[source.source_name] = source.fetch_records(
                    year_from=year_from,
                    year_to=year_to,
                    max_results=max_results,
                    **kwargs,
                )
            except Exception as exc:
                collected[source.source_name] = []
                errors[source.source_name] = str(exc)
        return collected, errors

    def deduplicate(self, publications: List[Publication]) -> List[Publication]:
        return self.deduplication_service.deduplicate(publications)

    def normalize(self, publications: Iterable[Publication]) -> List[Publication]:
        return self.normalization_service.normalize_batch(publications)

    def match(self, publications: Iterable[Publication]) -> List[Publication]:
        return self.matching_service.match(publications)

    def enrich(self, publications: Iterable[Publication]) -> List[Publication]:
        enriched = []
        for publication in publications:
            publication.raw_data = {
                **(publication.raw_data or {}),
                "pipeline": {
                    "canonical_key": publication.canonical_key,
                    "match_type": publication.match_type,
                    "match_score": publication.match_score,
                },
            }
            enriched.append(publication)
        return enriched
