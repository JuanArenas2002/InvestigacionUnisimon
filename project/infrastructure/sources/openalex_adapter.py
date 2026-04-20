from typing import List, Optional

from extractors.openalex.extractor import OpenAlexExtractor

from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.domain.ports.source_port import SourcePort


class OpenAlexAdapter(SourcePort):
    SOURCE_NAME = "openalex"

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
        extractor = OpenAlexExtractor()
        records = extractor.extract(year_from=year_from, year_to=year_to, max_results=max_results)
        return [self._to_publication(record) for record in records]

    @staticmethod
    def _to_publication(record) -> Publication:
        authors = [
            Author(
                name=str(author.get("name") or "").strip(),
                orcid=author.get("orcid"),
                is_institutional=bool(author.get("is_institutional", False)),
                external_ids={
                    "openalex": str(author.get("openalex_id"))
                } if author.get("openalex_id") else {},
                metadata={k: v for k, v in author.items() if v is not None},
            )
            for author in (record.authors or [])
            if author.get("name")
        ]
        return Publication(
            source_name=record.source_name,
            source_id=record.source_id,
            doi=record.doi,
            pmid=record.pmid,
            pmcid=record.pmcid,
            title=record.title,
            publication_year=record.publication_year,
            publication_date=record.publication_date,
            publication_type=record.publication_type,
            language=record.language,
            source_journal=record.source_journal,
            issn=record.issn,
            is_open_access=record.is_open_access,
            oa_status=record.oa_status,
            authors=authors,
            citation_count=record.citation_count,
            citations_by_year=record.citations_by_year,
            url=record.url,
            raw_data=record.raw_data or {},
            extracted_at=record.extracted_at,
        )
