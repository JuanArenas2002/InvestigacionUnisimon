from typing import List, Optional

from extractors.cvlac.extractor import CvlacExtractor

from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.ports.source_port import SourcePort


class CvlacAdapter(SourcePort):
    SOURCE_NAME = "cvlac"

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
        cvlac_codes = kwargs.get("cvlac_codes") or []
        if not cvlac_codes:
            return []

        extractor = CvlacExtractor()
        records = extractor.extract(
            year_from=year_from,
            year_to=year_to,
            max_results=max_results,
            cvlac_codes=cvlac_codes,
        )
        return [self._to_publication(record) for record in records]

    @staticmethod
    def _to_publication(record) -> Publication:
        authors = [
            Author(
                name=str(author.get("name") or "").strip(),
                orcid=author.get("orcid"),
                is_institutional=bool(author.get("is_institutional", False)),
            )
            for author in (record.authors or [])
            if author.get("name")
        ]
        return Publication(
            source_name=record.source_name,
            source_id=record.source_id,
            doi=record.doi,
            title=record.title,
            publication_year=record.publication_year,
            publication_type=record.publication_type,
            source_journal=record.source_journal,
            issn=record.issn,
            authors=authors,
            citation_count=record.citation_count,
            url=record.url,
            raw_data=record.raw_data or {},
            extracted_at=record.extracted_at,
        )
