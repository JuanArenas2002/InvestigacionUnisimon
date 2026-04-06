from typing import List, Optional

from config import institution
from extractors.scopus.extractor import ScopusExtractor

from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.ports.source_port import SourcePort


class ScopusAdapter(SourcePort):
    SOURCE_NAME = "scopus"

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
        extractor = ScopusExtractor()
        query = kwargs.get("query")
        affiliation_id = kwargs.get("affiliation_id") or institution.scopus_affiliation_id

        if query is None and affiliation_id:
            query = f"AF-ID({affiliation_id})"
            if year_from:
                query += f" AND PUBYEAR > {int(year_from) - 1}"
            if year_to:
                query += f" AND PUBYEAR < {int(year_to) + 1}"

        records = extractor.extract(query=query, max_results=max_results, affiliation_id=affiliation_id)
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
            publication_date=record.publication_date,
            publication_type=record.publication_type,
            source_journal=record.source_journal,
            issn=record.issn,
            is_open_access=record.is_open_access,
            oa_status=record.oa_status,
            authors=authors,
            citation_count=record.citation_count,
            url=record.url,
            raw_data=record.raw_data or {},
            extracted_at=record.extracted_at,
        )
