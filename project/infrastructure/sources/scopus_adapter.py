from typing import List, Optional

from config import institution, scopus_config
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
        explicit_affiliation = kwargs.get("affiliation_id") or institution.scopus_affiliation_id
        affiliation_ids = kwargs.get("affiliation_ids") or scopus_config.affiliation_ids

        ids_to_query: List[str] = []
        if explicit_affiliation:
            ids_to_query.append(str(explicit_affiliation).strip())
        for af_id in affiliation_ids:
            af = str(af_id).strip()
            if af and af not in ids_to_query:
                ids_to_query.append(af)

        all_records = []
        if query is not None:
            all_records.extend(extractor.extract(query=query, max_results=max_results, affiliation_id=None))
        else:
            for af_id in ids_to_query:
                if max_results and len(all_records) >= max_results:
                    break
                scopus_query = f"AF-ID({af_id})"
                if year_from:
                    scopus_query += f" AND PUBYEAR > {int(year_from) - 1}"
                if year_to:
                    scopus_query += f" AND PUBYEAR < {int(year_to) + 1}"
                all_records.extend(
                    extractor.extract(
                        query=scopus_query,
                        max_results=max_results,
                        affiliation_id=af_id,
                    )
                )

        if max_results:
            all_records = all_records[:max_results]

        return [self._to_publication(record) for record in all_records]

    @staticmethod
    def _to_publication(record) -> Publication:
        authors = [
            Author(
                name=str(author.get("name") or "").strip(),
                orcid=author.get("orcid"),
                is_institutional=bool(author.get("is_institutional", False)),
                external_ids={
                    "scopus": str(author.get("scopus_id"))
                } if author.get("scopus_id") else {},
                metadata={k: v for k, v in author.items() if v is not None},
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
