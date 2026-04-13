"""Modelo de BD para registros de OpenAlex."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, func, JSONB
from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


class OpenalexRecord(Base, SourceRecordMixin):
    __tablename__ = "openalex_records"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    openalex_work_id = Column(String(100), nullable=False, index=True)
    title = Column(String(1000), nullable=False)
    authors_json = Column(JSONB, nullable=True, default=list)
    publication_year = Column(Integer, nullable=True, index=True)
    publication_date = Column(String(50), nullable=True)
    publication_type = Column(String(100), nullable=True)
    source_journal = Column(String(500), nullable=True)
    issn = Column(String(20), nullable=True)
    doi = Column(String(100), nullable=True, unique=True, index=True)
    pmid = Column(String(50), nullable=True)
    pmcid = Column(String(50), nullable=True)
    language = Column(String(10), nullable=True)
    is_open_access = Column(Integer, nullable=True)
    oa_status = Column(String(50), nullable=True)
    citation_count = Column(Integer, default=0)
    citations_by_year = Column(JSONB, nullable=True, default=dict)
    url = Column(Text, nullable=True)
    status = Column(String(30), nullable=False, default="pending", index=True)
    raw_data = Column(JSONB, nullable=True, default=dict)
    extracted_at = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


def build_openalex_kwargs(record, raw, kwargs):
    kwargs["openalex_work_id"] = record.source_id or ""
    kwargs["authors_json"] = record.authors or []
    kwargs["citations_by_year"] = record.citations_by_year or {}
    kwargs["pmid"] = record.pmid
    kwargs["pmcid"] = record.pmcid
    kwargs["language"] = record.language
    kwargs["is_open_access"] = record.is_open_access
    kwargs["oa_status"] = record.oa_status
    kwargs["extracted_at"] = record.extracted_at


SOURCE_REGISTRY.register(
    SourceDefinition(
        name="openalex",
        model_class=OpenalexRecord,
        id_attr="openalex_work_id",
        author_id_key="openalex",
        build_specific_kwargs=build_openalex_kwargs,
    )
)
