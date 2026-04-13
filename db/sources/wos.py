"""Modelo de BD para registros de WoS (Web of Science)."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, func, JSONB
from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


class WosRecord(Base, SourceRecordMixin):
    __tablename__ = "wos_records"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    wos_id = Column(String(100), nullable=False, index=True)
    accession_number = Column(String(50), nullable=True, index=True)
    title = Column(String(1000), nullable=False)
    authors_json = Column(JSONB, nullable=True, default=list)
    publication_year = Column(Integer, nullable=True, index=True)
    publication_type = Column(String(100), nullable=True)
    source_journal = Column(String(500), nullable=True)
    issn = Column(String(20), nullable=True)
    doi = Column(String(100), nullable=True, unique=True, index=True)
    citation_count = Column(Integer, default=0)
    citations_by_year = Column(JSONB, nullable=True, default=dict)
    url = Column(Text, nullable=True)
    status = Column(String(30), nullable=False, default="pending", index=True)
    raw_data = Column(JSONB, nullable=True, default=dict)
    extracted_at = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


def build_wos_kwargs(record, raw, kwargs):
    kwargs["wos_id"] = record.source_id or ""
    kwargs["accession_number"] = raw.get("accession_number")
    kwargs["authors_json"] = record.authors or []
    kwargs["citations_by_year"] = record.citations_by_year or {}
    kwargs["extracted_at"] = record.extracted_at


SOURCE_REGISTRY.register(
    SourceDefinition(
        name="wos",
        model_class=WosRecord,
        id_attr="wos_id",
        author_id_key="wos",
        build_specific_kwargs=build_wos_kwargs,
    )
)
