"""
Modelo de BD para registros de Google Scholar.

Almacena cada publicación extraída de Google Scholar de forma
desnormalizada, con todos los datos tal como vienen del extractor.

La reconciliación vincula estos registros a canonical_publications.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Float,
    DateTime,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


class GoogleScholarRecord(Base, SourceRecordMixin):
    """
    Registro de una publicación extraída de Google Scholar.
    
    Campos específicos:
    - scholar_id: ID del perfil en Google Scholar (ej: V94aovUAAAAJ)
    - source_url: URL del artículo en Google Scholar
    - citation_count: Número de citas (desde Google Scholar)
    - citations_by_year: Diccionario año→citas
    """
    
    __tablename__ = "google_scholar_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # --- Identificadores ---
    google_scholar_id = Column(String(50), nullable=False, index=True)
    scholar_profile_id = Column(String(50), nullable=True, index=True, 
                               comment="ID del perfil del que se extrajo")
    
    # --- Metadatos principales (desnormalizados) ---
    title = Column(String(1000), nullable=False)
    authors_json = Column(JSONB, nullable=True, default=list,
                         comment="[{\"name\":\"...\",\"orcid\":None,...}]")
    publication_year = Column(Integer, nullable=True, index=True)
    publication_date = Column(String(50), nullable=True)
    publication_type = Column(String(100), nullable=True)
    
    # --- Publicación / revista ---
    source_journal = Column(String(500), nullable=True)
    issn = Column(String(20), nullable=True)
    
    # --- Identificadores bibliográficos ---
    doi = Column(String(100), nullable=True, unique=True, index=True)
    
    # --- Métricas ---
    citation_count = Column(Integer, default=0, nullable=False)
    citations_by_year = Column(JSONB, nullable=True, default=dict,
                              comment="{\"2024\": 5, \"2023\": 10}")
    
    # --- URLs ---
    url = Column(Text, nullable=True)
    
    # --- Control ---
    status = Column(String(30), nullable=False, default="pending", index=True,
                   comment="pending | linked | flagged_review | rejected")
    raw_data = Column(JSONB, nullable=True, default=dict,
                     comment="Datos crudos del extractor")
    
    extracted_at = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), 
                       onupdate=func.now(), nullable=False)
    
    # --- Relaciones (automáticas vía SourceRecordMixin) ---
    # canonical_publication: CanonicalPublication (backref auto)
    
    def __repr__(self):
        return (
            f"<GoogleScholarRecord(id={self.id}, title='{self.title[:40]}...', "
            f"scholar_id={self.google_scholar_id}, year={self.publication_year})>"
        )


# =============================================================
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def build_google_scholar_kwargs(record, raw, kwargs):
    """
    Construye los parámetros específicos de Google Scholar
    desde un StandardRecord.
    
    Args:
        record: StandardRecord desde el extractor
        raw: raw_data del StandardRecord
        kwargs: dict de acumulación (modifica in-place)
    """
    kwargs["google_scholar_id"] = record.source_id or ""
    kwargs["scholar_profile_id"] = raw.get("scholar_profile_id")
    kwargs["authors_json"] = record.authors or []
    kwargs["citations_by_year"] = record.citations_by_year or {}
    kwargs["extracted_at"] = record.extracted_at


# =============================================================
# REGISTRAR EN REGISTRY
# =============================================================

SOURCE_REGISTRY.register(
    SourceDefinition(
        name="google_scholar",
        model_class=GoogleScholarRecord,
        id_attr="google_scholar_id",        # Columna que contiene el ID propio
        author_id_key="google_scholar",     # Clave en authors.external_ids
        build_specific_kwargs=build_google_scholar_kwargs,
    )
)
