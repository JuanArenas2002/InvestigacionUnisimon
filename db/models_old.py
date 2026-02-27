"""
Modelos SQLAlchemy para la base de datos de reconciliación bibliográfica.

Tablas principales:
  - journals               : Revistas normalizadas (ISSN, nombre)
  - institutions            : Instituciones (ROR, nombre)
  - canonical_publications  : Registro "dorado" unificado de cada producto
  - authors                 : Autores (con IDs de múltiples fuentes)
  - publication_authors     : Relación N:M publicaciones ↔ autores
  - external_records        : Registro crudo de cada fuente antes/después de reconciliar
  - reconciliation_log      : Auditoría de cada decisión de match

Flujo:
  1. Cada extractor inserta en external_records con status='pending'
  2. El motor de reconciliación busca match (DOI → fuzzy)
  3. Si match → vincula a canonical_publication existente
  4. Si no  → crea nueva canonical_publication
  5. Cada decisión se registra en reconciliation_log
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Text,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
    Enum,
    func,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# =============================================================
# BASE
# =============================================================

class Base(DeclarativeBase):
    """Clase base para todos los modelos"""
    pass


# =============================================================
# REVISTAS (normalización)
# =============================================================

class Journal(Base):
    """
    Revista normalizada.  Una sola fila por ISSN; evita
    almacenar el mismo nombre de revista miles de veces.
    """
    __tablename__ = "journals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    issn: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    publisher: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relación inversa
    publications = relationship("CanonicalPublication", back_populates="journal")

    def __repr__(self):
        return f"<Journal(id={self.id}, issn={self.issn}, name='{self.name[:40]}')>"


# =============================================================
# INSTITUCIONES (normalización)
# =============================================================

class Institution(Base):
    """
    Institución/afiliación normalizada (ROR como clave natural).
    """
    __tablename__ = "institutions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ror_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # education, company, …

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relación inversa
    authors = relationship("AuthorInstitution", back_populates="institution")

    def __repr__(self):
        return f"<Institution(id={self.id}, ror={self.ror_id}, name='{self.name[:40]}')>"


# =============================================================
# PUBLICACIÓN CANÓNICA (registro "dorado")
# =============================================================

class CanonicalPublication(Base):
    """
    Registro unificado y deduplicado de una publicación.
    Cada fila representa UN producto bibliográfico único,
    independientemente de en cuántas fuentes aparezca.
    """
    __tablename__ = "canonical_publications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- Identificadores ---
    doi: Mapped[Optional[str]] = mapped_column(String(255), unique=True, nullable=True, index=True)
    pmid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    pmcid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # --- Metadatos ---
    title: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    publication_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    publication_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    publication_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    language: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    # --- Revista (FK normalizada) ---
    journal_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("journals.id", ondelete="SET NULL"), nullable=True, index=True
    )
    # Campos denormalizados para retrocompatibilidad y consultas rápidas
    source_journal: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    issn: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # --- Open Access ---
    is_open_access: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    oa_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # --- Métricas consolidadas ---
    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    institutional_authors_count: Mapped[int] = mapped_column(Integer, default=0)

    # --- Procedencia por campo (qué fuente aportó cada dato) ---
    field_provenance: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, default=dict,
        comment="Dict campo→fuente. Ej: {'doi':'openalex','source_journal':'scopus'}",
    )

    # --- Control ---
    sources_count: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- Relaciones ---
    journal = relationship("Journal", back_populates="publications")
    external_records = relationship(
        "ExternalRecord", back_populates="canonical_publication", lazy="dynamic"
    )
    authors = relationship(
        "PublicationAuthor", back_populates="publication", lazy="dynamic"
    )

    # --- Índices compuestos y constraints ---
    __table_args__ = (
        Index("ix_canon_year_title", "publication_year", "normalized_title"),
    )

    def __repr__(self):
        return (
            f"<CanonicalPublication(id={self.id}, doi={self.doi}, "
            f"year={self.publication_year}, title='{self.title[:50]}...')>"
        )


# =============================================================
# AUTORES
# =============================================================

class Author(Base):
    """
    Autor individual. Puede tener IDs de múltiples fuentes.
    """
    __tablename__ = "authors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- Nombre ---
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    normalized_name: Mapped[Optional[str]] = mapped_column(String(300), nullable=True, index=True)

    # --- Identificadores externos ---
    orcid: Mapped[Optional[str]] = mapped_column(String(50), unique=True, nullable=True, index=True)
    openalex_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    scopus_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    wos_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    cvlac_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # --- Afiliación ---
    is_institutional: Mapped[bool] = mapped_column(Boolean, default=False)

    # --- Procedencia de campos ---
    field_provenance: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, comment="{campo: fuente} ej: {orcid: 'openalex', scopus_id: 'scopus'}"
    )

    # --- Control ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- Relaciones ---
    publications = relationship(
        "PublicationAuthor", back_populates="author", lazy="dynamic"
    )
    institutions = relationship(
        "AuthorInstitution", back_populates="author", lazy="dynamic"
    )

    def __repr__(self):
        return f"<Author(id={self.id}, name='{self.name}', orcid={self.orcid})>"


# =============================================================
# AUTOR ↔ INSTITUCIÓN  (N:M)
# =============================================================

class AuthorInstitution(Base):
    """Relación muchos-a-muchos entre autores e instituciones."""
    __tablename__ = "author_institutions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=False
    )
    institution_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("institutions.id", ondelete="CASCADE"), nullable=False
    )

    # --- Relaciones ---
    author = relationship("Author", back_populates="institutions")
    institution = relationship("Institution", back_populates="authors")

    __table_args__ = (
        UniqueConstraint("author_id", "institution_id", name="uq_author_institution"),
    )


# =============================================================
# PUBLICACIÓN ↔ AUTOR  (N:M)
# =============================================================

class PublicationAuthor(Base):
    """Relación muchos-a-muchos entre publicaciones y autores"""
    __tablename__ = "publication_authors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    publication_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("canonical_publications.id", ondelete="CASCADE"), nullable=False
    )
    author_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=False
    )
    is_institutional: Mapped[bool] = mapped_column(Boolean, default=False)
    author_position: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # --- Relaciones ---
    publication = relationship("CanonicalPublication", back_populates="authors")
    author = relationship("Author", back_populates="publications")

    __table_args__ = (
        UniqueConstraint("publication_id", "author_id", name="uq_pub_author"),
    )


# =============================================================
# REGISTROS EXTERNOS (crudos de cada fuente)
# =============================================================

class ExternalRecord(Base):
    """
    Registro tal como viene de cada fuente bibliográfica.
    Se almacena ANTES de reconciliar para no perder datos originales.
    Después de reconciliar, se vincula a una canonical_publication.
    """
    __tablename__ = "external_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # --- ¿De qué fuente viene? ---
    source_name: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )  # openalex, scopus, wos, cvlac, datos_abiertos
    source_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # ID interno de esa fuente

    # --- Dedup key: hash determinista para evitar duplicados ---
    dedup_hash: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, unique=True, index=True
    )

    # --- Campos clave para reconciliación ---
    doi: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    publication_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    authors_text: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )  # Nombres de autores concatenados para fuzzy match
    normalized_authors: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # --- Data cruda completa (JSONB para consultas/índices) ---
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # --- Estado de reconciliación ---
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", index=True
    )  # pending, matched, new_canonical, manual_review, rejected
    canonical_publication_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("canonical_publications.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # --- Info del match ---
    match_type: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )  # doi_exact, fuzzy_high_confidence, fuzzy_combined, manual_review, no_match
    match_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    reconciled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Control ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- Relaciones ---
    canonical_publication = relationship(
        "CanonicalPublication", back_populates="external_records"
    )
    reconciliation_logs = relationship(
        "ReconciliationLog", back_populates="external_record", lazy="dynamic"
    )

    __table_args__ = (
        Index("ix_ext_source_doi", "source_name", "doi"),
        Index("ix_ext_source_year_title", "source_name", "publication_year", "normalized_title"),
        UniqueConstraint("source_name", "source_id", name="uq_source_record"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_ext_status",
        ),
    )

    def __repr__(self):
        return (
            f"<ExternalRecord(id={self.id}, source={self.source_name}, "
            f"doi={self.doi}, status={self.status})>"
        )


# =============================================================
# LOG DE RECONCILIACIÓN (auditoría)
# =============================================================

class ReconciliationLog(Base):
    """
    Registro de auditoría de cada decisión de reconciliación.
    Permite rastrear POR QUÉ se vinculó (o no) un registro.
    """
    __tablename__ = "reconciliation_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    external_record_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("external_records.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    canonical_publication_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("canonical_publications.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # --- Detalle del match (JSONB para consultas/filtrado) ---
    match_type: Mapped[str] = mapped_column(String(50), nullable=False)
    match_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    match_details: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True
    )  # { title_score, author_score, year_match, … }

    # --- Decisión ---
    action: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # linked_existing, created_new, flagged_review, rejected

    # --- Timestamp ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # --- Relaciones ---
    external_record = relationship(
        "ExternalRecord", back_populates="reconciliation_logs"
    )

    __table_args__ = (
        CheckConstraint(
            "action IN ('linked_existing','created_new','flagged_review','rejected','manual_resolved')",
            name="ck_log_action",
        ),
    )

    def __repr__(self):
        return (
            f"<ReconciliationLog(ext={self.external_record_id}, "
            f"canon={self.canonical_publication_id}, "
            f"type={self.match_type}, score={self.match_score})>"
        )
