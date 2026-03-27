"""
Base SQLAlchemy y mixin compartido para todas las tablas de fuentes.

Este módulo NO importa nada del proyecto — es el punto raíz de la jerarquía
de importaciones para evitar dependencias circulares.

Importado por:
  - db/models.py        (core models)
  - sources/*.py        (cada plugin de fuente)
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Integer,
    String,
    Text,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    declared_attr,
    backref as sa_backref,
)


# =============================================================
# BASE
# =============================================================

class Base(DeclarativeBase):
    """Clase base para todos los modelos. Instancia única compartida."""
    pass


# =============================================================
# MIXIN: COLUMNAS COMUNES PARA TABLAS POR FUENTE
# =============================================================

class SourceRecordMixin:
    """
    Columnas comunes que comparten todas las tablas de registros por fuente.

    Al usar `backref` en `canonical_publication`, cada subclase crea
    automáticamente su relación inversa en CanonicalPublication
    (ej: CanonicalPublication.openalex_records) sin que CanonicalPublication
    necesite conocer las fuentes de antemano.

    Para agregar una nueva fuente basta crear su archivo en sources/
    y registrarla — este mixin hace el resto.
    """

    # --- Deduplicación ---
    dedup_hash: Mapped[Optional[str]] = mapped_column(
        String(64), unique=True, nullable=True, index=True
    )

    # --- Campos clave para reconciliación ---
    doi: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    publication_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    authors_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_authors: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # --- Metadatos tipados (comunes a todas las fuentes) ---
    publication_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    publication_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    source_journal: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    issn: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    language: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    is_open_access: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    oa_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    url: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    # --- Data cruda completa ---
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # --- Estado de reconciliación ---
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="pending", index=True
    )
    match_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    match_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    reconciled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- Timestamps ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- FK a canonical_publication ---
    @declared_attr
    def canonical_publication_id(cls) -> Mapped[Optional[int]]:
        return mapped_column(
            Integer,
            ForeignKey("canonical_publications.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        )

    @declared_attr
    def canonical_publication(cls):
        """
        Relación hacia CanonicalPublication.

        Usa backref para crear automáticamente el atributo inverso en
        CanonicalPublication (ej: CanonicalPublication.openalex_records)
        cuando cada fuente es importada, sin necesidad de modificar
        CanonicalPublication.
        """
        return relationship(
            "CanonicalPublication",
            backref=sa_backref(cls.__tablename__, lazy="select"),
        )

    # --- Propiedades (cada modelo implementa) ---
    @property
    def source_name(self) -> str:
        raise NotImplementedError

    @property
    def source_id(self) -> Optional[str]:
        raise NotImplementedError

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}(id={self.id}, "
            f"doi={self.doi}, status={self.status})>"
        )
