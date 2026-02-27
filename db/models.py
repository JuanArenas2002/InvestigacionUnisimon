"""
Modelos SQLAlchemy para la base de datos de reconciliación bibliográfica.

Arquitectura de tablas por fuente:
  - journals               : Revistas normalizadas (ISSN, nombre)
  - institutions            : Instituciones (ROR, nombre)
  - canonical_publications  : Registro "dorado" unificado de cada producto
  - authors                 : Autores (con IDs de múltiples fuentes)
  - publication_authors     : Relación N:M publicaciones ↔ autores
  - openalex_records        : Registros completos de OpenAlex
  - scopus_records          : Registros completos de Scopus
  - wos_records             : Registros completos de Web of Science
  - cvlac_records           : Registros completos de CvLAC
  - datos_abiertos_records  : Registros completos de Datos Abiertos
  - reconciliation_log      : Auditoría de cada decisión de match

Flujo:
  1. Cada extractor inserta en su tabla propia con status='pending'
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
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    declared_attr,
)


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
    __tablename__ = "journals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    issn: Mapped[Optional[str]] = mapped_column(String(20), unique=True, nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    publisher: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    publications = relationship("CanonicalPublication", back_populates="journal")

    def __repr__(self):
        return f"<Journal(id={self.id}, issn={self.issn}, name='{self.name[:40]}')>"


# =============================================================
# INSTITUCIONES (normalización)
# =============================================================

class Institution(Base):
    __tablename__ = "institutions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ror_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

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

    # --- Revista ---
    journal_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("journals.id", ondelete="SET NULL"), nullable=True, index=True
    )
    source_journal: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    issn: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # --- Open Access ---
    is_open_access: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    oa_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # --- Métricas ---
    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    institutional_authors_count: Mapped[int] = mapped_column(Integer, default=0)

    # --- Procedencia ---
    field_provenance: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, default=dict,
        comment="Dict campo→fuente. Ej: {'doi':'openalex','source_journal':'scopus'}",
    )

    # --- Control ---
    sources_count: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- Relaciones ---
    journal = relationship("Journal", back_populates="publications")
    authors = relationship("PublicationAuthor", back_populates="publication", lazy="dynamic")

    # Relaciones a cada tabla de fuente
    openalex_records = relationship("OpenalexRecord", back_populates="canonical_publication", lazy="dynamic")
    scopus_records = relationship("ScopusRecord", back_populates="canonical_publication", lazy="dynamic")
    wos_records = relationship("WosRecord", back_populates="canonical_publication", lazy="dynamic")
    cvlac_records = relationship("CvlacRecord", back_populates="canonical_publication", lazy="dynamic")
    datos_abiertos_records = relationship("DatosAbiertosRecord", back_populates="canonical_publication", lazy="dynamic")

    __table_args__ = (
        Index("ix_canon_year_title", "publication_year", "normalized_title"),
    )

    def get_all_source_records(self, session) -> list:
        """Retorna todos los registros de todas las fuentes para esta publicación."""
        records = []
        for model_cls in SOURCE_MODELS.values():
            rows = session.query(model_cls).filter_by(canonical_publication_id=self.id).all()
            records.extend(rows)
        return records

    def __repr__(self):
        return (
            f"<CanonicalPublication(id={self.id}, doi={self.doi}, "
            f"year={self.publication_year}, title='{self.title[:50]}...')>"
        )


# =============================================================
# AUTORES
# =============================================================

class Author(Base):
    __tablename__ = "authors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    normalized_name: Mapped[Optional[str]] = mapped_column(String(300), nullable=True, index=True)

    orcid: Mapped[Optional[str]] = mapped_column(String(50), unique=True, nullable=True, index=True)
    openalex_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    scopus_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    wos_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    cvlac_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    is_institutional: Mapped[bool] = mapped_column(Boolean, default=False)
    field_provenance: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    publications = relationship("PublicationAuthor", back_populates="author", lazy="dynamic")
    institutions = relationship("AuthorInstitution", back_populates="author", lazy="dynamic")

    def __repr__(self):
        return f"<Author(id={self.id}, name='{self.name}', orcid={self.orcid})>"


# =============================================================
# AUTOR ↔ INSTITUCIÓN (N:M)
# =============================================================

class AuthorInstitution(Base):
    __tablename__ = "author_institutions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=False)
    institution_id: Mapped[int] = mapped_column(Integer, ForeignKey("institutions.id", ondelete="CASCADE"), nullable=False)

    author = relationship("Author", back_populates="institutions")
    institution = relationship("Institution", back_populates="authors")

    __table_args__ = (
        UniqueConstraint("author_id", "institution_id", name="uq_author_institution"),
    )


# =============================================================
# PUBLICACIÓN ↔ AUTOR (N:M)
# =============================================================

class PublicationAuthor(Base):
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

    publication = relationship("CanonicalPublication", back_populates="authors")
    author = relationship("Author", back_populates="publications")

    __table_args__ = (
        UniqueConstraint("publication_id", "author_id", name="uq_pub_author"),
    )


# =============================================================
# MIXIN: COLUMNAS COMUNES PARA TABLAS POR FUENTE
# =============================================================

class SourceRecordMixin:
    """
    Columnas comunes que comparten todas las tablas de registros por fuente.
    Incluye campos de reconciliación, metadatos compartidos y raw_data.
    """

    # --- Deduplicación ---
    dedup_hash: Mapped[Optional[str]] = mapped_column(String(64), unique=True, nullable=True, index=True)

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
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending", index=True)
    match_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    match_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    reconciled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # --- Timestamps ---
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
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
        return relationship("CanonicalPublication", back_populates=cls.__tablename__)

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


# =============================================================
# OPENALEX RECORDS
# =============================================================

class OpenalexRecord(SourceRecordMixin, Base):
    """Registros completos de OpenAlex API."""
    __tablename__ = "openalex_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    openalex_work_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
        comment="ID de OpenAlex, ej: https://openalex.org/W12345"
    )
    pmid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    pmcid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    __table_args__ = (
        Index("ix_oalex_year_title", "publication_year", "normalized_title"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_oalex_status",
        ),
    )

    @property
    def source_name(self) -> str:
        return "openalex"

    @property
    def source_id(self) -> Optional[str]:
        return self.openalex_work_id


# =============================================================
# SCOPUS RECORDS
# =============================================================

class ScopusRecord(SourceRecordMixin, Base):
    """Registros completos de Scopus API (Elsevier)."""
    __tablename__ = "scopus_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scopus_doc_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, unique=True, index=True,
        comment="Scopus document ID (dc:identifier sin prefijo)"
    )
    volume: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    issue: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    page_range: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    abstract: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    author_keywords: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_scopus_year_title", "publication_year", "normalized_title"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_scopus_status",
        ),
    )

    @property
    def source_name(self) -> str:
        return "scopus"

    @property
    def source_id(self) -> Optional[str]:
        return self.scopus_doc_id


# =============================================================
# WEB OF SCIENCE RECORDS
# =============================================================

class WosRecord(SourceRecordMixin, Base):
    """Registros completos de Web of Science API (Clarivate)."""
    __tablename__ = "wos_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wos_uid: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, unique=True, index=True,
        comment="WoS UID, ej: WOS:000123456789"
    )

    __table_args__ = (
        Index("ix_wos_year_title", "publication_year", "normalized_title"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_wos_status",
        ),
    )

    @property
    def source_name(self) -> str:
        return "wos"

    @property
    def source_id(self) -> Optional[str]:
        return self.wos_uid


# =============================================================
# CVLAC RECORDS
# =============================================================

class CvlacRecord(SourceRecordMixin, Base):
    """Registros extraídos de CvLAC (Minciencias Colombia) por scraping."""
    __tablename__ = "cvlac_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cvlac_code: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    cvlac_product_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
    )

    __table_args__ = (
        Index("ix_cvlac_year_title", "publication_year", "normalized_title"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_cvlac_status",
        ),
    )

    @property
    def source_name(self) -> str:
        return "cvlac"

    @property
    def source_id(self) -> Optional[str]:
        return self.cvlac_product_id


# =============================================================
# DATOS ABIERTOS RECORDS
# =============================================================

class DatosAbiertosRecord(SourceRecordMixin, Base):
    """Registros de Datos Abiertos Colombia (datos.gov.co) vía SODA API."""
    __tablename__ = "datos_abiertos_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    datos_source_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
    )
    dataset_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)

    __table_args__ = (
        Index("ix_datos_year_title", "publication_year", "normalized_title"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_datos_status",
        ),
    )

    @property
    def source_name(self) -> str:
        return "datos_abiertos"

    @property
    def source_id(self) -> Optional[str]:
        return self.datos_source_id


# =============================================================
# DICCIONARIO DE MODELOS POR FUENTE
# =============================================================

SOURCE_MODELS = {
    "openalex": OpenalexRecord,
    "scopus": ScopusRecord,
    "wos": WosRecord,
    "cvlac": CvlacRecord,
    "datos_abiertos": DatosAbiertosRecord,
}

SOURCE_TABLE_NAMES = [m.__tablename__ for m in SOURCE_MODELS.values()]


# =============================================================
# LOG DE RECONCILIACIÓN (auditoría)
# =============================================================

class ReconciliationLog(Base):
    """
    Auditoría de cada decisión de reconciliación.
    Usa source_name + source_record_id para referenciar el registro origen.
    """
    __tablename__ = "reconciliation_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source_record_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    canonical_publication_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("canonical_publications.id", ondelete="SET NULL"),
        nullable=True, index=True,
    )

    match_type: Mapped[str] = mapped_column(String(50), nullable=False)
    match_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    match_details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    action: Mapped[str] = mapped_column(String(30), nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        CheckConstraint(
            "action IN ('linked_existing','created_new','flagged_review','rejected','manual_resolved')",
            name="ck_log_action",
        ),
        Index("ix_recon_log_source", "source_name", "source_record_id"),
    )

    def get_source_record(self, session):
        """Carga el registro de origen desde su tabla correspondiente."""
        model_cls = SOURCE_MODELS.get(self.source_name)
        if model_cls:
            return session.query(model_cls).get(self.source_record_id)
        return None

    def __repr__(self):
        return (
            f"<ReconciliationLog(source={self.source_name}:{self.source_record_id}, "
            f"canon={self.canonical_publication_id}, type={self.match_type})>"
        )


# =============================================================
# HELPERS PARA CONSULTAS CROSS-SOURCE
# =============================================================

def get_source_model(source_name: str):
    """Retorna la clase del modelo para una fuente dada."""
    model = SOURCE_MODELS.get(source_name)
    if not model:
        raise ValueError(f"Fuente desconocida: {source_name}. Válidas: {list(SOURCE_MODELS.keys())}")
    return model


def count_source_records_for_canonical(session, canonical_id: int) -> int:
    """Cuenta el total de registros de TODAS las fuentes para una publicación canónica."""
    total = 0
    for model_cls in SOURCE_MODELS.values():
        total += (
            session.query(func.count(model_cls.id))
            .filter_by(canonical_publication_id=canonical_id)
            .scalar() or 0
        )
    return total


def find_record_by_doi_across_sources(session, doi: str, exclude_source: str = None,
                                       exclude_id: int = None):
    """
    Busca en TODAS las tablas de fuentes un registro con este DOI
    que ya esté reconciliado (canonical_publication_id IS NOT NULL).
    """
    for sname, model_cls in SOURCE_MODELS.items():
        q = (
            session.query(model_cls)
            .filter(model_cls.doi == doi, model_cls.canonical_publication_id.isnot(None))
        )
        if exclude_source == sname and exclude_id is not None:
            q = q.filter(model_cls.id != exclude_id)
        result = q.first()
        if result:
            return result
    return None


def get_all_source_records_for_canonical(session, canonical_id: int) -> list:
    """Retorna todos los registros de todas las fuentes para una publicación canónica."""
    records = []
    for model_cls in SOURCE_MODELS.values():
        rows = session.query(model_cls).filter_by(canonical_publication_id=canonical_id).all()
        records.extend(rows)
    return records


def count_all_source_records(session) -> int:
    """Cuenta el total de registros en todas las tablas de fuentes."""
    total = 0
    for model_cls in SOURCE_MODELS.values():
        total += session.query(func.count(model_cls.id)).scalar() or 0
    return total


def count_source_records_by_status(session) -> dict:
    """Conteo de registros agrupados por status, sumando todas las fuentes."""
    from collections import Counter
    counts = Counter()
    for model_cls in SOURCE_MODELS.values():
        rows = (
            session.query(model_cls.status, func.count(model_cls.id))
            .group_by(model_cls.status)
            .all()
        )
        for status, cnt in rows:
            counts[status] += cnt
    return dict(counts)


def count_source_records_by_source(session) -> dict:
    """Conteo de registros por fuente."""
    counts = {}
    for sname, model_cls in SOURCE_MODELS.items():
        counts[sname] = session.query(func.count(model_cls.id)).scalar() or 0
    return counts
