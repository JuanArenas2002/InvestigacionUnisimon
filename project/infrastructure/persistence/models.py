"""
Modelos SQLAlchemy para la base de datos de reconciliación bibliográfica.

Arquitectura:
  - journals               : Revistas normalizadas (ISSN, nombre)
  - institutions            : Instituciones (ROR, nombre)
  - canonical_publications  : Registro "dorado" unificado de cada producto
  - authors                 : Autores (IDs de fuentes en JSONB external_ids)
  - publication_authors     : Relación N:M publicaciones ↔ autores
  - [tablas de fuente]      : Auto-descubiertas desde sources/ (ver abajo)
  - reconciliation_log      : Auditoría de cada decisión de match

Para agregar una nueva fuente de datos:
  1. Crea  sources/nueva_fuente.py  (modelo + builder + registro).
  2. Ejecuta la migración SQL correspondiente.
  No modifiques este archivo ni ningún otro.
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
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship,
)
from sqlalchemy.ext.hybrid import hybrid_property

from project.infrastructure.persistence.models_base import Base, SourceRecordMixin  # noqa: F401
from project.infrastructure.persistence.source_registry import SOURCE_REGISTRY


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

    Las relaciones hacia las tablas de fuente (openalex_records,
    scopus_records, etc.) son creadas automáticamente por el backref
    definido en SourceRecordMixin cuando cada fuente es importada.
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

    # --- Contenido enriquecido ---
    abstract: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Resumen. Primera fuente que lo aporte gana (provenance registrado).",
    )
    keywords: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Palabras clave separadas por coma.",
    )
    source_url: Mapped[Optional[str]] = mapped_column(
        String(1000), nullable=True,
        comment="URL a la página de la publicación en la plataforma de origen.",
    )
    page_range: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Rango de páginas (ej: '123-145').",
    )
    publisher: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Editorial / publisher.",
    )
    journal_coverage: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Cobertura de la revista (ej: 'SCI-E', 'ESCI').",
    )

    # --- Clasificación temática ---
    knowledge_area: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Área de conocimiento principal (Scopus subject area / OpenAlex domain).",
    )
    cine_code: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True,
        comment="Código CINE / ISCED (clasificación disciplinar Minciencias).",
    )

    # --- Autores resumidos ---
    first_author: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Nombre del primer autor.",
    )
    corresponding_author: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Nombre del autor de correspondencia.",
    )
    coauthorships_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Número total de co-autores del trabajo.",
    )

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

    # --- Conflictos entre fuentes ---
    field_conflicts: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, default=dict,
        comment="Conflictos detectados entre fuentes.",
    )

    # --- Citas por fuente ---
    citations_by_source: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, default=dict,
        comment="Citas reportadas por cada fuente. citation_count = max de este dict.",
    )

    # --- Control ---
    sources_count: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- Estado de publicación ---
    estado_publicacion: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="Avalado",
        server_default="Avalado",
        comment="Estado de la publicación: Avalado, Revisión, Rechazado",
    )

    # --- Relaciones core ---
    journal = relationship("Journal", back_populates="publications")
    authors = relationship("PublicationAuthor", back_populates="publication", lazy="dynamic")

    # Nota: las relaciones hacia openalex_records, scopus_records, etc.
    # son creadas automáticamente por SourceRecordMixin.canonical_publication
    # (via backref) cuando cada módulo sources/*.py es importado.

    __table_args__ = (
        Index("ix_canon_year_title", "publication_year", "normalized_title"),
    )

    def get_all_source_records(self, session) -> list:
        """Retorna todos los registros de todas las fuentes para esta publicación."""
        records = []
        for model_cls in SOURCE_REGISTRY.models.values():
            rows = session.query(model_cls).filter_by(canonical_publication_id=self.id).all()
            records.extend(rows)
        return records

    def __repr__(self):
        return (
            f"<CanonicalPublication(id={self.id}, doi={self.doi}, "
            f"year={self.publication_year}, title='{self.title[:50]}...')>"
        )


# =============================================================
# PARES DE PUBLICACIONES POSIBLEMENTE DUPLICADAS
# =============================================================

class PossibleDuplicatePair(Base):
    """
    Par de publicaciones canónicas detectadas como posiblemente duplicadas.
    Persiste casos detectados durante reconciliación para consulta posterior.
    canonical_id_1 < canonical_id_2 (invariante de unicidad).
    """
    __tablename__ = "possible_duplicate_pairs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    canonical_id_1: Mapped[int] = mapped_column(
        Integer, ForeignKey("canonical_publications.id", ondelete="CASCADE"), nullable=False, index=True
    )
    canonical_id_2: Mapped[int] = mapped_column(
        Integer, ForeignKey("canonical_publications.id", ondelete="CASCADE"), nullable=False, index=True
    )
    similarity_score: Mapped[float] = mapped_column(Float, nullable=False)
    match_method: Mapped[str] = mapped_column(String(50), nullable=False, default="title")
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending",
        comment="pending | merged | dismissed",
    )

    __table_args__ = (
        UniqueConstraint("canonical_id_1", "canonical_id_2", name="uq_dup_pair"),
        CheckConstraint("canonical_id_1 < canonical_id_2", name="chk_dup_order"),
    )

    __mapper_args__ = {"confirm_deleted_rows": False}

    def __repr__(self):
        return (
            f"<PossibleDuplicatePair(id={self.id}, "
            f"{self.canonical_id_1}↔{self.canonical_id_2}, "
            f"score={self.similarity_score:.1f}, status={self.status})>"
        )


# =============================================================
# AUTORES
# =============================================================

class Author(Base):
    __tablename__ = "authors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    normalized_name: Mapped[Optional[str]] = mapped_column(String(300), nullable=True, index=True)

    cedula: Mapped[Optional[str]] = mapped_column(
        String(30), unique=True, nullable=True, index=True,
        comment="Cédula de ciudadanía colombiana del autor.",
    )

    orcid: Mapped[Optional[str]] = mapped_column(String(50), unique=True, nullable=True, index=True)

    # IDs de fuentes externas en un único JSONB.
    # Formato: {"openalex": "A123", "scopus": "456", "wos": "...", "cvlac": "..."}
    # Al agregar una nueva fuente sólo hay que añadir la clave — no requiere
    # migración de esquema en esta tabla.
    external_ids: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True, default=dict,
        comment="IDs de autor por fuente. {\"openalex\":\"A123\",\"scopus\":\"456\"}",
    )

    is_institutional: Mapped[bool] = mapped_column(Boolean, default=False)
    field_provenance: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Estado de verificación del autor
    verification_status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="auto_detected",
        comment="auto_detected | verified | needs_review | flagged",
    )

    # Referencia a posible duplicado detectado por similitud de nombre
    possible_duplicate_of: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="SET NULL"), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    publications = relationship("PublicationAuthor", back_populates="author", lazy="selectin")
    institutions = relationship("AuthorInstitution", back_populates="author", lazy="dynamic")

    def get_external_id(self, source: str) -> Optional[str]:
        """Retorna el ID de autor para la fuente indicada, o None."""
        return (self.external_ids or {}).get(source)

    def set_external_id(self, source: str, value: str) -> None:
        """Asigna el ID de autor para la fuente indicada (reemplaza el dict)."""
        self.external_ids = {**(self.external_ids or {}), source: value}

    # ── Propiedades de compatibilidad ────────────────────────
    # Permiten que el código existente siga usando  author.openalex_id
    # sin modificar cada archivo.  Las columnas ya no existen en la BD;
    # estos accessors delegan en external_ids.

    @property
    def openalex_id(self) -> Optional[str]:
        return (self.external_ids or {}).get("openalex")

    @openalex_id.setter
    def openalex_id(self, value: Optional[str]) -> None:
        self.external_ids = {**(self.external_ids or {}), "openalex": value}

    @property
    def scopus_id(self) -> Optional[str]:
        return (self.external_ids or {}).get("scopus")

    @scopus_id.setter
    def scopus_id(self, value: Optional[str]) -> None:
        self.external_ids = {**(self.external_ids or {}), "scopus": value}

    @property
    def wos_id(self) -> Optional[str]:
        return (self.external_ids or {}).get("wos")

    @wos_id.setter
    def wos_id(self, value: Optional[str]) -> None:
        self.external_ids = {**(self.external_ids or {}), "wos": value}

    @property
    def cvlac_id(self) -> Optional[str]:
        return (self.external_ids or {}).get("cvlac")

    @cvlac_id.setter
    def cvlac_id(self, value: Optional[str]) -> None:
        self.external_ids = {**(self.external_ids or {}), "cvlac": value}

    @property
    def google_scholar_id(self) -> Optional[str]:
        return (self.external_ids or {}).get("google_scholar")

    @google_scholar_id.setter
    def google_scholar_id(self, value: Optional[str]) -> None:
        self.external_ids = {**(self.external_ids or {}), "google_scholar": value}

    def __repr__(self):
        return f"<Author(id={self.id}, name='{self.name}', orcid={self.orcid})>"


# =============================================================
# AUTOR ↔ INSTITUCIÓN (N:M)
# =============================================================

class AuthorInstitution(Base):
    __tablename__ = "author_institutions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=False
    )
    institution_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("institutions.id", ondelete="CASCADE"), nullable=False
    )

    # Rango de vigencia de la afiliación
    start_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    end_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)

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
# LOG DE RECONCILIACIÓN (auditoría)
# =============================================================

class ReconciliationLog(Base):
    """Auditoría de cada decisión de reconciliación."""
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
        model_cls = SOURCE_REGISTRY.models.get(self.source_name)
        if model_cls:
            return session.query(model_cls).get(self.source_record_id)
        return None

    def __repr__(self):
        return (
            f"<ReconciliationLog(source={self.source_name}:{self.source_record_id}, "
            f"canon={self.canonical_publication_id}, type={self.match_type})>"
        )


# =============================================================
# BIBLIOMETRIC ANALYSIS: DISCIPLINARY FIELDS & RESEARCH THRESHOLDS
# =============================================================

class DisciplinaryField(Base):
    __tablename__ = "disciplinary_fields"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_code: Mapped[str] = mapped_column(
        String(50), nullable=False, unique=True, index=True,
        comment="CIENCIAS_SALUD, CIENCIAS_BASICAS, INGENIERIA, CIENCIAS_SOCIALES, ARTES_HUMANIDADES",
    )
    field_name_es: Mapped[str] = mapped_column(String(200), nullable=False)
    field_name_en: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    typical_h_index_range: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    citation_culture: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    thresholds = relationship("FieldParameter", back_populates="field", lazy="selectin")


class FieldParameter(Base):
    __tablename__ = "field_parameters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("disciplinary_fields.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    parameter_name: Mapped[str] = mapped_column(String(100), nullable=False)
    parameter_type: Mapped[str] = mapped_column(String(15), nullable=False, default="float")
    value: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    field = relationship("DisciplinaryField", back_populates="thresholds")

    __table_args__ = (
        UniqueConstraint("field_id", "parameter_name", name="uq_field_parameter"),
        Index("ix_field_param_name", "field_id", "parameter_name"),
    )

    def get_value(self):
        import json
        if self.parameter_type == "int":
            return int(self.value)
        elif self.parameter_type == "float":
            return float(self.value)
        elif self.parameter_type == "bool":
            return self.value.lower() in ("true", "1", "yes")
        elif self.parameter_type == "json":
            return json.loads(self.value)
        return self.value

    def __repr__(self):
        return f"<FieldParameter(field_id={self.field_id}, param='{self.parameter_name}', value={self.value})>"


ResearchThreshold = FieldParameter  # alias de compatibilidad


# =============================================================
# USUARIOS
# =============================================================

import bcrypt  # noqa: E402


# =============================================================
# AUDIT LOG DE AUTORES
# =============================================================

class AuthorAuditLog(Base):
    """Historial de cada cambio sobre un autor (creación, actualización, fusión)."""
    __tablename__ = "author_audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=True, index=True
    )
    change_type: Mapped[str] = mapped_column(
        String(30), nullable=False,
        comment="created | updated | merged_into | merged_from | verified | deleted",
    )
    before_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    after_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    field_changes: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    changed_by: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_author_audit_author_id", "author_id"),
        Index("ix_author_audit_created", "created_at"),
    )

    def __repr__(self):
        return f"<AuthorAuditLog(author_id={self.author_id}, type={self.change_type})>"


# =============================================================
# CONFLICTOS ENTRE FUENTES PARA AUTORES
# =============================================================

class AuthorConflict(Base):
    """Registra cuando dos fuentes aportan valores distintos para el mismo campo de un autor."""
    __tablename__ = "author_conflicts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=False, index=True
    )
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    existing_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    existing_source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    new_source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolution: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_author_conflicts_unresolved", "resolved", "created_at"),
    )

    def __repr__(self):
        return (
            f"<AuthorConflict(author_id={self.author_id}, field={self.field_name}, "
            f"resolved={self.resolved})>"
        )


# =============================================================
# CREDENCIALES DE INVESTIGADORES
# =============================================================

class ResearcherCredential(Base):
    """
    Credenciales de acceso para investigadores.
    Un investigador (Author) puede tener múltiples credenciales,
    pero solo una puede estar activa a la vez.
    El factor de login es la cédula del investigador.
    """
    __tablename__ = "researcher_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    author_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("authors.id", ondelete="CASCADE"), nullable=False, index=True
    )

    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)

    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, index=True,
        comment="Solo una credencial activa por investigador.",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    activated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Cuándo se activó esta credencial."
    )
    last_login: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Último acceso exitoso."
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Expiración opcional de la credencial."
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    author = relationship("Author", lazy="selectin")

    __table_args__ = (
        Index("ix_researcher_credentials_author_active", "author_id", "is_active"),
    )

    def verify_password(self, password: str) -> bool:
        """Verifica si la contraseña coincide con el hash."""
        return bcrypt.checkpw(password.encode(), self.password_hash.encode())

    def is_expired(self) -> bool:
        """Retorna True si la credencial ha expirado."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    @classmethod
    def hash_password(cls, password: str) -> str:
        """Genera hash bcrypt de la contraseña."""
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def __repr__(self):
        return (
            f"<ResearcherCredential(id={self.id}, author_id={self.author_id}, "
            f"is_active={self.is_active})>"
        )


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(150), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def verify_password(self, password: str) -> bool:
        return bcrypt.checkpw(password.encode(), self.password_hash.encode())

    @classmethod
    def hash_password(cls, password: str) -> str:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}')>"


# =============================================================
# AUTO-DESCUBRIMIENTO DE FUENTES
# =============================================================
# Importar el paquete sources/ dispara sources/__init__.py que usa
# pkgutil para importar cada módulo fuente. Cada módulo llama
# SOURCE_REGISTRY.register(...), creando automáticamente el backref
# en CanonicalPublication y registrando el modelo en el registry.
#
# IMPORTANTE: este import va DESPUÉS de definir CanonicalPublication
# para que el forward-ref "CanonicalPublication" en SourceRecordMixin
# resuelva correctamente.
# =============================================================

import sources  # noqa: F401, E402


# =============================================================
# EXPORTS DE COMPATIBILIDAD
# =============================================================

SOURCE_MODELS = SOURCE_REGISTRY.models
SOURCE_TABLE_NAMES = [m.__tablename__ for m in SOURCE_MODELS.values()]

# Re-exportar clases de fuente para importaciones directas existentes
from sources.openalex import OpenalexRecord          # noqa: F401, E402
from sources.scopus import ScopusRecord              # noqa: F401, E402
from sources.wos import WosRecord                    # noqa: F401, E402
from sources.cvlac import CvlacRecord                # noqa: F401, E402
from sources.datos_abiertos import DatosAbiertosRecord  # noqa: F401, E402


# =============================================================
# HELPERS PARA CONSULTAS CROSS-SOURCE
# =============================================================

def get_source_model(source_name: str):
    """Retorna la clase del modelo para una fuente dada."""
    model = SOURCE_REGISTRY.models.get(source_name)
    if not model:
        raise ValueError(
            f"Fuente desconocida: {source_name}. Válidas: {list(SOURCE_REGISTRY.names)}"
        )
    return model


def count_source_records_for_canonical(session, canonical_id: int) -> int:
    """Cuenta el total de registros de TODAS las fuentes para una publicación canónica."""
    total = 0
    for model_cls in SOURCE_REGISTRY.models.values():
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
    for sname, model_cls in SOURCE_REGISTRY.models.items():
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
    for model_cls in SOURCE_REGISTRY.models.values():
        rows = session.query(model_cls).filter_by(canonical_publication_id=canonical_id).all()
        records.extend(rows)
    return records


def count_all_source_records(session) -> int:
    """Cuenta el total de registros en todas las tablas de fuentes."""
    total = 0
    for model_cls in SOURCE_REGISTRY.models.values():
        total += session.query(func.count(model_cls.id)).scalar() or 0
    return total


def count_source_records_by_status(session) -> dict:
    """Conteo de registros agrupados por status, sumando todas las fuentes."""
    from collections import Counter
    counts = Counter()
    for model_cls in SOURCE_REGISTRY.models.values():
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
    return {
        sname: session.query(func.count(model_cls.id)).scalar() or 0
        for sname, model_cls in SOURCE_REGISTRY.models.items()
    }


def get_thresholds_by_field(session, field_code: str) -> dict:
    """Obtiene todos los parámetros de una disciplina como diccionario."""
    field = session.query(DisciplinaryField).filter_by(field_code=field_code).first()
    if not field:
        raise ValueError(f"Campo disciplinar no encontrado: {field_code}")
    return {param.parameter_name: param.get_value() for param in field.thresholds}
