"""
Plugin de fuente: CvLAC (Minciencias Colombia).

Referencia: https://scienti.minciencias.gov.co/cvlac/
"""

from typing import Optional

from sqlalchemy import Integer, String, Text, Index, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


# =============================================================
# MODELO
# =============================================================

class CvlacRecord(SourceRecordMixin, Base):
    """Registros extraídos de CvLAC (Minciencias Colombia) por web scraping."""
    __tablename__ = "cvlac_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ── Identificadores ──────────────────────────────────────
    cvlac_code: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True,
        comment="Código CvLAC del investigador (cod_rh en la URL)",
    )
    cvlac_product_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
        comment="ID único del producto en CvLAC",
    )
    isbn: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)

    # ── Tipo de producto ─────────────────────────────────────
    product_type: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Tipo de producto CvLAC: Artículo, Libro, Capítulo, Patente, Software, etc.",
    )

    # ── Contenido ────────────────────────────────────────────
    abstract: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    keywords: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # ── Ubicación en la publicación ──────────────────────────
    volume:    Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    issue:     Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    pages:     Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    editorial: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    # ── Clasificación Minciencias ────────────────────────────
    visibility: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True,
        comment="Visibilidad declarada: Nacional, Internacional, No Aplica",
    )
    category: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True,
        comment="Categoría de la publicación según convocatoria Minciencias: A1, A2, B, C",
    )

    # ── Contexto institucional ───────────────────────────────
    research_group: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Nombre del grupo de investigación al que se asocia el producto",
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
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def _build_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de CvLAC (Minciencias / Metrik Unisimon)."""
    kwargs["cvlac_product_id"] = record.source_id
    # Metrik Unisimon usa "cc" como cédula del investigador; legacy usa "cvlac_code"
    kwargs["cvlac_code"] = (
        raw.get("cc")
        or raw.get("cvlac_code")
        or (raw.get("_investigador") or {}).get("cc")
    )
    kwargs["isbn"]             = raw.get("isbn")
    kwargs["product_type"]     = raw.get("product_type") or raw.get("tipo_producto") or raw.get("tipo")
    kwargs["abstract"]         = raw.get("abstract") or raw.get("resumen")
    kwargs["keywords"]         = raw.get("keywords") or raw.get("palabras_clave")
    kwargs["volume"]           = raw.get("volume") or raw.get("volumen")
    kwargs["issue"]            = raw.get("issue") or raw.get("numero")
    kwargs["pages"]            = raw.get("pages") or raw.get("paginas")
    kwargs["editorial"]        = raw.get("editorial")
    kwargs["visibility"]       = raw.get("visibility") or raw.get("visibilidad")
    kwargs["category"]         = raw.get("category") or raw.get("categoria")
    kwargs["research_group"]   = raw.get("research_group") or raw.get("grupo_investigacion")


# =============================================================
# REGISTRO
# =============================================================

SOURCE_REGISTRY.register(SourceDefinition(
    name="cvlac",
    model_class=CvlacRecord,
    id_attr="cvlac_product_id",
    author_id_key="cvlac",
    build_specific_kwargs=_build_kwargs,
))
