"""
Plugin de fuente: GrupLAC (Minciencias Colombia).

GrupLAC es el sistema de información de grupos de investigación de Minciencias.
Referencia: https://scienti.minciencias.gov.co/gruplac/

Campos únicos de esta fuente:
  - Información del grupo de investigación (nombre, ID, clasificación)
  - Estado de vinculación del investigador al grupo
  - Líder del grupo

Nota sobre reconciliación:
  GrupLAC reporta los productos de un grupo. El título de la publicación
  está disponible y permite reconciliar con canónicas existentes.
  Su principal aporte es enriquecer:
    1. La vinculación publicación → grupo de investigación
    2. El estado de clasificación del grupo en la convocatoria
"""

from typing import Optional

from sqlalchemy import Integer, String, Text, Index, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


# =============================================================
# MODELO
# =============================================================

class GruplacRecord(SourceRecordMixin, Base):
    """Registros de producción extraídos de GrupLAC (Minciencias)."""
    __tablename__ = "gruplac_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ── Identificadores ──────────────────────────────────────
    gruplac_product_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
        comment="ID único del producto en GrupLAC",
    )
    gruplac_group_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True,
        comment="Código GrupLAC del grupo de investigación",
    )

    # ── Información del grupo ─────────────────────────────────
    group_name: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True,
        comment="Nombre del grupo de investigación en GrupLAC",
    )
    group_leader: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Nombre del líder del grupo de investigación",
    )
    group_classification: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True,
        comment="Clasificación Minciencias del grupo: A1, A, B, C, D, Sin clasificar",
    )
    group_institution: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True,
        comment="Institución a la que pertenece el grupo",
    )

    # ── Estado de vinculación del investigador ────────────────
    author_link_status: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Estado de vinculación del investigador al grupo (ej: Activo, Inactivo)",
    )
    author_role: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Rol del investigador en el grupo (Investigador, Estudiante, etc.)",
    )

    # ── Tipo de producto ─────────────────────────────────────
    product_type: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Tipo de producto según GrupLAC",
    )

    # ── Contenido ────────────────────────────────────────────
    abstract: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_gruplac_year_title", "publication_year", "normalized_title"),
        Index("ix_gruplac_group_id", "gruplac_group_id"),
        CheckConstraint(
            "status IN ('pending','matched','new_canonical','manual_review','rejected')",
            name="ck_gruplac_status",
        ),
    )

    @property
    def source_name(self) -> str:
        return "gruplac"

    @property
    def source_id(self) -> Optional[str]:
        return self.gruplac_product_id


# =============================================================
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def _build_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de GrupLAC."""
    kwargs["gruplac_product_id"]   = record.source_id
    kwargs["gruplac_group_id"]     = raw.get("gruplac_group_id") or raw.get("cod_grupo")
    kwargs["group_name"]           = raw.get("group_name")        or raw.get("nombre_grupo")
    kwargs["group_leader"]         = raw.get("group_leader")      or raw.get("lider_grupo")
    kwargs["group_classification"] = raw.get("group_classification") or raw.get("clasificacion_grupo")
    kwargs["group_institution"]    = raw.get("group_institution") or raw.get("institucion")
    kwargs["author_link_status"]   = raw.get("author_link_status") or raw.get("estado_vinculacion")
    kwargs["author_role"]          = raw.get("author_role")       or raw.get("rol_investigador")
    kwargs["product_type"]         = raw.get("product_type")      or raw.get("tipo_producto")
    kwargs["abstract"]             = raw.get("abstract")          or raw.get("resumen")


# =============================================================
# REGISTRO
# =============================================================

SOURCE_REGISTRY.register(SourceDefinition(
    name="gruplac",
    model_class=GruplacRecord,
    id_attr="gruplac_product_id",
    author_id_key="cvlac",   # GrupLAC usa el mismo ID de investigador que CvLAC
    build_specific_kwargs=_build_kwargs,
))
