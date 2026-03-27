"""
Plugin de fuente: Datos Abiertos Colombia (datos.gov.co).

Dataset de producción científica publicado por Minciencias vía SODA API.
Referencia: https://www.datos.gov.co/

Campos únicos de esta fuente:
  - Pesos (absoluto, relativo, escalafón) para evaluación docente
  - Ventana de citación (3 o 5 años según convocatoria)
  - ID Minciencias oficial
  - Clase, tipo, subtipo del producto
"""

from typing import Optional

from sqlalchemy import Float, Integer, String, Index, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


# =============================================================
# MODELO
# =============================================================

class DatosAbiertosRecord(SourceRecordMixin, Base):
    """Registros de Datos Abiertos Colombia (datos.gov.co) vía SODA API."""
    __tablename__ = "datos_abiertos_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ── Identificadores ──────────────────────────────────────
    datos_source_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
        comment="ID del registro en el dataset de Datos Abiertos",
    )
    id_minciencias: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True,
        comment="ID oficial del producto en el sistema Minciencias",
    )
    dataset_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True,
        comment="ID del dataset de origen en datos.gov.co",
    )
    isbn: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)

    # ── Tipo de producto ─────────────────────────────────────
    clase: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Clase del producto (ej: Artículo, Libro, Software)",
    )
    product_type: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Tipo de producto bibliográfico según Minciencias",
    )
    subtipo: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Subtipo del producto (ej: Artículo de investigación científica)",
    )

    # ── Ubicación en la publicación ──────────────────────────
    volume:    Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    issue:     Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    pages:     Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    editorial: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    # ── Cobertura geográfica ─────────────────────────────────
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    city:    Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # ── Clasificación Minciencias ────────────────────────────
    classification: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True,
        comment="Clasificación de la revista/producto: A1, A2, B, C, D",
    )
    visibility: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True,
        comment="Visibilidad: Nacional, Internacional, No Aplica",
    )

    # ── Pesos de evaluación (convocatoria Minciencias) ───────
    peso_absoluto: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
        comment="Peso absoluto del producto en la convocatoria de medición de grupos",
    )
    peso_relativo: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
        comment="Peso relativo normalizado por la producción del grupo",
    )
    peso_escalafon: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
        comment="Peso usado para escalafón docente (decreto 1279 Colombia)",
    )
    ventana: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True,
        comment="Ventana de citación usada para el cálculo (ej: '3 años', '5 años')",
    )

    # ── Contexto institucional ───────────────────────────────
    research_group: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Grupo de investigación que reporta el producto",
    )

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
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def _build_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de Datos Abiertos Colombia."""
    kwargs["datos_source_id"] = record.source_id
    kwargs["id_minciencias"]  = raw.get("id_minciencias") or raw.get("id-minciencias")
    kwargs["dataset_id"]      = raw.get("dataset_id")
    kwargs["isbn"]            = raw.get("isbn")

    # Tipo / clase / subtipo
    kwargs["clase"]        = raw.get("clase") or raw.get("class")
    kwargs["product_type"] = raw.get("product_type") or raw.get("tipo_producto") or raw.get("tipo")
    kwargs["subtipo"]      = raw.get("subtipo") or raw.get("subtype")

    # Ubicación
    kwargs["volume"]    = raw.get("volume")    or raw.get("volumen")
    kwargs["issue"]     = raw.get("issue")     or raw.get("numero")
    kwargs["pages"]     = raw.get("pages")     or raw.get("paginas")
    kwargs["editorial"] = raw.get("editorial")

    # Geografía
    kwargs["country"] = raw.get("country") or raw.get("pais")
    kwargs["city"]    = raw.get("city")    or raw.get("ciudad")

    # Clasificación
    kwargs["classification"] = raw.get("classification") or raw.get("clasificacion")
    kwargs["visibility"]     = raw.get("visibility")     or raw.get("visibilidad")

    # Pesos de evaluación
    def _to_float(val):
        try:
            return float(str(val).replace(",", ".")) if val is not None else None
        except (ValueError, TypeError):
            return None

    kwargs["peso_absoluto"]  = _to_float(raw.get("peso_absoluto")  or raw.get("pesoAbsoluto"))
    kwargs["peso_relativo"]  = _to_float(raw.get("peso_relativo")  or raw.get("pesoRelativo"))
    kwargs["peso_escalafon"] = _to_float(raw.get("peso_escalafon") or raw.get("pesoEscalafon"))
    kwargs["ventana"]        = raw.get("ventana") or raw.get("ventana_citacion")

    # Grupo
    kwargs["research_group"] = raw.get("research_group") or raw.get("grupo_investigacion") or raw.get("grupo")


# =============================================================
# REGISTRO
# =============================================================

SOURCE_REGISTRY.register(SourceDefinition(
    name="datos_abiertos",
    model_class=DatosAbiertosRecord,
    id_attr="datos_source_id",
    author_id_key=None,   # Datos Abiertos no tiene ID de autor propio
    build_specific_kwargs=_build_kwargs,
))
