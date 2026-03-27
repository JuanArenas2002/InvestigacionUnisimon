"""
Plugin de fuente: OpenAlex.

Contiene:
  - Modelo SQLAlchemy OpenalexRecord
  - Constructor de kwargs específicos
  - Auto-registro en SOURCE_REGISTRY

Para agregar otra fuente, copia este archivo como plantilla.
"""

from typing import Optional

from sqlalchemy import Integer, String, Text, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


# =============================================================
# MODELO
# =============================================================

class OpenalexRecord(SourceRecordMixin, Base):
    """
    Registros completos de OpenAlex API.
    Referencia: https://docs.openalex.org/api-entities/works
    """
    __tablename__ = "openalex_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ── Identificadores ──────────────────────────────────────
    openalex_work_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True,
        comment="ID de OpenAlex, ej: https://openalex.org/W12345",
    )
    pmid:  Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    pmcid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # ── Contenido ────────────────────────────────────────────
    abstract: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Resumen reconstruido desde abstract_inverted_index",
    )
    keywords: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Palabras clave separadas por coma (campo 'keywords' de OpenAlex 2024+)",
    )

    # ── Clasificación temática ───────────────────────────────
    concepts: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True,
        comment="[{display_name, score, wikidata_id, level}]",
    )
    topics: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True,
        comment="[{display_name, score, domain, field, subfield}] — OpenAlex Topics 2024+",
    )
    mesh_terms: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True,
        comment="Medical Subject Headings cuando viene de PubMed",
    )

    # ── Editorial ────────────────────────────────────────────
    publisher: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Editorial de la revista (source.host_organization_name)",
    )

    # ── Open Access detallado ────────────────────────────────
    oa_url: Mapped[Optional[str]] = mapped_column(
        String(1000), nullable=True,
        comment="URL directa a versión open access (best_oa_location.url)",
    )
    pdf_url: Mapped[Optional[str]] = mapped_column(
        String(1000), nullable=True,
        comment="URL directa al PDF (best_oa_location.pdf_url)",
    )
    license: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Licencia: cc-by, cc-by-nc, publisher-specific-oa, etc.",
    )

    # ── Métricas ─────────────────────────────────────────────
    referenced_works_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Cantidad de referencias bibliográficas de este trabajo",
    )
    apc_paid_usd: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Cargo por procesamiento de artículo (APC) en USD",
    )

    # ── Financiación ─────────────────────────────────────────
    grants: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True,
        comment="[{funder, funder_display_name, award_id}]",
    )

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
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def _build_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de OpenAlex."""
    kwargs["openalex_work_id"] = record.source_id
    kwargs["pmid"]  = record.pmid
    kwargs["pmcid"] = record.pmcid

    # Fallbacks comunes desde raw_data
    source_info = raw.get("source", {}) if isinstance(raw.get("source"), dict) else {}
    if not kwargs.get("source_journal"):
        kwargs["source_journal"] = source_info.get("display_name") or source_info.get("name")
    kwargs["publisher"] = source_info.get("host_organization_name") or raw.get("publisher")
    if not kwargs.get("issn"):
        kwargs["issn"] = source_info.get("issn_l")
    if kwargs.get("is_open_access") is None:
        oa_info = raw.get("open_access", {})
        if isinstance(oa_info, dict):
            kwargs["is_open_access"] = oa_info.get("is_oa")
    if not kwargs.get("citation_count"):
        kwargs["citation_count"] = int(raw.get("cited_by_count") or 0)

    # Abstract
    kwargs["abstract"] = raw.get("abstract") or raw.get("_abstract")

    # Palabras clave
    kw_list = raw.get("keywords", [])
    kw_parts = []
    if isinstance(kw_list, list):
        for k in kw_list:
            if not k:
                continue
            if isinstance(k, dict):
                text = k.get("display_name") or k.get("keyword") or k.get("id", "")
                if text:
                    kw_parts.append(str(text))
            else:
                kw_parts.append(str(k))
    kwargs["keywords"] = ", ".join(kw_parts) or None

    # Temática
    kwargs["concepts"]   = raw.get("concepts") or raw.get("topics_legacy")
    kwargs["topics"]     = raw.get("topics")
    kwargs["mesh_terms"] = raw.get("mesh") or []

    # Open Access detallado
    best_oa = raw.get("best_oa_location") or {}
    if isinstance(best_oa, dict):
        kwargs["oa_url"]  = best_oa.get("url") or best_oa.get("landing_page_url")
        kwargs["pdf_url"] = best_oa.get("pdf_url")
        kwargs["license"] = best_oa.get("license")

    # Métricas
    kwargs["referenced_works_count"] = raw.get("referenced_works_count")
    apc_info = raw.get("apc_paid") or {}
    if isinstance(apc_info, dict) and apc_info.get("value"):
        kwargs["apc_paid_usd"] = int(apc_info["value"])

    # Financiación
    kwargs["grants"] = raw.get("grants") or []


# =============================================================
# REGISTRO
# =============================================================

SOURCE_REGISTRY.register(SourceDefinition(
    name="openalex",
    model_class=OpenalexRecord,
    id_attr="openalex_work_id",
    author_id_key="openalex",
    build_specific_kwargs=_build_kwargs,
))
