"""
Plugin de fuente: Web of Science (Clarivate).

Referencia: https://developer.clarivate.com/apis/wos
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

class WosRecord(SourceRecordMixin, Base):
    """Registros completos de Web of Science API (Clarivate)."""
    __tablename__ = "wos_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ── Identificadores ──────────────────────────────────────
    wos_uid: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, unique=True, index=True,
        comment="WoS UID, ej: WOS:000123456789",
    )
    accession_number: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Número de acceso interno WoS",
    )
    pmid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # ── Ubicación en la publicación ──────────────────────────
    volume:     Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    issue:      Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    page_range: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    early_access_date: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True,
        comment="Fecha de publicación anticipada (Early Access)",
    )
    issn_electronic: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True,
        comment="ISSN de la versión electrónica",
    )

    # ── Contenido ────────────────────────────────────────────
    abstract:        Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    author_keywords: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # ── Clasificación WoS ───────────────────────────────────
    wos_categories: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Categorías Web of Science separadas por '; '",
    )
    research_areas: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Áreas de investigación WoS separadas por '; '",
    )

    # ── Editorial / Evento ───────────────────────────────────
    publisher:        Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    conference_title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # ── Métricas de citas ────────────────────────────────────
    times_cited_all_databases: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Citas contadas en todas las colecciones WoS",
    )
    citing_patents_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Número de patentes que citan este artículo",
    )

    # ── Financiación ─────────────────────────────────────────
    funding_orgs: Mapped[Optional[dict]] = mapped_column(
        JSONB, nullable=True,
        comment="[{organization, grant_numbers:[]}]",
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
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def _build_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de Web of Science."""
    kwargs["wos_uid"] = record.source_id

    # Fallbacks comunes
    if not kwargs.get("source_journal"):
        kwargs["source_journal"] = raw.get("sourceTitle") or raw.get("source_title")
    if not kwargs.get("publication_date"):
        kwargs["publication_date"] = raw.get("publishDate") or raw.get("publish_date")

    # Campos específicos WoS
    kwargs["accession_number"]          = raw.get("accessionNumber") or raw.get("accession_number")
    kwargs["pmid"]                      = raw.get("pmid")
    kwargs["volume"]                    = raw.get("volume")
    kwargs["issue"]                     = raw.get("issue")
    kwargs["page_range"]                = raw.get("pageRange") or raw.get("page_range")
    kwargs["early_access_date"]         = raw.get("earlyAccessDate") or raw.get("early_access_date")
    kwargs["issn_electronic"]           = raw.get("eissn") or raw.get("issn_electronic")
    kwargs["abstract"]                  = raw.get("abstract")
    kwargs["author_keywords"]           = raw.get("authorKeywords") or raw.get("author_keywords")
    kwargs["wos_categories"]            = raw.get("wosCategories") or raw.get("wos_categories")
    kwargs["research_areas"]            = raw.get("researchAreas") or raw.get("research_areas")
    kwargs["publisher"]                 = raw.get("publisher")
    kwargs["conference_title"]          = raw.get("conferenceTitle") or raw.get("conference_title")
    kwargs["times_cited_all_databases"] = raw.get("timesCitedAllDatabases") or raw.get("times_cited")
    kwargs["citing_patents_count"]      = raw.get("citingPatentsCount") or raw.get("citing_patents")
    kwargs["funding_orgs"]              = raw.get("fundingOrgs") or raw.get("funding_orgs") or []


# =============================================================
# REGISTRO
# =============================================================

SOURCE_REGISTRY.register(SourceDefinition(
    name="wos",
    model_class=WosRecord,
    id_attr="wos_uid",
    author_id_key="wos",
    build_specific_kwargs=_build_kwargs,
))
