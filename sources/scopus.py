"""
Plugin de fuente: Scopus (Elsevier).

Referencia: https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl
"""

from typing import Optional

from sqlalchemy import Integer, String, Text, Index, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.models_base import Base, SourceRecordMixin
from db.source_registry import SOURCE_REGISTRY, SourceDefinition


# =============================================================
# MODELO
# =============================================================

class ScopusRecord(SourceRecordMixin, Base):
    """Registros completos de Scopus API (Elsevier)."""
    __tablename__ = "scopus_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # ── Identificadores ──────────────────────────────────────
    scopus_doc_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, unique=True, index=True,
        comment="Scopus document ID (dc:identifier sin prefijo SCOPUS_ID:)",
    )
    eid: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True,
        comment="Electronic ID de Scopus, formato 2-s2.0-XXXXXXXX",
    )
    pmid: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    isbn: Mapped[Optional[str]] = mapped_column(
        String(30), nullable=True,
        comment="ISBN para libros y capítulos de libro",
    )

    # ── Ubicación en la publicación ──────────────────────────
    volume:     Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    issue:      Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    page_range: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # ── Contenido ────────────────────────────────────────────
    abstract: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    author_keywords: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Palabras clave asignadas por el autor (authkeywords)",
    )
    index_keywords: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Vocabulario controlado de Scopus (idxterms)",
    )

    # ── Tipo y subtipo ───────────────────────────────────────
    subtype_description: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Article, Review, Conference Paper, Book Chapter, etc.",
    )

    # ── Evento (conferencias) ────────────────────────────────
    conference_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # ── Editorial y cobertura ────────────────────────────────
    publisher: Mapped[Optional[str]] = mapped_column(
        String(300), nullable=True,
        comment="Editorial que publica la revista/conferencia",
    )
    journal_coverage: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Tipo de cobertura en Scopus: Open Access, Subscribed, Free, etc.",
    )

    # ── Financiación ─────────────────────────────────────────
    funding_agency: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    funding_number: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True,
        comment="Número o código del grant de financiación",
    )

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
# CONSTRUCTOR DE KWARGS ESPECÍFICOS
# =============================================================

def _build_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de Scopus."""
    kwargs["scopus_doc_id"] = record.source_id
    kwargs["volume"]        = raw.get("prism:volume")
    kwargs["issue"]         = raw.get("prism:issueIdentifier")
    kwargs["page_range"]    = raw.get("prism:pageRange")
    kwargs["abstract"]      = raw.get("dc:description")

    # authkeywords puede llegar como str o como lista [{\"$\": \"term\"}, ...]
    _ak = raw.get("authkeywords")
    if isinstance(_ak, list):
        kwargs["author_keywords"] = " | ".join(
            k.get("$", "") if isinstance(k, dict) else str(k)
            for k in _ak if k
        ) or None
    else:
        kwargs["author_keywords"] = _ak

    # Fallbacks comunes
    if not kwargs.get("source_journal"):
        kwargs["source_journal"] = raw.get("prism:publicationName")
    if not kwargs.get("publication_type"):
        kwargs["publication_type"] = raw.get("subtypeDescription")
    if kwargs.get("is_open_access") is None:
        flag = raw.get("openaccessFlag")
        if flag is not None:
            kwargs["is_open_access"] = (
                flag if isinstance(flag, bool) else str(flag).lower() == "true"
            )
    if not kwargs.get("citation_count"):
        kwargs["citation_count"] = int(raw.get("citedby-count") or 0)
    if not kwargs.get("publication_date"):
        kwargs["publication_date"] = raw.get("prism:coverDate")
    if not kwargs.get("issn"):
        kwargs["issn"] = raw.get("prism:issn")

    # Campos específicos Scopus
    kwargs["eid"]                 = raw.get("eid")
    kwargs["pmid"]                = raw.get("pubmed-id") or raw.get("pmid")
    kwargs["isbn"]                = raw.get("prism:isbn") or raw.get("isbn")
    kwargs["index_keywords"]      = raw.get("idxterms") or raw.get("index_keywords")
    kwargs["subtype_description"] = raw.get("subtypeDescription")
    kwargs["conference_name"]     = raw.get("confname") or raw.get("conference_name")
    kwargs["funding_agency"]      = raw.get("fund-agency") or raw.get("fund_agency")
    kwargs["funding_number"]      = raw.get("fund-no") or raw.get("fund_number")
    kwargs["publisher"]           = raw.get("prism:publisher") or raw.get("dc:publisher") or raw.get("publisher")
    kwargs["journal_coverage"]    = raw.get("coverageType") or raw.get("journal_coverage")


# =============================================================
# REGISTRO
# =============================================================

SOURCE_REGISTRY.register(SourceDefinition(
    name="scopus",
    model_class=ScopusRecord,
    id_attr="scopus_doc_id",
    author_id_key="scopus",
    build_specific_kwargs=_build_kwargs,
))
