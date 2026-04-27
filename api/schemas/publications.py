# ── Merge de publicaciones ─────────────────────────────
from pydantic import BaseModel, Field, computed_field

class MergePublicationsRequest(BaseModel):
    keep_id: int = Field(..., description="ID de la publicación a conservar (keeper)")
    merge_id: int = Field(..., description="ID de la publicación a fusionar/eliminar (removable)")

class MergePublicationsResponse(BaseModel):
    kept_publication_id: int
    merged_publication_id: int
    message: str = ""
"""
Schemas Pydantic para publicaciones canónicas.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


# ── Estado de publicación ────────────────────────────────
class EstadoPublicacion(BaseModel):
    """
    Estado de una publicación canónica.
    `id` es Optional porque en listados paginados se obtiene solo el nombre
    almacenado en el campo `estado_publicacion` del modelo. Para obtener el
    id completo, consultar GET /publications/estados.
    """
    id: Optional[int] = None
    nombre: str


# ── Lectura ─────────────────────────────────────────────────

class PublicationBase(BaseModel):
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    title: str
    normalized_title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    publication_type: Optional[str] = None
    language: Optional[str] = None
    journal_id: Optional[int] = None
    source_journal: Optional[str] = None
    issn: Optional[str] = None
    abstract: Optional[str] = None
    keywords: Optional[str] = None
    publisher: Optional[str] = None
    page_range: Optional[str] = None
    source_url: Optional[str] = None
    is_open_access: Optional[bool] = None
    oa_status: Optional[str] = None
    citation_count: int = 0
    citations_by_source: Optional[dict] = Field(
        None,
        description="Citas por plataforma. Ej: {'openalex': 45, 'scopus': 52, 'wos': 38}. "
                    "citation_count = max de este dict.",
    )
    institutional_authors_count: int = 0
    sources_count: int = 1
    field_provenance: Optional[dict] = Field(
        None,
        description="Procedencia de cada campo: {campo: fuente_que_lo_aportó}",
    )


class CategoriaMinciencias(BaseModel):
    nme_clase_pd: Optional[str] = None
    nme_tipo_medicion_pd: Optional[str] = None
    nme_tipologia_pd: Optional[str] = None
    cod_grupo_gr: Optional[str] = None
    nme_grupo_gr: Optional[str] = None
    nme_convocatoria: Optional[str] = None


class PublicationRead(PublicationBase):
    id: int
    created_at: datetime
    updated_at: datetime
    estado: Optional[EstadoPublicacion] = None
    categoria_minciencias: Optional[CategoriaMinciencias] = Field(
        None,
        description="Categoría Minciencias del artículo según datos abiertos (grupo institucional).",
    )

    model_config = {"from_attributes": True}


class DatosAbiertosInfo(BaseModel):
    """Vínculo con datos abiertos de Minciencias."""
    open_data_record_id: Optional[int] = None
    id_producto_pd: Optional[str] = None
    nme_clase_pd: Optional[str] = None
    nme_tipo_medicion_pd: Optional[str] = None
    nme_tipologia_pd: Optional[str] = None
    cod_grupo_gr: Optional[str] = None
    nme_grupo_gr: Optional[str] = None
    nme_convocatoria: Optional[str] = None
    ano_convo: Optional[str] = None
    match_score: float
    match_method: str


class PublicationDetail(PublicationRead):
    """Publicación con registros externos y autores"""
    external_records: List["ExternalRecordBrief"] = []
    authors: List["PublicationAuthorRead"] = []
    source_links: dict = Field(
        default_factory=dict,
        description="ID por fuente: {openalex: id, scopus: id, ...}. El frontend construye la URL completa.",
    )
    field_provenance: Optional[dict] = Field(
        None,
        description="Procedencia de cada campo: indica qué fuente aportó cada dato al registro canónico.",
    )
    field_conflicts: Optional[dict] = Field(
        None,
        description="Conflictos entre fuentes. Ej: {'is_open_access': {'openalex': 'true', 'scopus': 'false'}}",
    )
    estado: Optional[EstadoPublicacion] = None
    datos_abiertos: List[DatosAbiertosInfo] = Field(
        default_factory=list,
        description="Vínculos con productos registrados en Minciencias (datos abiertos).",
    )


class PublicationAuthorRead(BaseModel):
    author_id: int
    author_name: str
    is_institutional: bool
    author_position: Optional[int] = None
    # Identificadores externos
    orcid: Optional[str] = None
    openalex_id: Optional[str] = None
    scopus_id: Optional[str] = None
    wos_id: Optional[str] = None
    cvlac_id: Optional[str] = None
    model_config = {"from_attributes": True}

    @classmethod
    def from_pa_author(cls, pa, author) -> "PublicationAuthorRead":
        """Constructor conveniente desde un par (PublicationAuthor, Author)."""
        return cls(
            author_id=author.id,
            author_name=author.name,
            is_institutional=pa.is_institutional,
            author_position=pa.author_position,
            orcid=author.orcid,
            openalex_id=author.openalex_id,
            scopus_id=author.scopus_id,
            wos_id=author.wos_id,
            cvlac_id=author.cvlac_id,
        )

    @computed_field(
        description="IDs del autor por plataforma (orcid, openalex, scopus, wos, cvlac). El frontend construye la URL completa."
    )
    @property
    def profile_links(self) -> dict:
        """Devuelve los IDs por plataforma para que el frontend construya las URLs."""
        links = {}
        if self.orcid:
            links["orcid"] = self.orcid
        if self.openalex_id:
            oid = self.openalex_id
            if oid.startswith("https://openalex.org/"):
                oid = oid[len("https://openalex.org/"):]
            links["openalex"] = oid
        if self.scopus_id:
            links["scopus"] = self.scopus_id
        if self.wos_id:
            links["wos"] = self.wos_id
        if self.cvlac_id:
            links["cvlac"] = self.cvlac_id
        return links


class ExternalRecordBrief(BaseModel):
    id: int
    source_name: str
    source_id: Optional[str] = None
    doi: Optional[str] = None
    status: str
    match_type: Optional[str] = None
    match_score: Optional[float] = None

    model_config = {"from_attributes": True}


class PublicationExistsResponse(BaseModel):
    exists: bool
    canonical_id: Optional[int] = None
    source: Optional[str] = None
    match_method: Optional[str] = None


class FieldCoverageResponse(BaseModel):
    total: int
    with_doi: int
    with_journal: int
    with_issn: int
    with_year: int
    with_type: int
    with_language: int
    with_oa_info: int

    @property
    def pct_doi(self) -> float:
        return (self.with_doi / self.total * 100) if self.total else 0.0


class YearDistribution(BaseModel):
    year: int
    count: int


# ── Publicaciones duplicadas ─────────────────────────────────

class DuplicatePublicationPair(BaseModel):
    """Par de publicaciones candidatas a ser duplicadas."""
    canonical_id_1: int
    canonical_id_2: int
    doi_1: Optional[str] = None
    doi_2: Optional[str] = None
    title_1: str
    title_2: str
    type_1: Optional[str] = Field(None, description="Tipo de publicación 1")
    type_2: Optional[str] = Field(None, description="Tipo de publicación 2")
    year_1: Optional[int] = None
    year_2: Optional[int] = None
    sources_1: List[str] = Field(default_factory=list)
    sources_2: List[str] = Field(default_factory=list)
    similarity_score: float = Field(..., description="Similitud del título (0-1)")
    same_doi: bool = Field(False, description="Ambas tienen el mismo DOI")
    same_year: bool = Field(False, description="Mismo año de publicación")
    recommendation: str = Field("", description="Recomendación: merge, review, o keep_both")
    author_similarity: Optional[float] = Field(None, description="Similitud de autores entre ambas publicaciones (0-1)")
    author_diff_1: List[int] = Field(default_factory=list, description="IDs de autores que están en la publicación 1 pero no en la 2")
    author_diff_2: List[int] = Field(default_factory=list, description="IDs de autores que están en la publicación 2 pero no en la 1")
    authors_1: List[PublicationAuthorRead] = Field(default_factory=list, description="Autores de la publicación 1")
    authors_2: List[PublicationAuthorRead] = Field(default_factory=list, description="Autores de la publicación 2")
    # Campos de detección multi-idioma
    match_method: str = Field(
        "title",
        description="Método de detección: 'title' (fuzzy normal), 'doi' (DOI duplicado), 'translated_title' (título traducido)",
    )
    translated_title_1: Optional[str] = Field(
        None,
        description="Traducción al inglés del título 1 (solo presente si match_method='translated_title')",
    )
    translated_title_2: Optional[str] = Field(
        None,
        description="Traducción al inglés del título 2 (solo presente si match_method='translated_title')",
    )


class DuplicatePublicationsSummary(BaseModel):
    """Resumen del análisis de duplicados."""
    total_pairs: int = 0
    high_confidence: int = Field(0, description="Pares con similitud >= 0.95 (casi seguros)")
    medium_confidence: int = Field(0, description="Pares con similitud 0.85-0.95")
    low_confidence: int = Field(0, description="Pares con similitud 0.80-0.85")
    same_doi_different_id: int = Field(0, description="Pares con mismo DOI pero diferente ID canónico")
    translation_matches: int = Field(0, description="Pares detectados únicamente por título traducido (cross-language)")
    pairs: List[DuplicatePublicationPair] = []


class AutoMergeDuplicatesRequest(BaseModel):
    min_similarity: float = Field(
        0.95, ge=0.5, le=1.0,
        description="Similitud mínima para hacer merge automático (0-1). Default 0.95.",
    )
    dry_run: bool = Field(
        False,
        description="Si True, simula el merge y retorna qué se haría sin modificar la BD.",
    )
    only_same_year: bool = Field(
        True,
        description="Si True, solo fusiona pares del mismo año de publicación.",
    )
    skip_doi_conflicts: bool = Field(
        True,
        description=(
            "Si True (default), omite pares donde ambos tienen DOI y son distintos. "
            "DOIs distintos casi siempre indican papers distintos."
        ),
    )
    require_shared_author: bool = Field(
        False,
        description=(
            "Si True, solo fusiona pares que comparten al menos 1 autor institucional. "
            "Recomendado para mayor seguridad."
        ),
    )
    skip_type_conflicts: bool = Field(
        True,
        description=(
            "Si True (default), omite pares donde ambos tienen tipo de publicación "
            "definido y son distintos. Ej: ARTICLE vs REVIEW son productos distintos "
            "aunque el título sea idéntico."
        ),
    )


class AutoMergeDuplicatesResponse(BaseModel):
    dry_run: bool
    pairs_evaluated: int = 0
    pairs_merged: int = 0
    pairs_skipped: int = 0
    merged_pairs: List[dict] = Field(default_factory=list)
    skipped_pairs: List[dict] = Field(default_factory=list)


# --- Rebuild modelos anidados para Pydantic v2 ---
PublicationRead.model_rebuild()
