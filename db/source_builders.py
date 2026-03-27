"""
Constructores de campos específicos por fuente.

Cada función recibe:
  - record : StandardRecord del extractor
  - raw    : dict con raw_data original
  - kwargs : dict de campos comunes ya populados (se modifica en-place)

Responsabilidades:
  - Agregar el ID específico de la fuente (ej: openalex_work_id)
  - Agregar campos propios del modelo (abstract, keywords, etc.)
  - Aplicar fallbacks sobre campos comunes si estos llegaron vacíos

Para agregar una nueva fuente, copia una de estas funciones como plantilla,
ajusta los nombres de campos y regístrala en db/models.py con SOURCE_REGISTRY.register().
"""


# =============================================================
# OPENALEX
# =============================================================

def build_openalex_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de OpenAlex."""
    kwargs["openalex_work_id"] = record.source_id
    kwargs["pmid"]  = record.pmid
    kwargs["pmcid"] = record.pmcid

    # Fallbacks comunes desde raw_data
    source_info = raw.get("source", {}) if isinstance(raw.get("source"), dict) else {}
    if not kwargs.get("source_journal"):
        kwargs["source_journal"] = source_info.get("display_name") or source_info.get("name")
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

    # Palabras clave (campo introducido en OpenAlex 2024)
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
    kwargs["concepts"]  = raw.get("concepts") or raw.get("topics_legacy")
    kwargs["topics"]    = raw.get("topics")
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
# SCOPUS
# =============================================================

def build_scopus_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de Scopus."""
    kwargs["scopus_doc_id"] = record.source_id
    kwargs["volume"]        = raw.get("prism:volume")
    kwargs["issue"]         = raw.get("prism:issueIdentifier")
    kwargs["page_range"]    = raw.get("prism:pageRange")
    kwargs["abstract"]      = raw.get("dc:description")

    # authkeywords puede llegar como str o como lista [{"$": "term"}, ...]
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
            kwargs["is_open_access"] = flag if isinstance(flag, bool) else str(flag).lower() == "true"
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


# =============================================================
# WEB OF SCIENCE
# =============================================================

def build_wos_kwargs(record, raw: dict, kwargs: dict) -> None:
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
# CVLAC
# =============================================================

def build_cvlac_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de CvLAC (Minciencias)."""
    kwargs["cvlac_product_id"] = record.source_id
    kwargs["cvlac_code"]       = raw.get("cvlac_code")
    kwargs["isbn"]             = raw.get("isbn")
    kwargs["product_type"]     = raw.get("product_type") or raw.get("tipo_producto")
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
# DATOS ABIERTOS COLOMBIA
# =============================================================

def build_datos_abiertos_kwargs(record, raw: dict, kwargs: dict) -> None:
    """Campos específicos de Datos Abiertos Colombia."""
    kwargs["datos_source_id"] = record.source_id
    kwargs["dataset_id"]      = raw.get("dataset_id")
    kwargs["isbn"]            = raw.get("isbn")
    kwargs["product_type"]    = raw.get("product_type") or raw.get("tipo_producto")
    kwargs["volume"]          = raw.get("volume") or raw.get("volumen")
    kwargs["issue"]           = raw.get("issue") or raw.get("numero")
    kwargs["pages"]           = raw.get("pages") or raw.get("paginas")
    kwargs["editorial"]       = raw.get("editorial")
    kwargs["country"]         = raw.get("country") or raw.get("pais")
    kwargs["city"]            = raw.get("city") or raw.get("ciudad")
    kwargs["classification"]  = raw.get("classification") or raw.get("clasificacion")
    kwargs["visibility"]      = raw.get("visibility") or raw.get("visibilidad")
    kwargs["research_group"]  = raw.get("research_group") or raw.get("grupo_investigacion")
