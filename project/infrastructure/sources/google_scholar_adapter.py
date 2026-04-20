import logging
from typing import List, Optional, Dict, Any

from extractors.google_scholar.extractor import GoogleScholarExtractor

from project.domain.models.author import Author
from project.domain.models.publication import Publication
from project.domain.ports.source_port import SourcePort

logger = logging.getLogger(__name__)


class GoogleScholarAdapter(SourcePort):
    """Adapter para Google Scholar (web scraping través de scholarly)."""
    
    SOURCE_NAME = "google_scholar"

    @property
    def source_name(self) -> str:
        return self.SOURCE_NAME

    def fetch_records(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        **kwargs,
    ) -> List[Publication]:
        """
        Extrae publicaciones de Google Scholar.
        
        Args:
            year_from: Año inicial del filtro
            year_to: Año final del filtro
            max_results: Límite de registros
            **kwargs: Debe contener 'scholar_ids' (lista de Scholar IDs)
        
        Returns:
            Lista de Publication objects normalizados
        """
        scholar_ids = kwargs.get("scholar_ids") or []
        if not scholar_ids:
            logger.warning("[GoogleScholarAdapter] No scholar_ids provided")
            return []

        try:
            extractor = GoogleScholarExtractor()
            records = extractor.extract(
                year_from=year_from,
                year_to=year_to,
                max_results=max_results,
                scholar_ids=scholar_ids,
            )
            logger.info("[GoogleScholarAdapter] Extracted %d records", len(records))
            return [self._to_publication(record) for record in records]
        except Exception as e:
            logger.error("[GoogleScholarAdapter] Error fetching records: %s", e)
            return []

    @staticmethod
    def _to_publication(record) -> Publication:
        """
        Convierte StandardRecord a Publication domain object.
        
        Preserva toda la información disponible del extractor:
        - DOI para matching exacto
        - Citation count y métricas
        - Metadatos completos en raw_data
        """
        
        # Procesar autores: cada uno es un dict con name, orcid, is_institutional, etc.
        authors = []
        for author_data in (record.authors or []):
            if isinstance(author_data, dict):
                name = str(author_data.get("name") or "").strip()
                if name:
                    external_ids = {}
                    if author_data.get("orcid"):
                        external_ids["orcid"] = author_data.get("orcid")
                    if author_data.get("google_scholar_id"):
                        external_ids["google_scholar_id"] = author_data.get("google_scholar_id")
                    
                    author = Author(
                        name=name,
                        orcid=author_data.get("orcid"),
                        is_institutional=bool(author_data.get("is_institutional", False)),
                        external_ids=external_ids if external_ids else {},
                        metadata={
                            k: v for k, v in author_data.items() 
                            if v is not None and k not in ["name", "orcid", "google_scholar_id"]
                        },
                    )
                    authors.append(author)
        
        # Retornar Publication con TODOS los campos disponibles
        return Publication(
            # --- Fuente ---
            source_name=record.source_name,
            source_id=record.source_id,
            
            # --- Identificadores ---
            doi=record.doi,
            pmid=getattr(record, "pmid", None),
            pmcid=getattr(record, "pmcid", None),
            
            # --- Metadatos principales ---
            title=record.title,
            publication_year=record.publication_year,
            publication_date=getattr(record, "publication_date", None),
            publication_type=record.publication_type,
            language=getattr(record, "language", None),
            
            # --- Fuente / revista ---
            source_journal=record.source_journal,
            issn=record.issn,
            
            # --- Open Access ---
            is_open_access=getattr(record, "is_open_access", None),
            oa_status=getattr(record, "oa_status", None),
            
            # --- Autores ---
            authors=authors,
            
            # --- Métricas ---
            citation_count=record.citation_count,
            citations_by_year=getattr(record, "citations_by_year", {}),
            
            # --- URL ---
            url=record.url,
            
            # --- Datos crudos preservados ---
            raw_data=record.raw_data or {},
            
            # --- Normalización ---
            normalized_title=getattr(record, "normalized_title", None),
            authors_text=getattr(record, "authors_text", None),
            normalized_authors=getattr(record, "normalized_authors", None),
            
            # --- Timestamp ---
            extracted_at=record.extracted_at,
        )
