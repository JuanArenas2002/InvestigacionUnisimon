"""
Domain Services: Lógica de negocio pura del pipeline.

Estos servicios NO dependen de FastAPI, SQLAlchemy u otros frameworks.
Solo trabajan con entidades de dominio.
"""
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging

from ..entities.publication import Publication, Journal, CoveragePeriod
from shared.normalizers import normalize_publication_type


logger = logging.getLogger("pipeline.domain")


@dataclass
class CoverageCheckResult:
    """Resultado de verificación de cobertura."""
    journal_found: bool
    journal_found_via: Optional[str] = None
    scopus_journal_title: Optional[str] = None
    scopus_publisher: Optional[str] = None
    journal_status: Optional[str] = None
    coverage_from: Optional[int] = None
    coverage_to: Optional[int] = None
    coverage_periods: List[CoveragePeriod] = None
    in_coverage: str = "Sin datos"  # "Sí", "No", "Sin datos"
    journal_subject_areas: Optional[str] = None
    resolved_issn: Optional[str] = None
    resolved_eissn: Optional[str] = None
    
    def __post_init__(self):
        if self.coverage_periods is None:
            self.coverage_periods = []


class CoverageService:
    """
    Servicio de dominio: Lógica de verificación de cobertura.
    
    Responsabilidades:
    - Validar si una publicación está en cobertura de Scopus.
    - Enriquecer metadatos de revistas.
    - Determinar reconciliación necesaria.
    """
    
    @staticmethod
    def check_publication_coverage(
        publication: Publication,
        coverage_result: CoverageCheckResult,
    ) -> Publication:
        """
        Integra el resultado de cobertura en la publicación.
        
        Retorna: Publicación enriquecida con información de cobertura.
        """
        publication.journal_found = coverage_result.journal_found
        publication.journal_found_via = coverage_result.journal_found_via
        publication.in_coverage = coverage_result.in_coverage
        
        if coverage_result.journal_found:
            publication.journal = Journal(
                issn=coverage_result.resolved_issn,
                eissn=coverage_result.resolved_eissn,
                title=coverage_result.scopus_journal_title,
                publisher=coverage_result.scopus_publisher,
                status=coverage_result.journal_status,
                coverage_periods=coverage_result.coverage_periods,
            )
        
        return publication
    
    @staticmethod
    def determine_if_in_coverage(
        publication: Publication,
        coverage_periods: List[CoveragePeriod],
    ) -> str:
        """
        Determina si una publicación está dentro de los períodos de cobertura.
        
        Retorna: "Sí", "No" o "Sin datos".
        """
        if not publication.publication_year:
            return "Sin datos"
        
        if not coverage_periods:
            return "Sin datos"
        
        for period in coverage_periods:
            if period.contains_year(publication.publication_year):
                return "Sí"
        
        return "No"
    
    @staticmethod
    def enrich_with_openalex(
        publication: Publication,
        openalex_data: Dict[str, Any],
    ) -> Publication:
        """
        Enriquece una publicación con datos de OpenAlex.
        
        Se usa como fallback cuando Scopus no resuelve.
        """
        if not openalex_data:
            return publication
        
        publication._openalex = openalex_data
        
        # Llenar campos vacíos desde OpenAlex
        if not publication.title and openalex_data.get("title"):
            publication.title = openalex_data["title"]
        
        if not publication.publication_type and openalex_data.get("publication_type"):
            publication.publication_type = normalize_publication_type(openalex_data["publication_type"])
        
        if not publication.citation_count and openalex_data.get("citation_count"):
            publication.citation_count = openalex_data["citation_count"]
        
        return publication


class ExtractionService:
    """
    Servicio de dominio: Orquestación de lógica de extracción.
    
    Responsabilidades:
    - Orquestar colaboración entre extractores.
    - Aplicar deduplicación.
    - Validar registros extraídos.
    """
    
    @staticmethod
    def should_skip_record(
        record: Dict[str, Any],
        seen_keys: set,
        source_name: str,
    ) -> bool:
        """
        Determina si un registro debe ser saltado por deduplicación.
        
        Niveles de deduplicación:
        1. Hash determinista (source + ID + DOI + título + año)
        2. source_name + source_id
        3. source_name + DOI normalizado
        4. source_name + título normalizado + año
        """
        # Nivel 1: Hash determinista
        doi_norm = (record.get("doi") or "").strip().lower().replace("https://doi.org/", "")
        title_norm = (record.get("title") or "").strip().lower()
        
        hash_key = f"{source_name}|{record.get('source_id')}|{doi_norm}|{title_norm}|{record.get('publication_year')}"
        if hash_key in seen_keys:
            return True
        seen_keys.add(hash_key)
        
        # Nivel 2: source + ID
        id_key = f"{source_name}|{record.get('source_id')}"
        if id_key in seen_keys:
            return True
        
        # Nivel 3: source + DOI
        if doi_norm:
            doi_key = f"{source_name}|{doi_norm}"
            if doi_key in seen_keys:
                return True
        
        # Nivel 4: source + título + año
        if title_norm and record.get("publication_year"):
            title_key = f"{source_name}|{title_norm}|{record.get('publication_year')}"
            if title_key in seen_keys:
                return True
        
        return False
    
    @staticmethod
    def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Valida si un registro tiene datos mínimos requeridos.
        
        Retorna: (es_válido, mensaje_error_si_aplica)
        """
        if not record.get("source_id"):
            return False, "Falta source_id"
        
        if not record.get("title"):
            return False, "Falta título"
        
        return True, None


class ReconciliationService:
    """
    Servicio de dominio: Lógica de reconciliación.
    
    Responsabilidades:
    - Definir estrategia de matching (DOI → Fuzzy → Nuevo).
    - Enriquecer publicaciones canónicas.
    - Gestionar field provenance.
    """
    
    @staticmethod
    def should_enrich_field(
        canonical_value: Any,
        source_value: Any,
        field_name: str,
    ) -> bool:
        """
        Determina si un campo de la canónica debe enriquecerse desde la fuente.
        
        Regla: Enriquecer si canonical está vacío y source tiene valor.
        """
        if field_name == "title":
            # Normalizados para comparación
            return not canonical_value or canonical_value == ""
        
        return (canonical_value is None or canonical_value == "") and source_value not in (None, "")
    
    @staticmethod
    def build_field_provenance(
        enriched_fields: Dict[str, str],
        source_name: str,
    ) -> Dict[str, str]:
        """
        Construye el diccionario de provenance para campos enriquecidos.
        
        Retorna: {"field1": "source_name", "field2": "source_name", ...}
        """
        return {field: source_name for field in enriched_fields}
