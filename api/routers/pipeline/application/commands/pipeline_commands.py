"""
Application Layer: Use Cases y Commands

Commands son acciones inmutables que representan intenciones del usuario.
Use Cases orquestan servicios de dominio e infraestructura.
"""
from typing import List, Dict, Any, Optional
import logging

from ...domain import Publication, CoverageService, ReconciliationService, ExtractionService
from ...shared.dtos import PublicationIn, CoverageResultOut, ReconciliationStats


logger = logging.getLogger("pipeline.application")


class CheckPublicationCoverageCommand:
    """
    Comando: Verificar cobertura de una publicación en Scopus.
    
    Responsabilidades:
    - Orquestar busca en Scopus.
    - Integrar otros servicios (rescue OpenAlex, etc.).
    - Retornar resultado estructurado.
    """
    
    def __init__(self, scope_extractor, openalex_service):
        """
        Args:
            scope_extractor: Adaptador de extractor Scopus
            openalex_service: Servicio de OpenAlex
        """
        self.scopus_extractor = scope_extractor
        self.openalex_service = openalex_service
    
    def execute(
        self,
        publications: List[Dict[str, Any]],
        max_workers: int = 1,
    ) -> List[CoverageResultOut]:
        """
        Executa verificación de cobertura para lista de publicaciones.
        
        Retorna: Lista de resultados de cobertura.
        """
        # Aquí irá la orquestación de lógica
        # Se llamará al extractor Scopus y se integrarán los resultados
        logger.info(f"Iniciando verificación de cobertura para {len(publications)} publicaciones")
        
        results = []
        # TODO: Implementar orquestación
        
        return results


class ExtractFromSourceCommand:
    """
    Comando: Extraer registros desde una fuente externa (OpenAlex, Scopus, etc.).
    
    Responsabilidades:
    - Orquestar extractor apropiado.
    - Deduplicar registros.
    - Reconciliar automáticamente.
    """
    
    def __init__(self, extractor_adapter, reconciliation_service):
        """
        Args:
            extractor_adapter: Adaptador de extractor (OpenAlexAdapter, ScopusAdapter, etc.)
            reconciliation_service: Servicio de reconciliación
        """
        self.extractor = extractor_adapter
        self.reconciliation = reconciliation_service
    
    def execute(
        self,
        affiliation_id: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: int = 1000,
    ) -> Dict[str, Any]:
        """
        Ejecuta extracción desde fuente.
        
        Retorna: Diccionario con estadísticas de extracción y reconciliación.
        """
        logger.info(
            f"Extrayendo desde {self.extractor.__class__.__name__} "
            f"(affiliation_id={affiliation_id}, years={year_from}-{year_to})"
        )
        
        # Deduplicación
        seen_keys = set()
        
        # Extract
        records = self.extractor.extract(
            affiliation_id=affiliation_id,
            year_from=year_from,
            year_to=year_to,
            max_results=max_results,
        )
        
        # Filtrar duplicados
        deduped = []
        for record in records:
            if not ExtractionService.should_skip_record(record, seen_keys, self.extractor.source_name):
                is_valid, error_msg = ExtractionService.validate_record(record)
                if is_valid:
                    deduped.append(record)
                else:
                    logger.warning(f"Registro inválido: {error_msg}")
        
        logger.info(f"Extraídos {len(records)}, deduplicados {len(deduped)}")
        
        # Reconcile
        stats = self.reconciliation.reconcile_batch(deduped)
        
        return {
            "extracted": len(records),
            "inserted": len(deduped),
            "reconciliation": stats,
        }


class ReconcilePublicationsCommand:
    """
    Comando: Reconciliar publicaciones pendientes.
    
    Responsabilidades:
    - Procesar registros pendientes en lotes.
    - Aplicar estrategia cascada: DOI → Fuzzy → Nuevo.
    - Rastrear provenance de campos.
    """
    
    def __init__(self, reconciliation_service):
        """
        Args:
            reconciliation_service: Servicio de reconciliación del domain
        """
        self.reconciliation = reconciliation_service
    
    def execute(self, batch_size: int = 500) -> Dict[str, Any]:
        """
        Reconcilia un lote de registros pendientes.
        
        Retorna: Estadísticas de reconciliación.
        """
        logger.info(f"Iniciando reconciliación (batch_size={batch_size})")
        
        # TODO: Implementar cascada de reconciliación
        
        return {
            "total_processed": 0,
            "doi_exact_matches": 0,
            "fuzzy_matches": 0,
            "new_created": 0,
        }


class EnrichFromOpenAlexCommand:
    """
    Comando: Enriquecer publicaciones no resueltas desde OpenAlex.
    
    Strategy de rescate fallback:
    - Stage 1: Buscar por DOI
    - Stage 2: Buscar por título + año
    - Stage 3: Buscar por ISSN
    - etc.
    """
    
    def __init__(self, openalex_service, publication_repo):
        """
        Args:
            openalex_service: Servicio de OpenAlex
            publication_repo: Repositorio de publicaciones
        """
        self.openalex = openalex_service
        self.publication_repo = publication_repo
    
    def execute(
        self,
        publications: List[Publication],
        strategy: str = "cascada",
    ) -> List[Publication]:
        """
        Enriquece publicaciones desde OpenAlex.
        
        Args:
            publications: Lista de publicaciones sin resolver
            strategy: "cascada", "doi_only", etc.
        
        Retorna: Publicaciones enriquecidas.
        """
        logger.info(f"Enriqueciendo {len(publications)} publicaciones desde OpenAlex (strategy={strategy})")
        
        enriched = []
        for pub in publications:
            # Intentar búsqueda según estrategia
            openalex_data = None
            
            if strategy == "cascada":
                # Stage 1: DOI
                if pub.doi:
                    openalex_data = self.openalex.search_by_doi(pub.doi)
                
                # Stage 2: Título + Año
                if not openalex_data and pub.title and pub.publication_year:
                    openalex_data = self.openalex.search_by_title_and_year(
                        pub.title,
                        pub.publication_year,
                    )
                
                # Stage 3: ISSN
                if not openalex_data and pub.issn:
                    openalex_data = self.openalex.search_by_issn(pub.issn)
            
            if openalex_data:
                pub = CoverageService.enrich_with_openalex(pub, openalex_data)
            
            enriched.append(pub)
        
        return enriched
