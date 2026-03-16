"""Domain Layer: Entidades y lógica de negocio pura."""
from .entities.publication import Publication, Journal, CoveragePeriod
from .services.pipeline_services import CoverageService, ExtractionService, ReconciliationService, CoverageCheckResult
from .repositories.repository_interfaces import PublicationRepository, ExternalRecordRepository, CoverageRepository

__all__ = [
    "Publication",
    "Journal",
    "CoveragePeriod",
    "CoverageService",
    "ExtractionService",
    "ReconciliationService",
    "CoverageCheckResult",
    "PublicationRepository",
    "ExternalRecordRepository",
    "CoverageRepository",
]
