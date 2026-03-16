"""
Router de Administración y Mantenimiento.

Endpoints para:
- Limpieza de datos
- Auto-deduplicación
- Reportes de calidad
- Validación de integridad
"""

import logging
from typing import Optional
from fastapi import APIRouter, Query, BackgroundTasks, HTTPException, Depends
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.schemas.common import MessageResponse
from scripts.clean_data import DataCleaner
from scripts.auto_deduplicate import AutoDeduplicator
from scripts.quality_reports import QualityReporter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["Administración"])


# =============================================================
# LIMPIEZA DE DATOS
# =============================================================

@router.post(
    "/clean-data",
    response_model=MessageResponse,
    summary="Ejecutar limpieza de datos"
)
def clean_data(
    source: str = Query(
        "all",
        description="Fuente a limpiar: openalex|scopus|wos|cvlac|datos_abiertos|all"
    ),
    limit: Optional[int] = Query(None, description="Límite de registros a procesar"),
    db: Session = Depends(get_db),
):
    """
    Ejecuta validación de criterios de inclusión/exclusión sobre registros pendientes.
    
    Marca registros como:
    - accepted: Completos y válidos
    - pending_review: Incompletos pero con potencial
    - rejected: Datos inválidos/corrompidos
    
    Genera reporte CSV en `reports/clean_data_report_{timestamp}.csv`
    """
    try:
        session = db
        cleaner = DataCleaner(session, dry_run=False)
        
        if source == "all":
            sources = ["openalex", "scopus", "wos", "cvlac", "datos_abiertos"]
        else:
            sources = [source]
        
        for src in sources:
            cleaner.clean_source(src, limit=limit)
        
        cleaner.export_reports()
        
        return MessageResponse(
            status="success",
            message=f"✅ Limpieza completada. "
                    f"Aceptados: {cleaner.stats['accepted']}, "
                    f"Revisión: {cleaner.stats['pending_review']}, "
                    f"Rechazados: {cleaner.stats['rejected_incomplete']}",
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================
# AUTO-DEDUPLICACIÓN
# =============================================================

@router.post(
    "/auto-deduplicate",
    response_model=MessageResponse,
    summary="Ejecutar auto-deduplicación"
)
def auto_deduplicate(
    threshold: float = Query(
        0.95,
        ge=0.0,
        le=1.0,
        description="Umbral de fusión automática (0.95 = 95%)"
    ),
    source: str = Query(
        "all",
        description="Fuente a deduplicar"
    ),
    limit: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Ejecuta auto-deduplicación de publicaciones canónicas.
    
    Compara publicaciones y:
    - Score >= threshold → Fusionar automáticamente
    - Score >= 0.85 → Marcar para revisión manual
    - Score < 0.85 → Saltar
    
    Genera reporte en `reports/dedup_report_{timestamp}.csv`
    """
    try:
        session = db
        deduplicator = AutoDeduplicator(
            session,
            fuzzy_threshold=threshold,
            manual_review_threshold=0.85,
            dry_run=False,
        )
        
        if source == "all":
            sources = ["openalex", "scopus", "wos", "cvlac", "datos_abiertos"]
        else:
            sources = [source]
        
        for src in sources:
            deduplicator.deduplicate_source(src, limit=limit)
        
        deduplicator.export_reports()
        
        return MessageResponse(
            status="success",
            message=f"✅ Deduplicación completada. "
                    f"Fusionadas: {deduplicator.stats['merged_auto']}, "
                    f"Revisión: {deduplicator.stats['marked_manual_review']}",
        )
    
    except Exception as e:
        import traceback
        logger.error(f"❌ Error en auto-deduplicación: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error en deduplicación: {str(e)}"
        )


# =============================================================
# REPORTES DE CALIDAD
# =============================================================

@router.get(
    "/quality/metrics",
    summary="Obtener métricas de calidad"
)
def get_quality_metrics(
    output: str = Query(
        "json",
        description="Formato: json|csv|html"
    ),
    db: Session = Depends(get_db),
):
    """
    Genera métricas de calidad de datos:
    
    - Cobertura de identificadores (DOI, ISSN, PMID, etc.)
    - Distribución temporal y por tipo
    - Tasas de reconciliación
    - Calidad general (0-100)
    - Alertas críticas
    
    Exporta en formato solicitado.
    """
    try:
        session = db
        reporter = QualityReporter(session)
        metrics = reporter.generate_report()
        
        if output == "csv":
            reporter.export_csv()
            return MessageResponse(
                status="success",
                message="✅ Reporte CSV generado (reports/quality_metrics_*.csv)"
            )
        elif output == "html":
            reporter.export_html()
            return MessageResponse(
                status="success",
                message="✅ Reporte HTML generado (reports/quality_report_*.html)"
            )
        else:  # json
            return {
                "timestamp": metrics.timestamp,
                "total_canonical": metrics.total_canonical,
                "total_authors": metrics.total_authors,
                "coverage": {
                    "doi_pct": f"{metrics.pct_with_doi:.1%}",
                    "issn_pct": f"{metrics.pct_with_issn:.1%}",
                    "year_pct": f"{metrics.pct_with_year:.1%}",
                    "oa_known_pct": f"{metrics.pct_open_access_known:.1%}",
                },
                "reconciliation": {
                    "doi_exact_pct": f"{metrics.pct_doi_exact:.1%}",
                    "fuzzy_pct": f"{metrics.pct_fuzzy:.1%}",
                    "manual_review_pct": f"{metrics.pct_manual_review:.1%}",
                },
                "authors": {
                    "total": metrics.total_authors,
                    "with_orcid_pct": f"{metrics.pct_authors_with_orcid:.1%}",
                },
                "quality_score": f"{metrics.quality_score:.1f}/100",
                "alerts": metrics.alerts,
            }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/health",
    response_model=MessageResponse,
    summary="Estado de salud del sistema"
)
def health_check(db: Session = Depends(get_db)):
    """Verifica la salud de la BD y API."""
    try:
        session = db
        # Intentar una query simple
        from db.models import CanonicalPublication
        count = session.query(CanonicalPublication).count()
        
        return MessageResponse(
            status="ok",
            message=f"✅ Sistema operativo. {count} publicaciones en BD."
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Error en BD: {str(e)}")
