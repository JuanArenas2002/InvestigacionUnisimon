"""
Schemas para métricas generales de autores desde el inventario local.
"""

from typing import Optional, List, Dict
from pydantic import BaseModel, Field


class PublicationTypeDistribution(BaseModel):
    """Distribución de publicaciones por tipo"""
    type_name: str = Field(..., description="Tipo de publicación (Article, Review, etc.)")
    count: int = Field(..., description="Cantidad de publicaciones")
    percentage: float = Field(..., description="Porcentaje del total")


class JournalMetrics(BaseModel):
    """Métricas de una revista específica"""
    journal_name: str = Field(..., description="Nombre de la revista")
    issn: Optional[str] = Field(None, description="ISSN de la revista")
    publications_count: int = Field(..., description="Publicaciones en esta revista")
    total_citations: int = Field(..., description="Citas totales en esta revista")
    avg_citations: float = Field(..., description="Promedio de citas por publicación")


class YearMetrics(BaseModel):
    """Métricas por año de publicación"""
    year: int = Field(..., description="Año de publicación")
    publications_count: int = Field(..., description="Publicaciones en ese año")
    citations_count: int = Field(..., description="Citas acumuladas en ese año")
    avg_citations_per_pub: float = Field(..., description="Promedio de citas por publicación")


class OpenAccessMetrics(BaseModel):
    """Información sobre acceso abierto"""
    total_with_oa_status: int = Field(..., description="Publicaciones con estado OA conocido")
    open_access_count: int = Field(..., description="Publicaciones en acceso abierto")
    open_access_percentage: float = Field(..., description="Porcentaje de acceso abierto")
    oa_status_distribution: Dict[str, int] = Field(..., description="Distribución por estado OA")


class GeneralMetrics(BaseModel):
    """Métricas generales de productividad"""
    total_publications: int = Field(..., description="Total de publicaciones")
    total_citations: int = Field(..., description="Total de citas acumuladas")
    years_active_from: Optional[int] = Field(..., description="Año de primera publicación")
    years_active_to: Optional[int] = Field(..., description="Año de última publicación")
    years_active_count: Optional[int] = Field(..., description="Años con actividad")
    avg_publications_per_year: float = Field(..., description="Promedio de pubs por año")
    h_index: int = Field(..., description="Índice H (publicaciones con al menos H citas)")
    cpp: float = Field(..., description="Citas por publicación")
    most_cited_pub_count: int = Field(..., description="Máximo de citas en una publicación")


class AuthorGeneralMetricsResponse(BaseModel):
    """Respuesta completa de métricas generales del autor"""
    
    # Información del autor
    author_id: int = Field(..., description="ID del autor en la BD local")
    author_name: str = Field(..., description="Nombre del autor")
    orcid: Optional[str] = Field(None, description="ORCID del autor")
    
    # Métricas generales
    general_metrics: GeneralMetrics = Field(..., description="Estadísticas generales de productividad")
    
    # Time series
    publications_by_year: List[YearMetrics] = Field(..., description="Desglose por año")
    
    # Distribuciones
    publication_types: List[PublicationTypeDistribution] = Field(..., description="Distribución por tipo")
    top_journals: List[JournalMetrics] = Field(..., description="Top 10 revistas donde publica")
    
    # Open Access
    open_access: OpenAccessMetrics = Field(..., description="Análisis de acceso abierto")
    
    # Datos adicionales
    institutional_publications: int = Field(..., description="Publicaciones con afiliación institucional")
    languages: Dict[str, int] = Field(..., description="Distribución de idiomas")
    
    class Config:
        json_schema_extra = {
            "example": {
                "author_id": 123,
                "author_name": "Juan Pérez García",
                "orcid": "0000-0001-2345-6789",
                "general_metrics": {
                    "total_publications": 45,
                    "total_citations": 320,
                    "years_active_from": 2015,
                    "years_active_to": 2026,
                    "years_active_count": 11,
                    "avg_publications_per_year": 4.1,
                    "h_index": 8,
                    "cpp": 7.1,
                    "most_cited_pub_count": 45
                },
                "publications_by_year": [
                    {
                        "year": 2026,
                        "publications_count": 5,
                        "citations_count": 12,
                        "avg_citations_per_pub": 2.4
                    }
                ],
                "publication_types": [
                    {
                        "type_name": "Article",
                        "count": 38,
                        "percentage": 84.4
                    }
                ],
                "top_journals": [
                    {
                        "journal_name": "Science",
                        "issn": "0036-8075",
                        "publications_count": 3,
                        "total_citations": 85,
                        "avg_citations": 28.3
                    }
                ],
                "open_access": {
                    "total_with_oa_status": 40,
                    "open_access_count": 25,
                    "open_access_percentage": 62.5,
                    "oa_status_distribution": {
                        "gold": 10,
                        "green": 8,
                        "hybrid": 7
                    }
                },
                "institutional_publications": 35,
                "languages": {
                    "eng": 40,
                    "spa": 5
                }
            }
        }


class AuthorMetricsErrorResponse(BaseModel):
    """Respuesta de error para endpoints de métricas"""
    status: str = Field(..., description="always 'error'")
    detail: str = Field(..., description="Descripción del error")
    author_id: Optional[int] = Field(None, description="ID del autor que no se encontró")
