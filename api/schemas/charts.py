"""
Schemas Pydantic para generación de gráficos de investigadores y exportación de datos.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


class InvestigatorChartRequest(BaseModel):
    """Solicitud simplificada para generar gráfico de investigador"""
    
    author_id: str = Field(
        ..., 
        description="ID del autor en Scopus (AU-ID). El nombre se obtiene automáticamente.",
        example="57193767797"
    )
    affiliation_ids: Optional[List[str]] = Field(
        None,
        description="IDs de afiliación (AF-ID). Si omite, busca todas.",
        example=["60106970", "60112687"]
    )
    year_from: Optional[int] = Field(
        None,
        description="Año inicial para filtrar publicaciones",
        ge=1900,
        le=2100,
        example=2015
    )
    year_to: Optional[int] = Field(
        None,
        description="Año final para filtrar publicaciones",
        ge=1900,
        le=2100,
        example=2025
    )

    class Config:
        json_schema_extra = {
            "example": {
                "author_id": "57193767797",
                "affiliation_ids": ["60106970", "60112687"],
                "year_from": 2015,
                "year_to": 2025
            }
        }


class PublicationYearData(BaseModel):
    """Datos de publicaciones por año"""
    year: int
    count: int
    percentage: float = Field(description="Porcentaje del total")


class ChartStatistics(BaseModel):
    """Estadísticas del gráfico"""
    total_publications: int
    min_year: int
    max_year: int
    avg_per_year: float
    peak_year: int
    peak_publications: int
    active_years: int
    publications_by_year: List[PublicationYearData]


class InvestigatorChartResponse(BaseModel):
    """Respuesta con información del gráfico generado"""
    
    success: bool
    message: str
    investigator_name: str
    institution_name: str
    filename: str = Field(description="Nombre del archivo PNG generado")
    file_path: str = Field(description="Ruta relativa del archivo")
    statistics: ChartStatistics
    query_used: str = Field(description="Query de Scopus utilizada")
    generated_at: str = Field(description="Timestamp de generación (ISO 8601)")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Gráfico generado correctamente",
                "investigator_name": "Aroca-Martínez, Gustavo J.",
                "institution_name": "Universidad Simón Bolívar",
                "filename": "grafico_aroca_martinez_20260317_123456.png",
                "file_path": "reports/charts/grafico_aroca_martinez_20260317_123456.png",
                "statistics": {
                    "total_publications": 83,
                    "min_year": 2015,
                    "max_year": 2024,
                    "avg_per_year": 8.3,
                    "peak_year": 2024,
                    "peak_publications": 18,
                    "active_years": 10,
                    "publications_by_year": [
                        {"year": 2024, "count": 18, "percentage": 21.7}
                    ]
                },
                "query_used": "AU-ID ( 57193767797 ) AND (AF-ID ( 60106970 ) OR AF-ID ( 60112687 ))",
                "generated_at": "2026-03-17T12:34:56.789Z"
            }
        }


class ChartGenerationError(BaseModel):
    """Error en la generación de gráfico"""
    
    success: bool = False
    error: str
    details: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "error": "No se encontraron publicaciones",
                "details": "La query no devolvió resultados. Verifique AU-ID y AF-ID."
            }
        }


# ════════════════════════════════════════════════════════════════════════════════
# SCHEMAS PARA EXPORTACIÓN A EXCEL
# ════════════════════════════════════════════════════════════════════════════════

class PublicationsExportRequest(BaseModel):
    """Solicitud para exportar publicaciones a Excel"""
    
    author_id: str = Field(
        ...,
        description="ID del autor en Scopus (AU-ID)",
        example="57193767797"
    )
    affiliation_ids: Optional[List[str]] = Field(
        None,
        description="IDs de afiliación (AF-ID) opcionales",
        example=["60106970"]
    )
    year_from: Optional[int] = Field(
        None,
        description="Año inicial",
        ge=1900,
        le=2100
    )
    year_to: Optional[int] = Field(
        None,
        description="Año final",
        ge=1900,
        le=2100
    )


class PublicationsExportResponse(BaseModel):
    """Respuesta de exportación a Excel"""
    
    success: bool
    investigator_name: str
    filename: str = Field(description="Nombre del archivo XLSX")
    file_path: str = Field(description="Ruta del archivo")
    total_publications: int
    size_mb: float = Field(description="Tamaño del archivo en MB")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "investigator_name": "Aroca-Martínez, Gustavo J.",
                "filename": "produccion_aroca_martinez_g_20260317_093800.xlsx",
                "file_path": "reports/exports/produccion_aroca_martinez_g_20260317_093800.xlsx",
                "total_publications": 105,
                "size_mb": 2.35
            }
        }

