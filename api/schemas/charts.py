"""
Schemas Pydantic para generación de gráficos de investigadores y exportación de datos.
"""

from typing import Optional, List, Dict
from pydantic import BaseModel, Field
from api.services.analysis import CampoDisciplinar


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
    campo: CampoDisciplinar = Field(
        CampoDisciplinar.CIENCIAS_SALUD,
        description="Campo disciplinar para aplicar umbrales de evaluación específicos",
        example="CIENCIAS_SALUD"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "author_id": "57193767797",
                "affiliation_ids": ["60106970", "60112687"],
                "year_from": 2015,
                "year_to": 2025,
                "campo": "CIENCIAS_SALUD"
            }
        }


class PublicationYearData(BaseModel):
    """Datos de publicaciones por año"""
    year: int
    count: int
    percentage: float = Field(description="Porcentaje del total")
    citations: int = Field(default=0, description="Total de citaciones ese año")
    avg_citations_per_publication: float = Field(default=0.0, description="Promedio de citas por publicación")


class PublicationDetail(BaseModel):
    """Detalle de una publicación individual"""
    id: int
    title: str
    year: int
    doi: Optional[str] = None
    citations: int = Field(default=0, description="Número de citaciones")
    publication_type: Optional[str] = None
    source_journal: Optional[str] = None
    url: Optional[str] = None
    authors_count: int = Field(default=1, description="Número total de autores")
    is_open_access: bool = Field(default=False)


class CitationYearData(BaseModel):
    """Datos de citaciones por año"""
    year: int
    citations: int
    publications: int


class ChartStatistics(BaseModel):
    """Estadísticas del gráfico"""
    total_publications: int
    total_citations: int
    min_year: int
    max_year: int
    avg_per_year: float
    peak_year: int
    peak_publications: int
    active_years: int
    h_index: Optional[int] = None
    citation_per_publication: float = Field(default=0.0, description="Promedio de citaciones por publicación")
    percent_cited: float = Field(default=0.0, description="Porcentaje de publicaciones citadas")
    publications_by_year: List[PublicationYearData]
    publications_detail: Optional[List[PublicationDetail]] = Field(None, description="Lista completa de publicaciones")
    citations_by_year: Optional[List[CitationYearData]] = Field(None, description="Citaciones agregadas por año")


class InvestigatorChartResponse(BaseModel):
    """Respuesta con información del gráfico generado"""
    
    success: bool
    message: str
    investigator_name: str
    institution_name: str
    filename: str = Field(description="Nombre del archivo PNG generado")
    file_path: str = Field(description="Ruta relativa del archivo PNG")
    pdf_path: Optional[str] = Field(None, description="Ruta del archivo PDF generado (solo si se requirió)")
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
# SCHEMAS PARA ANÁLISIS SCOPUS (DATOS SOLO)
# ════════════════════════════════════════════════════════════════════════════════

class ScopusAnalysisRequest(BaseModel):
    """Solicitud para analizar datos de Scopus sin generar gráficos ni Excel"""
    
    query: Optional[str] = Field(
        None,
        description="Query personalizada de Scopus (ej: 'TITLE-ABS-KEY(machine learning) AND YEAR > 2020')",
        example="TITLE-ABS-KEY(machine learning) AND PUBYEAR > 2020"
    )
    author_id: Optional[str] = Field(
        None,
        description="AU-ID del autor en Scopus (alternativa a query)",
        example="57193767797"
    )
    affiliation_ids: Optional[List[str]] = Field(
        None,
        description="AF-IDs para filtrar por institución (usado con author_id)",
        example=["60106970", "60112687"]
    )
    year_from: Optional[int] = Field(
        None,
        description="Año inicial (filtro adicional)",
        ge=1900,
        le=2100
    )
    year_to: Optional[int] = Field(
        None,
        description="Año final (filtro adicional)",
        ge=1900,
        le=2100
    )
    max_results: Optional[int] = Field(
        None,
        description="Máximo de publicaciones a analizar (default: 5000)",
        ge=1,
        le=10000
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "TITLE-ABS-KEY(machine learning) AND PUBYEAR > 2020"
            }
        }


class ScopusAnalysisResponse(BaseModel):
    """Respuesta con análisis completo de datos Scopus"""
    
    success: bool
    message: str
    query_used: str
    total_publications: int
    total_citations: int
    
    # Estadísticas
    statistics: ChartStatistics
    
    # Datos para graficar (sin generar imágenes)
    publications_by_year: List[PublicationYearData]
    citations_by_year: List[CitationYearData]
    publications_detail: List[PublicationDetail]
    
    # Análisis adicional
    top_cited_publications: Optional[List[PublicationDetail]] = Field(
        None,
        description="Top 10 publicaciones más citadas"
    )
    publication_types_distribution: Optional[Dict[str, int]] = Field(
        None,
        description="Distribución de tipos de publicación"
    )
    journals_distribution: Optional[Dict[str, int]] = Field(
        None,
        description="Top 20 revistas/journals más frecuentes"
    )
    
    # Análisis profesional completo
    findings_positive: Optional[List[str]] = Field(
        None,
        description="Hallazgos positivos del perfil bibliométrico"
    )
    findings_negative: Optional[List[str]] = Field(
        None,
        description="Aspectos a mejorar identificados en el análisis"
    )
    findings_notes: Optional[List[str]] = Field(
        None,
        description="Notas aclaratorias sobre las métricas (ej: sobre recencia)"
    )
    
    generated_at: str


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


# ════════════════════════════════════════════════════════════════════════════════
# SCHEMAS PROFESIONALES — ENDPOINT MULTI-FUENTE CON BD
# ════════════════════════════════════════════════════════════════════════════════

class AuthorDataRequest(BaseModel):
    """
    Solicitud de datos bibliométricos de un autor desde BD.
    
    Agnóstica de fuente: obtiene datos unificados de la BD canónica.
    """
    
    author_id: int = Field(
        ...,
        description="ID del autor en tabla 'authors' (BD local)",
        example=1
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
                "author_id": 1,
                "year_from": 2015,
                "year_to": 2025
            }
        }


class SourceIdentifiers(BaseModel):
    """Identificadores del autor en diferentes bases de datos"""
    scopus: Optional[str] = Field(None, description="Scopus Author ID (AU-ID)")
    openalex: Optional[str] = Field(None, description="OpenAlex Author ID")
    wos: Optional[str] = Field(None, description="Web of Science ResearcherID")
    cvlac: Optional[str] = Field(None, description="CvLAC ID (Colombia)")


class BibliometricMetrics(BaseModel):
    """Indicadores bibliométricos calculados"""
    
    total_publications: int = Field(description="Total de artículos publicados")
    total_citations: int = Field(description="Total de citas recibidas")
    h_index: int = Field(description="Índice h (h papers con h+ citas)")
    cpp: float = Field(description="Citas por publicación (promedio)")
    median_citations: float = Field(description="Mediana de citas por artículo")
    percent_cited: float = Field(description="Porcentaje de artículos citados (%)")


class YearlyMetrics(BaseModel):
    """Datos agregados por año"""
    year: int
    publications: int
    citations: int
    cpp: float = Field(description="Citas por publicación ese año")


class AuthorDataResponse(BaseModel):
    """
    Respuesta profesional: datos bibliométricos completos de un autor.
    
    Incluye información de autor, métricas, series temporales y detalles.
    """
    
    success: bool = True
    author_id: int
    author_name: str
    source_ids: SourceIdentifiers
    year_range: str = Field(description="Rango de años (ej: '2015 - 2025')")
    extraction_date: str = Field(description="Fecha de extracción (ISO 8601)")
    
    metrics: BibliometricMetrics
    yearly_data: List[YearlyMetrics] = Field(description="Serie temporal de publicaciones y citas")
    
    source_distribution: Dict[str, int] = Field(
        description="Distribución de registros por fuente (ej: {scopus: 50, openalex: 45})"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "author_id": 1,
                "author_name": "Juan Arenas",
                "source_ids": {
                    "scopus": "57193767797",
                    "openalex": "A1234567890",
                    "wos": "AAH-1234-2022",
                    "cvlac": "00123456789"
                },
                "year_range": "2015 - 2025",
                "extraction_date": "2026-03-17T12:34:56.789Z",
                "metrics": {
                    "total_publications": 62,
                    "total_citations": 850,
                    "h_index": 15,
                    "cpp": 13.7,
                    "median_citations": 8.5,
                    "percent_cited": 85.5
                },
                "yearly_data": [
                    {"year": 2015, "publications": 5, "citations": 120, "cpp": 24.0},
                    {"year": 2016, "publications": 6, "citations": 95, "cpp": 15.8}
                ],
                "source_distribution": {
                    "scopus": 62,
                    "openalex": 58,
                    "wos": 45
                }
            }
        }


class AuthorDataErrorResponse(BaseModel):
    """Error en consulta de datos de autor"""
    
    success: bool = False
    error: str = Field(description="Mensaje de error")
    details: Optional[str] = Field(None, description="Detalles adicionales")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "error": "Autor no encontrado",
                "details": "El ID 999 no existe en la tabla 'authors'"
            }
        }


class GenerateChartRequest(BaseModel):
    """
    Solicitud para generar gráfico PNG desde BD (v2).
    
    Agnóstica de fuente: obtiene datos unificados y genera PNG.
    """
    
    author_id: int = Field(
        ...,
        description="ID del autor en tabla 'authors' (BD local)",
        example=1
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
    institution_name: str = Field(
        "Universidad Simón Bolívar",
        description="Nombre de institución para pie gráfico",
        example="Universidad Simón Bolívar"
    )
    campo: str = Field(
        "CIENCIAS_SALUD",
        description="Campo disciplinar (CIENCIAS_SALUD, CIENCIAS_BASICAS, INGENIERIA, etc.)",
        example="CIENCIAS_SALUD"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "author_id": 1,
                "year_from": 2015,
                "year_to": 2025,
                "institution_name": "Universidad Simón Bolívar",
                "campo": "CIENCIAS_SALUD"
            }
        }


class GenerateChartResponse(BaseModel):
    """
    Respuesta: gráfico PNG generado desde datos de BD.
    """
    
    success: bool = True
    investigator_name: str
    filename: str = Field(description="Nombre del archivo PNG")
    file_path: str = Field(description="Ruta relativa del archivo")
    file_size_mb: float = Field(description="Tamaño en MB")
    metrics: BibliometricMetrics = Field(description="Métricas incluidas en el gráfico")
    year_range: str = Field(description="Rango de años mostrado")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "investigator_name": "Juan Arenas",
                "filename": "grafico_juan_arenas_20260318_141530.png",
                "file_path": "reports/charts/grafico_juan_arenas_20260318_141530.png",
                "file_size_mb": 2.15,
                "metrics": {
                    "total_publications": 62,
                    "total_citations": 850,
                    "h_index": 15,
                    "cpp": 13.7,
                    "median_citations": 8.5,
                    "percent_cited": 85.5
                },
                "year_range": "2015 - 2025"
            }
        }


class GenerateChartErrorResponse(BaseModel):
    """Error en generación de gráfico"""
    
    success: bool = False
    error: str = Field(description="Mensaje de error")
    details: Optional[str] = Field(None, description="Detalles adicionales")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "error": "Autor no encontrado",
                "details": "El ID 1 no existe en la tabla 'authors'"
            }
        }

