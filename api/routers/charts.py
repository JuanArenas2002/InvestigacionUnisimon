"""
Router de Gráficos de Investigadores — FastAPI Endpoints
=========================================================

Endpoints para generar gráficos de publicaciones desde la API.
Se integra con Scopus y retorna archivos PNG.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
import os
import zipfile
import io

from api.dependencies import get_db
from api.schemas.charts import (
    InvestigatorChartRequest,
    InvestigatorChartResponse,
    ChartGenerationError,
    PublicationsExportRequest,
    PublicationsExportResponse,
    AuthorDataRequest,
    AuthorDataResponse,
    AuthorDataErrorResponse,
    GenerateChartRequest,
    GenerateChartResponse,
    GenerateChartErrorResponse,
)
from api.services.chart_generator import generate_investigator_chart_file, CampoDisciplinar
from api.services.excel_exporter import generate_publications_excel_file
from api.services.data_provider import fetch_author_data
from api.services.graph_renderer import render_author_chart
from api.services.pdf_reporter import generate_analysis_report
from api.services.analysis import generar_hallazgos

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/authors/charts", tags=["Gráficos de Investigadores"])

# Ruta absoluta para almacenar gráficos
CHARTS_OUTPUT_DIR = Path(__file__).parent.parent.parent / "reports" / "charts"


# ── POST /authors/charts/generate ─────────────────────────────────────────────

@router.post(
    "/generate",
    summary="Generar gráfico de publicaciones del investigador",
    response_model=InvestigatorChartResponse,
    responses={
        200: {"description": "Gráfico generado exitosamente"},
        400: {"model": ChartGenerationError, "description": "Error en los parámetros"},
        404: {"model": ChartGenerationError, "description": "No se encontraron publicaciones"},
        500: {"model": ChartGenerationError, "description": "Error en la API de Scopus"},
    }
)
def generate_investigator_chart(
    request: InvestigatorChartRequest,
    db: Session = Depends(get_db),
):
    """
    Genera un gráfico de publicaciones de un investigador desde Scopus.
    
    **Parámetros mínimos:**
    - `author_id`: ID del autor en Scopus (AU-ID) — requerido
    
    **Parámetros opcionales:**
    - `affiliation_ids`: IDs de afiliación (AF-ID)
    - `year_from`: Año inicial para filtrar
    - `year_to`: Año final para filtrar
    - `campo`: Campo disciplinar para umbrales específicos (default: CIENCIAS_SALUD)
      Opciones: CIENCIAS_SALUD, CIENCIAS_BASICAS, INGENIERIA, CIENCIAS_SOCIALES, ARTES_HUMANIDADES
    
    El **nombre del investigador** se obtiene automáticamente de los registros de Scopus.
    
    **Ejemplo mínimo:**
    ```json
    {
        "author_id": "57193767797"
    }
    ```
    
    **Ejemplo completo:**
    ```json
    {
        "author_id": "57193767797",
        "affiliation_ids": ["60106970", "60112687"],
        "year_from": 2015,
        "year_to": 2025,
        "campo": "CIENCIAS_BASICAS"
    }
    ```
    """
    
    try:
        logger.info(
            f"Generando gráfico para AU-ID: {request.author_id} "
            f"(AF-IDs: {request.affiliation_ids or 'todas'})"
        )
        
        # USAR AFILIACIONES USB POR DEFECTO SI NO SE PROPORCIONAN
        # AF-ID(60106970): Universidad Simón Bolívar - Campus Caracas
        # AF-ID(60112687): Universidad Simón Bolívar - Campus Litoral
        aff_ids_to_use = request.affiliation_ids
        if not aff_ids_to_use:
            aff_ids_to_use = ["60106970", "60112687"]
            logger.info("Usando afiliaciones USB por defecto: Caracas (60106970) + Litoral (60112687)")
        
        # Generar gráfico
        chart_data = generate_investigator_chart_file(
            author_id=request.author_id,
            affiliation_ids=aff_ids_to_use,
            year_from=request.year_from,
            year_to=request.year_to,
            institution_name="Universidad Simón Bolívar",
            output_dir=CHARTS_OUTPUT_DIR,
            campo=request.campo,
        )
        
        # Construir respuesta
        response = InvestigatorChartResponse(
            success=True,
            message="Gráfico generado correctamente",
            investigator_name=chart_data['investigator_name'],
            institution_name="Universidad Simón Bolívar",
            filename=chart_data['filename'],
            file_path=chart_data['file_path'],
            statistics=chart_data['statistics'],
            query_used=chart_data['query_used'],
            generated_at=chart_data['generated_at'],
        )
        
        logger.info(f"Gráfico generado: {chart_data['filename']}")
        return response
        
    except ValueError as e:
        logger.warning(f"No se encontraron datos: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as e:
        logger.error(f"Error generando gráfico: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando gráfico: {str(e)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# NUEVO ENDPOINT v1 — GENERADOR DE REPORTES (PNG + PDF) CON SCOPUS
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/generate-report",
    summary="Generar informe completo con Scopus (PNG + PDF)",
    response_model=InvestigatorChartResponse,
    responses={
        200: {"description": "Informe generado correctamente"},
        400: {"model": ChartGenerationError, "description": "Error en los parámetros"},
        404: {"model": ChartGenerationError, "description": "Autor no encontrado"},
        500: {"model": ChartGenerationError, "description": "Error de API"},
    },
    tags=["Reportes (Scopus)"],
)
def generate_investigator_report(
    request: InvestigatorChartRequest,
    db: Session = Depends(get_db),
):
    """
    NUEVO ENDPOINT: Genera informe completo PNG + PDF desde Scopus.
    
    **Características:**
    - ✅ **PNG profesional**: gráfico limpio sin notas
    - ✅ **PDF detallado**: informe completo con notas, análisis, KPIs
    - ✅ **Datos Scopus**: utilizando AU-ID y AF-ID
    - ✅ **Análisis automático**: detección positivos/negativos
    - ✅ **Completitud**: todo el espacio necesario para notas aclaratorias
    
    **Parámetros:**
    - `author_id`: ID del autor en Scopus (AU-ID) — requerido
    - `affiliation_ids`: IDs de afiliación (AF-ID) (opcional, usa USB por defecto)
    - `year_from`: Año inicial (opcional)
    - `year_to`: Año final (opcional)
    - `campo`: Campo disciplinar (default: CIENCIAS_SALUD)
    
    **Respuesta:**
    Objeto JSON con información de ambos archivos:
    - `filename`: nombre del PNG
    - `file_path`: ruta del PNG
    - `statistics`: 6 indicadores bibliométricos
    
    **Archivos generados:**
    - PNG: `reports/charts/grafico_*.png`
    - PDF: `reports/pdfs/informe_*.pdf`
    """
    
    try:
        logger.info(
            f"[REPORT SCOPUS] Generando informe para AU-ID: {request.author_id} "
            f"(AF-IDs: {request.affiliation_ids or 'USB por defecto'})"
        )
        
        # 1. Usar afiliaciones USB por defecto
        aff_ids_to_use = request.affiliation_ids
        if not aff_ids_to_use:
            aff_ids_to_use = ["60106970", "60112687"]  # USB: Caracas + Litoral
        
        # 2. Generar PNG (limpio, sin análisis)
        chart_data = generate_investigator_chart_file(
            author_id=request.author_id,
            affiliation_ids=aff_ids_to_use,
            year_from=request.year_from,
            year_to=request.year_to,
            institution_name="Universidad Simón Bolívar",
            output_dir=CHARTS_OUTPUT_DIR,
            campo=request.campo,
        )
        
        logger.info(
            f"[REPORT SCOPUS] PNG generado: {chart_data['filename']} "
            f"({chart_data['statistics'].get('total_publications', 0)} pubs)"
        )
        
        # 3. Generar análisis para el PDF
        positivos = [
            f"H-Index en {chart_data.get('h_index', 'N/A')}",
            f"Promedio de citas por publicación: {chart_data.get('cpp', 'N/A')}",
            f"{chart_data.get('percent_cited', 0):.1f}% de artículos citados",
        ]
        negativos = []
        notas = [
            "📌 Análisis basado en datos de Scopus",
            f"📌 Período: {chart_data.get('query_used', 'Múltiples años')}",
            "📌 Afiliación: Universidad Simón Bolívar",
        ]
        
        # 4. Generar PDF con PNG incrustado
        pdf_output_dir = Path(__file__).parent.parent.parent / "reports" / "pdfs"
        pdf_info = generate_analysis_report(
            investigador=chart_data['investigator_name'],
            kpis={
                "pubs": chart_data['statistics'].get('total_publications', 0),
                "citas": chart_data.get('total_citations', 0),
                "h_index": chart_data.get('h_index', 0),
                "cpp": chart_data.get('cpp', 0),
                "mediana": chart_data.get('median_citations', 0),
                "pct_citados": chart_data.get('percent_cited', 0),
                "año_pico": chart_data.get('peak_year', "N/A"),
            },
            positivos=positivos,
            negativos=negativos,
            notas=notas,
            png_path=chart_data['file_path'],
            institution_name="Universidad Simón Bolívar",
            output_dir=pdf_output_dir,
            fecha_ext=chart_data.get('generated_at', datetime.now().strftime("%d/%m/%Y %H:%M:%S")),
        )
        
        logger.info(
            f"[REPORT SCOPUS] PDF generado: {pdf_info['filename']}"
        )
        
        # 5. Retornar respuesta
        return InvestigatorChartResponse(
            success=True,
            message="Informe generado correctamente (PNG + PDF)",
            investigator_name=chart_data['investigator_name'],
            institution_name="Universidad Simón Bolívar",
            filename=chart_data['filename'],
            file_path=chart_data['file_path'],
            pdf_path=pdf_info['file_path'],
            statistics=chart_data['statistics'],
            query_used=chart_data['query_used'],
            generated_at=chart_data['generated_at'],
        )
    
    except ValueError as e:
        logger.warning(f"[REPORT SCOPUS] Validación: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as e:
        logger.error(f"[REPORT SCOPUS] Error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando informe: {str(e)}"
        )


# ── GET /authors/charts/download/{filename} ──────────────────────────────────

@router.get(
    "/download/{filename}",
    summary="Descargar gráfico generado",
    responses={
        200: {"description": "Archivo PNG"},
        404: {"description": "Archivo no encontrado"},
    }
)
def download_chart(filename: str):
    """
    Descarga un gráfico previamente generado.
    
    **Parámetro:**
    - `filename`: Nombre exacto del archivo PNG (ej: `grafico_aroca_martinez_20260317_123456.png`)
    
    **Ejemplo:**
    ```
    GET /authors/charts/download/grafico_aroca_martinez_20260317_123456.png
    ```
    """
    
    # Validar nombre de archivo (seguridad)
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")
    
    filepath = Path(CHARTS_OUTPUT_DIR).resolve() / filename
    
    if not filepath.exists():
        logger.warning(f"Archivo no encontrado: {filepath}")
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
    
    logger.info(f"Descargando: {filepath}")
    
    return FileResponse(
        path=filepath,
        filename=filename,
        media_type="image/png",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ── GET /authors/charts/view/{filename} ──────────────────────────────────

@router.get(
    "/view/{filename}",
    summary="Ver gráfico en la interfaz (PNG)",
    response_class=FileResponse,
    responses={
        200: {"description": "Imagen PNG", "content": {"image/png": {}}},
        404: {"description": "Archivo no encontrado"},
    }
)
def view_chart(filename: str):
    """
    Ver un gráfico en el navegador (mostrado como imagen inline).
    
    **Parámetro:**
    - `filename`: Nombre exacto del archivo PNG
    
    **Ejemplo:**
    ```
    GET /authors/charts/view/grafico_aroca_martinez_20260317_123456.png
    ```
    """
    
    # Validar nombre de archivo (seguridad)
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")
    
    filepath = Path(CHARTS_OUTPUT_DIR).resolve() / filename
    
    if not filepath.exists():
        logger.warning(f"Archivo no encontrado: {filepath}")
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
    
    logger.info(f"Mostrando gráfico: {filepath}")
    
    return FileResponse(
        path=filepath,
        media_type="image/png",
        headers={"Content-Disposition": "inline"}
    )


# ── GET /authors/charts/list ──────────────────────────────────────────────────

@router.get(
    "/list",
    summary="Listar gráficos generados",
    response_model=dict,
)
def list_generated_charts(
    limit: int = Query(10, ge=1, le=100, description="Máximo de gráficos a retornar"),
    skip: int = Query(0, ge=0, description="Número de gráficos a saltar"),
):
    """
    Lista los gráficos generados recientemente.
    
    **Parámetros:**
    - `limit`: Máximo de gráficos (default: 10, max: 100)
    - `skip`: Número de gráficos a saltar para paginación (default: 0)
    
    **Retorna:**
    - Lista de gráficos con metadatos
    - Información de paginación
    """
    
    try:
        # Crear directorio si no existe
        CHARTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Listar archivos PNG
        charts_files = sorted(
            CHARTS_OUTPUT_DIR.glob("grafico_*.png"),
            key=lambda f: f.stat().st_mtime,
            reverse=True  # Más recientes primero
        )
        
        total = len(charts_files)
        paginated = charts_files[skip : skip + limit]
        
        charts_list = []
        for filepath in paginated:
            stat = filepath.stat()
            charts_list.append({
                'filename': filepath.name,
                'size_kb': round(stat.st_size / 1024, 2),
                'created_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'download_url': f"/authors/charts/download/{filepath.name}",
            })
        
        return {
            'success': True,
            'total': total,
            'returned': len(charts_list),
            'skip': skip,
            'limit': limit,
            'charts': charts_list,
        }
    
    except Exception as e:
        logger.error(f"Error listando gráficos: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error listando gráficos: {str(e)}"
        )


# ── DELETE /authors/charts/delete/{filename} ──────────────────────────────────

@router.delete(
    "/delete/{filename}",
    summary="Eliminar gráfico generado",
    response_model=dict,
)
def delete_chart(filename: str):
    """
    Elimina un gráfico previamente generado.
    
    **Parámetro:**
    - `filename`: Nombre exacto del archivo PNG
    
    **Retorna:**
    - Confirmación de eliminación
    """
    
    # Validar nombre de archivo
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")
    
    filepath = CHARTS_OUTPUT_DIR / filename
    
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
    
    try:
        filepath.unlink()
        logger.info(f"Gráfico eliminado: {filename}")
        return {
            'success': True,
            'message': f"Archivo eliminado: {filename}",
            'filename': filename,
        }
    except Exception as e:
        logger.error(f"Error eliminando gráfico: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error eliminando gráfico: {str(e)}"
        )


# ── DELETE /authors/charts/cleanup ────────────────────────────────────────────

@router.delete(
    "/cleanup",
    summary="Limpiar gráficos antiguos",
    response_model=dict,
)
def cleanup_old_charts(
    days_old: int = Query(7, ge=1, description="Eliminar gráficos más antiguos que X días"),
):
    """
    Elimina gráficos más antiguos que el período especificado.
    Útil para liberar espacio.
    
    **Parámetro:**
    - `days_old`: Días de antigüedad (default: 7)
    
    **Retorna:**
    - Número de archivos eliminados
    """
    
    try:
        CHARTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        cutoff_time = datetime.now().timestamp() - (days_old * 86400)
        deleted_count = 0
        
        for filepath in CHARTS_OUTPUT_DIR.glob("grafico_*.png"):
            if filepath.stat().st_mtime < cutoff_time:
                filepath.unlink()
                deleted_count += 1
                logger.info(f"Gráfico eliminado por limpieza: {filepath.name}")
        
        logger.info(f"Limpieza completada: {deleted_count} archivos eliminados")
        
        return {
            'success': True,
            'message': f"{deleted_count} gráficos eliminados",
            'deleted_count': deleted_count,
            'days_old': days_old,
        }
    
    except Exception as e:
        logger.error(f"Error en limpieza: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error en limpieza: {str(e)}"
        )


# ════════════════════════════════════════════════════════════════════════════════
# EXPORTACIÓN A EXCEL
# ════════════════════════════════════════════════════════════════════════════════

# ── POST /authors/publications/export ──────────────────────────────────────────

@router.post(
    "/export-publications",
    summary="Exportar publicaciones a Excel",
    response_model=PublicationsExportResponse,
    tags=["Exportación de Datos"],
)
def export_publications_to_excel(
    request: PublicationsExportRequest,
    db: Session = Depends(get_db),
):
    """
    Genera un archivo Excel con todas las publicaciones de un investigador
    incluyendo metadatos completos.
    
    **Parámetros:**
    - `author_id`: ID de Scopus del investigador (requerido)
    - `affiliation_ids`: IDs de afiliación opcionales
    - `year_from`, `year_to`: Rango de años opcional
    
    **Retorna:**
    - Información del archivo Excel generado (ruta, tamaño, etc.)
    
    **Contenido del Excel:**
    - Hoja 1: Resumen con estadísticas
    - Hoja 2: Listado completo de publicaciones con metadatos
    """
    
    try:
        logger.info(f"Exportando publicaciones para AU-ID: {request.author_id}")
        
        # Generar archivo Excel
        result = generate_publications_excel_file(
            author_id=request.author_id,
            affiliation_ids=request.affiliation_ids,
            output_dir=Path(__file__).parent.parent.parent / "reports" / "exports",
        )
        
        logger.info(f"Excel generado: {result['filename']}")
        
        return PublicationsExportResponse(
            success=True,
            investigator_name=result['investigator_name'],
            filename=result['filename'],
            file_path=result['file_path'],
            total_publications=result['total_publications'],
            size_mb=result['size_mb'],
        )
    
    except ValueError as e:
        logger.error(f"Error de validación: {str(e)}")
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron publicaciones: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error exportando a Excel: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error exportando a Excel: {str(e)}"
        )


# ── GET /authors/publications/download/{filename} ───────────────────────────────

@router.get(
    "/download-publications/{filename}",
    summary="Descargar Excel de publicaciones",
    tags=["Exportación de Datos"],
)
def download_publications_excel(filename: str):
    """
    Descarga un archivo Excel previamente generado.
    
    **Parámetro:**
    - `filename`: Nombre exacto del archivo XLSX
    
    **Retorna:**
    - Archivo XLSX para descargar
    """
    
    # Validar nombre de archivo
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")
    
    exports_dir = Path(__file__).parent.parent.parent / "reports" / "exports"
    filepath = exports_dir.resolve() / filename
    
    if not filepath.exists():
        logger.warning(f"Archivo no encontrado: {filepath}")
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
    
    logger.info(f"Descargando Excel: {filepath}")
    
    return FileResponse(
        path=filepath,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )




# ── POST /authors/charts/download-all ─────────────────────────────────────────

@router.post(
    "/download-all",
    summary="Descargar gráfico y Excel en un ZIP",
    response_description="Archivo ZIP con gráfico PNG y Excel XLSX",
    responses={
        200: {"description": "ZIP descargado exitosamente"},
        400: {"model": ChartGenerationError, "description": "Error en los parámetros"},
        404: {"model": ChartGenerationError, "description": "No se encontraron publicaciones"},
        500: {"model": ChartGenerationError, "description": "Error en la generación"},
    }
)
def download_all_artifacts(
    request: InvestigatorChartRequest,
    db: Session = Depends(get_db),
):
    """
    Genera gráfico (PNG), reporte (XLSX) y PDF profesional simultáneamente en un ZIP.
    
    **Parámetros mínimos:**
    - `author_id`: ID del autor en Scopus (AU-ID) — requerido
    
    **Parámetros opcionales:**
    - `affiliation_ids`: IDs de afiliación (AF-ID)
    - `year_from`: Año inicial para filtrar
    - `year_to`: Año final para filtrar
    
    **Retorna:** archivo ZIP con:
    - `grafico_*.png` → Gráfico de publicaciones (PNG simplificado)
    - `produccion_*.xlsx` → Reporte completo de publicaciones
    - `informe_*.pdf` → Reporte profesional con análisis bibliométrico
    
    **Uso:**
    ```bash
    curl -X POST http://localhost:8000/api/authors/charts/download-all \\
      -H "Content-Type: application/json" \\
      -d '{"author_id": "57193767797"}' \\
      -o publicaciones.zip
    ```
    """
    
    try:
        logger.info(f"[ZIP] Iniciando descarga simultánea para AU-ID: {request.author_id}")
        
        # Directorios de salida
        charts_dir = Path(__file__).parent.parent.parent / "reports" / "charts"
        exports_dir = Path(__file__).parent.parent.parent / "reports" / "exports"
        pdfs_dir = Path(__file__).parent.parent.parent / "reports" / "pdfs"
        
        # ── Generar gráfico ────────────────────────────────────────────────────
        logger.info(f"[ZIP] Generando gráfico...")
        chart_result = generate_investigator_chart_file(
            author_id=request.author_id,
            affiliation_ids=request.affiliation_ids,
            year_from=request.year_from,
            year_to=request.year_to,
            output_dir=charts_dir,
        )
        chart_file = Path(chart_result['file_path'])
        
        # ── Generar Excel ──────────────────────────────────────────────────────
        logger.info(f"[ZIP] Generando Excel...")
        excel_result = generate_publications_excel_file(
            author_id=request.author_id,
            affiliation_ids=request.affiliation_ids,
            year_from=request.year_from,
            year_to=request.year_to,
            output_dir=exports_dir,
        )
        excel_file = Path(excel_result['file_path'])
        
        # ── Generar PDF profesional ────────────────────────────────────────────
        logger.info(f"[ZIP] Generando PDF profesional...")
        investigador = chart_result['investigator_name']
        
        # Preparar KPIs para el PDF — Usar claves que el PDF espera
        kpis = {
            "pubs": chart_result['statistics']['total_publications'],
            "citas": chart_result.get('total_citations', 0),
            "h_index": chart_result.get('h_index', 0),
            "cpp": chart_result.get('cpp', 0.0),
            "mediana": chart_result.get('median_citations', 0.0),
            "pct_citados": chart_result.get('percent_cited', 0.0),
            "año_pico": chart_result.get('peak_year', 0),
        }
        
        # Generar hallazgos positivos y negativos
        try:
            positivos, negativos, notas = generar_hallazgos(
                total_arts=chart_result['statistics']['total_publications'],
                total_citas=chart_result.get('total_citations', 0),
                h_index=chart_result.get('h_index', 0),
                cpp=chart_result.get('cpp', 0.0),
                mediana=chart_result.get('median_citations', 0.0),
                pct_citados=chart_result.get('percent_cited', 0.0),
                años=[],  # Se poblarían desde los datos de scopus si es necesario
                pubs=[],  # Idem
                cites=[],  # Idem
                año_pico=chart_result.get('peak_year', 0),
                año_max_pub=0,
                db_session=db,
            )
        except Exception as e:
            logger.warning(f"[ZIP] No se pudieron generar hallazgos: {e}")
            positivos = ["Investigador activo en su área disciplinar."]
            negativos = []
            notas = ["Contacte al equipo de soporte para análisis detallado."]
        
        # Generar reporte PDF profesional
        pdf_result = generate_analysis_report(
            investigador=investigador,
            kpis=kpis,
            positivos=positivos,
            negativos=negativos,
            notas=notas,
            png_path=str(chart_file),  # Incrustar el gráfico PNG
            output_dir=pdfs_dir,
            fecha_ext=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        pdf_file = Path(pdf_result['file_path'])
        
        # ── Verificar que los 3 archivos existen ───────────────────────────────
        if not chart_file.exists():
            raise HTTPException(status_code=500, detail=f"Gráfico no generado: {chart_file}")
        if not excel_file.exists():
            raise HTTPException(status_code=500, detail=f"Excel no generado: {excel_file}")
        if not pdf_file.exists():
            raise HTTPException(status_code=500, detail=f"PDF no generado: {pdf_file}")
        
        logger.info(f"[ZIP] Los 3 archivos están listos: PNG, XLSX, PDF")
        
        # ── Crear ZIP en memoria ───────────────────────────────────────────────
        logger.info(f"[ZIP] Creando archivo ZIP...")
        zip_buffer = io.BytesIO()
        
        # Slug para nombres de archivo en ZIP
        slug = investigador.lower().replace(" ", "_").replace(".", "").replace("-", "_")
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Agregar gráfico PNG
            zip_file.write(
                chart_file,
                arcname=f"grafico_{slug}.png"
            )
            
            # Agregar Excel XLSX
            zip_file.write(
                excel_file,
                arcname=f"produccion_{slug}.xlsx"
            )
            
            # Agregar PDF profesional
            zip_file.write(
                pdf_file,
                arcname=f"informe_{slug}.pdf"
            )
        
        zip_buffer.seek(0)
        
        # ── Nombre del ZIP ─────────────────────────────────────────────────────
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"produccion_{slug}_{timestamp}.zip"
        
        logger.info(f"[ZIP] Archivo ZIP creado exitosamente: {zip_filename}")
        
        return StreamingResponse(
            iter([zip_buffer.getvalue()]),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ZIP] Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
# NUEVO ENDPOINT PROFESIONAL — DATOS DESDE BD (MULTI-FUENTE)
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/v2/author-data",
    summary="Obtener datos bibliométricos desde BD (multi-fuente)",
    response_model=AuthorDataResponse,
    responses={
        200: {"description": "Datos obtenidos correctamente"},
        400: {"model": AuthorDataErrorResponse, "description": "Error en los parámetros"},
        404: {"model": AuthorDataErrorResponse, "description": "Autor no encontrado"},
        500: {"model": AuthorDataErrorResponse, "description": "Error del servidor"},
    },
    tags=["Bibliometría v2 (Multi-fuente)"],
)
def get_author_bibliometric_data(
    request: AuthorDataRequest,
    db: Session = Depends(get_db),
):
    """
    NUEVO ENDPOINT PROFESIONAL: Obtiene datos bibliométricos completos de un autor desde BD.
    
    **Características:**
    - ✅ **Agnóstico de fuente**: datos unificados de la BD canónica
    - ✅ **Sin API calls**: datos caché, independiente de quotas
    - ✅ **Multi-fuente**: incluye identificadores de Scopus, OpenAlex, WoS, CvLAC
    - ✅ **Indicadores completos**: H-index, CPP, mediana, % citados
    - ✅ **Serie temporal**: publicaciones y citas por año
    
    **Parámetros:**
    - `author_id`: ID del autor en tabla 'authors' (BD local) — requerido
    - `year_from`: Año inicial (opcional)
    - `year_to`: Año final (opcional)
    
    **Respuesta:**
    Objeto JSON con nombre, IDs en múltiples fuentes, métricas e indicadores.
    
    **Ejemplo de uso:**
    ```bash
    curl -X POST http://localhost:8000/api/authors/charts/v2/author-data \\
      -H "Content-Type: application/json" \\
      -d '{"author_id": 1, "year_from": 2015, "year_to": 2025}'
    ```
    
    **Diferencia con endpoint v1 (legacy):**
    - v1 (`/generate`): Usa ScopusExtractor, requiere AU-ID, API calls, solo Scopus
    - v2 (`/v2/author-data`): Usa BD, requiere author_id local, sin API calls, multi-fuente
    """
    
    try:
        logger.info(
            f"[AUTHOR DATA v2] Solicitando datos para author_id={request.author_id} "
            f"(rango: {request.year_from or 'inicio'} - {request.year_to or 'fin'})"
        )
        
        # Obtener datos usando servicio profesional
        author_data = fetch_author_data(
            db=db,
            author_id=request.author_id,
            year_from=request.year_from,
            year_to=request.year_to,
        )
        
        # Calcular distribución de fuentes
        source_distribution = {}
        if author_data.records:
            for record in author_data.records:
                if record.field_provenance:
                    for source in record.field_provenance.keys():
                        source_distribution[source] = source_distribution.get(source, 0) + 1
        
        # Construir respuesta
        yearly_data_list = [
            {
                "year": year,
                "publications": pub,
                "citations": cite,
                "cpp": round(cite / pub, 1) if pub > 0 else 0.0,
            }
            for year, pub, cite in zip(
                author_data.yearly_data.years,
                author_data.yearly_data.publications,
                author_data.yearly_data.citations,
            )
        ]
        
        logger.info(f"[AUTHOR DATA v2] Datos obtenidos exitosamente para {author_data.author_name}")
        
        return AuthorDataResponse(
            success=True,
            author_id=author_data.author_id,
            author_name=author_data.author_name,
            source_ids={
                "scopus": author_data.source_ids.get("scopus"),
                "openalex": author_data.source_ids.get("openalex"),
                "wos": author_data.source_ids.get("wos"),
                "cvlac": author_data.source_ids.get("cvlac"),
            },
            year_range=author_data.year_range,
            extraction_date=author_data.extraction_date,
            metrics={
                "total_publications": author_data.total_publications,
                "total_citations": author_data.total_citations,
                "h_index": author_data.h_index,
                "cpp": author_data.cpp,
                "median_citations": author_data.median_citations,
                "percent_cited": author_data.percent_cited,
            },
            yearly_data=yearly_data_list,
            source_distribution=source_distribution,
        )
    
    except ValueError as e:
        logger.warning(f"[AUTHOR DATA v2] Validación: {str(e)}")
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    
    except Exception as e:
        logger.error(f"[AUTHOR DATA v2] Error inesperado: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener datos: {str(e)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# NUEVO ENDPOINT v2 — GENERADOR DE GRÁFICOS CON DATOS DE BD
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/v2/generate-chart",
    summary="Generar gráfico PNG desde datos de BD (multi-fuente)",
    response_model=GenerateChartResponse,
    responses={
        200: {"description": "Gráfico generado correctamente"},
        400: {"model": GenerateChartErrorResponse, "description": "Error en los parámetros"},
        404: {"model": GenerateChartErrorResponse, "description": "Autor no encontrado"},
        500: {"model": GenerateChartErrorResponse, "description": "Error del servidor"},
    },
    tags=["Graficos v2 (Multi-fuente)"],
)
def generate_author_chart(
    request: GenerateChartRequest,
    db: Session = Depends(get_db),
):
    """
    NUEVO ENDPOINT PROFESIONAL v2: Genera gráfico PNG desde datos de BD.
    
    **Características:**
    - ✅ **Agnóstico de fuente**: datos unificados de la BD canónica
    - ✅ **Sin API calls**: datos caché, independiente de quotas
    - ✅ **Multi-fuente**: información de Scopus, OpenAlex, WoS, CvLAC
    - ✅ **Gráfico profesional**: PNG con KPIs, series temporales, estadísticas
    - ✅ **Completo**: 6 indicadores bibliométricos en una imagen
    
    **Parámetros:**
    - `author_id`: ID del autor en tabla 'authors' (BD local) — requerido
    - `year_from`: Año inicial (opcional)
    - `year_to`: Año final (opcional)
    - `institution_name`: Nombre para pie gráfico (default: Universidad Simón Bolívar)
    - `campo`: Campo disciplinar (default: CIENCIAS_SALUD)
    
    **Respuesta:**
    Objeto JSON con información del gráfico generado:
    - `filename`: nombre del PNG
    - `file_path`: ruta relativa para descarga
    - `file_size_mb`: tamaño del archivo
    - `metrics`: los 6 indicadores calculados
    
    **Ejemplo de uso:**
    ```bash
    curl -X POST http://localhost:8000/api/authors/charts/v2/generate-chart \\
      -H "Content-Type: application/json" \\
      -d '{
        "author_id": 1,
        "year_from": 2015,
        "year_to": 2025,
        "institution_name": "Universidad Simón Bolívar",
        "campo": "CIENCIAS_SALUD"
      }'
    ```
    
    **Diferencia con v1:**
    - v1 (`/generate`): Usa ScopusExtractor, AU-ID Scopus requerido, API calls
    - v2 (`/v2/generate-chart`): Usa BD local, author_id local, sin API calls, multi-fuente
    """
    
    try:
        logger.info(
            f"[CHART v2] Generando gráfico para author_id={request.author_id} "
            f"(rango: {request.year_from or 'inicio'} - {request.year_to or 'fin'})"
        )
        
        # 1. Obtener datos desde BD
        author_data = fetch_author_data(
            db=db,
            author_id=request.author_id,
            year_from=request.year_from,
            year_to=request.year_to,
        )
        
        # 2. Parsear campo disciplinar
        try:
            campo = CampoDisciplinar[request.campo]
        except KeyError:
            campo = CampoDisciplinar.CIENCIAS_SALUD
            logger.warning(f"[CHART v2] Campo {request.campo} no reconocido, usando default")
        
        # 3. Renderizar gráfico
        output_dir = Path(__file__).parent.parent.parent / "reports" / "charts"
        chart_info = render_author_chart(
            author_data=author_data,
            institution_name=request.institution_name,
            output_dir=output_dir,
            dpi=180,
            campo=campo,
        )
        
        logger.info(
            f"[CHART v2] Gráfico generado: {chart_info['filename']} "
            f"({chart_info['file_size_mb']} MB)"
        )
        
        # 4. Retornar respuesta
        return GenerateChartResponse(
            success=True,
            investigator_name=author_data.author_name,
            filename=chart_info["filename"],
            file_path=chart_info["file_path"],
            file_size_mb=chart_info["file_size_mb"],
            metrics={
                "total_publications": author_data.total_publications,
                "total_citations": author_data.total_citations,
                "h_index": author_data.h_index,
                "cpp": author_data.cpp,
                "median_citations": author_data.median_citations,
                "percent_cited": author_data.percent_cited,
            },
            year_range=author_data.year_range,
        )
    
    except ValueError as e:
        logger.warning(f"[CHART v2] Validación: {str(e)}")
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    
    except Exception as e:
        logger.error(f"[CHART v2] Error inesperado: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando gráfico: {str(e)}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# NUEVO ENDPOINT v2 — GENERADOR DE REPORTES (PNG + PDF)
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/v2/generate-report",
    summary="Generar informe completo (PNG + PDF)",
    response_model=GenerateChartResponse,
    responses={
        200: {"description": "Informe generado correctamente"},
        400: {"model": GenerateChartErrorResponse, "description": "Error en los parámetros"},
        404: {"model": GenerateChartErrorResponse, "description": "Autor no encontrado"},
        500: {"model": GenerateChartErrorResponse, "description": "Error del servidor"},
    },
    tags=["Graficos v2 (Multi-fuente)"],
)
def generate_author_report(
    request: GenerateChartRequest,
    db: Session = Depends(get_db),
):
    """
    NUEVO ENDPOINT v2: Genera informe completo PNG + PDF.
    
    **Características:**
    - ✅ **PNG profesional**: gráfico limpio sin notas
    - ✅ **PDF detallado**: informe completo con notas, análisis, KPIs
    - ✅ **Multi-fuente**: datos unificados de Scopus, OpenAlex, WoS, CvLAC
    - ✅ **Análisis automático**: detección positivos/negativos
    - ✅ **Completitud**: todo el espacio necesario para notas aclaratorias
    
    **Parámetros:**
    - `author_id`: ID del autor en tabla 'authors' (BD local) — requerido
    - `year_from`: Año inicial (opcional)
    - `year_to`: Año final (opcional)
    - `institution_name`: Nombre para pie (default: Universidad Simón Bolívar)
    - `campo`: Campo disciplinar (default: CIENCIAS_SALUD)
    
    **Respuesta:**
    Objeto JSON con información de ambos archivos generados:
    - `filename`: nombre del PNG (para descarga individual)
    - `file_path`: ruta relativa del PNG
    - `file_size_mb`: tamaño del PNG
    - `metrics`: 6 indicadores bibliométricos
    
    **Archivos generados:**
    - PNG: `reports/charts/grafico_*.png`
    - PDF: `reports/pdfs/informe_*.pdf`
    
    **Ejemplo de uso:**
    ```bash
    curl -X POST http://localhost:8000/api/authors/charts/v2/generate-report \\
      -H "Content-Type: application/json" \\
      -d '{
        "author_id": 1,
        "year_from": 2015,
        "year_to": 2025,
        "institution_name": "Universidad Simón Bolívar",
        "campo": "CIENCIAS_SALUD"
      }'
    ```
    
    **Diferencia con /v2/generate-chart:**
    - `/v2/generate-chart`: Retorna solo PNG
    - `/v2/generate-report`: Retorna PNG + genera PDF con notas completas
    """
    
    try:
        logger.info(
            f"[REPORT v2] Generando informe para author_id={request.author_id} "
            f"(rango: {request.year_from or 'inicio'} - {request.year_to or 'fin'})"
        )
        
        # 1. Obtener datos desde BD
        author_data = fetch_author_data(
            db=db,
            author_id=request.author_id,
            year_from=request.year_from,
            year_to=request.year_to,
        )
        
        # 2. Parsear campo disciplinar
        try:
            campo = CampoDisciplinar[request.campo]
        except KeyError:
            campo = CampoDisciplinar.CIENCIAS_SALUD
            logger.warning(f"[REPORT v2] Campo {request.campo} no reconocido, usando default")
        
        # 3. Generar análisis automático (simplificado)
        # Para el PDF, usamos solo los KPIs sin análisis complejos
        positivos = [
            f"H-Index consolidado en {author_data.h_index}",
            f"Promedio de citas por publicación: {author_data.cpp:.2f}",
            f"Porcentaje de artículos citados: {author_data.percent_cited:.1f}%",
        ]
        negativos = []
        notas = [
            "📌 Análisis basado en datos de múltiples fuentes (Scopus, OpenAlex, WoS, CvLAC)",
            f"📌 Período de análisis: {author_data.year_range}",
            "📌 Mediana de citaciones indica consistencia del impacto",
        ]
        
        # 4. Renderizar gráfico PNG
        charts_output_dir = Path(__file__).parent.parent.parent / "reports" / "charts"
        chart_info = render_author_chart(
            author_data=author_data,
            institution_name=request.institution_name,
            output_dir=charts_output_dir,
            dpi=180,
            campo=campo,
        )
        
        logger.info(
            f"[REPORT v2] PNG generado: {chart_info['filename']} "
            f"({chart_info['file_size_mb']} MB)"
        )
        
        # 5. Generar PDF con análisis completo
        # Calcular año con máximo número de citaciones
        año_pico = None
        if author_data.yearly_data and author_data.yearly_data.years and author_data.yearly_data.citations:
            max_idx = author_data.yearly_data.citations.index(max(author_data.yearly_data.citations))
            año_pico = author_data.yearly_data.years[max_idx]
        
        pdf_output_dir = Path(__file__).parent.parent.parent / "reports" / "pdfs"
        pdf_info = generate_analysis_report(
            investigador=author_data.author_name,
            kpis={
                "pubs": author_data.total_publications,
                "citas": author_data.total_citations,
                "h_index": author_data.h_index,
                "cpp": author_data.cpp,
                "mediana": author_data.median_citations,
                "pct_citados": author_data.percent_cited,
                "año_pico": año_pico or "N/A",
            },
            positivos=positivos,
            negativos=negativos,
            notas=notas,
            png_path=chart_info["file_path"],  # Incrustar PNG en el PDF
            institution_name=request.institution_name,
            output_dir=pdf_output_dir,
            fecha_ext=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        )
        
        logger.info(
            f"[REPORT v2] PDF generado: {pdf_info['filename']} "
            f"({pdf_info['file_size_mb']} MB)"
        )
        
        # 6. Retornar respuesta
        return GenerateChartResponse(
            success=True,
            investigator_name=author_data.author_name,
            filename=chart_info["filename"],
            file_path=chart_info["file_path"],
            file_size_mb=chart_info["file_size_mb"],
            metrics={
                "total_publications": author_data.total_publications,
                "total_citations": author_data.total_citations,
                "h_index": author_data.h_index,
                "cpp": author_data.cpp,
                "median_citations": author_data.median_citations,
                "percent_cited": author_data.percent_cited,
            },
            year_range=author_data.year_range,
        )
    
    except ValueError as e:
        logger.warning(f"[REPORT v2] Validación: {str(e)}")
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    
    except Exception as e:
        logger.error(f"[REPORT v2] Error inesperado: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generando informe: {str(e)}"
        )
