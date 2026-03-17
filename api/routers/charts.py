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
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
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
)
from api.services.chart_generator import generate_investigator_chart_file
from api.services.excel_exporter import generate_publications_excel_file

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
        "year_to": 2025
    }
    ```
    """
    
    try:
        logger.info(
            f"Generando gráfico para AU-ID: {request.author_id} "
            f"(AF-IDs: {request.affiliation_ids or 'todas'})"
        )
        
        # Generar gráfico
        chart_data = generate_investigator_chart_file(
            author_id=request.author_id,
            affiliation_ids=request.affiliation_ids,
            year_from=request.year_from,
            year_to=request.year_to,
            institution_name="Universidad Simón Bolívar",
            output_dir=CHARTS_OUTPUT_DIR,
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


# ════════════════════════════════════════════════════════════════════════════════
# VISUALIZACIÓN EN HTML
# ════════════════════════════════════════════════════════════════════════════════

# ── GET /authors/charts/view-report/{filename} ────────────────────────────────

@router.get(
    "/view-report/{filename}",
    summary="Ver reporte completo (gráfico + datos)",
    response_class=HTMLResponse,
    tags=["Visualización"],
)
def view_chart_report(filename: str):
    """
    Visualiza un reporte HTML completo con:
    - Gráfico PNG
    - Información del investigador
    - Estadísticas
    - Botones para descargar gráfico y Excel
    
    **Parámetro:**
    - `filename`: Nombre del archivo gráfico (ej: grafico_aroca_martinez_20260317_123456.png)
    """
    
    # Validar nombre
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Nombre de archivo inválido")
    
    # Extraer nombre del investigador del filename
    # Formato: grafico_NOMBRE_TIMESTAMP.png
    parts = filename.replace("grafico_", "").replace(".png", "").split("_")
    investigator_slug = "_".join(parts[:-1])  # Todo excepto el timestamp
    investigator_name = investigator_slug.replace("_", " ").title()
    
    # Verificar que el archivo existe
    filepath = Path(CHARTS_OUTPUT_DIR).resolve() / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filename}")
    
    # Generar HTML profesional
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Reporte de Publicaciones - {investigator_name}</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }}
            
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 12px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                overflow: hidden;
            }}
            
            .header {{
                background: linear-gradient(135deg, #1F4E78 0%, #2E5C8A 100%);
                color: white;
                padding: 40px;
                text-align: center;
            }}
            
            .header h1 {{
                font-size: 2.5em;
                margin-bottom: 10px;
            }}
            
            .header p {{
                font-size: 1.1em;
                opacity: 0.95;
            }}
            
            .content {{
                padding: 40px;
            }}
            
            .chart-section {{
                margin-bottom: 40px;
                border-bottom: 2px solid #E5EBF0;
                padding-bottom: 40px;
            }}
            
            .chart-container {{
                text-align: center;
                background: #F6F9FC;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 20px;
            }}
            
            .chart-container img {{
                max-width: 100%;
                height: auto;
                border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }}
            
            .actions {{
                display: flex;
                gap: 15px;
                justify-content: center;
                flex-wrap: wrap;
                margin-bottom: 30px;
            }}
            
            .btn {{
                display: inline-block;
                padding: 12px 24px;
                border: none;
                border-radius: 6px;
                font-size: 1em;
                cursor: pointer;
                text-decoration: none;
                transition: all 0.3s ease;
                font-weight: 600;
            }}
            
            .btn-primary {{
                background: #0969DA;
                color: white;
            }}
            
            .btn-primary:hover {{
                background: #0860CA;
                transform: translateY(-2px);
                box-shadow: 0 8px 16px rgba(9, 105, 218, 0.3);
            }}
            
            .btn-secondary {{
                background: #6C757D;
                color: white;
            }}
            
            .btn-secondary:hover {{
                background: #5C636A;
                transform: translateY(-2px);
                box-shadow: 0 8px 16px rgba(108, 117, 125, 0.3);
            }}
            
            .btn-success {{
                background: #2DA44E;
                color: white;
            }}
            
            .btn-success:hover {{
                background: #26843E;
                transform: translateY(-2px);
                box-shadow: 0 8px 16px rgba(45, 164, 78, 0.3);
            }}
            
            .info-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-top: 20px;
            }}
            
            .info-card {{
                background: #F6F9FC;
                padding: 20px;
                border-radius: 8px;
                border-left: 4px solid #0969DA;
            }}
            
            .info-card h3 {{
                color: #57606A;
                font-size: 0.85em;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 8px;
            }}
            
            .info-card .value {{
                font-size: 2em;
                font-weight: bold;
                color: #1F4E78;
            }}
            
            .footer {{
                background: #F6F9FC;
                padding: 20px 40px;
                text-align: center;
                color: #57606A;
                font-size: 0.9em;
                border-top: 1px solid #E5EBF0;
            }}
            
            .timestamp {{
                color: #999;
                font-size: 0.85em;
                margin-top: 10px;
            }}
            
            @media (max-width: 768px) {{
                .header {{
                    padding: 30px 20px;
                }}
                
                .header h1 {{
                    font-size: 1.8em;
                }}
                
                .content {{
                    padding: 20px;
                }}
                
                .actions {{
                    gap: 10px;
                }}
                
                .btn {{
                    padding: 10px 16px;
                    font-size: 0.9em;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Reporte de Producción Científica</h1>
                <p>{investigator_name}</p>
            </div>
            
            <div class="content">
                <div class="chart-section">
                    <h2 style="color: #1F4E78; margin-bottom: 20px;">Gráfico de Publicaciones por Año</h2>
                    
                    <div class="chart-container">
                        <img src="/api/authors/charts/view/{filename}" alt="Gráfico de publicaciones" />
                    </div>
                    
                    <div class="actions">
                        <a href="/api/authors/charts/download/{filename}" class="btn btn-primary" download>
                            ⬇️ Descargar Gráfico (PNG)
                        </a>
                        <a href="/api/authors/charts/export-publications" class="btn btn-success">
                            📥 Exportar a Excel
                        </a>
                    </div>
                </div>
                
                <div class="info-grid">
                    <div class="info-card">
                        <h3>📈 Investigador</h3>
                        <div class="value">{investigator_name}</div>
                    </div>
                    <div class="info-card">
                        <h3>📅 Última actualización</h3>
                        <div class="value" style="font-size: 1.2em;">Hoy</div>
                    </div>
                    <div class="info-card">
                        <h3>💾 Tipo de archivo</h3>
                        <div class="value" style="font-size: 1.2em;">PNG</div>
                    </div>
                </div>
            </div>
            
            <div class="footer">
                <p>🔬 Sistema de Análisis de Producción Científica</p>
                <p class="timestamp">Generado automáticamente • Datos de Scopus</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html_content


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
    Genera gráfico (PNG) y reporte (XLSX) simultáneamente y los retorna como ZIP.
    
    **Parámetros mínimos:**
    - `author_id`: ID del autor en Scopus (AU-ID) — requerido
    
    **Parámetros opcionales:**
    - `affiliation_ids`: IDs de afiliación (AF-ID)
    - `year_from`: Año inicial para filtrar
    - `year_to`: Año final para filtrar
    
    **Retorna:** archivo ZIP con:
    - `grafico_*.png` → Gráfico de publicaciones
    - `produccion_*.xlsx` → Reporte completo de publicaciones
    
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
        
        # Generar gráfico
        logger.info(f"[ZIP] Generando gráfico...")
        chart_result = generate_investigator_chart_file(
            author_id=request.author_id,
            affiliation_ids=request.affiliation_ids,
            year_from=request.year_from,
            year_to=request.year_to,
            output_dir=charts_dir,
        )
        chart_file = Path(chart_result['file_path'])
        
        # Generar Excel
        logger.info(f"[ZIP] Generando Excel...")
        excel_result = generate_publications_excel_file(
            author_id=request.author_id,
            affiliation_ids=request.affiliation_ids,
            output_dir=exports_dir,
        )
        excel_file = Path(excel_result['file_path'])
        
        # Verificar que ambos archivos existen
        if not chart_file.exists():
            raise HTTPException(status_code=500, detail=f"Gráfico no generado: {chart_file}")
        if not excel_file.exists():
            raise HTTPException(status_code=500, detail=f"Excel no generado: {excel_file}")
        
        logger.info(f"[ZIP] Ambos archivos listos: {chart_file.name}, {excel_file.name}")
        
        # Crear ZIP en memoria
        logger.info(f"[ZIP] Creando archivo ZIP...")
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Agregar gráfico
            zip_file.write(
                chart_file,
                arcname=f"grafico_{chart_result['investigator_name'].lower().replace(' ', '_')}.png"
            )
            
            # Agregar Excel
            zip_file.write(
                excel_file,
                arcname=f"produccion_{chart_result['investigator_name'].lower().replace(' ', '_')}.xlsx"
            )
        
        zip_buffer.seek(0)
        
        # Nombre del ZIP
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"produccion_{chart_result['investigator_name'].lower().replace(' ', '_')}_{timestamp}.zip"
        
        logger.info(f"[ZIP] ZIP creado exitosamente: {zip_filename}")
        
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
