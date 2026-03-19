"""
api/services/data_provider.py
=============================
Servicio profesional de obtención y agregación de datos bibliométricos.

Agnóstico de fuente (Scopus, OpenAlex, WoS, etc.) y reutilizable.
Evita duplicación de código y facilita extensión a nuevas fuentes.

Funciones principales:
  - fetch_author_data()     : Coordinador principal
  - aggregate_by_year()     : Agrupa publicaciones y citas por año
  - build_metrics()         : Calcula indicadores (H-index, CPP, mediana)
  - build_publication_df()  : Crea DataFrame para análisis
"""

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_

from db.models import (
    CanonicalPublication,
    PublicationAuthor,
    Author,
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# TIPOS Y DATACLASSES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class YearlyAggregation:
    """Datos agregados por año"""
    years: List[int]
    publications: List[int]
    citations: List[int]


@dataclass
class AuthorData:
    """Datos completos de un autor"""
    author_id: int
    author_name: str
    source_ids: Dict[str, str]  # {source_name: external_id}
    records: List[CanonicalPublication]
    extraction_date: str
    year_range: str
    
    yearly_data: YearlyAggregation
    df_publications: pd.DataFrame
    
    total_publications: int
    total_citations: int
    h_index: int
    cpp: float
    median_citations: float
    percent_cited: float


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES BASE — SIN LÓGICA DE FUENTE
# ══════════════════════════════════════════════════════════════════════════════

def _apply_year_filter(
    records: List[CanonicalPublication],
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
) -> List[CanonicalPublication]:
    """
    Filtra registros por rango de años.
    
    Args:
        records: Lista de CanonicalPublication
        year_from: Año inicial (inclusive)
        year_to: Año final (inclusive)
    
    Returns:
        Registros filtrados
    """
    filtered = records
    if year_from:
        filtered = [r for r in filtered if r.publication_year and r.publication_year >= year_from]
    if year_to:
        filtered = [r for r in filtered if r.publication_year and r.publication_year <= year_to]
    
    return filtered


def _extract_author_info(
    records: List[CanonicalPublication],
    author_id: int,
) -> Tuple[str, Dict[str, str]]:
    """
    Extrae nombre y IDs externos del autor desde los registros.
    
    Args:
        records: Lista de CanonicalPublication con authors relacionados
        author_id: ID del autor en BD
    
    Returns:
        Tupla (author_name, source_ids_dict)
        Ejemplo: ("Juan Arenas", {"scopus": "57193767797", "openalex": "A1234567"})
    """
    author_name = "Investigador"
    source_ids = {}
    
    for record in records:
        for author in record.authors:
            if author.author_id == author_id:
                author_name = author.author.name or "Investigador"
                
                # Recolectar IDs externos
                if author.author.scopus_id:
                    source_ids["scopus"] = author.author.scopus_id
                if author.author.openalex_id:
                    source_ids["openalex"] = author.author.openalex_id
                if author.author.wos_id:
                    source_ids["wos"] = author.author.wos_id
                if author.author.cvlac_id:
                    source_ids["cvlac"] = author.author.cvlac_id
                
                break
        
        if author_name != "Investigador":
            break
    
    return author_name, source_ids


def _aggregate_by_year(
    records: List[CanonicalPublication],
) -> YearlyAggregation:
    """
    Agrupa publicaciones y citas por año.
    
    Args:
        records: Registros filtrados (deben tener publication_year)
    
    Returns:
        YearlyAggregation con años, pubs y citas ordenados
    """
    pub_by_year = Counter()
    citations_by_year = Counter()
    
    for record in records:
        if record.publication_year:
            year = record.publication_year
            pub_by_year[year] += 1
            citations_by_year[year] += (record.citation_count or 0)
    
    if not pub_by_year:
        raise ValueError("No se encontraron publicaciones en el rango especificado")
    
    years = sorted(pub_by_year.keys())
    pubs = [pub_by_year[year] for year in years]
    cites = [citations_by_year.get(year, 0) for year in years]
    
    return YearlyAggregation(
        years=years,
        publications=pubs,
        citations=cites,
    )


def _build_publication_dataframe(
    records: List[CanonicalPublication],
) -> pd.DataFrame:
    """
    Construye DataFrame de publicaciones para cálculos de indicadores.
    
    Args:
        records: Registros filtrados
    
    Returns:
        DataFrame con columnas: Año, Citas, Título, DOI
    """
    data = []
    for record in records:
        if record.publication_year:
            data.append({
                'Año': record.publication_year,
                'Citas': record.citation_count or 0,
                'Título': record.title or '[S.T.]',
                'DOI': record.doi or '[N/A]',
            })
    
    return pd.DataFrame(data) if data else pd.DataFrame()


def _calculate_metrics(
    df_publications: pd.DataFrame,
    years: List[int],
    publications: List[int],
    citations: List[int],
) -> Dict[str, Any]:
    """
    Calcula indicadores bibliométricos desde datos agregados.
    
    Args:
        df_publications: DataFrame con columna 'Citas'
        years: Años ordenados
        publications: Publicaciones por año
        citations: Citas por año
    
    Returns:
        Dict con métricas: h_index, cpp, median_citations, percent_cited
    """
    total_pubs = sum(publications)
    total_cites = sum(citations)
    
    # CPP (Citas Per Publication)
    cpp = round(total_cites / total_pubs, 1) if total_pubs > 0 else 0.0
    
    # H-index
    if not df_publications.empty:
        citas_sorted = sorted(df_publications['Citas'].tolist(), reverse=True)
        h_index = sum(1 for i, c in enumerate(citas_sorted, 1) if c >= i)
    else:
        h_index = 0
    
    # Mediana de citas
    if not df_publications.empty and len(df_publications) > 0:
        median_citations = round(df_publications['Citas'].median(), 1)
    else:
        median_citations = 0.0
    
    # Porcentaje de artículos citados
    if not df_publications.empty and len(df_publications) > 0:
        percent_cited = round(
            (len(df_publications[df_publications['Citas'] >= 1]) / len(df_publications)) * 100,
            1
        )
    else:
        percent_cited = 0.0
    
    return {
        'h_index': h_index,
        'cpp': cpp,
        'median_citations': median_citations,
        'percent_cited': percent_cited,
        'total_publications': total_pubs,
        'total_citations': total_cites,
    }


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL — ORQUESTADORA
# ══════════════════════════════════════════════════════════════════════════════

def fetch_author_data(
    db: Session,
    author_id: int,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
) -> AuthorData:
    """
    FUNCIÓN PRINCIPAL: Obtiene todos los datos bibliométricos de un autor desde BD.
    
    Orquesta el flujo completo:
      1. Obtiene registros canónicos del autor
      2. Filtra por años
      3. Extrae nombre e IDs externos
      4. Agrupa por año
      5. Calcula indicadores
      6. Construye DataFrame
      7. Retorna objeto completo
    
    Args:
        db: Session de SQLAlchemy
        author_id: ID del autor en tabla 'authors'
        year_from: Año inicial (opcional)
        year_to: Año final (opcional)
    
    Returns:
        AuthorData con todos los datos calculados y organizados
    
    Raises:
        ValueError: Si no hay registros o el autor no existe
    """
    
    # 1. OBTENER AUTOR DE BD
    logger.info(f"[DATA_PROVIDER] Consultando autor {author_id} desde BD")
    author = db.query(Author).filter(Author.id == author_id).first()
    if not author:
        raise ValueError(f"Autor con ID {author_id} no existe")
    
    # 2. OBTENER PUBLICACIONES CANÓNICAS
    logger.info(f"[DATA_PROVIDER] Obteniendo publicaciones de {author_id}")
    records = db.query(CanonicalPublication)\
        .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.publication_id)\
        .filter(PublicationAuthor.author_id == author_id)\
        .all()
    
    if not records:
        raise ValueError(f"No se encontraron publicaciones para autor {author_id}")
    
    logger.info(f"[DATA_PROVIDER] Registros encontrados: {len(records)}")
    
    # 3. APLICAR FILTRO DE AÑOS
    records_filtered = _apply_year_filter(records, year_from, year_to)
    
    if not records_filtered:
        raise ValueError("No se encontraron publicaciones en el rango especificado")
    
    # 4. EXTRAER INFORMACIÓN DEL AUTOR
    author_name, source_ids = _extract_author_info(records_filtered, author_id)
    logger.info(f"[DATA_PROVIDER] Autor: {author_name}, IDs: {source_ids}")
    
    # 5. AGREGAR POR AÑO
    yearly_data = _aggregate_by_year(records_filtered)
    
    # 6. CONSTRUIR DATAFRAME
    df_pubs = _build_publication_dataframe(records_filtered)
    
    # 7. CALCULAR MÉTRICAS
    metrics = _calculate_metrics(
        df_pubs,
        yearly_data.years,
        yearly_data.publications,
        yearly_data.citations,
    )
    
    # 8. CALCULAR RANGO DE AÑOS
    year_min = min(yearly_data.years) if yearly_data.years else ""
    year_max = max(yearly_data.years) if yearly_data.years else ""
    year_range = f"{year_min} - {year_max}"
    
    # 9. RETORNAR OBJETO COMPLETO
    return AuthorData(
        author_id=author_id,
        author_name=author_name,
        source_ids=source_ids,
        records=records_filtered,
        extraction_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        year_range=year_range,
        yearly_data=yearly_data,
        df_publications=df_pubs,
        total_publications=metrics['total_publications'],
        total_citations=metrics['total_citations'],
        h_index=metrics['h_index'],
        cpp=metrics['cpp'],
        median_citations=metrics['median_citations'],
        percent_cited=metrics['percent_cited'],
    )
